"""
bootstrap_geodkv.py — Prefect flow til at initialisere geodkv-schema i PostGIS.

Trin:
  1. Sørg for at schema 'geodkv' + metadata-tabeller findes
  2. Live-introspect DAF GraphQL (https://graphql.datafordeler.dk/GEODKV/v2)
  3. Parse entiteter ud af introspection-resultatet
  4. Detect drift mod sidste snapshot (første kørsel = baseline, ingen drift)
  5. Auto-generér DDL pr. entitet og opret alle entitets-tabeller (idempotent)
  6. Upsert _sync_metadata med alle entiteter (status='pending')
  7. Log én række i _schema_runs med fuldt snapshot + drift-resultat

Forudsætninger på Prefect-serveren:
  - Block 'daf-geodkv-api' (Secret) med DAF API-nøglen
  - Block 'pg-db-auth' (SqlAlchemyConnector) til PostGIS-databasen
  - PostGIS-extension aktiveret i den database connector'en peger på
  - pip-pakken 'httpx' installeret i Prefect-venv'et:
      sudo -iu prefect /opt/prefect/venv/bin/pip install httpx
"""

from __future__ import annotations

import json
import re

import httpx
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from prefect_sqlalchemy import SqlAlchemyConnector
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ---------------------------------------------------------------------------
# Konfiguration
# ---------------------------------------------------------------------------

DAF_GRAPHQL_URL = "https://graphql.datafordeler.dk/GEODKV/v2"
SCHEMA_NAME = "geodkv"

# Skalar GraphQL-typer → Postgres-typer
SCALAR_TO_PG: dict[str, str] = {
    "String": "text",
    "Int": "integer",
    "Long": "bigint",
    "Boolean": "boolean",
    "UUID": "uuid",
    "DafDateTime": "timestamptz",
    "Char": "char(1)",
}

# Mønster for spatial-typer fra DAF, fx 'SpatialMultiPolygonZEpsg25832Type'
SPATIAL_PATTERN = re.compile(
    r"^Spatial(?P<multi>Multi)?(?P<shape>Point|LineString|Polygon)"
    r"(?P<z>Z)?Epsg(?P<srid>\d+)Type$"
)


# ---------------------------------------------------------------------------
# Hjælpere: navngivning og type-mapping
# ---------------------------------------------------------------------------

def to_snake_case(name: str) -> str:
    """camelCase / PascalCase → snake_case. Acronymer bliver til ét ord."""
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    s2 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1)
    return s2.lower()


def entity_to_table(graphql_type: str) -> str:
    # GEODKV_Afvandingsgroeft → afvandingsgroeft
    return to_snake_case(graphql_type.removeprefix("GEODKV_"))


def graphql_type_to_pg(type_name: str | None) -> str | None:
    """Returnér Postgres-type for en GraphQL-typenavn, eller None hvis ukendt."""
    if not type_name:
        return None
    if type_name in SCALAR_TO_PG:
        return SCALAR_TO_PG[type_name]
    m = SPATIAL_PATTERN.match(type_name)
    if m:
        multi = m.group("multi") or ""
        shape = m.group("shape")
        z = m.group("z") or ""
        srid = m.group("srid")
        return f"geometry({multi}{shape}{z}, {srid})"
    return None


def unwrap_type(type_ref: dict | None) -> tuple[str | None, bool]:
    """Skræl NON_NULL/LIST-wrappers af; returnér (typenavn, non_null-flag)."""
    non_null = False
    while type_ref:
        kind = type_ref.get("kind")
        if kind == "NON_NULL":
            non_null = True
            type_ref = type_ref.get("ofType")
        elif kind == "LIST":
            type_ref = type_ref.get("ofType")
        else:
            return type_ref.get("name"), non_null
    return None, non_null


# ---------------------------------------------------------------------------
# GraphQL introspection
# ---------------------------------------------------------------------------

INTROSPECTION_QUERY = """
query Introspect {
  __schema {
    types {
      name
      kind
      fields {
        name
        type {
          kind name
          ofType {
            kind name
            ofType {
              kind name
              ofType { kind name }
            }
          }
        }
      }
    }
  }
}
"""


@task(retries=2, retry_delay_seconds=10)
def fetch_introspection(api_key: str) -> dict:
    logger = get_run_logger()
    response = httpx.post(
        DAF_GRAPHQL_URL,
        params={"apikey": api_key},
        json={"query": INTROSPECTION_QUERY},
        timeout=60.0,
    )
    response.raise_for_status()
    data = response.json()
    if "errors" in data:
        raise RuntimeError(f"GraphQL-fejl: {data['errors']}")
    type_count = len(data["data"]["__schema"]["types"])
    logger.info(f"Hentede introspection fra DAF ({type_count} types i alt)")
    return data["data"]["__schema"]


@task
def parse_entities(schema_data: dict) -> list[dict]:
    """Find alle entiteter — defineret som GEODKV_-typer med datafordelerRowId."""
    logger = get_run_logger()
    entities: list[dict] = []
    for t in schema_data["types"]:
        name = t.get("name") or ""
        if not name.startswith("GEODKV_"):
            continue
        if t.get("kind") != "OBJECT":
            continue
        # Hop Connection/Edge/Page-helpers over
        if name.endswith(("Connection", "Edge")):
            continue

        fields = t.get("fields") or []
        field_names = {f["name"] for f in fields}
        # Entiteter har altid datafordelerRowId
        if "datafordelerRowId" not in field_names:
            continue

        parsed_fields = []
        for f in fields:
            type_name, non_null = unwrap_type(f["type"])
            parsed_fields.append({
                "name": f["name"],
                "type": type_name,
                "non_null": non_null,
            })

        entities.append({
            "graphql_type": name,
            "entity_name": name.removeprefix("GEODKV_"),
            "table_name": entity_to_table(name),
            "has_kommunekode": "kommunekode" in field_names,
            "fields": parsed_fields,
        })

    entities.sort(key=lambda e: e["graphql_type"])
    logger.info(f"Identificerede {len(entities)} entiteter")
    return entities


# ---------------------------------------------------------------------------
# DDL-generering
# ---------------------------------------------------------------------------

def build_create_table(entity: dict) -> tuple[str, list[str]]:
    """Returnér (DDL-streng, liste over felter sprunget over pga. ukendt type)."""
    lines: list[str] = []
    skipped: list[str] = []
    geom_col: str | None = None

    for f in entity["fields"]:
        col_name = to_snake_case(f["name"])
        pg_type = graphql_type_to_pg(f["type"])
        if pg_type is None:
            skipped.append(f"{f['name']} ({f['type']})")
            continue
        null_part = " NOT NULL" if f["non_null"] else ""
        lines.append(f'  "{col_name}" {pg_type}{null_part}')
        if pg_type.startswith("geometry("):
            geom_col = col_name

    # Primær nøgle: datafordeler_row_id (UUID, antaget unik pr. række)
    lines.append('  PRIMARY KEY ("datafordeler_row_id")')

    table = f"{SCHEMA_NAME}.{entity['table_name']}"
    ddl = (
        f"CREATE TABLE IF NOT EXISTS {table} (\n"
        + ",\n".join(lines)
        + "\n);"
    )

    # GIST-index på geometri, hvis tabellen har en geometrikolonne
    if geom_col:
        idx_name = f"{entity['table_name']}_{geom_col}_gix"
        ddl += (
            f'\nCREATE INDEX IF NOT EXISTS {idx_name} '
            f'ON {table} USING GIST ("{geom_col}");'
        )

    return ddl, skipped


# ---------------------------------------------------------------------------
# Database-tasks
# ---------------------------------------------------------------------------

METADATA_DDL = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};

CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}._sync_metadata (
    entity_name        text PRIMARY KEY,
    graphql_type       text NOT NULL,
    table_name         text NOT NULL,
    has_kommunekode    boolean NOT NULL DEFAULT false,
    status             text NOT NULL DEFAULT 'pending',
    last_sync_at       timestamptz,
    last_event_id      bigint,
    record_count       bigint,
    last_error         text,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}._schema_runs (
    id                 bigserial PRIMARY KEY,
    run_at             timestamptz NOT NULL DEFAULT now(),
    entity_count       integer NOT NULL,
    schema_snapshot    jsonb NOT NULL,
    drift_detected     boolean NOT NULL DEFAULT false,
    drift_summary      text,
    notes              text
);
"""


@task
def ensure_schema_and_metadata_tables(engine: Engine) -> None:
    logger = get_run_logger()
    with engine.connect() as conn:
        conn.execute(text(METADATA_DDL))
        conn.commit()
    logger.info(f"Schema '{SCHEMA_NAME}' + metadata-tabeller er klar")


@task
def detect_drift(engine: Engine, entities: list[dict]) -> dict:
    """Sammenlign mod sidste snapshot. Første kørsel = baseline, ingen drift."""
    logger = get_run_logger()

    with engine.connect() as conn:
        result = conn.execute(text(
            f"SELECT schema_snapshot FROM {SCHEMA_NAME}._schema_runs "
            f"ORDER BY run_at DESC LIMIT 1"
        ))
        rows = result.fetchall()

    if not rows:
        logger.info("Ingen tidligere snapshot — baseline etableres ved denne kørsel")
        return {"is_first_run": True, "drift_detected": False, "summary": None}

    previous = rows[0][0]
    if isinstance(previous, str):
        previous = json.loads(previous)

    prev_map = {e["graphql_type"]: e for e in previous.get("entities", [])}
    curr_map = {e["graphql_type"]: e for e in entities}

    added = sorted(set(curr_map) - set(prev_map))
    removed = sorted(set(prev_map) - set(curr_map))

    field_changes: list[str] = []
    for name in sorted(set(curr_map) & set(prev_map)):
        prev_fields = {f["name"]: f for f in prev_map[name]["fields"]}
        curr_fields = {f["name"]: f for f in curr_map[name]["fields"]}
        f_added = sorted(set(curr_fields) - set(prev_fields))
        f_removed = sorted(set(prev_fields) - set(curr_fields))
        f_changed: list[str] = []
        for fn in sorted(set(curr_fields) & set(prev_fields)):
            pf, cf = prev_fields[fn], curr_fields[fn]
            if pf["type"] != cf["type"] or pf["non_null"] != cf["non_null"]:
                f_changed.append(
                    f"{fn}: {pf['type']}/{pf['non_null']} "
                    f"→ {cf['type']}/{cf['non_null']}"
                )
        if f_added or f_removed or f_changed:
            parts = []
            if f_added:
                parts.append(f"+{f_added}")
            if f_removed:
                parts.append(f"-{f_removed}")
            if f_changed:
                parts.append(f"~{f_changed}")
            field_changes.append(f"{name}: " + " ".join(parts))

    drift = bool(added or removed or field_changes)
    summary_parts: list[str] = []
    if added:
        summary_parts.append(f"Tilføjede entiteter: {added}")
    if removed:
        summary_parts.append(f"Fjernede entiteter: {removed}")
    if field_changes:
        summary_parts.append("Felt-ændringer:\n  " + "\n  ".join(field_changes))
    summary = "\n".join(summary_parts) if summary_parts else None

    if drift:
        logger.warning(f"Schema-drift opdaget:\n{summary}")
    else:
        logger.info("Ingen schema-drift siden sidste kørsel")

    return {"is_first_run": False, "drift_detected": drift, "summary": summary}


@task
def create_entity_tables(
    engine: Engine,
    entities: list[dict],
) -> dict:
    logger = get_run_logger()
    skipped_by_entity: dict[str, list[str]] = {}
    created = 0
    with engine.connect() as conn:
        for entity in entities:
            ddl, skipped = build_create_table(entity)
            conn.execute(text(ddl))
            created += 1
            if skipped:
                skipped_by_entity[entity["entity_name"]] = skipped
                logger.warning(
                    f"{entity['entity_name']}: sprunget over felter med "
                    f"ukendt type: {skipped}"
                )
        conn.commit()
    logger.info(f"Oprettede/verificerede {created} entitets-tabeller")
    return {"created": created, "skipped": skipped_by_entity}


@task
def populate_sync_metadata(
    engine: Engine,
    entities: list[dict],
) -> None:
    logger = get_run_logger()
    sql = text(f"""
        INSERT INTO {SCHEMA_NAME}._sync_metadata
            (entity_name, graphql_type, table_name, has_kommunekode, status)
        VALUES
            (:entity_name, :graphql_type, :table_name, :has_kommunekode, 'pending')
        ON CONFLICT (entity_name) DO UPDATE SET
            graphql_type    = EXCLUDED.graphql_type,
            table_name      = EXCLUDED.table_name,
            has_kommunekode = EXCLUDED.has_kommunekode,
            updated_at      = now()
    """)
    with engine.connect() as conn:
        for entity in entities:
            conn.execute(
                sql,
                {
                    "entity_name": entity["entity_name"],
                    "graphql_type": entity["graphql_type"],
                    "table_name": entity["table_name"],
                    "has_kommunekode": entity["has_kommunekode"],
                },
            )
        conn.commit()
    logger.info(f"Upsertede {len(entities)} rækker i _sync_metadata")


@task
def log_schema_run(
    engine: Engine,
    entities: list[dict],
    drift: dict,
    skipped_fields: dict[str, list[str]],
) -> None:
    logger = get_run_logger()
    snapshot = {"entities": entities}
    notes_parts: list[str] = []
    if drift["is_first_run"]:
        notes_parts.append("Baseline (første kørsel)")
    if skipped_fields:
        notes_parts.append(
            f"Felter sprunget over: {json.dumps(skipped_fields, ensure_ascii=False)}"
        )
    notes = " | ".join(notes_parts) if notes_parts else None

    sql = text(f"""
        INSERT INTO {SCHEMA_NAME}._schema_runs
            (entity_count, schema_snapshot, drift_detected, drift_summary, notes)
        VALUES
            (:entity_count, :snapshot, :drift_detected, :summary, :notes)
    """)
    with engine.connect() as conn:
        conn.execute(
            sql,
            {
                "entity_count": len(entities),
                "snapshot": json.dumps(snapshot, ensure_ascii=False),
                "drift_detected": drift["drift_detected"],
                "summary": drift["summary"],
                "notes": notes,
            },
        )
        conn.commit()
    logger.info("Skrev række til _schema_runs")


# ---------------------------------------------------------------------------
# Hoved-flow
# ---------------------------------------------------------------------------

@flow(name="bootstrap_geodkv", log_prints=True)
def bootstrap_geodkv() -> None:
    api_key = Secret.load("daf-geodkv-api").get()
    connector = SqlAlchemyConnector.load("pg-db-auth")
    # Hent SQLAlchemy-engine direkte; bypass'er prefect-sqlalchemy's buggy
    # connection-wrapper (lækker '__prefect_kind' ind i psycopg2's DSN).
    engine = connector.get_engine()

    # Metadata-tabeller skal eksistere FØR vi kan slå sidste snapshot op
    ensure_schema_and_metadata_tables(engine)

    schema_data = fetch_introspection(api_key)
    entities = parse_entities(schema_data)

    drift = detect_drift(engine, entities)
    table_result = create_entity_tables(engine, entities)
    populate_sync_metadata(engine, entities)
    log_schema_run(engine, entities, drift, table_result["skipped"])


if __name__ == "__main__":
    # Lokal/manuel kørsel — kører flowet direkte i denne proces.
    # Til at registrere som Prefect-deployment fra serverens git-clone:
    #
    #     from prefect import flow
    #     flow.from_source(
    #         source="https://github.com/TuneClausen/prefect.git",
    #         entrypoint="bootstrap_geodkv.py:bootstrap_geodkv",
    #     ).deploy(name="bootstrap-geodkv", work_pool_name="default-pool")
    bootstrap_geodkv()
