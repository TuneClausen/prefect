from prefect import flow
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect.blocks.system import Secret


@flow(log_prints=True)
def geodkv_flow():
    db = SqlAlchemyConnector.load("pg-db-auth")
    api_key = Secret.load("daf-geodkv-api").get()
    
    with db as connection:
        print(f"Connected. Key starter med {api_key[:4]}...")
