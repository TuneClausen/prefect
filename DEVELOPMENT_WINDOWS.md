# Udviklings-setup på Windows 11 (open source-stack)

Det her dokument beskriver hvordan du sætter dit prefect-repo op til lokal
udvikling på en helt frisk Windows 11-maskine — uden noget forudinstalleret.
Værktøjsvalget er drevet af open source-præference: VSCodium frem for VS Code,
Git for Windows, Python fra python.org, DBeaver Community frem for proprietær
DB-klient.

Slutresultatet er det samme som på Kubuntu-PC'en: du kan køre flows direkte i
en lokal Python-proces, debugge med breakpoints, loade samme blocks som dine
deployments på prefect01 bruger, og iterere i sekunder i stedet for minutter.

---

## Forudsætninger

- Windows 11
- Netværksadgang til prefect01 (192.168.0.41:4200) og PostGIS-DB
- Admin-rettigheder til at installere programmer

---

## Step 1 — Aktiver OpenSSH-klienten

Windows 11 leveres normalt med OpenSSH, men kun klient-delen og ikke altid
aktiveret. Tjek og aktiver:

```powershell
# Tjek
Get-WindowsCapability -Online | Where-Object Name -like "OpenSSH.Client*"

# Hvis "NotPresent", aktiver:
Add-WindowsCapability -Online -Name OpenSSH.Client~~~~0.0.1.0
```

Du skal kunne SSH'e til prefect01 for at se serverens pakke-versioner i Step 4.

---

## Step 2 — Installer Python 3.12

Hent fra https://www.python.org/downloads/windows/ — vælg den nyeste 3.12.x
"Windows installer (64-bit)". Officiel CPython, PSF-licens (open source).

**Vigtigt under installation:**
- ☑ Sæt flueben i "Add python.exe to PATH" på første skærm
- Vælg "Customize installation" hvis du vil installere for alle brugere

Verificer i en ny PowerShell:

```powershell
python --version    # Skal vise Python 3.12.x
pip --version
```

> **Tradeoff:** Du kan også få Python fra Microsoft Store, men det installerer
> til en sandkasse-mappe der nogle gange laver mærkelige PATH-konflikter med
> venv. python.org-installeren er mere forudsigelig.

---

## Step 3 — Installer Git for Windows

Hent fra https://git-scm.com/download/win — open source (GPL). Du får:

- `git` (kommandolinje)
- **Git Bash** (Unix-lignende shell — meget brugbar når du følger Linux-tutorials)
- En credential helper

**Under installation:**
- "Choose the default editor used by Git" → vælg "Use Notepad" eller "Use Visual
  Studio Code as Git's default editor" (VSCodium fungerer ikke direkte her,
  notepad er fint at starte med)
- "Adjusting your PATH environment" → vælg "Git from the command line and also
  from 3rd-party software" (standard)
- "Choosing the SSH executable" → "Use bundled OpenSSH"
- Lad resten stå på standard

Verificer:

```powershell
git --version
```

Konfigurer dit Git-navn og email:

```powershell
git config --global user.name "Tune Clausen"
git config --global user.email "din@email.dk"
```

---

## Step 4 — Find serverens pakke-versioner

SSH til prefect01 og tjek hvad Python og de kritiske pakker kører i Prefect's
venv:

```powershell
ssh prefect-admin@192.168.0.41
# inde på serveren:
sudo -iu prefect /opt/prefect/venv/bin/python --version
sudo -iu prefect /opt/prefect/venv/bin/pip freeze | grep -iE "^(prefect|sqlalchemy|psycopg2|httpx)"
exit
```

Eksempel-output:
```
Python 3.12.x
httpx==0.27.2
prefect==3.x.x
prefect-sqlalchemy==0.5.x
psycopg2-binary==2.9.9
SQLAlchemy==2.0.x
```

Noter versionerne — du skal pinne dem i `requirements.txt` (Step 7).

---

## Step 5 — Installer VSCodium

Hent fra https://vscodium.com — open source build af VS Code uden Microsofts
telemetry og branding. Default extension-marketplace er OpenVSX, så du undgår
også Microsofts proprietære marketplace.

Vælg "User Installer" eller "System Installer" — User er nemmest hvis du er
eneste bruger på maskinen.

Verificer (i en ny terminal — installeren tilføjer `codium` til PATH):

```powershell
codium --version
```

---

## Step 6 — Klon repoet

I PowerShell (eller Git Bash):

```powershell
# Vælg en mappe til dine kode-repos, fx:
cd $env:USERPROFILE\Documents
mkdir -p code
cd code

git clone https://github.com/TuneClausen/prefect.git
cd prefect
```

---

## Step 7 — Opret venv og installer dependencies

Windows bruger en anden sti til venv-aktivering end Linux. Bemærk
`.venv\Scripts\activate` i stedet for `.venv/bin/activate`:

```powershell
python -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
```

Hvis PowerShell klager over execution policy ("cannot be loaded because running
scripts is disabled"), tillad det for din egen bruger én gang:

```powershell
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
```

Opret `requirements.txt` i repo-roden med de versioner du fandt i Step 4
(eksempel-skabelon i `dev-setup/requirements.txt`):

```
prefect==3.x.x
prefect-sqlalchemy==0.5.x
sqlalchemy==2.0.x
psycopg2-binary==2.9.9
httpx==0.27.2
```

Installer:

```powershell
pip install -r requirements.txt
```

> **Tradeoff (samme som på Linux):** Jeg pinner top-level deps, ikke
> transitive. Pragmatisk men ikke deterministisk på tværs af maskiner.
> Hvis du senere vil have låsefiler, kig på `uv` eller `pip-tools`.

---

## Step 8 — Konfigurer Prefect-profil

Lav en profil der peger på prefect01, så CLI'en og scripts ved hvor blocks
hentes fra:

```powershell
prefect profile create homelab
prefect profile use homelab
prefect config set PREFECT_API_URL="http://192.168.0.41:4200/api"
prefect config view
```

Verificer at du faktisk taler med serveren:

```powershell
prefect deployment ls
```

Du burde se `bootstrap_geodkv/bootstrap-geodkv` og evt. andre deployments.

---

## Step 9 — Installer en PostgreSQL-klient (DBeaver Community)

Du har brug for noget der kan tale med PostGIS — både til at debugge data og
til at lade `test_blocks.py` verificere DB-adgang. To gode open source-valg:

**DBeaver Community** (anbefales — GUI-baseret, kan håndtere geometri):
- Hent fra https://dbeaver.io/download/ → "Windows (installer)"
- Apache-licens, open source

**Eller kun `psql` (kommandolinje):**
- Hent PostgreSQL-installeren fra https://www.postgresql.org/download/windows/
- Under installation: fravælg "PostgreSQL Server" og "Stack Builder"
- Behold kun "Command Line Tools"
- Tilføj `C:\Program Files\PostgreSQL\<version>\bin` til PATH manuelt hvis
  installeren ikke gør det

Test forbindelse (eksempel med psql):

```powershell
psql -h <db-host> -p 5432 -U <db-user> -d <db-name> -c "SELECT version();"
```

---

## Step 10 — Verificer at blocks kan loades

Brug `scripts/test_blocks.py` fra dev-setup-mappen (det er platform-agnostisk —
samme fil virker på både Linux og Windows). Læg den under `scripts/` i repoet.

Kør:

```powershell
python scripts\test_blocks.py
```

Tre OK-linjer = du har fuld block-adgang og DB-forbindelse fra Windows-PC'en.
Fejl her er typisk:

- Netværks-blokering (firewall mellem din PC og prefect01 eller DB-VM)
- Forkert profil-konfiguration (`prefect config view` viser den forkerte URL)
- PostGIS-VM lytter ikke på den IP din block peger på fra dit subnet

---

## Step 11 — VSCodium-konfiguration

Åbn repoet:

```powershell
codium .
```

### Vælg Python-interpreter
1. `Ctrl+Shift+P`
2. "Python: Select Interpreter"
3. Vælg `.venv\Scripts\python.exe`

### Installer Python-extension
VSCodium bruger OpenVSX som default. Den åbne `ms-python.python` extension er
tilgængelig der (selvom Microsoft udgiver den, er pakkens kode primært
open source og debugger-delen `debugpy` er fuldt open source).

1. Klik Extensions-ikonet (eller `Ctrl+Shift+X`)
2. Søg "Python"
3. Installer "Python" af ms-python (eller OpenVSX-aliaset hvis det er rebrandet)

> **Open source-purist alternativ:** Hvis du vil holde dig 100% til
> open source uden Microsoft-pakker, kan du bruge `python-lsp-server`
> via `pip install python-lsp-server` plus VSCodium's "Python LSP"
> extension. Det dækker basis-features (linting, completion, definitions),
> men debugger-integration kræver lidt mere konfiguration. Mit råd: start
> med ms-python.python via OpenVSX, og skift hvis det generer dig.

### Debug-konfiguration
Læg `.vscode/launch.json` fra dev-setup-mappen i repoet. Den virker uændret på
Windows — Python's debugger (`debugpy`) håndterer platforms-forskelle.

---

## Step 12 — Dagligt udviklings-workflow

### Aktivér venv (hver gang du åbner ny terminal)

```powershell
.venv\Scripts\activate
```

Eller konfigurer VSCodium til at gøre det automatisk — det sker faktisk når du
åbner en ny "Terminal" inde i VSCodium efter du har valgt interpreter.

### Kør et flow lokalt

```powershell
python bootstrap_geodkv.py
```

Flowet kører i din lokale proces på Windows-PC'en og logger til prefect01's UI.

### Debug med breakpoints
1. Sæt breakpoint ved at klikke i margin
2. Tryk F5 og vælg "Debug: bootstrap_geodkv"
3. Inspicér variabler i Variables-panelet
4. Step gennem koden med F10/F11

### Test små stykker isoleret
Lav små eksperiment-filer under `scripts/` (`test_introspection.py`, osv.) —
de er hurtigere at iterere end at køre hele flowet.

### Når noget virker

```powershell
git add bootstrap_geodkv.py
git commit -m "..."
git push
```

Dit deployment på prefect01 cloner repoet ved næste flow-run.

---

## Step 13 — Hold environments i sync

Når Prefect-serveren bliver opdateret, gentag Step 4 og opdater
`requirements.txt` med de nye versioner:

```powershell
pip install -r requirements.txt --upgrade
```

---

## Windows-specifikke faldgruber

1. **Sti-separator.** Python håndterer både `/` og `\` på Windows, men når du
   skriver SQL-stier eller læser uploaded filer skal du være konsistent. Brug
   `pathlib.Path` i scripts — den abstraherer over forskellen.

2. **CRLF vs LF i Git.** Default Git for Windows konverterer line-endings.
   Det kan give kaos i shell-scripts der køres på Linux. For et repo der
   indeholder scripts der skal køre på prefect01 (Linux), tilføj en
   `.gitattributes`-fil med:
   ```
   * text=auto eol=lf
   ```

3. **`.venv` i en OneDrive-synkroniseret mappe.** Hvis dit `Documents`-bibliotek
   ligger under OneDrive, vil OneDrive prøve at synce hele `.venv`-mappen
   (tusinder af små filer). Det er langsomt og kan korrumpere venv'et. Læg
   dine repos uden for OneDrive — fx `C:\code\` eller en separat mappe.

4. **PowerShell vs Git Bash.** Git Bash giver dig Unix-lignende kommandoer
   (`ls`, `cat`, `grep`) som de fleste tutorials antager. PowerShell har sine
   egne ækvivalenter. Brug det der falder dig naturligt. VSCodium's
   integrerede terminal kan konfigureres til at åbne enten — søg "Default
   Profile" i indstillingerne.

5. **Windows Defender og virus-scanning på .venv.** Defender kan langsomme
   pip-installationer betragteligt. Hvis du oplever installation tager 5+
   minutter, kan du tilføje `.venv`-mapper til Defender-undtagelser.

---

## Filerne du genbruger fra dev-setup

Disse er identiske med Linux-versionen — kopier dem ind i dit repo som de er:

- `requirements.txt` — pin-skabelon
- `.vscode/launch.json` — debug-config
- `scripts/test_blocks.py` — sanity-check
- `.env.example` — placeholder
