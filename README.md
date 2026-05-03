# agg_writer

Moduł do zapisu zagregowanych danych z pipeline'ów analitycznych do różnych
backendów (PostgreSQL, MSSQL, SQLite, CSV, Parquet) z różnymi strategiami
ładowania (append, upsert, full refresh, SCD2, watermark incremental).

## Quick start

```python
from agg_writer import AggWriter
from agg_writer.credentials import CredentialStore, Credential

# Jednorazowo — zapis credenciala
store = CredentialStore.auto()
store.set("postgres-dwh", Credential(
    username="tomek_etl",
    password="sekret",
    metadata={"host": "dwh.local", "port": 5432, "database": "warehouse"},
))

# Użycie
writer = AggWriter(
    backend="postgres",
    strategy="upsert",
    credential="postgres-dwh",
)
writer.write(
    df_workload,
    table="fct_operator_workload",
    keys=["operator_id", "report_date"],
)
```

## Instalacja

```bash
python -m venv .venv
# Windows:
.venv\Scripts\Activate.ps1
# Linux:
source .venv/bin/activate

pip install -e ".[dev]"
```

## Wymagania systemowe

### Backend MSSQL (tylko jeśli używasz)

Wymaga **Microsoft ODBC Driver 18** zainstalowanego systemowo.

**Windows**: zwykle już jest, jeśli nie — instalator ze strony Microsoft.

**RHEL 8/9** (wymaga root'a, jednorazowo):

```bash
curl https://packages.microsoft.com/config/rhel/9/prod.repo | \
  sudo tee /etc/yum.repos.d/mssql-release.repo
sudo ACCEPT_EULA=Y dnf install -y msodbcsql18 unixODBC-devel
```

**Debian 12** (wymaga root'a, jednorazowo):

```bash
curl -sSL https://packages.microsoft.com/keys/microsoft.asc | \
  sudo gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
curl -sSL https://packages.microsoft.com/config/debian/12/prod.list | \
  sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev
```

Po instalacji systemowej, `pip install pyodbc` w venv'ie użytkownika działa
bez dodatkowych uprawnień.

## Zarządzanie credentialami

### Windows

Credentials trzymane w **Windows Credential Manager** przez bibliotekę
`keyring`. Działa bez dodatkowej konfiguracji.

### RHEL / Linux bez root'a

Credentials w **szyfrowanym pliku** `~/.agg_writer/creds.enc` (Fernet /
AES-128-CBC + HMAC, klucz derivowany z master password przez PBKDF2-HMAC-SHA256,
600_000 iteracji).

Master password podawany interaktywnie (`getpass`) albo przez zmienną
środowiskową:

```bash
export AGG_WRITER_MASTER_PASSWORD='twoje_master_haslo'
python twoj_skrypt.py
```

Dla cron'a — ustaw zmienną w crontabie albo w osobnym skrypcie wrapper.

### Uwaga o uwierzytelnianiu do MSSQL

Moduł wspiera **wyłącznie SQL Authentication** (user/password) do MSSQL.
Integrated Security / Windows Auth / Kerberos **nie są wspierane** — moduł
musi działać z RHEL, gdzie konfiguracja Kerberos wymaga root'a i zespołu AD.

## Struktura projektu

```
src/agg_writer/
├── core.py                  # Fasada AggWriter
├── backends/                # Jak się łączyć i fizycznie pisać
│   ├── base.py
│   ├── postgres.py
│   ├── mssql.py
│   ├── sqlite.py
│   ├── csv_backend.py
│   └── parquet.py
├── strategies/              # Logika ładowania
│   ├── base.py
│   ├── append.py
│   ├── upsert.py
│   ├── full_refresh.py
│   ├── scd2.py
│   └── watermark.py
├── credentials/             # Bezpieczne przechowywanie haseł
│   ├── base.py
│   ├── factory.py
│   └── providers/
│       ├── keyring_provider.py
│       ├── encrypted_file.py
│       └── environment.py
├── input_adapters.py        # DataFrame | dict | Iterator → normalizacja
├── schema.py
├── config.py
└── exceptions.py
```

## Testy

```bash
pytest                              # wszystkie
pytest tests/test_credentials       # tylko credentials
pytest -m "not integration"         # bez testów wymagających dockera
pytest --cov                        # z pokryciem
```

## Linting

```bash
ruff check src tests
ruff format src tests
mypy src
```
