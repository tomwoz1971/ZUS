# agg_writer

Moduł do zapisu zagregowanych danych z pipeline'ów analitycznych
do różnych backendów z różnymi strategiami ładowania.

## Cel biznesowy

Ujednolicenie zapisu wyników raportów (workload operatorów, metryki sprzętu,
VPN audit, device activation) do wielu targetów jednym API. Obecnie każdy
skrypt raportowy ma własną logikę zapisu — to powoduje duplikację i błędy
(np. ciche failowanie export'u Excela przez zduplikowane nazwy arkuszy).

## Stos technologiczny

- Python 3.11+
- SQLAlchemy 2.0 (Core + ORM z automap/reflection)
- pandas (główny format danych wejściowych)
- Pydantic v2 (konfiguracja i walidacja)
- pyodbc (MSSQL read-only source w organizacji)
- psycopg2-binary (PostgreSQL data warehouse)
- pyarrow (Parquet)
- cryptography (szyfrowanie credentials na RHEL)
- keyring (Windows Credential Manager)
- pytest + pytest-asyncio (testy)

## Architektura

Dwuwarstwowa:

1. **Backend** (`backends/`) — jak się łączyć i jak fizycznie pisać
   do konkretnego targetu (Postgres, MSSQL, SQLite, CSV, Parquet).
2. **Strategy** (`strategies/`) — jaką logikę zastosować
   (append, upsert, full refresh, SCD2, watermark incremental).

Fasada `AggWriter` w `core.py` łączy jedno z drugim:

```python
writer = AggWriter(backend="postgres", strategy="upsert", credential="postgres-dwh")
writer.write(df, table="operator_workload", keys=["operator_id", "date"])
```

Nie każda kombinacja backend×strategy ma sens — np. SCD2 na CSV to
append z metadanymi, upsert w CSV wymaga przepisania pliku. Backend
deklaruje wspierane strategie przez `supported_strategies: set[str]`.

## Konwencje nazewnictwa

- Python: `snake_case` dla funkcji i zmiennych, `PascalCase` dla klas
- Tabele PostgreSQL i kolumny: `snake_case` (zgodnie z automap'em SQLAlchemy)
- Kolumny techniczne SCD2: `valid_from`, `valid_to`, `is_current`, `row_hash`
- Kolumny audit: `loaded_at`, `load_id`, `source_system`
- Nazwy tabel stage: prefix `stg_`, fact: `fct_`, wymiary: `dim_`

## Dane wejściowe — 3 formaty

1. `pandas.DataFrame` — domyślny, dla raportów < 1M wierszy
2. `list[dict]` / `dict` — dla małych zapisów typu log/audit
3. `Iterator[dict]` / generator — dla streamingu z dużych query
   MSSQL (np. historia ticketów za rok)

Wewnętrznie wszystko normalizowane do iteratora batchy przez
`input_adapters.normalize_input()`.

## Ważne decyzje projektowe

### Ogólne

- **Domyślny batch size: 10_000** — konfigurowalny per backend
- **Idempotencja**: każdy zapis dostaje `load_id` (UUID4), logowany
  do tabeli `_load_history` jeśli backend to wspiera
- **Transakcyjność**: strategia pracuje w jednej transakcji per write,
  z wyjątkiem CSV/Parquet (tam transakcji nie ma — zapis do pliku tmp
  + atomic rename)
- **Hashowanie wierszy w SCD2**: SHA-256 z posortowanych po kluczu kolumn
  biznesowych (bez kolumn audit)

### Uwierzytelnianie — założenia architektoniczne

- **PostgreSQL**: user/password (standard), credential w CredentialStore
- **MSSQL**: **WYŁĄCZNIE SQL Authentication (user/password)**.
  NIE wspieramy Windows Authentication / Integrated Security / Kerberos.

  Powód: moduł musi działać z RHEL (skrypty ETL w cron'ie), gdzie
  Integrated Security wymagałaby konfiguracji Kerberos/AD, keytaba,
  uprawnień root'a i współpracy z zespołem AD. To niewykonalne w naszym
  kontekście organizacyjnym.

  W praktyce: w connection string dla pyodbc zawsze `UID=...;PWD=...;`,
  NIGDY `Trusted_Connection=yes`. Walidujemy to w backendzie MSSQL —
  jeśli credential nie ma password, rzucamy CredentialError.

- **SQLite**: brak auth (plik lokalny), credential opcjonalny
- **CSV/Parquet**: brak auth, credential nieużywany

## Zarządzanie credentialami

Subsystem `agg_writer.credentials` z pluggable providers.

### Wymagania

- **Windows**: działa bez dodatkowej konfiguracji (Windows Credential Manager
  przez bibliotekę `keyring`)
- **RHEL/Linux bez root'a**: używa szyfrowanego pliku (`cryptography.fernet`
  + PBKDF2-HMAC-SHA256, 600_000 iteracji). Plik w `~/.agg_writer/creds.enc`
  z uprawnieniami 0600. Master password z `getpass` albo zmiennej
  środowiskowej `AGG_WRITER_MASTER_PASSWORD`.
- **NIE wymagamy** gnome-keyring, D-Bus, libsecret, secretstorage ani
  dbus-python — te biblioteki wymagają pakietów systemowych i root'a
  na RHEL, których nie mamy.

### Zasady bezpieczeństwa

- Hasła NIGDY nie trafiają do logów (nawet na DEBUG)
- `Credential.__repr__` maskuje hasło (`password='***'`)
- `Credential.password` to `pydantic.SecretStr`
- Master password trzymany tylko w pamięci procesu, nigdy nie zapisywany
- Plik enc zapisywany atomic (tmp + os.replace), żeby nie zostawić pustego
  pliku przy crashu
- Backup starego pliku przy rotacji (`.enc.bak`, tylko ostatni)
- Sprawdzamy uprawnienia pliku przy odczycie, ostrzegamy jeśli > 0600
  (na Windows ten check pomijamy)

### Python API (CLI na później)

```python
from agg_writer.credentials import CredentialStore, Credential

store = CredentialStore.auto()
store.set("postgres-dwh", Credential(
    username="tomek_etl",
    password="sekret",
    metadata={"host": "dwh.local", "port": 5432, "database": "warehouse"},
))
cred = store.get("postgres-dwh")
store.rotate("postgres-dwh", new_password="nowe_haslo")
store.list_entries()
store.delete("postgres-dwh")
```

## Typowe pułapki do unikania

- Duplikaty nazw arkuszy/tabel — walidacja przed zapisem
- `Optional[str]` vs liczby w filtrach — typy z Pydantic na wejściu
- Silent failures w batch insert — zawsze `executemany` z `returning`
  gdzie się da, albo jawny count po zapisie
- MultiIndex z groupby przed zapisem — zawsze `.reset_index()`
- Logowanie connection stringów — zawsze maskować hasło

## Testy

- Unit testy na SQLite in-memory dla wszystkich strategii
- Integration testy na dockerowym Postgres (testcontainers-python)
- MSSQL pokrywamy mockami + jednym smoke testem na dev-instancji
- CSV/Parquet — tmp_path fixture
- Credentials: osobne testy dla każdego providera, encrypted_file testowany
  dokładnie (wrong master password, korupcja pliku, wrong permissions)

## Komendy

```bash
# Instalacja dev
pip install -e ".[dev]"

# Testy
pytest                          # wszystkie
pytest tests/test_backends      # konkretny moduł
pytest -k "upsert"              # po nazwie

# Linting
ruff check src tests
ruff format src tests
mypy src
```

## Czego NIE robić

- Nie dodawać nowych zależności bez dyskusji — stos ma być minimalny
- Nie używać `pd.io.sql.to_sql()` bezpośrednio — za mało kontroli
  nad upsertem i typami
- Nie logować DataFrame'ów całych do konsoli (PII i pamięć)
- Nie pisać synchronicznego kodu blokującego w strategiach — zostawić
  przestrzeń na przyszły async
- NIE dodawać `secretstorage` / `dbus-python` do zależności
- NIE implementować Integrated Security / Kerberos dla MSSQL
- NIE commitować plików `.enc`, `.salt`, `credentials.*` — są w .gitignore
