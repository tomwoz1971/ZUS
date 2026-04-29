# zus_db_utils

Moduł do zapisu zagregowanych danych z pipeline'ów analitycznych
do różnych backendów z różnymi strategiami ładowania.

## Cel biznesowy

Ujednolicenie zapisu wyników raportów (workload operatorów, metryki sprzętu,
VPN audit, device activation) do wielu targetów jednym API. Obecnie każdy
skrypt raportowy ma własną logikę zapisu — to powoduje duplikację i błędy
(np. ciche failowanie export'u Excela przez zduplikowane nazwy arkuszy).

## Stos technologiczny

- Python 3.09.+
- SQLAlchemy 2.0 (Core + ORM z automap/reflection)
- pandas (główny format danych wejściowych)
- Pydantic v2 (konfiguracja i walidacja)
- pyodbc (MSSQL read-only source w organizacji)
- psycopg2-binary (PostgreSQL data warehouse)
- pyarrow (Parquet)
- pytest + pytest-asyncio (testy)

## Architektura

Dwuwarstwowa:

1. **Backend** (`backends/`) — jak się łączyć i jak fizycznie pisać
   do konkretnego targetu (Postgres, MSSQL, SQLite, CSV, Parquet).
2. **Strategy** (`strategies/`) — jaką logikę zastosować
   (append, upsert, full refresh, SCD2, watermark incremental).

Fasada `AggWriter` w `core.py` łączy jedno z drugim:

```python
writer = AggWriter(backend="postgres", strategy="upsert", ...)
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

- **Domyślny batch size: 10_000** — konfigurowalny per backend
- **Idempotencja**: każdy zapis dostaje `load_id` (UUID4), logowany
  do tabeli `_load_history` jeśli backend to wspiera
- **Transakcyjność**: strategia pracuje w jednej transakcji per write,
  z wyjątkiem CSV/Parquet (tam transakcji nie ma — zapis do pliku tmp
  + atomic rename)
- **MSSQL**: read-only w kontekście organizacyjnym, ale moduł wspiera
  również zapis (MERGE statement dla upsert, bo SQL Server nie ma
  ON CONFLICT). Docelowo używamy PostgreSQL jako targetu zapisów.
- **Hashowanie wierszy w SCD2**: MD5 z posortowanych po kluczu kolumn
  biznesowych (bez kolumn audit)

## Typowe pułapki do unikania

- Duplikaty nazw arkuszy/tabel — walidacja przed zapisem
- `Optional[str]` vs liczby w filtrach — typy z Pydantic na wejściu
- Silent failures w batch insert — zawsze `executemany` z `returning`
  gdzie się da, albo jawny count po zapisie
- MultiIndex z groupby przed zapisem — zawsze `.reset_index()`

## Testy

- Unit testy na SQLite in-memory dla wszystkich strategii
- Integration testy na dockerowym Postgres (testcontainers-python)
- MSSQL pokrywamy mockami + jednym smoke testem na dev-instancji
- CSV/Parquet — tmp_path fixture

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