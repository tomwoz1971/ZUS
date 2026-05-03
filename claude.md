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
   (append, upsert, full refresh, SCD2, watermark incremental,
   `incremental_quantity` — patrz osobna sekcja niżej).

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

## Dokumentacja kodu

Wszystkie publiczne funkcje, klasy i metody dokumentowane w stylu **Sphinx**
(reStructuredText). Format: krótka pierwsza linia, opcjonalny rozszerzony
opis, następnie pola `:param:`, `:returns:`, `:raises:`. Docstringi zwięzłe
— bez powtarzania sygnatury i typów (są w adnotacjach).

```python
def normalize_input(data: SupportedInput) -> pd.DataFrame:
    """Konwertuje wejście do DataFrame.

    :param data: DataFrame, dict lub list[dict]
    :returns: DataFrame z wierszami z ``data``
    :raises TypeError: gdy typ wejścia nie jest wspierany
    """
```

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

## Strategia `incremental_quantity`

Wyspecjalizowany wariant SCD2 dla tabel typu „klucz biznesowy + jeden
pomiar liczbowy w czasie". Używana, gdy skrypt periodycznie zbiera
wartości metryki dla zestawu obiektów i chcemy historię zmian bez
duplikatów dla niezmieniającej się wartości.

### Schemat tabeli docelowej

Tabela jest **przygotowywana w bazie z góry** (DDL poza modułem).
Wymagane kolumny:

- kolumny klucza biznesowego `a1, …, an` (typy dowolne, łącznie PK lub
  unique constraint nieobowiązkowy — strategia sama pilnuje unikalności
  „otwartego" wiersza)
- `ilosc` — kolumna pomiaru (nazwa konfigurowalna)
- `data_od` `DATETIME NOT NULL` — początek ważności
- `data_do` `DATETIME NULL` — koniec ważności (NULL = aktualnie obowiązujący)
- `id` — PK, generowany przez bazę (`IDENTITY` / `SERIAL` / `INTEGER PRIMARY KEY AUTOINCREMENT`)

Walidacja schematu jest **twarda**: jeśli któraś z kolumn
`ilosc/data_od/data_do/id` nie istnieje lub ma niezgodny typ →
`SchemaValidationError` przed jakimkolwiek zapisem.

### API

```python
IncrementalQuantity(
    keys: list[str],                      # kolumny klucza biznesowego
    quantity_col: str = "ilosc",
    valid_from_col: str = "data_od",
    valid_to_col: str = "data_do",
    id_col: str = "id",
    tolerance: Decimal | float | None = None,  # tolerancja porównania
)
```

Wejście: `DataFrame` lub pojedynczy wiersz (`dict`/`list[dict]`)
o kolumnach `keys + [quantity_col]`. Inne kolumny → ignorowane
z warningiem (jawne `extra="forbid"` opcjonalnie w configu).

### Algorytm zapisu (per wiersz wejścia)

1. Pobierz aktualnie otwarty rekord dla danego klucza
   (`WHERE keys=… AND data_do IS NULL`) z lockiem:
   - PostgreSQL/MSSQL: `SELECT … FOR UPDATE` / `WITH (UPDLOCK, HOLDLOCK)`
   - SQLite: cała operacja w `BEGIN IMMEDIATE` (brak row locków)
2. Jeśli **brak otwartego rekordu** → `INSERT` nowego z `data_od = now`,
   `data_do = NULL`.
3. Jeśli **otwarty rekord istnieje**:
   - porównaj `ilosc` (strict `=`, lub `abs(new − old) <= tolerance`
     gdy `tolerance` ustawione)
   - **równe** → nic nie rób, przejdź do następnego wiersza
   - **różne** → `UPDATE … SET data_do = now WHERE id = <id>`,
     następnie `INSERT` nowego rekordu z `data_od = now`, `data_do = NULL`

### Źródło czasu „now"

Generowane **client-side** (`datetime.utcnow()` na maszynie skryptu),
**zapisywane w bazie jako UTC**. Tę samą wartość `now` używamy
w obrębie jednego `write()` dla wszystkich wierszy (spójny snapshot
czasowy całego batcha). Override przez parametr `as_of: datetime | None`
do backfilli/testów.

### Transakcyjność i obsługa duplikatów wejścia

- Cały `write()` w jednej transakcji (rollback przy błędzie dowolnego wiersza).
- Duplikaty kluczy w jednym DataFrame (ten sam klucz w dwóch wierszach):
  **fail-fast `ValueError`** — wymagamy unikalności kluczy w wejściu.

### Wspierane backendy

Tylko: **PostgreSQL, MSSQL, SQLite**. Backend deklaruje to w
`supported_strategies`. CSV/Parquet/inne → `UnsupportedStrategyError`.

## Procedury odczytu dla `incremental_quantity`

Wszystkie w module `queries/incremental_quantity.py`. Walidacja
schematu twarda (jak przy zapisie). Wszystkie zwracają `pd.DataFrame`
z kolumnami czasu **skonwertowanymi z UTC do `Europe/Warsaw`** (tz-aware).

### `read_current(table, keys) -> DataFrame`

Aktualnie obowiązujące ilości — wszystkie rekordy z `data_do IS NULL`.

- Kolumny wynikowe: `keys + [quantity_col, valid_from_col]` (bez `id`).
- `valid_from_col` zwracany w `Europe/Warsaw`.

### `read_snapshots(table, keys, start, end, step) -> DataFrame`

Snapshot wartości **na koniec każdego kroku** w przedziale `[start, end]`
(LOCF — wartość obowiązująca dokładnie na timestamp końca kroku).

```python
read_snapshots(
    table: str,
    keys: list[str],
    start: datetime,           # naive UTC lub aware
    end: datetime,
    step: Literal["hour", "day", "week"] | timedelta,
) -> pd.DataFrame              # kolumny: keys + [ts, ilosc]
```

- `ts` = koniec kroku (włącznie).
- Wartość w kroku: rekord, dla którego `data_od <= ts < COALESCE(data_do, '9999-12-31')`.
- Klucze, dla których w danym kroku nie było żadnego rekordu → wiersz pomijany
  (nie ma `NULL`-ów dla nieistniejących).
- Siatka czasu generowana **server-side**:
  - Postgres: `generate_series(start, end, step)`
  - SQLite/MSSQL: rekurencyjny CTE (MSSQL z `OPTION (MAXRECURSION 0)` jeśli > 100 kroków)

### `read_increments(table, keys, start, end, step) -> DataFrame`

Przyrost = `snapshot(koniec_kroku_i) − snapshot(koniec_kroku_{i-1})`
dla każdego klucza i każdego kroku.

```python
read_increments(
    table: str,
    keys: list[str],
    start: datetime,
    end: datetime,
    step: Literal["hour", "day", "week"] | timedelta,
) -> pd.DataFrame              # kolumny: keys + [ts, przyrost]
```

- Pierwszy krok dla każdego klucza ma `przyrost = NULL` (brak baseline'u).
- Implementacja: `read_snapshots` + okno `LAG()` po `keys` po stronie DB.

## Konwencja stref czasowych

- **W bazie**: timestampy zapisywane jako UTC (typ `DATETIME` bez tz
  w MSSQL/SQLite, `TIMESTAMP WITHOUT TIME ZONE` w Postgres — interpretowane
  jako UTC po stronie aplikacji).
- **Generowanie `now` w zapisie**: `datetime.utcnow()` w skrypcie.
- **W odczycie**: timestampy zwracane jako tz-aware w `Europe/Warsaw`
  (konwersja w warstwie query, nie w bazie).
- Nie używamy lokalnego czasu serwera DB — zegar bazy nie jest źródłem prawdy.

## Czego NIE robić

- Nie dodawać nowych zależności bez dyskusji — stos ma być minimalny
- Nie używać `pd.io.sql.to_sql()` bezpośrednio — za mało kontroli
  nad upsertem i typami
- Nie logować DataFrame'ów całych do konsoli (PII i pamięć)
- Nie pisać synchronicznego kodu blokującego w strategiach — zostawić
  przestrzeń na przyszły async
