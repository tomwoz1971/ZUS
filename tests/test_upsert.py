from __future__ import annotations

from collections.abc import Iterator

import pandas as pd
import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, text
from sqlalchemy.engine import Engine

from zus_db_utils.exceptions import SchemaValidationError, UnsupportedStrategyError
from zus_db_utils.strategies.upsert import Upsert, UpsertResult


def _row_count(engine: Engine, table: str) -> int:
    from sqlalchemy import text

    with engine.connect() as conn:
        return int(conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one())


def _fetch_all(engine: Engine, table: str) -> list[dict]:
    from sqlalchemy import text

    with engine.connect() as conn:
        rows = conn.execute(text(f"SELECT * FROM {table}")).mappings().all()
    return [dict(r) for r in rows]


@pytest.fixture
def simple_table(engine: Engine) -> Iterator[str]:
    """Tabela z kluczem (a1, a2) i kolumna wartosci v1."""
    metadata = MetaData()
    Table(
        "prosta",
        metadata,
        Column("a1", String, nullable=False),
        Column("a2", String, nullable=False),
        Column("v1", Integer, nullable=True),
    )
    metadata.drop_all(engine)
    metadata.create_all(engine)
    try:
        yield "prosta"
    finally:
        metadata.drop_all(engine)


@pytest.fixture
def pk_table(engine: Engine) -> Iterator[str]:
    """Tabela z auto-PK i kluczem biznesowym (kod)."""
    metadata = MetaData()
    Table(
        "z_pk",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("kod", String, nullable=False),
        Column("wartosc", Integer, nullable=True),
    )
    metadata.drop_all(engine)
    metadata.create_all(engine)
    try:
        yield "z_pk"
    finally:
        metadata.drop_all(engine)


class TestInsert:
    def test_new_row_is_inserted(self, engine: Engine, simple_table: str) -> None:
        strat = Upsert(keys=["a1", "a2"])
        df = pd.DataFrame([{"a1": "x", "a2": "y", "v1": 10}])

        result = strat.write(engine, df, simple_table)

        assert result == UpsertResult(inserted=1, updated=0)
        assert _row_count(engine, simple_table) == 1

    def test_multiple_new_rows_inserted(self, engine: Engine, simple_table: str) -> None:
        strat = Upsert(keys=["a1", "a2"])
        df = pd.DataFrame([
            {"a1": "x", "a2": "y", "v1": 1},
            {"a1": "x", "a2": "z", "v1": 2},
            {"a1": "w", "a2": "y", "v1": 3},
        ])

        result = strat.write(engine, df, simple_table)

        assert result == UpsertResult(inserted=3, updated=0)
        assert _row_count(engine, simple_table) == 3

    def test_dict_input_supported(self, engine: Engine, simple_table: str) -> None:
        strat = Upsert(keys=["a1", "a2"])
        result = strat.write(engine, {"a1": "x", "a2": "y", "v1": 5}, simple_table)
        assert result.inserted == 1

    def test_list_of_dicts_supported(self, engine: Engine, simple_table: str) -> None:
        strat = Upsert(keys=["a1", "a2"])
        result = strat.write(
            engine, [{"a1": "x", "a2": "y", "v1": 5}], simple_table
        )
        assert result.inserted == 1


class TestUpdate:
    def test_existing_row_is_updated(self, engine: Engine, simple_table: str) -> None:
        strat = Upsert(keys=["a1", "a2"])
        strat.write(engine, pd.DataFrame([{"a1": "x", "a2": "y", "v1": 10}]), simple_table)

        result = strat.write(
            engine, pd.DataFrame([{"a1": "x", "a2": "y", "v1": 99}]), simple_table
        )

        assert result == UpsertResult(inserted=0, updated=1)
        assert _row_count(engine, simple_table) == 1
        rows = _fetch_all(engine, simple_table)
        assert rows[0]["v1"] == 99

    def test_mixed_insert_and_update(self, engine: Engine, simple_table: str) -> None:
        strat = Upsert(keys=["a1", "a2"])
        strat.write(
            engine,
            pd.DataFrame([{"a1": "x", "a2": "y", "v1": 10}]),
            simple_table,
        )

        result = strat.write(
            engine,
            pd.DataFrame([
                {"a1": "x", "a2": "y", "v1": 20},
                {"a1": "x", "a2": "z", "v1": 30},
            ]),
            simple_table,
        )

        assert result == UpsertResult(inserted=1, updated=1)
        assert _row_count(engine, simple_table) == 2

    def test_update_preserves_other_rows(self, engine: Engine, simple_table: str) -> None:
        strat = Upsert(keys=["a1", "a2"])
        strat.write(
            engine,
            pd.DataFrame([
                {"a1": "x", "a2": "y", "v1": 1},
                {"a1": "x", "a2": "z", "v1": 2},
            ]),
            simple_table,
        )

        strat.write(
            engine,
            pd.DataFrame([{"a1": "x", "a2": "y", "v1": 99}]),
            simple_table,
        )

        rows = {(r["a1"], r["a2"]): r["v1"] for r in _fetch_all(engine, simple_table)}
        assert rows[("x", "y")] == 99
        assert rows[("x", "z")] == 2


class TestKeyOnlyInput:
    def test_keys_only_updates_nothing(self, engine: Engine, simple_table: str) -> None:
        strat = Upsert(keys=["a1", "a2"])
        strat.write(engine, pd.DataFrame([{"a1": "x", "a2": "y", "v1": 10}]), simple_table)

        result = strat.write(
            engine, pd.DataFrame([{"a1": "x", "a2": "y"}]), simple_table
        )

        assert result == UpsertResult(inserted=0, updated=1)
        rows = _fetch_all(engine, simple_table)
        assert rows[0]["v1"] == 10

    def test_extra_input_cols_not_in_table_are_ignored(
        self, engine: Engine, simple_table: str
    ) -> None:
        strat = Upsert(keys=["a1", "a2"])
        df = pd.DataFrame([{"a1": "x", "a2": "y", "v1": 5, "nieistnieje": 999}])

        result = strat.write(engine, df, simple_table)

        assert result.inserted == 1
        rows = _fetch_all(engine, simple_table)
        assert "nieistnieje" not in rows[0]


class TestPartialUpdates:
    """Testy zachowania upsert przy częściowych aktualizacjach.
    
    DataFrame zawiera klucze + tylko podzbiór kolumn z tabeli.
    Kolumny nieobecne w DataFrame powinny:
    - przy UPDATE: zachować swoją aktualną wartość w bazie
    - przy INSERT: przyjąć DEFAULT lub NULL
    """

    @pytest.fixture
    def table_with_defaults(self, engine: Engine) -> Iterator[str]:
        """Tabela z 3 kolumnami wartości: v1, v2 (z DEFAULT), v3 (nullable)."""
        table_name = "partial_test"
        with engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                CREATE TABLE {table_name} (
                    key_col TEXT PRIMARY KEY,
                    v1 INTEGER,
                    v2 INTEGER DEFAULT 999,
                    v3 TEXT
                )
            """
                )
            )
        yield table_name
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))

    def test_update_preserves_omitted_columns(
        self, engine: Engine, table_with_defaults: str
    ) -> None:
        """UPDATE: kolumny nieobecne w DataFrame zachowują starą wartość."""
        strat = Upsert(keys=["key_col"])

        # Wstaw pełny wiersz
        strat.write(
            engine,
            pd.DataFrame([{"key_col": "A", "v1": 10, "v2": 20, "v3": "stary"}]),
            table_with_defaults,
        )

        # UPDATE z DataFrame zawierającym TYLKO key_col + v1 (brak v2, v3)
        result = strat.write(
            engine,
            pd.DataFrame([{"key_col": "A", "v1": 100}]),
            table_with_defaults,
        )

        assert result == UpsertResult(inserted=0, updated=1)

        rows = _fetch_all(engine, table_with_defaults)
        assert len(rows) == 1
        assert rows[0]["v1"] == 100  # zaktualizowane
        assert rows[0]["v2"] == 20  # NIEZMIENIONE (nie NULL!)
        assert rows[0]["v3"] == "stary"  # NIEZMIENIONE (nie NULL!)

    def test_insert_uses_defaults_for_omitted_columns(
        self, engine: Engine, table_with_defaults: str
    ) -> None:
        """INSERT: kolumny nieobecne w DataFrame przyjmują DEFAULT lub NULL."""
        strat = Upsert(keys=["key_col"])

        # INSERT z DataFrame zawierającym TYLKO key_col + v1 (brak v2, v3)
        result = strat.write(
            engine,
            pd.DataFrame([{"key_col": "B", "v1": 50}]),
            table_with_defaults,
        )

        assert result == UpsertResult(inserted=1, updated=0)

        rows = _fetch_all(engine, table_with_defaults)
        assert len(rows) == 1
        assert rows[0]["v1"] == 50  # wstawione z DataFrame
        assert rows[0]["v2"] == 999  # DEFAULT
        assert rows[0]["v3"] is None  # NULL (brak DEFAULT)

    def test_mixed_insert_update_partial_columns(
        self, engine: Engine, table_with_defaults: str
    ) -> None:
        """Mieszanka INSERT + UPDATE z częściowymi kolumnami."""
        strat = Upsert(keys=["key_col"])

        # Początkowy stan: 2 pełne wiersze
        strat.write(
            engine,
            pd.DataFrame([
                {"key_col": "X", "v1": 1, "v2": 2, "v3": "opis_X"},
                {"key_col": "Y", "v1": 3, "v2": 4, "v3": "opis_Y"},
            ]),
            table_with_defaults,
        )

        # UPSERT 1: UPDATE X (tylko v1, bez v2/v3)
        result1 = strat.write(
            engine,
            pd.DataFrame([{"key_col": "X", "v1": 111}]),
            table_with_defaults,
        )
        assert result1 == UpsertResult(inserted=0, updated=1)

        # UPSERT 2: INSERT Z (tylko v3, bez v1/v2)
        result2 = strat.write(
            engine,
            pd.DataFrame([{"key_col": "Z", "v3": "nowy_opis"}]),
            table_with_defaults,
        )
        assert result2 == UpsertResult(inserted=1, updated=0)

        rows = {r["key_col"]: r for r in _fetch_all(engine, table_with_defaults)}

        # X: UPDATE zachował v2, v3
        assert rows["X"]["v1"] == 111  # zaktualizowane
        assert rows["X"]["v2"] == 2  # NIEZMIENIONE
        assert rows["X"]["v3"] == "opis_X"  # NIEZMIENIONE

        # Z: INSERT użył DEFAULT dla v2, NULL dla v1
        assert rows["Z"]["v1"] is None  # NULL (nie było w DataFrame)
        assert rows["Z"]["v2"] == 999  # DEFAULT
        assert rows["Z"]["v3"] == "nowy_opis"  # z DataFrame


class TestTableWithPK:
    def test_upsert_does_not_touch_pk_column(
        self, engine: Engine, pk_table: str
    ) -> None:
        strat = Upsert(keys=["kod"])
        strat.write(engine, pd.DataFrame([{"kod": "A", "wartosc": 1}]), pk_table)
        strat.write(engine, pd.DataFrame([{"kod": "A", "wartosc": 2}]), pk_table)

        assert _row_count(engine, pk_table) == 1
        rows = _fetch_all(engine, pk_table)
        assert rows[0]["wartosc"] == 2


class TestBatching:
    def test_batch_size_respected(self, engine: Engine, simple_table: str) -> None:
        strat = Upsert(keys=["a1", "a2"], batch_size=3)
        df = pd.DataFrame([{"a1": str(i), "a2": "y", "v1": i} for i in range(7)])

        result = strat.write(engine, df, simple_table)

        assert result == UpsertResult(inserted=7, updated=0)
        assert _row_count(engine, simple_table) == 7

    def test_batch_size_with_updates(self, engine: Engine, simple_table: str) -> None:
        strat = Upsert(keys=["a1", "a2"], batch_size=2)
        initial = pd.DataFrame([{"a1": str(i), "a2": "y", "v1": i} for i in range(5)])
        strat.write(engine, initial, simple_table)

        updated = pd.DataFrame([{"a1": str(i), "a2": "y", "v1": i * 10} for i in range(5)])
        result = strat.write(engine, updated, simple_table)

        assert result == UpsertResult(inserted=0, updated=5)


class TestValidation:
    def test_empty_keys_raises(self) -> None:
        with pytest.raises(ValueError, match="keys"):
            Upsert(keys=[])

    def test_missing_key_col_in_input_raises(
        self, engine: Engine, simple_table: str
    ) -> None:
        strat = Upsert(keys=["a1", "a2"])
        df = pd.DataFrame([{"a1": "x", "v1": 5}])
        with pytest.raises(SchemaValidationError, match="Brak kolumn kluczowych w wejsciu"):
            strat.write(engine, df, simple_table)

    def test_missing_key_col_in_table_raises(
        self, engine: Engine, simple_table: str
    ) -> None:
        strat = Upsert(keys=["a1", "nieistnieje"])
        df = pd.DataFrame([{"a1": "x", "nieistnieje": "y"}])
        with pytest.raises(SchemaValidationError, match="nie ma wymaganych kolumn kluczowych"):
            strat.write(engine, df, simple_table)

    def test_unsupported_dialect_raises(self) -> None:
        from unittest.mock import MagicMock

        engine = MagicMock()
        engine.dialect.name = "oracle"
        strat = Upsert(keys=["a1"])
        with pytest.raises(UnsupportedStrategyError, match="oracle"):
            strat.write(engine, pd.DataFrame([{"a1": "x"}]), "t")

    def test_single_key_multi_row(self, engine: Engine, simple_table: str) -> None:
        strat = Upsert(keys=["a1"])
        df = pd.DataFrame([
            {"a1": "x", "a2": "y", "v1": 1},
            {"a1": "z", "a2": "w", "v1": 2},
        ])
        result = strat.write(engine, df, simple_table)
        assert result == UpsertResult(inserted=2, updated=0)
