from __future__ import annotations

from collections.abc import Iterator

import pandas as pd
import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table
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
