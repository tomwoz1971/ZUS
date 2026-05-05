from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

from zus_db_utils import AggWriter
from zus_db_utils.backends import SQLiteBackend
from zus_db_utils.exceptions import SchemaValidationError
from zus_db_utils.strategies.append import Append, AppendResult

T0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def _row_count(engine: Engine, table: str) -> int:
    with engine.connect() as conn:
        return int(conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one())


class TestAppendStrategy:
    def test_inserts_all_rows(self, engine: Engine, metryka_table: str) -> None:
        strat = Append()
        df = pd.DataFrame(
            [
                {"a1": "x", "a2": "y", "ilosc": 10, "data_od": T0},
                {"a1": "x", "a2": "z", "ilosc": 20, "data_od": T0},
            ]
        )

        result = strat.write(engine, df, metryka_table)

        assert isinstance(result, AppendResult)
        assert result.inserted == 2
        assert _row_count(engine, metryka_table) == 2

    def test_double_write_appends_without_dedup(self, engine: Engine, metryka_table: str) -> None:
        strat = Append()
        df = pd.DataFrame([{"a1": "x", "a2": "y", "ilosc": 10, "data_od": T0}])

        strat.write(engine, df, metryka_table)
        strat.write(engine, df, metryka_table)

        assert _row_count(engine, metryka_table) == 2

    def test_dict_input_supported(self, engine: Engine, metryka_table: str) -> None:
        result = Append().write(
            engine,
            {"a1": "x", "a2": "y", "ilosc": 10, "data_od": T0},
            metryka_table,
        )
        assert result.inserted == 1

    def test_list_of_dicts_input_supported(self, engine: Engine, metryka_table: str) -> None:
        result = Append().write(
            engine,
            [
                {"a1": "x", "a2": "y", "ilosc": 10, "data_od": T0},
                {"a1": "x", "a2": "z", "ilosc": 20, "data_od": T0},
            ],
            metryka_table,
        )
        assert result.inserted == 2

    def test_empty_dataframe_returns_zero(self, engine: Engine, metryka_table: str) -> None:
        df = pd.DataFrame(columns=["a1", "a2", "ilosc", "data_od"])
        result = Append().write(engine, df, metryka_table)
        assert result.inserted == 0
        assert _row_count(engine, metryka_table) == 0

    def test_extra_column_not_in_table_fails(self, engine: Engine, metryka_table: str) -> None:
        df = pd.DataFrame([{"a1": "x", "a2": "y", "ilosc": 10, "data_od": T0, "nieznana": 1}])
        with pytest.raises(SchemaValidationError, match="spoza tabeli"):
            Append().write(engine, df, metryka_table)

    def test_batching_inserts_all_rows(self, engine: Engine, metryka_table: str) -> None:
        strat = Append(batch_size=3)
        rows = [{"a1": "x", "a2": str(i), "ilosc": i, "data_od": T0} for i in range(10)]
        result = strat.write(engine, rows, metryka_table)
        assert result.inserted == 10
        assert _row_count(engine, metryka_table) == 10

    def test_invalid_batch_size_raises(self) -> None:
        with pytest.raises(ValueError, match="batch_size"):
            Append(batch_size=0)

    def test_transactional_rollback_on_failure(self, engine: Engine, metryka_table: str) -> None:
        # Drugi wiersz lamie NOT NULL na ``a1`` — caly batch ma byc cofniety.
        rows = [
            {"a1": "x", "a2": "y", "ilosc": 1, "data_od": T0},
            {"a1": None, "a2": "z", "ilosc": 2, "data_od": T0},
        ]
        with pytest.raises(IntegrityError):
            Append().write(engine, rows, metryka_table)
        assert _row_count(engine, metryka_table) == 0


class TestAppendViaAggWriter:
    def test_aggwriter_dispatches_append(self, tmp_path: Path) -> None:
        db_path = tmp_path / "agg_append.db"
        metadata = MetaData()
        Table(
            "metryka",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("a1", String, nullable=False),
            Column("ilosc", Integer, nullable=False),
            Column("data_od", DateTime, nullable=False),
            Column("data_do", DateTime, nullable=True),
        )
        backend = SQLiteBackend(path=str(db_path))
        metadata.create_all(backend.engine)
        backend.dispose()

        with AggWriter(
            backend="sqlite",
            strategy="append",
            backend_kwargs={"path": str(db_path)},
        ) as writer:
            result = writer.write(
                pd.DataFrame([{"a1": "x", "ilosc": 10, "data_od": T0}]),
                table="metryka",
            )
        assert result.inserted == 1
