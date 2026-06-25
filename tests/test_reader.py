from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from zus_db_utils import AggReader, AggWriter
from zus_db_utils.backends import SQLiteBackend

WARSAW = ZoneInfo("Europe/Warsaw")
T0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
T1 = datetime(2024, 1, 3, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def populated_db(tmp_path: Path) -> Iterator[str]:
    """Baza SQLite z tabela metryka i zapisanymi danymi przez AggWriter."""
    db_path = str(tmp_path / "reader.db")
    from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table

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
    backend = SQLiteBackend(path=db_path)
    metadata.create_all(backend.engine)
    backend.dispose()

    with AggWriter(
        backend="sqlite",
        strategy="incremental_quantity",
        backend_kwargs={"path": db_path},
        keys=["a1"],
    ) as writer:
        writer.write(pd.DataFrame([{"a1": "x", "ilosc": 10}]), table="metryka", as_of=T0)
        writer.write(pd.DataFrame([{"a1": "x", "ilosc": 30}]), table="metryka", as_of=T1)
        writer.write(pd.DataFrame([{"a1": "y", "ilosc": 5}]), table="metryka", as_of=T0)

    yield db_path


class TestConstruction:
    def test_empty_keys_raises(self) -> None:
        with pytest.raises(ValueError, match="keys"):
            AggReader(backend="sqlite", keys=[])

    def test_accepts_backend_instance(self, populated_db: str) -> None:
        backend = SQLiteBackend(path=populated_db)
        try:
            reader = AggReader(backend=backend, keys=["a1"])
            assert reader.backend is backend
        finally:
            backend.dispose()

    def test_context_manager_disposes(self, populated_db: str) -> None:
        with AggReader(
            backend="sqlite", keys=["a1"], backend_kwargs={"path": populated_db}
        ) as reader:
            assert reader.backend.name == "sqlite"


class TestReadCurrent:
    def test_returns_only_open_rows(self, populated_db: str) -> None:
        with AggReader(
            backend="sqlite", keys=["a1"], backend_kwargs={"path": populated_db}
        ) as reader:
            df = reader.read_current(table="metryka")
        assert len(df) == 2
        assert set(df.columns) == {"a1", "ilosc", "data_od"}
        assert df.set_index("a1").loc["x", "ilosc"] == 30
        assert df.set_index("a1").loc["y", "ilosc"] == 5

    def test_data_od_in_local_tz(self, populated_db: str) -> None:
        with AggReader(
            backend="sqlite", keys=["a1"], backend_kwargs={"path": populated_db}
        ) as reader:
            df = reader.read_current(table="metryka")
        ts = df["data_od"].iloc[0]
        assert ts.tz == WARSAW

    def test_filters_applied(self, populated_db: str) -> None:
        with AggReader(
            backend="sqlite", keys=["a1"], backend_kwargs={"path": populated_db}
        ) as reader:
            df = reader.read_current(table="metryka", filters={"a1": "x"})
        assert len(df) == 1
        assert df["a1"].iloc[0] == "x"

    def test_keys_override_per_call(self, populated_db: str) -> None:
        with AggReader(
            backend="sqlite", keys=["nieuzywane"], backend_kwargs={"path": populated_db}
        ) as reader:
            df = reader.read_current(table="metryka", keys=["a1"])
        assert len(df) == 2


class TestReadSnapshots:
    def test_daily_snapshots(self, populated_db: str) -> None:
        with AggReader(
            backend="sqlite", keys=["a1"], backend_kwargs={"path": populated_db}
        ) as reader:
            df = reader.read_snapshots(
                table="metryka",
                start=T0,
                end=T1,
                step="day",
            )
        assert set(df.columns) == {"a1", "ts", "ilosc"}
        assert len(df) > 0


class TestReadIncrements:
    def test_increments_first_step_nan(self, populated_db: str) -> None:
        with AggReader(
            backend="sqlite", keys=["a1"], backend_kwargs={"path": populated_db}
        ) as reader:
            df = reader.read_increments(
                table="metryka",
                start=T0,
                end=T1,
                step="day",
            )
        assert set(df.columns) == {"a1", "ts", "przyrost"}
        first_x = df[df["a1"] == "x"].sort_values("ts").iloc[0]
        assert pd.isna(first_x["przyrost"])
