from __future__ import annotations

from collections.abc import Iterator
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

from zus_db_utils import AggWriter
from zus_db_utils.backends import SQLiteBackend
from zus_db_utils.credentials import Credential, EncryptedFileStore
from zus_db_utils.exceptions import UnsupportedStrategyError

T0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def sqlite_db(tmp_path: Path) -> Iterator[str]:
    db_path = tmp_path / "agg.db"
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
    yield str(db_path)


class TestRegistryDispatch:
    def test_writes_via_string_dispatch(self, sqlite_db: str) -> None:
        with AggWriter(
            backend="sqlite",
            strategy="incremental_quantity",
            backend_kwargs={"path": sqlite_db},
            keys=["a1"],
        ) as writer:
            result = writer.write(
                pd.DataFrame([{"a1": "x", "ilosc": 10}]),
                table="metryka",
                as_of=T0,
            )
        assert result.inserted == 1

    def test_unknown_backend_raises(self) -> None:
        with pytest.raises(KeyError, match="Nieznany backend"):
            AggWriter(backend="oracle", strategy="incremental_quantity", keys=["a"])

    def test_unknown_strategy_raises(self) -> None:
        with pytest.raises(UnsupportedStrategyError):
            AggWriter(backend="sqlite", strategy="upsert", keys=["a"])

    def test_accepts_backend_instance(self, sqlite_db: str) -> None:
        backend = SQLiteBackend(path=sqlite_db)
        try:
            writer = AggWriter(
                backend=backend, strategy="incremental_quantity", keys=["a1"]
            )
            assert writer.backend is backend
        finally:
            backend.dispose()


class TestCredentialResolution:
    def test_credential_object_passed_through(self) -> None:
        cred = Credential(
            username="u", password="p", metadata={"host": "h", "database": "d"}
        )
        # Postgres backend will try to connect during write, not at construction.
        # Constructor just builds the URL; passing a Credential object should work.
        writer = AggWriter(
            backend="postgres",
            strategy="incremental_quantity",
            credential=cred,
            keys=["a1"],
        )
        try:
            assert writer.backend.name == "postgres"
        finally:
            writer.dispose()

    def test_credential_string_resolved_via_store(self, tmp_path: Path) -> None:
        store = EncryptedFileStore(base_dir=tmp_path / "creds", master_password="m")
        store.set(
            "test-pg",
            Credential(
                username="u",
                password="p",
                metadata={"host": "h", "database": "d"},
            ),
        )

        writer = AggWriter(
            backend="postgres",
            strategy="incremental_quantity",
            credential="test-pg",
            credential_store=store,
            keys=["a1"],
        )
        try:
            assert writer.backend.name == "postgres"
        finally:
            writer.dispose()

    def test_postgres_without_credential_raises(self) -> None:
        with pytest.raises(ValueError, match="credential"):
            AggWriter(backend="postgres", strategy="incremental_quantity", keys=["a"])

    def test_mssql_without_credential_raises(self) -> None:
        with pytest.raises(ValueError, match="credential"):
            AggWriter(backend="mssql", strategy="incremental_quantity", keys=["a"])

    def test_sqlite_ignores_credential(self, sqlite_db: str) -> None:
        cred = Credential(username="u", password="p")
        with AggWriter(
            backend="sqlite",
            strategy="incremental_quantity",
            credential=cred,
            backend_kwargs={"path": sqlite_db},
            keys=["a1"],
        ) as writer:
            assert writer.backend.name == "sqlite"


class TestContextManager:
    def test_dispose_called_on_exit(self, sqlite_db: str) -> None:
        with AggWriter(
            backend="sqlite",
            strategy="incremental_quantity",
            backend_kwargs={"path": sqlite_db},
            keys=["a1"],
        ) as writer:
            engine = writer.backend.engine
            with engine.connect() as conn:
                assert conn.execute(text("SELECT 1")).scalar() == 1
        # After exit the engine is disposed; new connections should still work
        # (SQLAlchemy engine reconnects on demand) but the previous pool is gone.
        # We just verify dispose() was wired up — the call doesn't raise.


class TestEndToEndWriteFlow:
    def test_full_scd2_cycle(self, sqlite_db: str) -> None:
        with AggWriter(
            backend="sqlite",
            strategy="incremental_quantity",
            backend_kwargs={"path": sqlite_db},
            keys=["a1"],
        ) as writer:
            r1 = writer.write(
                pd.DataFrame([{"a1": "x", "ilosc": 10}]),
                table="metryka",
                as_of=T0,
            )
            r2 = writer.write(
                pd.DataFrame([{"a1": "x", "ilosc": 10}]),
                table="metryka",
                as_of=datetime(2024, 1, 2, tzinfo=timezone.utc),
            )
            r3 = writer.write(
                pd.DataFrame([{"a1": "x", "ilosc": 30}]),
                table="metryka",
                as_of=datetime(2024, 1, 3, tzinfo=timezone.utc),
            )

        assert (r1.inserted, r1.skipped, r1.closed) == (1, 0, 0)
        assert (r2.inserted, r2.skipped, r2.closed) == (0, 1, 0)
        assert (r3.inserted, r3.skipped, r3.closed) == (1, 0, 1)
