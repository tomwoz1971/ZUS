from __future__ import annotations

from pathlib import Path

from sqlalchemy import text

from zus_db_utils.backends import SQLiteBackend


def test_in_memory_engine_works() -> None:
    backend = SQLiteBackend()
    try:
        with backend.engine.connect() as conn:
            assert conn.execute(text("SELECT 1")).scalar() == 1
    finally:
        backend.dispose()


def test_file_path_engine_persists(tmp_path: Path) -> None:
    db_path = tmp_path / "x.db"
    backend = SQLiteBackend(path=str(db_path))
    try:
        with backend.engine.begin() as conn:
            conn.execute(text("CREATE TABLE t (a INTEGER)"))
            conn.execute(text("INSERT INTO t VALUES (1)"))
    finally:
        backend.dispose()
    assert db_path.exists()

    reopened = SQLiteBackend(path=str(db_path))
    try:
        with reopened.engine.connect() as conn:
            assert conn.execute(text("SELECT a FROM t")).scalar() == 1
    finally:
        reopened.dispose()


def test_supported_strategies_declared() -> None:
    backend = SQLiteBackend()
    try:
        assert backend.supports("incremental_quantity")
        assert not backend.supports("upsert")
    finally:
        backend.dispose()


def test_name_is_sqlite() -> None:
    assert SQLiteBackend.name == "sqlite"
