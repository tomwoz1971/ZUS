from __future__ import annotations

import os

import pytest
from sqlalchemy import text

from zus_db_utils.backends import PostgresBackend
from zus_db_utils.credentials import Credential

POSTGRES_URL_DEFAULT = "postgresql+psycopg2://zus_test:zus_test@localhost/zus_test"


def _postgres_url() -> str:
    return os.environ.get("ZUS_TEST_POSTGRES_URL", POSTGRES_URL_DEFAULT)


def _skip_if_postgres_unavailable(backend: PostgresBackend) -> None:
    try:
        with backend.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        backend.dispose()
        pytest.skip(f"Postgres niedostepny: {exc}")


def test_from_url_engine_works() -> None:
    backend = PostgresBackend.from_url(_postgres_url())
    _skip_if_postgres_unavailable(backend)
    try:
        with backend.engine.connect() as conn:
            assert conn.execute(text("SELECT 1")).scalar() == 1
    finally:
        backend.dispose()


def test_from_credential_with_metadata_engine_works() -> None:
    cred = Credential(
        username="zus_test",
        password="zus_test",
        metadata={"host": "localhost", "port": 5432, "database": "zus_test"},
    )
    backend = PostgresBackend(credential=cred)
    _skip_if_postgres_unavailable(backend)
    try:
        with backend.engine.connect() as conn:
            assert conn.execute(text("SELECT 1")).scalar() == 1
    finally:
        backend.dispose()


def test_credential_kwargs_override_metadata() -> None:
    cred = Credential(
        username="zus_test",
        password="zus_test",
        metadata={"host": "wrong", "port": 9999, "database": "wrong"},
    )
    backend = PostgresBackend(
        credential=cred, host="localhost", port=5432, database="zus_test"
    )
    _skip_if_postgres_unavailable(backend)
    try:
        with backend.engine.connect() as conn:
            assert conn.execute(text("SELECT 1")).scalar() == 1
    finally:
        backend.dispose()


def test_missing_host_raises() -> None:
    cred = Credential(username="u", password="p", metadata={"database": "d"})
    with pytest.raises(ValueError, match="host"):
        PostgresBackend(credential=cred)


def test_missing_database_raises() -> None:
    cred = Credential(username="u", password="p", metadata={"host": "h"})
    with pytest.raises(ValueError, match="database"):
        PostgresBackend(credential=cred)


def test_supported_strategies_declared() -> None:
    assert "incremental_quantity" in PostgresBackend.supported_strategies


def test_name_is_postgres() -> None:
    assert PostgresBackend.name == "postgres"
