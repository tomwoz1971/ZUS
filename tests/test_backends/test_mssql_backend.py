from __future__ import annotations

import os

import pytest
from sqlalchemy import text

from zus_db_utils.backends import MSSQLBackend
from zus_db_utils.credentials import Credential
from zus_db_utils.exceptions import CredentialError

MSSQL_URL_DEFAULT = (
    "mssql+pyodbc://sa:ZusTest123!@localhost:1433/zus_test"
    "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
)


def _mssql_url() -> str:
    return os.environ.get("ZUS_TEST_MSSQL_URL", MSSQL_URL_DEFAULT)


def _skip_if_mssql_unavailable(backend: MSSQLBackend) -> None:
    try:
        with backend.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        backend.dispose()
        pytest.skip(f"MSSQL niedostepny: {exc}")


class TestValidation:
    def test_empty_password_rejected(self) -> None:
        cred = Credential(username="u", password="", metadata={"host": "h", "database": "d"})
        with pytest.raises(CredentialError, match="SQL Authentication"):
            MSSQLBackend(credential=cred)

    def test_trusted_connection_in_metadata_rejected(self) -> None:
        cred = Credential(
            username="u",
            password="p",
            metadata={"host": "h", "database": "d", "trusted_connection": "yes"},
        )
        with pytest.raises(CredentialError, match="trusted_connection"):
            MSSQLBackend(credential=cred)

    def test_trusted_connection_in_metadata_case_insensitive(self) -> None:
        cred = Credential(
            username="u",
            password="p",
            metadata={"host": "h", "database": "d", "Trusted_Connection": "yes"},
        )
        with pytest.raises(CredentialError, match="trusted_connection"):
            MSSQLBackend(credential=cred)

    def test_trusted_connection_in_url_rejected(self) -> None:
        url = (
            "mssql+pyodbc://localhost:1433/d"
            "?driver=ODBC+Driver+18+for+SQL+Server&Trusted_Connection=yes"
        )
        with pytest.raises(CredentialError, match="trusted_connection"):
            MSSQLBackend.from_url(url)

    def test_trusted_connection_with_spaces_in_url_rejected(self) -> None:
        url = "mssql+pyodbc:///?odbc_connect=DRIVER%3D%7B%7D%3BTrusted_Connection%3DYes%3B"
        with pytest.raises(CredentialError, match="trusted_connection"):
            MSSQLBackend.from_url(url)

    def test_missing_host_raises(self) -> None:
        cred = Credential(username="u", password="p", metadata={"database": "d"})
        with pytest.raises(ValueError, match="host"):
            MSSQLBackend(credential=cred)


class TestConstruction:
    def test_from_url_engine_works(self) -> None:
        backend = MSSQLBackend.from_url(_mssql_url())
        _skip_if_mssql_unavailable(backend)
        try:
            with backend.engine.connect() as conn:
                assert conn.execute(text("SELECT 1")).scalar() == 1
        finally:
            backend.dispose()

    def test_from_credential_engine_works(self) -> None:
        cred = Credential(
            username="sa",
            password="ZusTest123!",
            metadata={"host": "localhost", "port": 1433, "database": "zus_test"},
        )
        backend = MSSQLBackend(credential=cred)
        _skip_if_mssql_unavailable(backend)
        try:
            with backend.engine.connect() as conn:
                assert conn.execute(text("SELECT 1")).scalar() == 1
        finally:
            backend.dispose()


def test_supported_strategies_declared() -> None:
    assert "incremental_quantity" in MSSQLBackend.supported_strategies


def test_name_is_mssql() -> None:
    assert MSSQLBackend.name == "mssql"
