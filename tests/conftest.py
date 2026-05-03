from __future__ import annotations

import os
from collections.abc import Iterator

import pytest
from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool

POSTGRES_URL_DEFAULT = "postgresql+psycopg2://zus_test:zus_test@localhost/zus_test"
MSSQL_URL_DEFAULT = (
    "mssql+pyodbc://sa:ZusTest123!@localhost:1433/zus_test"
    "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
)


def _probe_engine(url: str, label: str) -> Engine:
    eng = create_engine(url)
    try:
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        eng.dispose()
        pytest.skip(f"{label} niedostepny ({url}): {exc}")
    return eng


@pytest.fixture(params=["sqlite", "postgresql", "mssql"])
def engine(request: pytest.FixtureRequest) -> Iterator[Engine]:
    if request.param == "sqlite":
        eng = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
    elif request.param == "postgresql":
        eng = _probe_engine(
            os.environ.get("ZUS_TEST_POSTGRES_URL", POSTGRES_URL_DEFAULT),
            "Postgres",
        )
    else:
        eng = _probe_engine(
            os.environ.get("ZUS_TEST_MSSQL_URL", MSSQL_URL_DEFAULT),
            "MSSQL",
        )
    try:
        yield eng
    finally:
        eng.dispose()


@pytest.fixture
def metryka_table(engine: Engine) -> Iterator[str]:
    metadata = MetaData()
    Table(
        "metryka",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("a1", String, nullable=False),
        Column("a2", String, nullable=False),
        Column("ilosc", Integer, nullable=False),
        Column("data_od", DateTime, nullable=False),
        Column("data_do", DateTime, nullable=True),
    )
    metadata.drop_all(engine)
    metadata.create_all(engine)
    try:
        yield "metryka"
    finally:
        metadata.drop_all(engine)
