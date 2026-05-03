from __future__ import annotations

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
)
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    try:
        yield eng
    finally:
        eng.dispose()


@pytest.fixture
def metryka_table(engine: Engine) -> str:
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
    metadata.create_all(engine)
    return "metryka"
