from __future__ import annotations

from typing import ClassVar

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool

from zus_db_utils.backends.base import Backend


class SQLiteBackend(Backend):
    """Backend dla SQLite — plik lokalny lub baza in-memory.

    :param path: sciezka do pliku ``.db``; ``None`` = baza in-memory
        ze wspolna pula (``StaticPool``), bezpieczna dla wielu polaczen
        w obrebie tego samego procesu (np. testy)
    """

    name: ClassVar[str] = "sqlite"
    supported_strategies: ClassVar[frozenset[str]] = frozenset({"incremental_quantity"})

    def __init__(self, path: str | None = None) -> None:
        if path is None:
            self._engine = create_engine(
                "sqlite://",
                connect_args={"check_same_thread": False},
                poolclass=StaticPool,
            )
        else:
            self._engine = create_engine(f"sqlite:///{path}")

    @property
    def engine(self) -> Engine:
        return self._engine
