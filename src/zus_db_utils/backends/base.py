from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar

from sqlalchemy.engine import Engine


class Backend(ABC):
    """Abstrakcyjny backend — zna sie na laczeniu i deklaruje wspierane strategie.

    Konkretne backendy:

    * :class:`zus_db_utils.backends.sqlite.SQLiteBackend`
    * :class:`zus_db_utils.backends.postgres.PostgresBackend`
    * :class:`zus_db_utils.backends.mssql.MSSQLBackend`

    :cvar name: krotki, lowercase identyfikator (np. ``"postgres"``)
    :cvar supported_strategies: zbior nazw strategii wspieranych przez backend
    """

    name: ClassVar[str]
    supported_strategies: ClassVar[frozenset[str]]

    @property
    @abstractmethod
    def engine(self) -> Engine:
        """Silnik SQLAlchemy do uzycia przez strategie i query."""

    def supports(self, strategy: str) -> bool:
        """Sprawdza czy backend wspiera strategie o podanej nazwie."""
        return strategy in self.supported_strategies

    def dispose(self) -> None:
        """Zamyka pulę połączeń (delegowane do ``Engine.dispose``)."""
        self.engine.dispose()
