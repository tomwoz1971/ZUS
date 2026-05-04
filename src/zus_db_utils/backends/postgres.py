from __future__ import annotations

from typing import ClassVar
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from zus_db_utils.backends.base import Backend
from zus_db_utils.credentials.models import Credential

DEFAULT_PORT = 5432


class PostgresBackend(Backend):
    """Backend dla PostgreSQL przez ``psycopg2``.

    Connection string budowany z ``Credential`` + ``credential.metadata``
    (klucze ``host``, ``port``, ``database``). Alternatywnie cala
    konstrukcje mozna pominac uzywajac :meth:`from_url`.

    :param credential: ``Credential`` z ``username``, ``password``
        i ``metadata`` zawierajacym ``host``, opcjonalnie ``port``,
        ``database``
    :param host: nadpisuje ``credential.metadata['host']``
    :param port: nadpisuje ``credential.metadata['port']`` (domyslnie 5432)
    :param database: nadpisuje ``credential.metadata['database']``
    :raises ValueError: gdy brak ``host`` lub ``database``
    """

    name: ClassVar[str] = "postgres"
    supported_strategies: ClassVar[frozenset[str]] = frozenset({"incremental_quantity"})

    def __init__(
        self,
        credential: Credential,
        *,
        host: str | None = None,
        port: int | None = None,
        database: str | None = None,
    ) -> None:
        meta = credential.metadata
        resolved_host = host or meta.get("host")
        resolved_port = port or meta.get("port") or DEFAULT_PORT
        resolved_db = database or meta.get("database")
        if not resolved_host or not resolved_db:
            raise ValueError(
                "PostgresBackend wymaga 'host' i 'database' w credential.metadata "
                "lub jako kwargs"
            )

        url = (
            f"postgresql+psycopg2://"
            f"{quote_plus(credential.username)}:"
            f"{quote_plus(credential.password.get_secret_value())}@"
            f"{resolved_host}:{resolved_port}/{resolved_db}"
        )
        self._engine = create_engine(url)

    @classmethod
    def from_url(cls, url: str) -> PostgresBackend:
        """Tworzy backend z gotowego SQLAlchemy URL (omija walidacje credentiala)."""
        instance = cls.__new__(cls)
        instance._engine = create_engine(url)
        return instance

    @property
    def engine(self) -> Engine:
        return self._engine
