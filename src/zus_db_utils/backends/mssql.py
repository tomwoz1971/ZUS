from __future__ import annotations

from typing import ClassVar
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from zus_db_utils.backends.base import Backend
from zus_db_utils.credentials.models import Credential
from zus_db_utils.exceptions import CredentialError

DEFAULT_PORT = 1433
DEFAULT_DRIVER = "ODBC Driver 18 for SQL Server"


class MSSQLBackend(Backend):
    """Backend dla MS SQL Server przez ``pyodbc``.

    Wymusza **SQL Authentication** (``UID`` + ``PWD``) — Windows
    Authentication / Integrated Security / Kerberos sa niewspierane,
    poniewaz modul musi dzialac z RHEL bez Kerberos/AD/keytaba.

    Walidacje:

    * ``credential.password`` nie moze byc puste,
    * ``credential.metadata`` nie moze zawierac klucza ``trusted_connection``
      (case-insensitive),
    * URL przekazany przez :meth:`from_url` nie moze zawierac
      podlancucha ``trusted_connection`` (case-insensitive,
      whitespace-stripped).

    :param credential: ``Credential`` z niepustym ``password``
        i metadata zawierajacym ``host``, opcjonalnie ``port``, ``database``
    :param host: nadpisuje ``credential.metadata['host']``
    :param port: nadpisuje ``credential.metadata['port']`` (domyslnie 1433)
    :param database: nadpisuje ``credential.metadata['database']``
    :param driver: nazwa sterownika ODBC (domyslnie ODBC Driver 18)
    :param trust_server_certificate: dodaje ``TrustServerCertificate=yes``
        do connection string (domyslnie ``True``, dla self-signed certow
        w dev)
    :raises CredentialError: gdy credential nie jest typu SQL Auth
        lub gdy zawiera ``trusted_connection``
    :raises ValueError: gdy brak ``host`` lub ``database``
    """

    name: ClassVar[str] = "mssql"
    supported_strategies: ClassVar[frozenset[str]] = frozenset({"incremental_quantity"})

    def __init__(
        self,
        credential: Credential,
        *,
        host: str | None = None,
        port: int | None = None,
        database: str | None = None,
        driver: str = DEFAULT_DRIVER,
        trust_server_certificate: bool = True,
    ) -> None:
        self._reject_trusted_connection_in_credential(credential)

        meta = credential.metadata
        resolved_host = host or meta.get("host")
        resolved_port = port or meta.get("port") or DEFAULT_PORT
        resolved_db = database or meta.get("database")
        if not resolved_host or not resolved_db:
            raise ValueError(
                "MSSQLBackend wymaga 'host' i 'database' w credential.metadata "
                "lub jako kwargs"
            )

        params = f"driver={quote_plus(driver)}"
        if trust_server_certificate:
            params += "&TrustServerCertificate=yes"

        url = (
            f"mssql+pyodbc://"
            f"{quote_plus(credential.username)}:"
            f"{quote_plus(credential.password.get_secret_value())}@"
            f"{resolved_host}:{resolved_port}/{resolved_db}?{params}"
        )
        self._engine = create_engine(url)

    @classmethod
    def from_url(cls, url: str) -> MSSQLBackend:
        """Tworzy backend z gotowego SQLAlchemy URL.

        Waliduje, ze URL nie zawiera ``trusted_connection``
        (case-insensitive, ignoruje whitespace).

        :raises CredentialError: gdy URL zawiera ``trusted_connection``
        """
        cls._reject_trusted_connection_in_url(url)
        instance = cls.__new__(cls)
        instance._engine = create_engine(url)
        return instance

    @property
    def engine(self) -> Engine:
        return self._engine

    @staticmethod
    def _reject_trusted_connection_in_credential(credential: Credential) -> None:
        if not credential.password.get_secret_value():
            raise CredentialError(
                "MSSQLBackend wymaga SQL Authentication (niepuste haslo) — "
                "Windows Authentication / Integrated Security nie jest wspierane"
            )
        meta_keys_lower = {str(k).lower() for k in credential.metadata}
        if "trusted_connection" in meta_keys_lower:
            raise CredentialError(
                "MSSQLBackend: 'trusted_connection' w credential.metadata "
                "jest niedozwolone — wymagamy SQL Authentication"
            )

    @staticmethod
    def _reject_trusted_connection_in_url(url: str) -> None:
        normalized = url.lower().replace(" ", "")
        if "trusted_connection" in normalized:
            raise CredentialError(
                "MSSQLBackend: URL zawiera 'trusted_connection' — "
                "Windows Authentication / Integrated Security nie jest wspierane"
            )
