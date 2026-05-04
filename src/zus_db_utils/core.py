from __future__ import annotations

from types import TracebackType
from typing import Any, Union

from zus_db_utils.backends import (
    Backend,
    MSSQLBackend,
    PostgresBackend,
    SQLiteBackend,
)
from zus_db_utils.credentials import Credential, CredentialStore
from zus_db_utils.exceptions import UnsupportedStrategyError
from zus_db_utils.strategies.incremental_quantity import (
    IncrementalQuantity,
    WriteResult,
)

BackendSpec = Union[str, Backend]
CredentialSpec = Union[str, Credential, None]

BACKEND_REGISTRY: dict[str, type[Backend]] = {
    SQLiteBackend.name: SQLiteBackend,
    PostgresBackend.name: PostgresBackend,
    MSSQLBackend.name: MSSQLBackend,
}

STRATEGY_REGISTRY: dict[str, type[Any]] = {
    "incremental_quantity": IncrementalQuantity,
}


class AggWriter:
    """Fasada laczaca backend, strategie i (opcjonalnie) credentiala.

    Przyklad uzycia z CLAUDE.md::

        writer = AggWriter(
            backend="postgres",
            strategy="incremental_quantity",
            credential="postgres-dwh",
            keys=["operator_id", "date"],
        )
        writer.write(df, table="operator_workload")

    SQLite jako backend nie wymaga credentiala — przekazany ``credential``
    jest ignorowany; ``backend_kwargs`` mozna uzyc do podania ``path=``.

    Wspiera kontekst manager (``with AggWriter(...) as writer: ...``) —
    automatycznie zamyka pulę połączeń backendu.

    :param backend: nazwa zarejestrowanego backendu (``"sqlite"`` /
        ``"postgres"`` / ``"mssql"``) albo gotowa instancja ``Backend``
    :param strategy: nazwa zarejestrowanej strategii (np.
        ``"incremental_quantity"``)
    :param credential: nazwa credentiala w :class:`CredentialStore`,
        gotowy obiekt :class:`Credential`, albo ``None``
    :param credential_store: stor uzywany przy stringowym ``credential``;
        domyslnie ``CredentialStore.auto()``
    :param backend_kwargs: dodatkowe kwargs przekazywane do backendu
        (np. ``{"path": "/tmp/x.db"}`` dla SQLite, ``{"host": ...}``
        dla Postgres/MSSQL)
    :param strategy_kwargs: kwargs do konstruktora strategii (np.
        ``keys=...`` dla ``incremental_quantity``)
    :raises KeyError: gdy nazwa backendu lub strategii niezarejestrowana
    :raises UnsupportedStrategyError: gdy backend nie wspiera strategii
    """

    def __init__(
        self,
        *,
        backend: BackendSpec,
        strategy: str,
        credential: CredentialSpec = None,
        credential_store: CredentialStore | None = None,
        backend_kwargs: dict[str, Any] | None = None,
        **strategy_kwargs: Any,
    ) -> None:
        resolved_credential = self._resolve_credential(credential, credential_store)
        self.backend = self._resolve_backend(
            backend, resolved_credential, backend_kwargs or {}
        )

        if not self.backend.supports(strategy):
            raise UnsupportedStrategyError(
                f"Backend {self.backend.name!r} nie wspiera strategii {strategy!r}; "
                f"wspierane: {sorted(self.backend.supported_strategies)}"
            )

        if strategy not in STRATEGY_REGISTRY:
            raise KeyError(
                f"Nieznana strategia {strategy!r}; "
                f"zarejestrowane: {sorted(STRATEGY_REGISTRY)}"
            )
        self.strategy = STRATEGY_REGISTRY[strategy](**strategy_kwargs)
        self._strategy_name = strategy

    def write(self, data: Any, table: str, **kwargs: Any) -> WriteResult:
        """Wywołuje ``strategy.write(backend.engine, data, table, **kwargs)``.

        Pozostale kwargs (np. ``as_of``) przekazywane bez zmian
        do strategii.
        """
        return self.strategy.write(self.backend.engine, data, table, **kwargs)  # type: ignore[no-any-return]

    def dispose(self) -> None:
        """Zamyka pule polaczen backendu."""
        self.backend.dispose()

    def __enter__(self) -> AggWriter:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.dispose()

    @staticmethod
    def _resolve_credential(
        credential: CredentialSpec, store: CredentialStore | None
    ) -> Credential | None:
        if credential is None or isinstance(credential, Credential):
            return credential
        store = store or CredentialStore.auto()
        return store.get(credential)

    @staticmethod
    def _resolve_backend(
        backend: BackendSpec,
        credential: Credential | None,
        backend_kwargs: dict[str, Any],
    ) -> Backend:
        if isinstance(backend, Backend):
            return backend
        if backend not in BACKEND_REGISTRY:
            raise KeyError(
                f"Nieznany backend {backend!r}; "
                f"zarejestrowane: {sorted(BACKEND_REGISTRY)}"
            )
        cls = BACKEND_REGISTRY[backend]
        if cls is SQLiteBackend:
            return cls(**backend_kwargs)
        if credential is None:
            raise ValueError(
                f"Backend {backend!r} wymaga credentiala (string lub obiekt Credential)"
            )
        if cls is PostgresBackend:
            return PostgresBackend(credential=credential, **backend_kwargs)
        if cls is MSSQLBackend:
            return MSSQLBackend(credential=credential, **backend_kwargs)
        raise KeyError(f"Brak fabryki dla backendu {backend!r}")
