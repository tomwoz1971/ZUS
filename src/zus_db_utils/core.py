from __future__ import annotations

import logging
from datetime import datetime
from types import TracebackType
from typing import Any, Union

import pandas as pd
from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.sql.expression import Selectable

from zus_db_utils.backends import (
    Backend,
    MSSQLBackend,
    PostgresBackend,
    SQLiteBackend,
)
from zus_db_utils.credentials import Credential, CredentialStore
from zus_db_utils.exceptions import UnsupportedStrategyError
from zus_db_utils.queries import incremental_quantity as iq_queries
from zus_db_utils.queries.incremental_quantity import Step
from zus_db_utils.strategies.incremental_quantity import (
    IncrementalQuantity,
    WriteResult,
)
from zus_db_utils.strategies.upsert import Upsert, UpsertResult

_log = logging.getLogger(__name__)

BackendSpec = Union[str, Backend]
CredentialSpec = Union[str, Credential, None]
SqlSpec = Union[str, TextClause, Selectable]

BACKEND_REGISTRY: dict[str, type[Backend]] = {
    SQLiteBackend.name: SQLiteBackend,
    PostgresBackend.name: PostgresBackend,
    MSSQLBackend.name: MSSQLBackend,
}

STRATEGY_REGISTRY: dict[str, type[Any]] = {
    "incremental_quantity": IncrementalQuantity,
    "upsert": Upsert,
}


def resolve_credential(
    credential: CredentialSpec, store: CredentialStore | None
) -> Credential | None:
    """Rozwiazuje specyfikacje credentiala do obiektu ``Credential`` lub ``None``.

    :param credential: ``None``, gotowy obiekt :class:`Credential`, albo nazwa
        credentiala do pobrania ze stora
    :param store: stor uzywany przy stringowym ``credential``; gdy ``None``
        i potrzebny — uzywany jest :meth:`CredentialStore.auto`
    :returns: obiekt :class:`Credential` albo ``None``
    """
    if credential is None or isinstance(credential, Credential):
        return credential
    store = store or CredentialStore.auto()
    return store.get(credential)


def resolve_backend(
    backend: BackendSpec,
    credential: Credential | None,
    backend_kwargs: dict[str, Any],
) -> Backend:
    """Buduje instancje backendu z nazwy lub przepuszcza gotowa instancje.

    :param backend: nazwa zarejestrowanego backendu (``"sqlite"`` /
        ``"postgres"`` / ``"mssql"``) albo gotowa instancja :class:`Backend`
    :param credential: credential wymagany przez backendy sieciowe
        (Postgres/MSSQL); ignorowany dla SQLite
    :param backend_kwargs: dodatkowe kwargs przekazywane do konstruktora backendu
    :returns: instancja :class:`Backend`
    :raises KeyError: gdy nazwa backendu nie jest zarejestrowana
    :raises ValueError: gdy backend sieciowy nie dostal credentiala
    """
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
        resolved_credential = resolve_credential(credential, credential_store)
        self.backend = resolve_backend(
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


class AggReader:
    """Fasada do odczytu danych — analogiczna do :class:`AggWriter`.

    Tam, gdzie ``AggWriter`` zapisuje dane przez strategie, ``AggReader``
    udostepnia spojny interfejs odczytu (SELECT) ponad tym samym backendem
    i mechanizmem credentiali. Obecnie deleguje do procedur z modulu
    :mod:`zus_db_utils.queries.incremental_quantity`:

    * :meth:`read_current` — aktualnie obowiazujace ilosci,
    * :meth:`read_snapshots` — snapshoty na siatce czasu,
    * :meth:`read_increments` — przyrosty miedzy krokami.

    Dodatkowo :meth:`read_sql` pozwala wykonac **dowolne zapytanie SQL**
    (escape hatch) z bezpiecznym wiazaniem parametrow.

    Przyklad uzycia::

        reader = AggReader(
            backend="postgres",
            credential="postgres-dwh",
            keys=["operator_id", "date"],
        )
        df = reader.read_current(table="operator_workload")

    SQLite jako backend nie wymaga credentiala — przekazany ``credential``
    jest ignorowany; ``backend_kwargs`` mozna uzyc do podania ``path=``.

    Wspiera kontekst manager (``with AggReader(...) as reader: ...``) —
    automatycznie zamyka pulę połączeń backendu.

    :param backend: nazwa zarejestrowanego backendu (``"sqlite"`` /
        ``"postgres"`` / ``"mssql"``) albo gotowa instancja :class:`Backend`
    :param keys: domyslne kolumny klucza biznesowego uzywane przez metody
        odczytu; mozna nadpisac per wywolanie
    :param credential: nazwa credentiala w :class:`CredentialStore`,
        gotowy obiekt :class:`Credential`, albo ``None``
    :param credential_store: stor uzywany przy stringowym ``credential``;
        domyslnie ``CredentialStore.auto()``
    :param backend_kwargs: dodatkowe kwargs przekazywane do backendu
        (np. ``{"path": "/tmp/x.db"}`` dla SQLite, ``{"host": ...}``
        dla Postgres/MSSQL)
    :param quantity_col: domyslna nazwa kolumny pomiaru (domyslnie ``"ilosc"``)
    :param valid_from_col: domyslna nazwa kolumny poczatku waznosci
        (domyslnie ``"data_od"``)
    :param valid_to_col: domyslna nazwa kolumny konca waznosci
        (domyslnie ``"data_do"``)
    :raises KeyError: gdy nazwa backendu jest niezarejestrowana
    :raises ValueError: gdy ``keys`` puste lub backend sieciowy bez credentiala
    """

    def __init__(
        self,
        *,
        backend: BackendSpec,
        keys: list[str],
        credential: CredentialSpec = None,
        credential_store: CredentialStore | None = None,
        backend_kwargs: dict[str, Any] | None = None,
        quantity_col: str = "ilosc",
        valid_from_col: str = "data_od",
        valid_to_col: str = "data_do",
    ) -> None:
        if not keys:
            raise ValueError("keys nie moze byc pusta lista")

        resolved_credential = resolve_credential(credential, credential_store)
        self.backend = resolve_backend(
            backend, resolved_credential, backend_kwargs or {}
        )
        self.keys = list(keys)
        self.quantity_col = quantity_col
        self.valid_from_col = valid_from_col
        self.valid_to_col = valid_to_col

    def read_current(
        self,
        table: str,
        *,
        keys: list[str] | None = None,
        filters: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Zwraca aktualnie obowiazujace ilosci (rekordy z ``valid_to IS NULL``).

        Deleguje do :func:`zus_db_utils.queries.incremental_quantity.read_current`.

        :param table: nazwa tabeli docelowej
        :param keys: kolumny klucza biznesowego; gdy ``None`` uzywane sa
            domyslne ``keys`` przekazane do konstruktora
        :param filters: opcjonalne filtry ``{kolumna: wartosc}``; skalar → ``=``,
            lista/krotka → ``IN``
        :returns: DataFrame z kolumnami ``keys + [quantity_col, valid_from_col]``;
            ``valid_from_col`` skonwertowany z UTC do ``Europe/Warsaw``
        :raises SchemaValidationError: gdy tabela nie ma wymaganych kolumn
        :raises UnsupportedStrategyError: gdy dialekt nie jest wspierany
        :raises ValueError: gdy filters odwoluje sie do nieistniejacych kolumn
        """
        df = iq_queries.read_current(
            self.backend.engine,
            table,
            keys or self.keys,
            quantity_col=self.quantity_col,
            valid_from_col=self.valid_from_col,
            valid_to_col=self.valid_to_col,
            filters=filters,
        )
        _log.info("read_current table=%r rows=%d", table, len(df))
        return df

    def read_snapshots(
        self,
        table: str,
        start: datetime,
        end: datetime,
        step: Step,
        *,
        keys: list[str] | None = None,
        filters: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Zwraca snapshoty wartosci na koniec kazdego kroku w ``[start, end]``.

        Deleguje do :func:`zus_db_utils.queries.incremental_quantity.read_snapshots`.

        :param table: nazwa tabeli
        :param start: poczatek siatki czasu (UTC; naive lub aware)
        :param end: koniec siatki (wlacznie)
        :param step: ``"hour" | "day" | "week"`` lub ``timedelta``
        :param keys: kolumny klucza biznesowego; gdy ``None`` uzywane sa
            domyslne ``keys`` przekazane do konstruktora
        :param filters: opcjonalne filtry ``{kolumna: wartosc}``; skalar → ``=``,
            lista/krotka → ``IN``
        :returns: DataFrame z kolumnami ``keys + ["ts", quantity_col]``;
            ``ts`` w tz ``Europe/Warsaw``
        :raises SchemaValidationError: gdy tabela nie ma wymaganych kolumn
        :raises UnsupportedStrategyError: gdy dialekt nie jest wspierany
        :raises ValueError: gdy ``step`` jest niepoprawny lub filters odwoluje
            sie do nieistniejacych kolumn
        """
        df = iq_queries.read_snapshots(
            self.backend.engine,
            table,
            keys or self.keys,
            start,
            end,
            step,
            quantity_col=self.quantity_col,
            valid_from_col=self.valid_from_col,
            valid_to_col=self.valid_to_col,
            filters=filters,
        )
        _log.info(
            "read_snapshots table=%r start=%s end=%s step=%s rows=%d",
            table, start, end, step, len(df),
        )
        return df

    def read_increments(
        self,
        table: str,
        start: datetime,
        end: datetime,
        step: Step,
        *,
        keys: list[str] | None = None,
        filters: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Zwraca przyrost ``ilosc`` wzgledem poprzedniego kroku per klucz.

        Deleguje do :func:`zus_db_utils.queries.incremental_quantity.read_increments`.
        Pierwszy krok kazdego klucza ma ``przyrost = NaN`` (brak baseline'u).

        :param table: nazwa tabeli
        :param start: poczatek siatki czasu (UTC; naive lub aware)
        :param end: koniec siatki (wlacznie)
        :param step: ``"hour" | "day" | "week"`` lub ``timedelta``
        :param keys: kolumny klucza biznesowego; gdy ``None`` uzywane sa
            domyslne ``keys`` przekazane do konstruktora
        :param filters: opcjonalne filtry ``{kolumna: wartosc}``; skalar → ``=``,
            lista/krotka → ``IN``
        :returns: DataFrame z kolumnami ``keys + ["ts", "przyrost"]``
        :raises SchemaValidationError: gdy tabela nie ma wymaganych kolumn
        :raises UnsupportedStrategyError: gdy dialekt nie jest wspierany
        :raises ValueError: gdy ``step`` jest niepoprawny lub filters odwoluje
            sie do nieistniejacych kolumn
        """
        df = iq_queries.read_increments(
            self.backend.engine,
            table,
            keys or self.keys,
            start,
            end,
            step,
            quantity_col=self.quantity_col,
            valid_from_col=self.valid_from_col,
            valid_to_col=self.valid_to_col,
            filters=filters,
        )
        _log.info(
            "read_increments table=%r start=%s end=%s step=%s rows=%d",
            table, start, end, step, len(df),
        )
        return df

    def read_sql(
        self,
        sql: SqlSpec,
        *,
        params: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Wykonuje dowolne zapytanie SQL i zwraca wynik jako ``DataFrame``.

        Metoda „ucieczki" (escape hatch) dla przypadkow nieobslugiwanych przez
        wyspecjalizowane metody ``read_current`` / ``read_snapshots`` /
        ``read_increments`` — np. JOIN-y, agregacje, widoki, CTE czy zapytania
        specyficzne dla dialektu.

        Zapytanie wykonywane jest w trybie tylko-do-odczytu (zwykle
        ``connect()`` bez ``begin()``); zaleca sie uzywanie jej wylacznie
        do ``SELECT``. Aby zapis odbywal sie kontrolowanie, korzystaj z
        :class:`AggWriter` i strategii.

        .. warning::
            Aby uniknac SQL injection, **nigdy** nie sklejaj wartosci
            uzytkownika do stringa SQL. Uzywaj parametrow zwiazanych —
            w SQL podaj ``:nazwa`` i przekaz wartosci w ``params``::

                df = reader.read_sql(
                    "SELECT * FROM metryka WHERE a1 = :a1 AND ilosc > :minq",
                    params={"a1": "x", "minq": 100},
                )

        :param sql: zapytanie jako ``str`` (zostanie owiniete w
            :func:`sqlalchemy.text`), gotowy :class:`~sqlalchemy.sql.elements.TextClause`
            albo obiekt selectable SQLAlchemy (np. wynik ``select(...)``)
        :param params: opcjonalne parametry zwiazane dla zapytania
            (``{nazwa: wartosc}``); mapowane na ``:nazwa`` w SQL
        :returns: :class:`pandas.DataFrame` z wynikiem zapytania
            (kolumny dokladnie takie jak zwrocone przez baze; bez konwersji
            stref czasowych — w odroznieniu od ``read_*``)
        :raises sqlalchemy.exc.SQLAlchemyError: gdy zapytanie jest niepoprawne
            lub wystapi blad bazy
        """
        statement: SqlSpec = text(sql) if isinstance(sql, str) else sql
        with self.backend.engine.connect() as conn:
            df = pd.read_sql_query(statement, conn, params=params)
        _log.info("read_sql rows=%d", len(df))
        return df

    def dispose(self) -> None:
        """Zamyka pule polaczen backendu."""
        self.backend.dispose()

    def __enter__(self) -> AggReader:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.dispose()
