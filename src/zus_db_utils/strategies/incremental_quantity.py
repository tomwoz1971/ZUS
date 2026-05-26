from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal

import pandas as pd
from sqlalchemy import MetaData, Table, and_, select
from sqlalchemy.engine import Engine

_log = logging.getLogger(__name__)

from zus_db_utils.exceptions import SchemaValidationError, UnsupportedStrategyError
from zus_db_utils.input_adapters import SupportedInput, normalize_input

SUPPORTED_DIALECTS = frozenset({"sqlite", "postgresql", "mssql"})
LOCKING_DIALECTS = frozenset({"postgresql", "mssql"})


CloseMissing = Literal["zero", "close_only"]


@dataclass(frozen=True)
class WriteResult:
    """Podsumowanie operacji zapisu strategii ``incremental_quantity``.

    :param inserted: liczba nowo wstawionych wierszy (otwartych)
    :param closed: liczba wierszy domknietych przez zmiane wartosci
    :param skipped: liczba wierszy wejsciowych pominietych (rowna ``ilosc``)
    :param missing_closed: liczba wierszy domknietych z powodu braku klucza
        w wejsciu (wynik ``close_missing``)
    """

    inserted: int
    closed: int
    skipped: int
    missing_closed: int = field(default=0)


class IncrementalQuantity:
    """Strategia zapisu typu SCD2 dla pojedynczego pomiaru liczbowego.

    Dla kazdego wiersza wejscia sprawdza istniejacy otwarty rekord
    (``valid_to IS NULL``) o tym samym kluczu biznesowym i:

    * jesli brak — wstawia nowy z ``valid_from = now``,
    * jesli istnieje i ``ilosc`` rowna — pomija wiersz,
    * jesli istnieje i ``ilosc`` rozna — zamyka stary
      (``valid_to = now``) i wstawia nowy.

    Wartosc ``now`` generowana po stronie klienta (UTC) i wspolna dla
    wszystkich wierszy w jednym ``write()``.

    :param keys: nazwy kolumn klucza biznesowego (kolejnosc bez znaczenia)
    :param quantity_col: kolumna pomiaru
    :param valid_from_col: kolumna poczatku waznosci (NOT NULL)
    :param valid_to_col: kolumna konca waznosci (NULL = otwarty)
    :param id_col: kolumna PK generowana przez baze
    :param tolerance: prog tolerancji porownania ``ilosc``;
        ``None`` = strict equality
    :param close_missing: co robic z otwartymi rekordami, ktorych kluczy
        nie ma w wejsciu:
        ``False`` — nic (domyslne),
        ``"close_only"`` — zamknij (ustaw ``valid_to = now``),
        ``"zero"`` — zamknij i wstaw nowy rekord z ``ilosc = 0``
    :raises ValueError: gdy ``keys`` puste
    """

    def __init__(
        self,
        keys: list[str],
        quantity_col: str = "ilosc",
        valid_from_col: str = "data_od",
        valid_to_col: str = "data_do",
        id_col: str = "id",
        tolerance: Decimal | float | None = None,
        close_missing: CloseMissing | Literal[False] = False,
    ) -> None:
        if not keys:
            raise ValueError("keys nie moze byc pusta lista")
        self.keys = list(keys)
        self.quantity_col = quantity_col
        self.valid_from_col = valid_from_col
        self.valid_to_col = valid_to_col
        self.id_col = id_col
        self.tolerance: Decimal | float | None = tolerance
        self.close_missing = close_missing

    def write(
        self,
        engine: Engine,
        data: SupportedInput,
        table: str,
        *,
        as_of: datetime | None = None,
    ) -> WriteResult:
        """Zapisuje wejscie do tabeli zgodnie z opisanym algorytmem.

        :param engine: silnik SQLAlchemy do bazy docelowej
        :param data: DataFrame, dict lub list[dict]; wymagane kolumny =
            ``keys + [quantity_col]``
        :param table: nazwa tabeli (musi istniec, schema waliduje sie twardo)
        :param as_of: nadpisanie ``now``; domyslnie ``datetime.now(timezone.utc)``
        :returns: ``WriteResult`` z licznikami operacji
        :raises SchemaValidationError: gdy tabela nie ma wymaganych kolumn
            lub w wejsciu brakuje ``keys``/``quantity_col``
        :raises ValueError: gdy w wejsciu sa zduplikowane klucze biznesowe
        :raises UnsupportedStrategyError: gdy dialekt bazy nie jest wspierany
        """
        _t0 = time.perf_counter()

        if engine.dialect.name not in SUPPORTED_DIALECTS:
            raise UnsupportedStrategyError(
                f"Strategia incremental_quantity wspiera: {sorted(SUPPORTED_DIALECTS)} "
                f"(jest: {engine.dialect.name})"
            )

        df = normalize_input(data)
        self._validate_dataframe(df)

        metadata = MetaData()
        tbl = Table(table, metadata, autoload_with=engine)
        self._validate_table(tbl)

        now = (as_of or datetime.now(timezone.utc)).replace(tzinfo=None)
        use_row_lock = engine.dialect.name in LOCKING_DIALECTS

        inserted = 0
        closed = 0
        skipped = 0
        missing_closed = 0

        records = df.to_dict(orient="records")
        input_key_tuples = frozenset(
            tuple(r[k] for k in self.keys) for r in records
        )

        with engine.begin() as conn:
            for record in records:
                key_filter = and_(*[tbl.c[k] == record[k] for k in self.keys])
                stmt = (
                    select(tbl.c[self.id_col], tbl.c[self.quantity_col])
                    .where(key_filter)
                    .where(tbl.c[self.valid_to_col].is_(None))
                )
                if use_row_lock:
                    stmt = stmt.with_for_update()
                existing = conn.execute(stmt).first()

                new_qty = record[self.quantity_col]

                if existing is None:
                    conn.execute(
                        tbl.insert().values(
                            **{k: record[k] for k in self.keys},
                            **{
                                self.quantity_col: new_qty,
                                self.valid_from_col: now,
                                self.valid_to_col: None,
                            },
                        )
                    )
                    inserted += 1
                    continue

                if self._values_equal(existing[1], new_qty):
                    skipped += 1
                    continue

                conn.execute(
                    tbl.update()
                    .where(tbl.c[self.id_col] == existing[0])
                    .values({self.valid_to_col: now})
                )
                conn.execute(
                    tbl.insert().values(
                        **{k: record[k] for k in self.keys},
                        **{
                            self.quantity_col: new_qty,
                            self.valid_from_col: now,
                            self.valid_to_col: None,
                        },
                    )
                )
                closed += 1
                inserted += 1

            if self.close_missing:
                open_stmt = select(
                    *[tbl.c[k] for k in self.keys],
                    tbl.c[self.id_col],
                ).where(tbl.c[self.valid_to_col].is_(None))
                if use_row_lock:
                    open_stmt = open_stmt.with_for_update()
                open_rows = conn.execute(open_stmt).all()

                for row in open_rows:
                    m = row._mapping
                    row_key = tuple(m[k] for k in self.keys)
                    if row_key in input_key_tuples:
                        continue
                    conn.execute(
                        tbl.update()
                        .where(tbl.c[self.id_col] == m[self.id_col])
                        .values({self.valid_to_col: now})
                    )
                    missing_closed += 1
                    if self.close_missing == "zero":
                        conn.execute(
                            tbl.insert().values(
                                **{k: m[k] for k in self.keys},
                                **{
                                    self.quantity_col: 0,
                                    self.valid_from_col: now,
                                    self.valid_to_col: None,
                                },
                            )
                        )
                        inserted += 1

        result = WriteResult(
            inserted=inserted,
            closed=closed,
            skipped=skipped,
            missing_closed=missing_closed,
        )
        _log.info(
            "write table=%r inserted=%d closed=%d skipped=%d missing_closed=%d elapsed=%.3fs",
            table,
            result.inserted,
            result.closed,
            result.skipped,
            result.missing_closed,
            time.perf_counter() - _t0,
        )
        return result

    def _validate_dataframe(self, df: pd.DataFrame) -> None:
        required = set(self.keys) | {self.quantity_col}
        missing = required - set(df.columns)
        if missing:
            raise SchemaValidationError(
                f"Brak kolumn w wejsciu: {sorted(missing)}"
            )
        if df.duplicated(subset=self.keys).any():
            dupes = df[df.duplicated(subset=self.keys, keep=False)][self.keys]
            raise ValueError(
                f"Zduplikowane klucze biznesowe w wejsciu:\n{dupes.to_string(index=False)}"
            )

    def _validate_table(self, tbl: Table) -> None:
        required = (
            set(self.keys)
            | {self.quantity_col, self.valid_from_col, self.valid_to_col, self.id_col}
        )
        actual = {c.name for c in tbl.columns}
        missing = required - actual
        if missing:
            raise SchemaValidationError(
                f"Tabela {tbl.name!r} nie ma wymaganych kolumn: {sorted(missing)}"
            )

    def _values_equal(self, old: object, new: object) -> bool:
        if self.tolerance is None:
            return old == new
        if old is None or new is None:
            return old is new
        return abs(Decimal(str(old)) - Decimal(str(new))) <= Decimal(str(self.tolerance))
