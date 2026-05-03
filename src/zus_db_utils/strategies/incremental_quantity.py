from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

import pandas as pd
from sqlalchemy import MetaData, Table, and_, select
from sqlalchemy.engine import Engine

from zus_db_utils.exceptions import SchemaValidationError
from zus_db_utils.input_adapters import SupportedInput, normalize_input


@dataclass(frozen=True)
class WriteResult:
    """Podsumowanie operacji zapisu strategii ``incremental_quantity``.

    :param inserted: liczba nowo wstawionych wierszy (otwartych)
    :param closed: liczba wierszy domknietych (ustawione ``valid_to``)
    :param skipped: liczba wierszy wejsciowych pominietych (rowna ``ilosc``)
    """

    inserted: int
    closed: int
    skipped: int


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
    ) -> None:
        if not keys:
            raise ValueError("keys nie moze byc pusta lista")
        self.keys = list(keys)
        self.quantity_col = quantity_col
        self.valid_from_col = valid_from_col
        self.valid_to_col = valid_to_col
        self.id_col = id_col
        self.tolerance: Decimal | float | None = tolerance

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
        """
        df = normalize_input(data)
        self._validate_dataframe(df)

        metadata = MetaData()
        tbl = Table(table, metadata, autoload_with=engine)
        self._validate_table(tbl)

        now = (as_of or datetime.now(timezone.utc)).replace(tzinfo=None)

        inserted = 0
        closed = 0
        skipped = 0

        with engine.begin() as conn:
            for record in df.to_dict(orient="records"):
                key_filter = and_(*[tbl.c[k] == record[k] for k in self.keys])
                existing = conn.execute(
                    select(tbl.c[self.id_col], tbl.c[self.quantity_col])
                    .where(key_filter)
                    .where(tbl.c[self.valid_to_col].is_(None))
                ).first()

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

        return WriteResult(inserted=inserted, closed=closed, skipped=skipped)

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
