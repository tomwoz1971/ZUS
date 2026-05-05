from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from sqlalchemy import MetaData, Table
from sqlalchemy.engine import Engine

from zus_db_utils.exceptions import SchemaValidationError
from zus_db_utils.input_adapters import SupportedInput, normalize_input

DEFAULT_BATCH_SIZE = 10_000


@dataclass(frozen=True)
class AppendResult:
    """Podsumowanie operacji ``append``.

    :param inserted: laczna liczba wstawionych wierszy
    """

    inserted: int


class Append:
    """Strategia ``append`` — INSERT bez deduplikacji.

    Wstawia wszystkie wiersze z wejscia do tabeli docelowej. Brak
    sprawdzania kluczy, brak upsertu, brak SCD2 — czysty INSERT
    w jednej transakcji, w batchach.

    Wszystkie kolumny wejscia musza istniec w tabeli; nadmiarowe
    kolumny powoduja :class:`SchemaValidationError`. Brakujace kolumny
    w wejsciu (wzgledem tabeli) zostaja wypelnione DEFAULT/NULL przez
    baze — ewentualne naruszenie ``NOT NULL`` zglasza baza.

    :param batch_size: rozmiar batcha dla ``executemany`` (domyslnie 10_000)
    :raises ValueError: gdy ``batch_size`` <= 0
    """

    def __init__(self, *, batch_size: int = DEFAULT_BATCH_SIZE) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size musi byc dodatni")
        self.batch_size = batch_size

    def write(
        self,
        engine: Engine,
        data: SupportedInput,
        table: str,
    ) -> AppendResult:
        """Wstawia wszystkie wiersze ``data`` do ``table`` w jednej transakcji.

        :param engine: silnik SQLAlchemy do bazy docelowej
        :param data: DataFrame, dict lub list[dict]
        :param table: nazwa istniejacej tabeli
        :returns: ``AppendResult`` z liczba wstawionych wierszy
        :raises SchemaValidationError: gdy wejscie zawiera kolumny
            spoza tabeli
        """
        df = normalize_input(data)
        if df.empty:
            return AppendResult(inserted=0)

        metadata = MetaData()
        tbl = Table(table, metadata, autoload_with=engine)
        self._validate_columns(df.columns, tbl)

        records: list[dict[str, Any]] = [
            {str(k): v for k, v in row.items()} for row in df.to_dict(orient="records")
        ]
        inserted = 0
        with engine.begin() as conn:
            for start in range(0, len(records), self.batch_size):
                batch = records[start : start + self.batch_size]
                conn.execute(tbl.insert(), batch)
                inserted += len(batch)

        return AppendResult(inserted=inserted)

    @staticmethod
    def _validate_columns(input_cols: Iterable[Any], tbl: Table) -> None:
        actual = {c.name for c in tbl.columns}
        extra = {str(c) for c in input_cols} - actual
        if extra:
            raise SchemaValidationError(
                f"Wejscie zawiera kolumny spoza tabeli {tbl.name!r}: {sorted(extra)}"
            )
