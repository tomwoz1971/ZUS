from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from sqlalchemy import MetaData, Table, and_, or_, select
from sqlalchemy.engine import Connection, Engine

from zus_db_utils.exceptions import SchemaValidationError, UnsupportedStrategyError
from zus_db_utils.input_adapters import SupportedInput, normalize_input

SUPPORTED_DIALECTS = frozenset({"sqlite", "postgresql", "mssql"})


@dataclass(frozen=True)
class UpsertResult:
    """Podsumowanie operacji zapisu strategii ``upsert``.

    :param inserted: liczba nowo wstawionych wierszy
    :param updated: liczba zaktualizowanych wierszy
    """

    inserted: int
    updated: int


class Upsert:
    """Strategia zapisu INSERT-or-UPDATE na podstawie klucza biznesowego.

    Dla kazdego wiersza wejscia sprawdza czy wiersz o danym kluczu
    biznesowym juz istnieje w tabeli i:

    * jesli brak — wstawia nowy wiersz,
    * jesli istnieje — aktualizuje wszystkie kolumny niebelace kluczem.

    Kolumny wejscia nieobecne w tabeli sa ignorowane. Operacja wykonywana
    w jednej transakcji; blad dowolnego wiersza powoduje rollback calego
    ``write()``.

    :param keys: nazwy kolumn klucza biznesowego; musza istniec zarowno
        w wejsciu jak i w tabeli docelowej
    :param batch_size: maksymalny rozmiar batcha przy odczycie
        istniejacych kluczy z bazy (domyslnie 10 000)
    :raises ValueError: gdy ``keys`` puste
    """

    def __init__(
        self,
        keys: list[str],
        batch_size: int = 10_000,
    ) -> None:
        if not keys:
            raise ValueError("keys nie moze byc pusta lista")
        self.keys = list(keys)
        self.batch_size = batch_size

    def write(
        self,
        engine: Engine,
        data: SupportedInput,
        table: str,
    ) -> UpsertResult:
        """Zapisuje wejscie do tabeli zgodnie z opisanym algorytmem.

        :param engine: silnik SQLAlchemy do bazy docelowej
        :param data: DataFrame, dict lub list[dict]; wymagane kolumny to
            co najmniej ``keys``
        :param table: nazwa tabeli (musi istniec, walidacja schematu twarda)
        :returns: ``UpsertResult`` z licznikami operacji
        :raises UnsupportedStrategyError: gdy dialekt bazy nie jest wspierany
        :raises SchemaValidationError: gdy tabela lub wejscie nie maja
            wymaganych kolumn kluczowych
        """
        if engine.dialect.name not in SUPPORTED_DIALECTS:
            raise UnsupportedStrategyError(
                f"Strategia upsert wspiera: {sorted(SUPPORTED_DIALECTS)} "
                f"(jest: {engine.dialect.name})"
            )

        df = normalize_input(data)
        self._validate_dataframe(df)

        metadata = MetaData()
        tbl = Table(table, metadata, autoload_with=engine)
        self._validate_table(tbl)

        table_col_names = {c.name for c in tbl.columns}
        value_cols = [c for c in df.columns if c not in self.keys and c in table_col_names]
        write_cols = self.keys + value_cols

        inserted = 0
        updated = 0

        with engine.begin() as conn:
            records = df[write_cols].to_dict(orient="records")
            for batch_start in range(0, len(records), self.batch_size):
                batch = records[batch_start : batch_start + self.batch_size]
                batch_inserted, batch_updated = self._upsert_batch(
                    conn, tbl, batch, value_cols
                )
                inserted += batch_inserted
                updated += batch_updated

        return UpsertResult(inserted=inserted, updated=updated)

    def _upsert_batch(
        self,
        conn: Connection,
        tbl: Table,
        records: list[dict],
        value_cols: list[str],
    ) -> tuple[int, int]:
        existing_key_set = self._fetch_existing_keys(conn, tbl, records)

        to_insert = []
        to_update = []
        for rec in records:
            key_tuple = tuple(rec[k] for k in self.keys)
            if key_tuple in existing_key_set:
                to_update.append(rec)
            else:
                to_insert.append(rec)

        if to_insert:
            conn.execute(tbl.insert(), to_insert)

        for rec in to_update:
            if value_cols:
                key_filter = and_(*[tbl.c[k] == rec[k] for k in self.keys])
                conn.execute(
                    tbl.update()
                    .where(key_filter)
                    .values({col: rec[col] for col in value_cols if col in rec})
                )

        return len(to_insert), len(to_update)

    def _fetch_existing_keys(
        self,
        conn: Connection,
        tbl: Table,
        records: list[dict],
    ) -> set[tuple]:
        key_cols = [tbl.c[k] for k in self.keys]
        if len(self.keys) == 1:
            k = self.keys[0]
            stmt = select(*key_cols).where(tbl.c[k].in_([r[k] for r in records]))
        else:
            stmt = select(*key_cols).where(
                or_(*[and_(*[tbl.c[k] == r[k] for k in self.keys]) for r in records])
            )
        rows = conn.execute(stmt).fetchall()
        return {tuple(row) for row in rows}

    def _validate_dataframe(self, df: pd.DataFrame) -> None:
        missing = set(self.keys) - set(df.columns)
        if missing:
            raise SchemaValidationError(
                f"Brak kolumn kluczowych w wejsciu: {sorted(missing)}"
            )

    def _validate_table(self, tbl: Table) -> None:
        actual = {c.name for c in tbl.columns}
        missing = set(self.keys) - actual
        if missing:
            raise SchemaValidationError(
                f"Tabela {tbl.name!r} nie ma wymaganych kolumn kluczowych: {sorted(missing)}"
            )
