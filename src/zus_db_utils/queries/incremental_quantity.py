from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Literal, Union
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import MetaData, Table, select, text
from sqlalchemy.engine import Engine

from zus_db_utils.exceptions import SchemaValidationError, UnsupportedStrategyError

Step = Union[Literal["hour", "day", "week"], timedelta]

LOCAL_TZ = ZoneInfo("Europe/Warsaw")


def read_current(
    engine: Engine,
    table: str,
    keys: list[str],
    *,
    quantity_col: str = "ilosc",
    valid_from_col: str = "data_od",
    valid_to_col: str = "data_do",
) -> pd.DataFrame:
    """Zwraca aktualnie obowiazujace ilosci (rekordy z ``valid_to IS NULL``).

    :param engine: silnik SQLAlchemy
    :param table: nazwa tabeli docelowej
    :param keys: kolumny klucza biznesowego
    :param quantity_col: kolumna pomiaru
    :param valid_from_col: kolumna poczatku waznosci
    :param valid_to_col: kolumna konca waznosci
    :returns: DataFrame z kolumnami ``keys + [quantity_col, valid_from_col]``;
        ``valid_from_col`` skonwertowany z UTC do ``Europe/Warsaw``
    :raises SchemaValidationError: gdy tabela nie ma wymaganych kolumn
    :raises UnsupportedStrategyError: gdy dialekt nie jest sqlite
    """
    _ensure_sqlite(engine)
    tbl = _reflect_and_validate(engine, table, keys, quantity_col, valid_from_col, valid_to_col)

    cols = [tbl.c[k] for k in keys] + [tbl.c[quantity_col], tbl.c[valid_from_col]]
    stmt = select(*cols).where(tbl.c[valid_to_col].is_(None))

    with engine.connect() as conn:
        df = pd.read_sql_query(stmt, conn)

    df[valid_from_col] = _utc_to_local(df[valid_from_col])
    return df


def read_snapshots(
    engine: Engine,
    table: str,
    keys: list[str],
    start: datetime,
    end: datetime,
    step: Step,
    *,
    quantity_col: str = "ilosc",
    valid_from_col: str = "data_od",
    valid_to_col: str = "data_do",
) -> pd.DataFrame:
    """Zwraca snapshoty wartosci na koniec kazdego kroku w ``[start, end]``.

    Dla danego ``ts`` aktywny jest rekord spelniajacy
    ``valid_from <= ts AND (valid_to IS NULL OR ts < valid_to)``.
    Klucze bez aktywnego rekordu w danym ``ts`` sa pomijane.

    :param engine: silnik SQLAlchemy (tylko SQLite na ten moment)
    :param table: nazwa tabeli
    :param keys: kolumny klucza biznesowego
    :param start: poczatek siatki czasu (UTC; naive lub aware)
    :param end: koniec siatki (wlacznie)
    :param step: ``"hour" | "day" | "week"`` lub ``timedelta``
    :returns: DataFrame z kolumnami ``keys + ["ts", quantity_col]``;
        ``ts`` w tz ``Europe/Warsaw``
    :raises SchemaValidationError: gdy tabela nie ma wymaganych kolumn
    :raises UnsupportedStrategyError: gdy dialekt nie jest sqlite
    :raises ValueError: gdy ``step`` jest niepoprawny
    """
    _ensure_sqlite(engine)
    _reflect_and_validate(engine, table, keys, quantity_col, valid_from_col, valid_to_col)

    start_iso = _to_iso_utc(start)
    end_iso = _to_iso_utc(end)
    modifier = _step_to_sqlite_modifier(step)

    key_select = ", ".join(f"t.{k}" for k in keys)
    key_order = ", ".join(f"t.{k}" for k in keys)

    sql = text(f"""
        WITH RECURSIVE grid(ts) AS (
            SELECT :start_dt
            UNION ALL
            SELECT datetime(ts, :step_mod) FROM grid
            WHERE datetime(ts, :step_mod) <= :end_dt
        )
        SELECT
            {key_select},
            g.ts AS ts,
            t.{quantity_col} AS {quantity_col}
        FROM grid g
        JOIN {table} t
          ON julianday(t.{valid_from_col}) <= julianday(g.ts)
         AND (t.{valid_to_col} IS NULL OR julianday(t.{valid_to_col}) > julianday(g.ts))
        ORDER BY {key_order}, g.ts
    """)

    with engine.connect() as conn:
        df = pd.read_sql_query(
            sql,
            conn,
            params={"start_dt": start_iso, "end_dt": end_iso, "step_mod": modifier},
        )

    df["ts"] = _utc_to_local(df["ts"])
    return df


def read_increments(
    engine: Engine,
    table: str,
    keys: list[str],
    start: datetime,
    end: datetime,
    step: Step,
    *,
    quantity_col: str = "ilosc",
    valid_from_col: str = "data_od",
    valid_to_col: str = "data_do",
) -> pd.DataFrame:
    """Zwraca przyrost ``ilosc`` wzgledem poprzedniego kroku per klucz.

    Implementacja: :func:`read_snapshots` + ``DataFrame.groupby(keys).diff()``.
    Pierwszy krok kazdego klucza ma ``przyrost = NaN`` (brak baseline'u).

    :returns: DataFrame z kolumnami ``keys + ["ts", "przyrost"]``
    """
    snaps = read_snapshots(
        engine,
        table,
        keys,
        start,
        end,
        step,
        quantity_col=quantity_col,
        valid_from_col=valid_from_col,
        valid_to_col=valid_to_col,
    )
    snaps = snaps.sort_values([*keys, "ts"]).reset_index(drop=True)
    snaps["przyrost"] = snaps.groupby(keys, sort=False)[quantity_col].diff()
    result = snaps.drop(columns=[quantity_col])
    assert isinstance(result, pd.DataFrame)
    return result


def _ensure_sqlite(engine: Engine) -> None:
    if engine.dialect.name != "sqlite":
        raise UnsupportedStrategyError(
            f"Procedury query wspieraja na razie tylko SQLite (jest: {engine.dialect.name})"
        )


def _reflect_and_validate(
    engine: Engine,
    table: str,
    keys: list[str],
    quantity_col: str,
    valid_from_col: str,
    valid_to_col: str,
) -> Table:
    metadata = MetaData()
    tbl = Table(table, metadata, autoload_with=engine)
    required = set(keys) | {quantity_col, valid_from_col, valid_to_col}
    actual = {c.name for c in tbl.columns}
    missing = required - actual
    if missing:
        raise SchemaValidationError(
            f"Tabela {tbl.name!r} nie ma wymaganych kolumn: {sorted(missing)}"
        )
    return tbl


def _step_to_sqlite_modifier(step: Step) -> str:
    if isinstance(step, timedelta):
        secs = int(step.total_seconds())
        if secs <= 0:
            raise ValueError("step (timedelta) musi byc dodatni")
        return f"+{secs} seconds"
    if step == "hour":
        return "+1 hour"
    if step == "day":
        return "+1 day"
    if step == "week":
        return "+7 days"
    raise ValueError(f"Nieobslugiwany step: {step!r}")


def _to_iso_utc(ts: datetime) -> str:
    if ts.tzinfo is not None:
        ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def _utc_to_local(s: pd.Series) -> pd.Series:
    converted = pd.to_datetime(s, utc=True)
    assert isinstance(converted, pd.Series)
    result = converted.dt.tz_convert(LOCAL_TZ)
    assert isinstance(result, pd.Series)
    return result
