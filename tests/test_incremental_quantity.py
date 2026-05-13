from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

from zus_db_utils.exceptions import SchemaValidationError
from zus_db_utils.queries.incremental_quantity import (
    read_current,
    read_increments,
    read_snapshots,
)
from zus_db_utils.strategies.incremental_quantity import IncrementalQuantity

WARSAW = ZoneInfo("Europe/Warsaw")
T0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
T1 = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
T2 = datetime(2024, 1, 3, 12, 0, 0, tzinfo=timezone.utc)


def _row_count(engine: Engine, table: str) -> int:
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one()
    return int(result)


class TestWrite:
    def test_first_write_inserts(self, engine: Engine, metryka_table: str) -> None:
        strat = IncrementalQuantity(keys=["a1", "a2"])
        df = pd.DataFrame([{"a1": "x", "a2": "y", "ilosc": 10}])

        result = strat.write(engine, df, metryka_table, as_of=T0)

        assert result.inserted == 1
        assert result.closed == 0
        assert result.skipped == 0
        assert _row_count(engine, metryka_table) == 1

    def test_same_value_is_skipped(self, engine: Engine, metryka_table: str) -> None:
        strat = IncrementalQuantity(keys=["a1", "a2"])
        df = pd.DataFrame([{"a1": "x", "a2": "y", "ilosc": 10}])
        strat.write(engine, df, metryka_table, as_of=T0)

        result = strat.write(engine, df, metryka_table, as_of=T1)

        assert result.inserted == 0
        assert result.closed == 0
        assert result.skipped == 1
        assert _row_count(engine, metryka_table) == 1

    def test_different_value_closes_and_inserts(
        self, engine: Engine, metryka_table: str
    ) -> None:
        strat = IncrementalQuantity(keys=["a1", "a2"])
        strat.write(
            engine,
            pd.DataFrame([{"a1": "x", "a2": "y", "ilosc": 10}]),
            metryka_table,
            as_of=T0,
        )

        result = strat.write(
            engine,
            pd.DataFrame([{"a1": "x", "a2": "y", "ilosc": 20}]),
            metryka_table,
            as_of=T1,
        )

        assert result.inserted == 1
        assert result.closed == 1
        assert result.skipped == 0
        assert _row_count(engine, metryka_table) == 2

        with engine.connect() as conn:
            rows = conn.execute(
                text(f"SELECT ilosc, data_od, data_do FROM {metryka_table} ORDER BY id")
            ).all()
        assert rows[0].ilosc == 10
        assert rows[0].data_do is not None
        assert rows[1].ilosc == 20
        assert rows[1].data_do is None

    def test_mixed_batch_with_multiple_keys(
        self, engine: Engine, metryka_table: str
    ) -> None:
        strat = IncrementalQuantity(keys=["a1", "a2"])
        strat.write(
            engine,
            pd.DataFrame(
                [
                    {"a1": "x", "a2": "y", "ilosc": 10},
                    {"a1": "x", "a2": "z", "ilosc": 50},
                ]
            ),
            metryka_table,
            as_of=T0,
        )

        result = strat.write(
            engine,
            pd.DataFrame(
                [
                    {"a1": "x", "a2": "y", "ilosc": 10},
                    {"a1": "x", "a2": "z", "ilosc": 60},
                    {"a1": "x", "a2": "new", "ilosc": 1},
                ]
            ),
            metryka_table,
            as_of=T1,
        )

        assert result.inserted == 2
        assert result.closed == 1
        assert result.skipped == 1
        assert _row_count(engine, metryka_table) == 4

    def test_dict_input_supported(self, engine: Engine, metryka_table: str) -> None:
        strat = IncrementalQuantity(keys=["a1", "a2"])
        result = strat.write(
            engine,
            {"a1": "x", "a2": "y", "ilosc": 10},
            metryka_table,
            as_of=T0,
        )
        assert result.inserted == 1

    def test_list_of_dicts_input_supported(
        self, engine: Engine, metryka_table: str
    ) -> None:
        strat = IncrementalQuantity(keys=["a1", "a2"])
        result = strat.write(
            engine,
            [{"a1": "x", "a2": "y", "ilosc": 10}],
            metryka_table,
            as_of=T0,
        )
        assert result.inserted == 1

    def test_duplicate_keys_in_input_fails(
        self, engine: Engine, metryka_table: str
    ) -> None:
        strat = IncrementalQuantity(keys=["a1", "a2"])
        df = pd.DataFrame(
            [
                {"a1": "x", "a2": "y", "ilosc": 10},
                {"a1": "x", "a2": "y", "ilosc": 20},
            ]
        )
        with pytest.raises(ValueError, match="Zduplikowane klucze"):
            strat.write(engine, df, metryka_table, as_of=T0)

    def test_missing_quantity_col_in_input_fails(
        self, engine: Engine, metryka_table: str
    ) -> None:
        strat = IncrementalQuantity(keys=["a1", "a2"])
        df = pd.DataFrame([{"a1": "x", "a2": "y"}])
        with pytest.raises(SchemaValidationError, match="Brak kolumn"):
            strat.write(engine, df, metryka_table, as_of=T0)

    def test_missing_table_column_fails(self, engine: Engine, metryka_table: str) -> None:
        strat = IncrementalQuantity(keys=["a1", "a2", "a3"])
        df = pd.DataFrame([{"a1": "x", "a2": "y", "a3": "z", "ilosc": 10}])
        with pytest.raises(SchemaValidationError, match="nie ma wymaganych kolumn"):
            strat.write(engine, df, metryka_table, as_of=T0)

    def test_tolerance_treats_close_values_as_equal(
        self, engine: Engine, metryka_table: str
    ) -> None:
        strat = IncrementalQuantity(keys=["a1", "a2"], tolerance=1)
        strat.write(
            engine,
            pd.DataFrame([{"a1": "x", "a2": "y", "ilosc": 10}]),
            metryka_table,
            as_of=T0,
        )

        result = strat.write(
            engine,
            pd.DataFrame([{"a1": "x", "a2": "y", "ilosc": 11}]),
            metryka_table,
            as_of=T1,
        )

        assert result.skipped == 1
        assert _row_count(engine, metryka_table) == 1

    def test_empty_keys_raises(self) -> None:
        with pytest.raises(ValueError, match="keys"):
            IncrementalQuantity(keys=[])


class TestReadCurrent:
    def test_returns_only_open_rows(self, engine: Engine, metryka_table: str) -> None:
        strat = IncrementalQuantity(keys=["a1", "a2"])
        strat.write(
            engine,
            pd.DataFrame([{"a1": "x", "a2": "y", "ilosc": 10}]),
            metryka_table,
            as_of=T0,
        )
        strat.write(
            engine,
            pd.DataFrame([{"a1": "x", "a2": "y", "ilosc": 20}]),
            metryka_table,
            as_of=T1,
        )
        strat.write(
            engine,
            pd.DataFrame([{"a1": "x", "a2": "z", "ilosc": 5}]),
            metryka_table,
            as_of=T0,
        )

        df = read_current(engine, metryka_table, keys=["a1", "a2"])

        assert len(df) == 2
        assert set(df.columns) == {"a1", "a2", "ilosc", "data_od"}
        assert df.set_index(["a1", "a2"]).loc[("x", "y"), "ilosc"] == 20
        assert df.set_index(["a1", "a2"]).loc[("x", "z"), "ilosc"] == 5

    def test_data_od_returned_in_local_tz(
        self, engine: Engine, metryka_table: str
    ) -> None:
        strat = IncrementalQuantity(keys=["a1", "a2"])
        strat.write(
            engine,
            pd.DataFrame([{"a1": "x", "a2": "y", "ilosc": 10}]),
            metryka_table,
            as_of=T0,
        )
        df = read_current(engine, metryka_table, keys=["a1", "a2"])
        ts = df["data_od"].iloc[0]
        assert ts.tz == WARSAW
        assert ts == T0.astimezone(WARSAW)


class TestReadSnapshots:
    def test_daily_snapshots_with_value_change(
        self, engine: Engine, metryka_table: str
    ) -> None:
        strat = IncrementalQuantity(keys=["a1", "a2"])
        strat.write(
            engine,
            pd.DataFrame([{"a1": "x", "a2": "y", "ilosc": 10}]),
            metryka_table,
            as_of=T0,
        )
        strat.write(
            engine,
            pd.DataFrame([{"a1": "x", "a2": "y", "ilosc": 30}]),
            metryka_table,
            as_of=T2,
        )

        df = read_snapshots(
            engine, metryka_table, keys=["a1", "a2"], start=T0, end=T2, step="day"
        )

        assert list(df.columns) == ["a1", "a2", "ts", "ilosc"]
        ilosc_by_ts = df.set_index("ts")["ilosc"].sort_index()
        assert list(ilosc_by_ts.values) == [10, 10, 30]

    def test_keys_without_record_in_step_are_skipped(
        self, engine: Engine, metryka_table: str
    ) -> None:
        strat = IncrementalQuantity(keys=["a1", "a2"])
        strat.write(
            engine,
            pd.DataFrame([{"a1": "x", "a2": "y", "ilosc": 10}]),
            metryka_table,
            as_of=T1,
        )
        df = read_snapshots(
            engine, metryka_table, keys=["a1", "a2"], start=T0, end=T2, step="day"
        )
        assert len(df) == 2

    def test_timedelta_step(self, engine: Engine, metryka_table: str) -> None:
        strat = IncrementalQuantity(keys=["a1", "a2"])
        strat.write(
            engine,
            pd.DataFrame([{"a1": "x", "a2": "y", "ilosc": 10}]),
            metryka_table,
            as_of=T0,
        )
        df = read_snapshots(
            engine,
            metryka_table,
            keys=["a1", "a2"],
            start=T0,
            end=T0 + timedelta(hours=12),
            step=timedelta(hours=6),
        )
        assert len(df) == 3


class TestFilters:
    def test_read_current_filter_by_equality(
        self, engine: Engine, metryka_table: str
    ) -> None:
        strat = IncrementalQuantity(keys=["a1", "a2"])
        strat.write(
            engine,
            pd.DataFrame([
                {"a1": "x", "a2": "y", "ilosc": 10},
                {"a1": "x", "a2": "z", "ilosc": 20},
            ]),
            metryka_table,
            as_of=T0,
        )

        df = read_current(engine, metryka_table, keys=["a1", "a2"], filters={"a2": "y"})

        assert len(df) == 1
        assert df.iloc[0]["a2"] == "y"

    def test_read_current_filter_by_in(
        self, engine: Engine, metryka_table: str
    ) -> None:
        strat = IncrementalQuantity(keys=["a1", "a2"])
        strat.write(
            engine,
            pd.DataFrame([
                {"a1": "x", "a2": "y", "ilosc": 10},
                {"a1": "x", "a2": "z", "ilosc": 20},
                {"a1": "x", "a2": "w", "ilosc": 30},
            ]),
            metryka_table,
            as_of=T0,
        )

        df = read_current(
            engine, metryka_table, keys=["a1", "a2"], filters={"a2": ["y", "z"]}
        )

        assert len(df) == 2
        assert set(df["a2"].tolist()) == {"y", "z"}

    def test_read_current_unknown_filter_col_raises(
        self, engine: Engine, metryka_table: str
    ) -> None:
        with pytest.raises(ValueError, match="Nieznane kolumny w filters"):
            read_current(
                engine, metryka_table, keys=["a1", "a2"], filters={"nieistnieje": 1}
            )

    def test_read_snapshots_filter_limits_keys(
        self, engine: Engine, metryka_table: str
    ) -> None:
        strat = IncrementalQuantity(keys=["a1", "a2"])
        strat.write(
            engine,
            pd.DataFrame([
                {"a1": "x", "a2": "y", "ilosc": 10},
                {"a1": "x", "a2": "z", "ilosc": 20},
            ]),
            metryka_table,
            as_of=T0,
        )

        df = read_snapshots(
            engine, metryka_table, keys=["a1", "a2"],
            start=T0, end=T1, step="day",
            filters={"a2": "y"},
        )

        assert all(df["a2"] == "y")

    def test_read_increments_filter_passes_through(
        self, engine: Engine, metryka_table: str
    ) -> None:
        strat = IncrementalQuantity(keys=["a1", "a2"])
        strat.write(
            engine,
            pd.DataFrame([
                {"a1": "x", "a2": "y", "ilosc": 10},
                {"a1": "x", "a2": "z", "ilosc": 50},
            ]),
            metryka_table,
            as_of=T0,
        )

        df = read_increments(
            engine, metryka_table, keys=["a1", "a2"],
            start=T0, end=T1, step="day",
            filters={"a2": "y"},
        )

        assert all(df["a2"] == "y")

    def test_filter_in_empty_list_raises(
        self, engine: Engine, metryka_table: str
    ) -> None:
        with pytest.raises(ValueError, match="pusta lista"):
            read_current(
                engine, metryka_table, keys=["a1", "a2"], filters={"a2": []}
            )


class TestReadIncrements:
    def test_first_step_is_nan_then_diffs(
        self, engine: Engine, metryka_table: str
    ) -> None:
        strat = IncrementalQuantity(keys=["a1", "a2"])
        strat.write(
            engine,
            pd.DataFrame([{"a1": "x", "a2": "y", "ilosc": 10}]),
            metryka_table,
            as_of=T0,
        )
        strat.write(
            engine,
            pd.DataFrame([{"a1": "x", "a2": "y", "ilosc": 30}]),
            metryka_table,
            as_of=T2,
        )

        df = read_increments(
            engine, metryka_table, keys=["a1", "a2"], start=T0, end=T2, step="day"
        )

        assert list(df.columns) == ["a1", "a2", "ts", "przyrost"]
        increments = df.sort_values("ts")["przyrost"].tolist()
        assert pd.isna(increments[0])
        assert increments[1] == 0
        assert increments[2] == 20
