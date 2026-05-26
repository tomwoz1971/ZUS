from __future__ import annotations

import logging

import pandas as pd
import pytest

from zus_db_utils import configure_file_logging
from zus_db_utils.strategies.incremental_quantity import IncrementalQuantity

T0 = __import__("datetime").datetime(2024, 1, 1, 12, 0, 0,
                                     tzinfo=__import__("datetime").timezone.utc)
T1 = __import__("datetime").datetime(2024, 1, 2, 12, 0, 0,
                                     tzinfo=__import__("datetime").timezone.utc)


@pytest.fixture(autouse=True)
def _clean_handlers():
    logger = logging.getLogger("zus_db_utils")
    before = list(logger.handlers)
    yield
    for h in logger.handlers[:]:
        if h not in before:
            logger.removeHandler(h)
            h.close()


def test_configure_file_logging_creates_file(tmp_path):
    log_file = tmp_path / "zus.log"
    handler = configure_file_logging(log_file)

    assert log_file.exists()
    assert isinstance(handler, logging.FileHandler)
    assert handler in logging.getLogger("zus_db_utils").handlers


def test_configure_file_logging_rotate(tmp_path):
    from logging.handlers import RotatingFileHandler

    log_file = tmp_path / "zus_rot.log"
    handler = configure_file_logging(log_file, rotate=True, max_bytes=1024, backup_count=2)

    assert isinstance(handler, RotatingFileHandler)


def test_write_emits_info_log(tmp_path, engine, metryka_table):
    log_file = tmp_path / "zus.log"
    configure_file_logging(log_file)

    strat = IncrementalQuantity(keys=["a1", "a2"])
    strat.write(
        engine,
        pd.DataFrame([{"a1": "x", "a2": "y", "ilosc": 10}]),
        metryka_table,
        as_of=T0,
    )

    content = log_file.read_text(encoding="utf-8")
    assert "write" in content
    assert f"table={metryka_table!r}" in content
    assert "inserted=1" in content
    assert "closed=0" in content
    assert "skipped=0" in content
    assert "elapsed=" in content


def test_write_log_contains_missing_closed(tmp_path, engine, metryka_table):
    log_file = tmp_path / "zus.log"
    configure_file_logging(log_file)

    strat_init = IncrementalQuantity(keys=["a1", "a2"])
    strat_init.write(
        engine,
        pd.DataFrame([{"a1": "x", "a2": "gone", "ilosc": 3}]),
        metryka_table,
        as_of=T0,
    )

    strat = IncrementalQuantity(keys=["a1", "a2"], close_missing="close_only")
    strat.write(
        engine,
        pd.DataFrame(columns=["a1", "a2", "ilosc"]),
        metryka_table,
        as_of=T1,
    )

    content = log_file.read_text(encoding="utf-8")
    assert "missing_closed=1" in content


def test_returned_handler_can_be_removed(tmp_path):
    log_file = tmp_path / "zus.log"
    handler = configure_file_logging(log_file)
    logger = logging.getLogger("zus_db_utils")

    logger.removeHandler(handler)
    handler.close()

    assert handler not in logger.handlers
