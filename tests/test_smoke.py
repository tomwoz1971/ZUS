import zus_db_utils
from zus_db_utils.exceptions import (
    SchemaValidationError,
    UnsupportedStrategyError,
    ZusDbUtilsError,
)


def test_version_is_set() -> None:
    # Wersja musi byc ustawiona i miec format semver (X.Y.Z).
    parts = zus_db_utils.__version__.split(".")
    assert len(parts) == 3
    assert all(p.isdigit() for p in parts)


def test_exception_hierarchy() -> None:
    assert issubclass(SchemaValidationError, ZusDbUtilsError)
    assert issubclass(UnsupportedStrategyError, ZusDbUtilsError)
