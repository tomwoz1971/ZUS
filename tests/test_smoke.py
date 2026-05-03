import zus_db_utils
from zus_db_utils.exceptions import (
    SchemaValidationError,
    UnsupportedStrategyError,
    ZusDbUtilsError,
)


def test_version_is_set() -> None:
    assert zus_db_utils.__version__ == "0.1.0"


def test_exception_hierarchy() -> None:
    assert issubclass(SchemaValidationError, ZusDbUtilsError)
    assert issubclass(UnsupportedStrategyError, ZusDbUtilsError)
