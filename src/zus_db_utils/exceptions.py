class ZusDbUtilsError(Exception):
    """Bazowy wyjatek modulu."""


class SchemaValidationError(ZusDbUtilsError):
    """Schema tabeli docelowej nie pasuje do wymagan strategii."""


class UnsupportedStrategyError(ZusDbUtilsError):
    """Backend nie wspiera zadanej strategii."""
