class ZusDbUtilsError(Exception):
    """Bazowy wyjatek modulu."""


class SchemaValidationError(ZusDbUtilsError):
    """Schema tabeli docelowej nie pasuje do wymagan strategii."""


class UnsupportedStrategyError(ZusDbUtilsError):
    """Backend nie wspiera zadanej strategii."""


class CredentialError(ZusDbUtilsError):
    """Blad zarzadzania credentialami (zly master password, korupcja pliku, itd.)."""


class CredentialNotFoundError(CredentialError):
    """Brak credentiala o podanej nazwie w storze."""
