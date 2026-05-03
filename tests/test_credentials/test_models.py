from __future__ import annotations

import pytest
from pydantic import SecretStr, ValidationError

from zus_db_utils.credentials import Credential


def test_password_is_secretstr() -> None:
    cred = Credential(username="u", password="haslo")
    assert isinstance(cred.password, SecretStr)
    assert cred.password.get_secret_value() == "haslo"


def test_repr_masks_password() -> None:
    cred = Credential(username="u", password="haslo123", metadata={"host": "x"})
    rendered = repr(cred)
    assert "haslo123" not in rendered
    assert "***" in rendered
    assert "u" in rendered
    assert "host" in rendered


def test_str_masks_password() -> None:
    cred = Credential(username="u", password="haslo123")
    assert "haslo123" not in str(cred)


def test_model_dump_does_not_leak_password_by_default() -> None:
    cred = Credential(username="u", password="haslo123")
    dumped = cred.model_dump()
    assert "haslo123" not in str(dumped["password"])


def test_metadata_default_empty() -> None:
    cred = Credential(username="u", password="x")
    assert cred.metadata == {}


def test_frozen() -> None:
    cred = Credential(username="u", password="x")
    with pytest.raises(ValidationError):
        cred.username = "v"  # type: ignore[misc]
