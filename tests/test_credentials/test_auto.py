from __future__ import annotations

import pytest

from zus_db_utils.credentials import (
    CredentialStore,
    EncryptedFileStore,
    KeyringStore,
)


def test_auto_picks_keyring_on_windows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("platform.system", lambda: "Windows")
    store = CredentialStore.auto()
    assert isinstance(store, KeyringStore)


def test_auto_picks_encrypted_file_on_linux(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("platform.system", lambda: "Linux")
    store = CredentialStore.auto()
    assert isinstance(store, EncryptedFileStore)


def test_auto_picks_encrypted_file_on_darwin(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("platform.system", lambda: "Darwin")
    store = CredentialStore.auto()
    assert isinstance(store, EncryptedFileStore)
