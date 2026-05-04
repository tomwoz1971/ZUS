from __future__ import annotations

from collections.abc import Iterator

import keyring
import pytest
from keyring.backend import KeyringBackend
from keyring.errors import PasswordDeleteError

from zus_db_utils.credentials import Credential, KeyringStore
from zus_db_utils.exceptions import CredentialNotFoundError


class _MemoryKeyring(KeyringBackend):
    priority = 1.0  # type: ignore[assignment]

    def __init__(self) -> None:
        self._store: dict[tuple[str, str], str] = {}

    def get_password(self, service: str, username: str) -> str | None:
        return self._store.get((service, username))

    def set_password(self, service: str, username: str, password: str) -> None:
        self._store[(service, username)] = password

    def delete_password(self, service: str, username: str) -> None:
        if (service, username) not in self._store:
            raise PasswordDeleteError("not found")
        del self._store[(service, username)]


@pytest.fixture
def memory_keyring() -> Iterator[_MemoryKeyring]:
    backup = keyring.get_keyring()
    backend = _MemoryKeyring()
    keyring.set_keyring(backend)
    try:
        yield backend
    finally:
        keyring.set_keyring(backup)


@pytest.fixture
def store(memory_keyring: _MemoryKeyring) -> KeyringStore:
    return KeyringStore()


def test_set_and_get_roundtrip(store: KeyringStore) -> None:
    cred = Credential(username="u", password="p", metadata={"host": "h"})
    store.set("a", cred)
    loaded = store.get("a")
    assert loaded.username == "u"
    assert loaded.password.get_secret_value() == "p"
    assert loaded.metadata == {"host": "h"}


def test_get_missing_raises(store: KeyringStore) -> None:
    with pytest.raises(CredentialNotFoundError):
        store.get("nope")


def test_list_entries_tracks_added_names(store: KeyringStore) -> None:
    store.set("b", Credential(username="u", password="p"))
    store.set("a", Credential(username="u", password="p"))
    assert store.list_entries() == ["a", "b"]


def test_delete_removes_from_index(store: KeyringStore) -> None:
    store.set("a", Credential(username="u", password="p"))
    store.delete("a")
    assert store.list_entries() == []
    with pytest.raises(CredentialNotFoundError):
        store.get("a")


def test_delete_missing_raises(store: KeyringStore) -> None:
    with pytest.raises(CredentialNotFoundError):
        store.delete("nope")


def test_rotate_changes_password(store: KeyringStore) -> None:
    store.set("a", Credential(username="u", password="old", metadata={"k": "v"}))
    store.rotate("a", "new")
    cred = store.get("a")
    assert cred.password.get_secret_value() == "new"
    assert cred.metadata == {"k": "v"}


def test_index_key_is_reserved(store: KeyringStore) -> None:
    with pytest.raises(ValueError, match="zarezerwowana"):
        store.set("__zus_db_utils_index__", Credential(username="u", password="p"))


def test_overwrite_does_not_duplicate_in_list(store: KeyringStore) -> None:
    store.set("a", Credential(username="u", password="p1"))
    store.set("a", Credential(username="u", password="p2"))
    assert store.list_entries() == ["a"]
    assert store.get("a").password.get_secret_value() == "p2"


class TestExplicitBackendInjection:
    def test_explicit_backend_is_used_for_set_and_get(self) -> None:
        explicit = _MemoryKeyring()
        store = KeyringStore(backend=explicit)
        store.set("a", Credential(username="u", password="p"))

        assert store.get("a").username == "u"
        assert explicit.get_password("zus_db_utils", "a") is not None

    def test_explicit_backend_isolated_from_global_keyring(self) -> None:
        explicit = _MemoryKeyring()
        store = KeyringStore(backend=explicit)
        store.set("a", Credential(username="u", password="p"))

        global_backend = _MemoryKeyring()
        backup = keyring.get_keyring()
        keyring.set_keyring(global_backend)
        try:
            assert global_backend.get_password("zus_db_utils", "a") is None
            assert store.get("a").username == "u"
        finally:
            keyring.set_keyring(backup)

    def test_explicit_backend_full_roundtrip(self) -> None:
        explicit = _MemoryKeyring()
        store = KeyringStore(backend=explicit)
        store.set("a", Credential(username="u", password="p"))
        store.set("b", Credential(username="v", password="q"))

        assert store.list_entries() == ["a", "b"]
        store.delete("a")
        assert store.list_entries() == ["b"]
        with pytest.raises(CredentialNotFoundError):
            store.get("a")
