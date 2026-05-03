from __future__ import annotations

import os
import platform
from pathlib import Path

import pytest

from zus_db_utils.credentials import Credential, EncryptedFileStore
from zus_db_utils.exceptions import CredentialError, CredentialNotFoundError


@pytest.fixture
def store(tmp_path: Path) -> EncryptedFileStore:
    return EncryptedFileStore(base_dir=tmp_path / "creds", master_password="master")


def test_set_and_get_roundtrip(store: EncryptedFileStore) -> None:
    cred = Credential(username="u", password="p", metadata={"host": "h"})
    store.set("a", cred)

    loaded = store.get("a")
    assert loaded.username == "u"
    assert loaded.password.get_secret_value() == "p"
    assert loaded.metadata == {"host": "h"}


def test_get_missing_raises(store: EncryptedFileStore) -> None:
    store.set("a", Credential(username="u", password="p"))
    with pytest.raises(CredentialNotFoundError):
        store.get("nope")


def test_list_entries_sorted(store: EncryptedFileStore) -> None:
    store.set("b", Credential(username="u", password="p"))
    store.set("a", Credential(username="u", password="p"))
    assert store.list_entries() == ["a", "b"]


def test_delete_removes_entry(store: EncryptedFileStore) -> None:
    store.set("a", Credential(username="u", password="p"))
    store.delete("a")
    assert store.list_entries() == []


def test_delete_missing_raises(store: EncryptedFileStore) -> None:
    store.set("a", Credential(username="u", password="p"))
    with pytest.raises(CredentialNotFoundError):
        store.delete("nope")


def test_rotate_changes_password(store: EncryptedFileStore) -> None:
    store.set("a", Credential(username="u", password="old", metadata={"k": "v"}))
    store.rotate("a", "new")
    cred = store.get("a")
    assert cred.password.get_secret_value() == "new"
    assert cred.username == "u"
    assert cred.metadata == {"k": "v"}


def test_wrong_master_password_raises(tmp_path: Path) -> None:
    EncryptedFileStore(
        base_dir=tmp_path / "creds", master_password="right"
    ).set("a", Credential(username="u", password="p"))

    other = EncryptedFileStore(base_dir=tmp_path / "creds", master_password="wrong")
    with pytest.raises(CredentialError, match="master password"):
        other.get("a")


def test_corrupted_file_raises(store: EncryptedFileStore) -> None:
    store.set("a", Credential(username="u", password="p"))
    store.creds_path.write_bytes(b"garbage_not_valid_fernet_token")
    with pytest.raises(CredentialError):
        store.get("a")


def test_missing_salt_with_existing_file_raises(tmp_path: Path) -> None:
    base = tmp_path / "creds"
    store = EncryptedFileStore(base_dir=base, master_password="m")
    store.set("a", Credential(username="u", password="p"))

    store.salt_path.unlink()
    other = EncryptedFileStore(base_dir=base, master_password="m")
    with pytest.raises(CredentialError, match="salt"):
        other.get("a")


def test_creates_backup_on_overwrite(store: EncryptedFileStore) -> None:
    store.set("a", Credential(username="u", password="p1"))
    first_bytes = store.creds_path.read_bytes()
    store.set("a", Credential(username="u", password="p2"))
    assert store.backup_path.exists()
    assert store.backup_path.read_bytes() == first_bytes


def test_master_password_from_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ZUS_DB_UTILS_MASTER_PASSWORD", "from-env")
    store = EncryptedFileStore(base_dir=tmp_path / "creds")
    store.set("a", Credential(username="u", password="p"))
    assert store.get("a").password.get_secret_value() == "p"


@pytest.mark.skipif(platform.system() == "Windows", reason="POSIX permissions only")
def test_warns_on_loose_permissions(store: EncryptedFileStore) -> None:
    store.set("a", Credential(username="u", password="p"))
    os.chmod(store.creds_path, 0o644)
    with pytest.warns(UserWarning, match="uprawnienia"):
        store.get("a")


@pytest.mark.skipif(platform.system() == "Windows", reason="POSIX permissions only")
def test_files_have_0600_after_save(store: EncryptedFileStore) -> None:
    store.set("a", Credential(username="u", password="p"))
    assert (store.creds_path.stat().st_mode & 0o777) == 0o600
    assert (store.salt_path.stat().st_mode & 0o777) == 0o600


def test_list_on_empty_store(tmp_path: Path) -> None:
    store = EncryptedFileStore(base_dir=tmp_path / "creds", master_password="m")
    assert store.list_entries() == []
