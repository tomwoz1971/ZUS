from __future__ import annotations

import base64
import getpass
import json
import os
import platform
import shutil
import warnings
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from zus_db_utils.credentials.models import Credential
from zus_db_utils.credentials.store import CredentialStore
from zus_db_utils.exceptions import CredentialError, CredentialNotFoundError

KDF_ITERATIONS = 600_000
SALT_BYTES = 16
ENV_MASTER_PASSWORD = "ZUS_DB_UTILS_MASTER_PASSWORD"
DEFAULT_DIR_NAME = ".zus_db_utils"


class EncryptedFileStore(CredentialStore):
    """Stor credentiali w szyfrowanym pliku.

    Klucz szyfrujacy (Fernet) wyprowadzany z master password przez
    PBKDF2-HMAC-SHA256 (600k iteracji). Salt trzymany obok pliku (jawnie),
    plik szyfrogramu zapisywany atomic'ie (tmp + ``os.replace``) z
    backupem ``creds.enc.bak``.

    :param base_dir: katalog na pliki credentiali; domyslnie
        ``~/.zus_db_utils``
    :param master_password: opcjonalne, statyczne master password
        (jezeli ``None``, czytane z env ``ZUS_DB_UTILS_MASTER_PASSWORD``
        lub przez ``getpass``)
    """

    def __init__(
        self,
        base_dir: Path | None = None,
        master_password: str | None = None,
    ) -> None:
        self.base_dir = Path(base_dir) if base_dir else Path.home() / DEFAULT_DIR_NAME
        self.creds_path = self.base_dir / "creds.enc"
        self.salt_path = self.base_dir / "creds.salt"
        self.backup_path = self.base_dir / "creds.enc.bak"
        self._master_password = master_password

    def get(self, name: str) -> Credential:
        data = self._load_all()
        if name not in data:
            raise CredentialNotFoundError(f"Brak credentiala o nazwie {name!r}")
        return Credential(**data[name])

    def set(self, name: str, credential: Credential) -> None:
        data = self._load_all() if self.creds_path.exists() else {}
        data[name] = self._serialize(credential)
        self._save_all(data)

    def delete(self, name: str) -> None:
        data = self._load_all()
        if name not in data:
            raise CredentialNotFoundError(f"Brak credentiala o nazwie {name!r}")
        del data[name]
        self._save_all(data)

    def list_entries(self) -> list[str]:
        if not self.creds_path.exists():
            return []
        return sorted(self._load_all().keys())

    def _serialize(self, credential: Credential) -> dict[str, Any]:
        return {
            "username": credential.username,
            "password": credential.password.get_secret_value(),
            "metadata": credential.metadata,
        }

    def _resolve_master_password(self) -> str:
        if self._master_password is not None:
            return self._master_password
        env_value = os.environ.get(ENV_MASTER_PASSWORD)
        if env_value:
            return env_value
        return getpass.getpass("Master password dla zus_db_utils credentials: ")

    def _derive_key(self, password: str, salt: bytes) -> bytes:
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=KDF_ITERATIONS,
        )
        return base64.urlsafe_b64encode(kdf.derive(password.encode("utf-8")))

    def _ensure_dir(self) -> None:
        self.base_dir.mkdir(parents=True, exist_ok=True)
        if platform.system() != "Windows":
            os.chmod(self.base_dir, 0o700)

    def _load_or_create_salt(self) -> bytes:
        self._ensure_dir()
        if self.salt_path.exists():
            return self.salt_path.read_bytes()
        salt = os.urandom(SALT_BYTES)
        self.salt_path.write_bytes(salt)
        if platform.system() != "Windows":
            os.chmod(self.salt_path, 0o600)
        return salt

    def _check_permissions(self, path: Path) -> None:
        if platform.system() == "Windows" or not path.exists():
            return
        mode = path.stat().st_mode & 0o777
        if mode > 0o600:
            warnings.warn(
                f"Plik {path} ma uprawnienia {oct(mode)}, oczekiwano 0o600",
                stacklevel=3,
            )

    def _load_all(self) -> dict[str, dict[str, Any]]:
        if not self.creds_path.exists():
            return {}
        self._check_permissions(self.creds_path)
        if not self.salt_path.exists():
            raise CredentialError(
                f"Plik {self.creds_path} istnieje, ale brak {self.salt_path} — "
                "nie da sie odszyfrowac"
            )
        salt = self.salt_path.read_bytes()
        key = self._derive_key(self._resolve_master_password(), salt)
        ciphertext = self.creds_path.read_bytes()
        try:
            plaintext = Fernet(key).decrypt(ciphertext)
        except InvalidToken as exc:
            raise CredentialError(
                "Nieprawidlowe master password lub uszkodzony plik credentiali"
            ) from exc
        loaded: dict[str, dict[str, Any]] = json.loads(plaintext.decode("utf-8"))
        return loaded

    def _save_all(self, data: dict[str, dict[str, Any]]) -> None:
        salt = self._load_or_create_salt()
        key = self._derive_key(self._resolve_master_password(), salt)
        ciphertext = Fernet(key).encrypt(json.dumps(data).encode("utf-8"))

        if self.creds_path.exists():
            shutil.copy2(self.creds_path, self.backup_path)
            if platform.system() != "Windows":
                os.chmod(self.backup_path, 0o600)

        tmp = self.creds_path.with_suffix(".enc.tmp")
        tmp.write_bytes(ciphertext)
        if platform.system() != "Windows":
            os.chmod(tmp, 0o600)
        os.replace(tmp, self.creds_path)
