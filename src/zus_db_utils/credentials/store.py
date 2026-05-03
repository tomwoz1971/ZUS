from __future__ import annotations

import platform
from abc import ABC, abstractmethod

from pydantic import SecretStr

from zus_db_utils.credentials.models import Credential


class CredentialStore(ABC):
    """Abstrakcyjny interfejs storu credentiali.

    Implementacje:

    * :class:`zus_db_utils.credentials.keyring_provider.KeyringStore`
      — Windows Credential Manager (przez biblioteke ``keyring``)
    * :class:`zus_db_utils.credentials.encrypted_file.EncryptedFileStore`
      — szyfrowany plik (RHEL/Linux bez root'a)
    """

    @abstractmethod
    def get(self, name: str) -> Credential:
        """Pobiera credential o podanej nazwie.

        :raises CredentialNotFoundError: gdy brak wpisu
        :raises CredentialError: gdy stor jest nieczytelny
        """

    @abstractmethod
    def set(self, name: str, credential: Credential) -> None:
        """Zapisuje credential pod podana nazwa (nadpisuje jesli istnieje)."""

    @abstractmethod
    def delete(self, name: str) -> None:
        """Usuwa credential.

        :raises CredentialNotFoundError: gdy brak wpisu
        """

    @abstractmethod
    def list_entries(self) -> list[str]:
        """Zwraca posortowana liste nazw credentiali w storze."""

    def rotate(self, name: str, new_password: str) -> None:
        """Zmienia haslo dla istniejacego credentiala.

        Pozostale pola (``username``, ``metadata``) bez zmian.

        :raises CredentialNotFoundError: gdy brak wpisu
        """
        existing = self.get(name)
        rotated = existing.model_copy(update={"password": SecretStr(new_password)})
        self.set(name, rotated)

    @classmethod
    def auto(cls) -> CredentialStore:
        """Wybiera providera odpowiedniego dla biezacej platformy.

        * Windows -> :class:`KeyringStore`
        * inne -> :class:`EncryptedFileStore`
        """
        from zus_db_utils.credentials.encrypted_file import EncryptedFileStore
        from zus_db_utils.credentials.keyring_provider import KeyringStore

        if platform.system() == "Windows":
            return KeyringStore()
        return EncryptedFileStore()
