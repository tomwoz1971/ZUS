from __future__ import annotations

import json
from typing import Any

import keyring

from zus_db_utils.credentials.models import Credential
from zus_db_utils.credentials.store import CredentialStore
from zus_db_utils.exceptions import CredentialNotFoundError

SERVICE_NAME = "zus_db_utils"
INDEX_KEY = "__zus_db_utils_index__"


class KeyringStore(CredentialStore):
    """Stor credentiali oparty o ``keyring`` (Windows Credential Manager).

    Kazdy credential trzymany jako pojedynczy wpis w ``keyring`` pod
    serwisem ``zus_db_utils``; lista nazw wpisow trzymana w osobnym
    wpisie indeksowym (``keyring`` nie udostepnia listowania natywnie).

    :param service: nazwa serwisu w keyring; domyslnie ``zus_db_utils``
    """

    def __init__(self, service: str = SERVICE_NAME) -> None:
        self._service = service

    def get(self, name: str) -> Credential:
        if name == INDEX_KEY:
            raise CredentialNotFoundError(f"Brak credentiala o nazwie {name!r}")
        raw = keyring.get_password(self._service, name)
        if raw is None:
            raise CredentialNotFoundError(f"Brak credentiala o nazwie {name!r}")
        payload: dict[str, Any] = json.loads(raw)
        return Credential(**payload)

    def set(self, name: str, credential: Credential) -> None:
        if name == INDEX_KEY:
            raise ValueError(f"{name!r} to nazwa zarezerwowana")
        payload = json.dumps(
            {
                "username": credential.username,
                "password": credential.password.get_secret_value(),
                "metadata": credential.metadata,
            }
        )
        keyring.set_password(self._service, name, payload)
        names = self._load_index()
        if name not in names:
            names.append(name)
            self._save_index(names)

    def delete(self, name: str) -> None:
        if keyring.get_password(self._service, name) is None:
            raise CredentialNotFoundError(f"Brak credentiala o nazwie {name!r}")
        keyring.delete_password(self._service, name)
        names = self._load_index()
        if name in names:
            names.remove(name)
            self._save_index(names)

    def list_entries(self) -> list[str]:
        return sorted(self._load_index())

    def _load_index(self) -> list[str]:
        raw = keyring.get_password(self._service, INDEX_KEY)
        if raw is None:
            return []
        loaded: list[str] = json.loads(raw)
        return loaded

    def _save_index(self, names: list[str]) -> None:
        keyring.set_password(self._service, INDEX_KEY, json.dumps(names))
