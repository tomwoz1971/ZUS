from __future__ import annotations

import json
from typing import Any

import keyring
from keyring.backend import KeyringBackend

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
    :param backend: opcjonalna instancja ``KeyringBackend``; gdy podana,
        operacje ida bezposrednio przez nia, omijajac globalne
        ``keyring._load_plugins()`` (przydatne, gdy plugin discovery
        wybucha — np. bug ``unhashable type: 'list'`` w starym
        ``importlib_metadata``)
    """

    def __init__(
        self,
        service: str = SERVICE_NAME,
        *,
        backend: KeyringBackend | None = None,
    ) -> None:
        self._service = service
        self._backend = backend

    def get(self, name: str) -> Credential:
        if name == INDEX_KEY:
            raise CredentialNotFoundError(f"Brak credentiala o nazwie {name!r}")
        raw = self._get_password(name)
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
        self._set_password(name, payload)
        names = self._load_index()
        if name not in names:
            names.append(name)
            self._save_index(names)

    def delete(self, name: str) -> None:
        if self._get_password(name) is None:
            raise CredentialNotFoundError(f"Brak credentiala o nazwie {name!r}")
        self._delete_password(name)
        names = self._load_index()
        if name in names:
            names.remove(name)
            self._save_index(names)

    def list_entries(self) -> list[str]:
        return sorted(self._load_index())

    def _load_index(self) -> list[str]:
        raw = self._get_password(INDEX_KEY)
        if raw is None:
            return []
        loaded: list[str] = json.loads(raw)
        return loaded

    def _save_index(self, names: list[str]) -> None:
        self._set_password(INDEX_KEY, json.dumps(names))

    def _get_password(self, name: str) -> str | None:
        if self._backend is not None:
            return self._backend.get_password(self._service, name)
        return keyring.get_password(self._service, name)

    def _set_password(self, name: str, value: str) -> None:
        if self._backend is not None:
            self._backend.set_password(self._service, name, value)
        else:
            keyring.set_password(self._service, name, value)

    def _delete_password(self, name: str) -> None:
        if self._backend is not None:
            self._backend.delete_password(self._service, name)
        else:
            keyring.delete_password(self._service, name)
