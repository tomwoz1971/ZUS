from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, SecretStr


class Credential(BaseModel):
    """Para uzytkownik/haslo plus metadane (host, port, baza itp.).

    Haslo jest typu :class:`pydantic.SecretStr` — nigdy nie pojawia sie
    w ``repr()``, ``str()`` ani serializacji domyslnej.

    :param username: nazwa uzytkownika
    :param password: haslo (przechowywane jako ``SecretStr``)
    :param metadata: dowolne dane pomocnicze (host, port, database, ...)
    """

    model_config = ConfigDict(frozen=True)

    username: str
    password: SecretStr
    metadata: dict[str, Any] = Field(default_factory=dict)

    def __repr__(self) -> str:
        return (
            f"Credential(username={self.username!r}, "
            f"password='***', metadata={self.metadata!r})"
        )
