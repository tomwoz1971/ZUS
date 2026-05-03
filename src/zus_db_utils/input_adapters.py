from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Union

import pandas as pd

SupportedInput = Union[pd.DataFrame, Mapping[str, object], Sequence[Mapping[str, object]]]


def normalize_input(data: SupportedInput) -> pd.DataFrame:
    """Konwertuje wejscie do DataFrame.

    Akceptuje DataFrame (zwracany bez kopii), pojedynczy ``dict``
    (jeden wiersz) lub ``list[dict]`` (wiele wierszy).

    :param data: DataFrame, mapping lub sekwencja mappingow
    :returns: DataFrame z wierszami z ``data``
    :raises TypeError: gdy typ wejscia nie jest wspierany
    :raises ValueError: gdy ``data`` to pusta sekwencja
    """
    if isinstance(data, pd.DataFrame):
        return data
    if isinstance(data, Mapping):
        return pd.DataFrame([dict(data)])
    if isinstance(data, Sequence) and not isinstance(data, (str, bytes)):
        if len(data) == 0:
            raise ValueError("Pusta sekwencja na wejsciu")
        if not all(isinstance(row, Mapping) for row in data):
            raise TypeError("Sekwencja musi zawierac wylacznie mapping/dict")
        return pd.DataFrame([dict(row) for row in data])
    raise TypeError(f"Nieobslugiwany typ wejscia: {type(data).__name__}")
