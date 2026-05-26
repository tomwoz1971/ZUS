from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Union

_PKG_LOGGER = "zus_db_utils"


def configure_file_logging(
    path: Union[str, os.PathLike],
    level: Union[int, str] = logging.INFO,
    *,
    fmt: str = "%(asctime)s %(levelname)-8s %(name)s %(message)s",
    rotate: bool = False,
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 3,
) -> logging.Handler:
    """Konfiguruje zapis logow pakietu ``zus_db_utils`` do pliku.

    Dodaje ``FileHandler`` (lub ``RotatingFileHandler``) do loggera
    ``zus_db_utils``. Nie modyfikuje root-loggera ani loggerow innych
    pakietow. Mozna wywolac wielokrotnie dla roznych plikow.

    :param path: sciezka do pliku logu (tworzona jesli nie istnieje)
    :param level: poziom logowania; domyslnie ``logging.INFO``
    :param fmt: format wiersza logu
    :param rotate: ``True`` = uzywaj ``RotatingFileHandler``
    :param max_bytes: rozmiar pliku przed rotacja (tylko gdy ``rotate=True``)
    :param backup_count: liczba zachowywanych kopii (tylko gdy ``rotate=True``)
    :returns: dodany handler (mozna go usunac przez
        ``logging.getLogger('zus_db_utils').removeHandler(handler)``)
    """
    logger = logging.getLogger(_PKG_LOGGER)
    logger.setLevel(level)

    if rotate:
        handler: logging.Handler = RotatingFileHandler(
            path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
    else:
        handler = logging.FileHandler(path, encoding="utf-8")

    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(fmt))
    logger.addHandler(handler)
    return handler
