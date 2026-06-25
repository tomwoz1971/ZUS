Logowanie — ``logging_config``
==============================

Pakiet używa standardowego modułu :mod:`logging` Pythona.
Logger główny to ``zus_db_utils``; poszczególne moduły używają
loggerów potomnych (np. ``zus_db_utils.strategies.incremental_quantity``).

Biblioteka **nie konfiguruje żadnych handlerów sama** — zgodnie z dobrą
praktyką dla bibliotek Pythona (PEP 396). Inicjalizację pozostawia
aplikacji wywołującej.

.. rubric:: Szybka konfiguracja pliku logu

Funkcja pomocnicza :func:`configure_file_logging` dodaje ``FileHandler``
(lub ``RotatingFileHandler``) do loggera ``zus_db_utils`` jednym wywołaniem::

    from zus_db_utils import configure_file_logging

    configure_file_logging("/var/log/etl/zus.log")

Przykładowy wpis w pliku logu po operacji ``write()``::

    2026-05-26 06:00:01,234 INFO     zus_db_utils.strategies.incremental_quantity \
    write table='operator_workload' inserted=3 closed=1 skipped=12 missing_closed=2 elapsed=0.042s

Operacje odczytu przez :class:`~zus_db_utils.core.AggReader` logowane są
na poziomie ``INFO`` przez logger ``zus_db_utils.core``::

    2026-06-25 06:00:02,001 INFO     zus_db_utils.core \
    read_current table='operator_workload' rows=128
    2026-06-25 06:00:02,310 INFO     zus_db_utils.core \
    read_snapshots table='operator_workload' start=... end=... step=day rows=512

.. rubric:: Ręczna konfiguracja

Jeśli chcesz zintegrować logi pakietu z istniejącą konfiguracją logowania
w aplikacji, użyj standardowego API::

    import logging

    logging.getLogger("zus_db_utils").setLevel(logging.INFO)
    # handler dodawany przez aplikację we własnym zakresie

.. rubric:: Usuwanie handlera

:func:`configure_file_logging` zwraca dodany handler — można go później usunąć::

    handler = configure_file_logging("/tmp/zus.log")
    # ...
    logging.getLogger("zus_db_utils").removeHandler(handler)
    handler.close()

.. autofunction:: zus_db_utils.logging_config.configure_file_logging
