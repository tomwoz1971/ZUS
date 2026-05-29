Historia zmian
==============

0.3.0 (2026-05-29)
------------------

* Nowa strategia :class:`~zus_db_utils.strategies.upsert.Upsert` —
  INSERT-or-UPDATE na podstawie klucza biznesowego; wspierana przez
  backendy PostgreSQL, MSSQL, SQLite.
* :class:`~zus_db_utils.strategies.upsert.UpsertResult` z licznikami
  ``inserted`` i ``updated``.

0.2.0 (2026-05-26)
------------------

* :class:`~zus_db_utils.strategies.incremental_quantity.IncrementalQuantity`:
  nowy parametr ``close_missing`` (``False`` / ``"close_only"`` / ``"zero"``) —
  obsługa kluczy dynamicznie znikających ze źródła danych.
* :class:`~zus_db_utils.strategies.incremental_quantity.WriteResult`:
  nowe pole ``missing_closed`` zliczające rekordy domknięte z powodu braku
  klucza w wejściu.
* Logowanie operacji ``write()`` na poziomie ``INFO`` (tabela, liczniki,
  czas wykonania) przez logger ``zus_db_utils.strategies.incremental_quantity``.
* Nowa funkcja :func:`~zus_db_utils.logging_config.configure_file_logging`
  — jednolinijkowa konfiguracja zapisu logów pakietu do pliku,
  z opcjonalną rotacją (``RotatingFileHandler``).

0.1.0 (2026-05-13)
------------------

* Pierwsza wersja publiczna.
* Fasada :class:`~zus_db_utils.core.AggWriter`.
* Strategia :class:`~zus_db_utils.strategies.incremental_quantity.IncrementalQuantity`.
* Backendy: PostgreSQL, MSSQL, SQLite.
* Moduł odczytu :mod:`zus_db_utils.queries.incremental_quantity` z funkcjami
  :func:`~zus_db_utils.queries.incremental_quantity.read_current`,
  :func:`~zus_db_utils.queries.incremental_quantity.read_snapshots`,
  :func:`~zus_db_utils.queries.incremental_quantity.read_increments`.
* Parametr ``filters`` (równość + IN) we wszystkich funkcjach odczytu.
* Zarządzanie credentialami: Windows Credential Manager i szyfrowany plik RHEL.
