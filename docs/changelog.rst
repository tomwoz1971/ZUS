Historia zmian
==============

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
