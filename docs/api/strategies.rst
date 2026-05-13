Strategie — ``strategies``
==========================

.. _strategies-incremental-quantity:

IncrementalQuantity
-------------------

Strategia SCD2 dla tabel z jednym pomiarem liczbowym w czasie.
Dla każdego wiersza wejścia:

1. Pobiera aktualnie otwarty rekord dla klucza biznesowego.
2. Brak rekordu → ``INSERT`` z ``data_od = now``.
3. Rekord istnieje, ``ilosc`` równa → pomiń (``skipped``).
4. Rekord istnieje, ``ilosc`` różna → zamknij stary (``data_do = now``),
   wstaw nowy (``inserted``, ``closed``).

Wartość ``now`` generowana po stronie klienta (UTC), wspólna dla całego
``write()`` — spójny snapshot czasowy batcha.

.. rubric:: Wspierane backendy

PostgreSQL, MSSQL, SQLite.
CSV / Parquet → :exc:`~zus_db_utils.exceptions.UnsupportedStrategyError`.

.. rubric:: Schemat tabeli docelowej

Wymagane kolumny (tworzone przez DDL poza modułem):

.. list-table::
   :header-rows: 1
   :widths: 20 15 65

   * - Kolumna
     - Typ
     - Opis
   * - ``<keys>``
     - dowolny
     - Kolumny klucza biznesowego
   * - ``ilosc``
     - liczbowy
     - Pomiar (nazwa konfigurowalna)
   * - ``data_od``
     - ``DATETIME NOT NULL``
     - Początek ważności (UTC)
   * - ``data_do``
     - ``DATETIME NULL``
     - Koniec ważności; ``NULL`` = rekord aktualny
   * - ``id``
     - ``IDENTITY`` / ``SERIAL``
     - PK generowany przez bazę

.. autoclass:: zus_db_utils.strategies.incremental_quantity.WriteResult
   :members:

.. autoclass:: zus_db_utils.strategies.incremental_quantity.IncrementalQuantity
   :members:
   :special-members: __init__
