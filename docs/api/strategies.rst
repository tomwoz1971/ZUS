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

Po zakończeniu ``write()`` emitowany jest log na poziomie ``INFO`` z tabelą
docelową, licznikami operacji i czasem wykonania. Zob. :mod:`zus_db_utils.logging_config`.

.. rubric:: Brakujące klucze — ``close_missing``

Gdy system źródłowy dynamicznie pomija klucze z wartością 0 (nie zwraca
ich w batchu), otwarte rekordy w bazie zostałyby bez domknięcia.
Parametr ``close_missing`` kontroluje to zachowanie:

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Wartość
     - Zachowanie
   * - ``False`` *(domyślnie)*
     - brak działania — otwarte rekordy nieobecnych kluczy pozostają bez zmian
   * - ``"close_only"``
     - zamknięcie otwartego rekordu (``data_do = now``) dla każdego klucza
       nieobecnego w wejściu; licznik ``missing_closed``
   * - ``"zero"``
     - jak ``"close_only"``, a następnie ``INSERT`` nowego rekordu
       z ``ilosc = 0``; dodatkowy licznik ``inserted``

Przykład::

    from zus_db_utils import AggWriter

    writer = AggWriter(
        backend="postgres",
        strategy="incremental_quantity",
        credential="postgres-dwh",
        keys=["operator_id"],
        close_missing="zero",
    )
    result = writer.write(df, table="operator_workload")
    print(result.missing_closed)   # liczba domkniętych brakujących rekordów

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
