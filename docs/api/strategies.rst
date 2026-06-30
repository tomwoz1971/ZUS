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

.. _strategies-upsert:

Upsert
------

Strategia INSERT-or-UPDATE na podstawie klucza biznesowego.
Dla każdego wiersza wejścia:

1. Sprawdza, czy wiersz o danym kluczu już istnieje w tabeli.
2. Brak rekordu → ``INSERT`` nowego wiersza.
3. Rekord istnieje → ``UPDATE`` wszystkich kolumn niebędących kluczem.

Kolumny wejścia nieobecne w tabeli są ignorowane. Operacja wykonywana
w jednej transakcji; błąd dowolnego wiersza powoduje rollback całego
``write()``.

Przykład::

    from zus_db_utils import AggWriter

    writer = AggWriter(
        backend="postgres",
        strategy="upsert",
        credential="postgres-dwh",
        keys=["operator_id", "report_date"],
    )
    result = writer.write(df, table="fct_operator_workload")
    print(result.inserted, result.updated)

.. rubric:: Częściowe aktualizacje (partial updates)

Strategia ``upsert`` bezpiecznie obsługuje DataFrame zawierający **tylko
podzbiór kolumn** z tabeli docelowej (klucze + wybrane wartości).
Kolumny **nieobecne** w DataFrame są traktowane w sposób zależny od operacji:

.. list-table::
   :header-rows: 1
   :widths: 20 40 40

   * - Operacja
     - Kolumny **w** DataFrame
     - Kolumny **poza** DataFrame
   * - **UPDATE** (istniejący klucz)
     - Aktualizowane na nowe wartości
     - **Zachowują aktualną wartość** w bazie
   * - **INSERT** (nowy klucz)
     - Wstawiane z podanymi wartościami
     - Przyjmują ``DEFAULT`` lub ``NULL``

**Przykład — aktualizacja tylko ceny**::

    # Tabela produkty: (id, nazwa, cena, opis, kategoria)
    # Wiersz id=1 istnieje z cena=2500, opis='Oryginalny opis', kategoria='Laptop'

    df_update_price = pd.DataFrame([
        {"id": 1, "cena": 2300.0},  # tylko klucz + cena
        {"id": 2, "cena": 450.0},   # nowy produkt (INSERT)
    ])

    writer = AggWriter(backend="postgres", strategy="upsert", keys=["id"])
    result = writer.write(df_update_price, "produkty")
    # id=1: UPDATE → cena=2300, opis='Oryginalny opis' (NIEZMIENIONY), kategoria='Laptop' (NIEZMIENIONY)
    # id=2: INSERT → cena=450, opis=NULL (brak DEFAULT), kategoria=NULL (brak DEFAULT)

**Zalety:**

* Bezpieczne — nie ryzykujesz utraty danych w pominiętych kolumnach.
* Elastyczne — różne DataFrame mogą aktualizować różne subsets kolumn.
* Wydajne — nie trzeba wcześniej odczytywać pełnych wierszy z bazy.

**Uwagi:**

* Przy **INSERT** kolumny bez ``DEFAULT`` i bez ``NOT NULL`` dostaną ``NULL``.
* Jeśli kolumna ma ``NOT NULL`` i brak ``DEFAULT``, INSERT się wywali.
* Implementacja: tylko kolumny obecne w DataFrame trafiają do SQL ``UPDATE``
  / ``INSERT`` — pozostałe kolumny są ignorowane w zapytaniu.

.. warning::
    **Pułapka Pandas NaN:** Gdy DataFrame tworzony jest z list dictów
    z różnymi kluczami, Pandas automatycznie **dodaje brakujące kolumny
    z wartością NaN**::

        df = pd.DataFrame([
            {"id": 1, "cena": 100},      # brak kolumny "opis"
            {"id": 2, "opis": "tekst"},  # brak kolumny "cena"
        ])
        # Wynik: DataFrame z kolumnami [id, cena, opis],
        # gdzie wiersz 1 ma opis=NaN, wiersz 2 ma cena=NaN

    Taki DataFrame **zaktualizuje** ``opis=NULL`` dla id=1 i ``cena=NULL``
    dla id=2! Jeśli chcesz **pominąć** kolumnę całkowicie, **nie** umieszczaj
    jej w żadnym dict-ie lub twórz osobne DataFrame dla każdego wiersza::

        # ✓ Bezpieczne — osobne zapisy, różne kolumny
        writer.write(pd.DataFrame([{"id": 1, "cena": 100}]), "produkty")
        writer.write(pd.DataFrame([{"id": 2, "opis": "tekst"}]), "produkty")

.. rubric:: Wspierane backendy

PostgreSQL, MSSQL, SQLite.
CSV / Parquet → :exc:`~zus_db_utils.exceptions.UnsupportedStrategyError`.

.. autoclass:: zus_db_utils.strategies.upsert.UpsertResult
   :members:

.. autoclass:: zus_db_utils.strategies.upsert.Upsert
   :members:
   :special-members: __init__
