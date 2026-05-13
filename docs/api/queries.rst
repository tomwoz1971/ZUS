Odczyt — ``queries``
====================

.. _queries-incremental-quantity:

incremental\_quantity
---------------------

Funkcje odczytu dla strategii :ref:`incremental_quantity <strategies-incremental-quantity>`.
Wszystkie zwracają :class:`pandas.DataFrame` z kolumnami czasu
skonwertowanymi z UTC do ``Europe/Warsaw``.

Wymagane kolumny tabeli docelowej: ``keys``, kolumna pomiaru (``ilosc``),
``data_od`` (``DATETIME NOT NULL``), ``data_do`` (``DATETIME NULL``), ``id``.

.. rubric:: Filtrowanie

Parametr ``filters`` przyjmuje słownik ``{kolumna: wartość}``:

* skalar → ``WHERE kolumna = wartość``
* lista/krotka → ``WHERE kolumna IN (wartości)``

Nazwy kolumn są walidowane przez reflection — nieznana kolumna to natychmiastowy
:exc:`ValueError`.

.. rubric:: Przykłady

.. code-block:: python

   from zus_db_utils.queries.incremental_quantity import (
       read_current, read_snapshots, read_increments,
   )
   from datetime import datetime, timezone

   # Bieżące wartości tylko dla regionu Warszawa
   df = read_current(engine, "fct_metryka", keys=["operator_id"],
                     filters={"region": "Warszawa"})

   # Snapshoty tygodniowe z filtrem IN
   df = read_snapshots(
       engine, "fct_metryka", keys=["operator_id"],
       start=datetime(2025, 1, 1, tzinfo=timezone.utc),
       end=datetime(2025, 3, 31, tzinfo=timezone.utc),
       step="week",
       filters={"typ": ["A", "B"]},
   )

   # Przyrosty miesięczne
   df = read_increments(
       engine, "fct_metryka", keys=["operator_id"],
       start=datetime(2025, 1, 1, tzinfo=timezone.utc),
       end=datetime(2025, 12, 31, tzinfo=timezone.utc),
       step="day",
   )

.. autofunction:: zus_db_utils.queries.incremental_quantity.read_current

.. autofunction:: zus_db_utils.queries.incremental_quantity.read_snapshots

.. autofunction:: zus_db_utils.queries.incremental_quantity.read_increments
