zus\_db\_utils
==============

Moduł do zapisu zagregowanych danych z pipeline'ów analitycznych
do różnych backendów (PostgreSQL, MSSQL, SQLite) z różnymi strategiami
ładowania.

.. rubric:: Szybki start

.. code-block:: python

   from zus_db_utils import AggWriter
   from zus_db_utils.credentials import CredentialStore, Credential

   store = CredentialStore.auto()
   store.set("postgres-dwh", Credential(
       username="etl_user",
       password="sekret",
       metadata={"host": "dwh.local", "port": 5432, "database": "warehouse"},
   ))

   writer = AggWriter(
       backend="postgres",
       strategy="incremental_quantity",
       credential="postgres-dwh",
       keys=["operator_id", "report_date"],
   )
   result = writer.write(df_workload, table="fct_operator_workload")

.. rubric:: Odczyt danych

.. code-block:: python

   from zus_db_utils.queries.incremental_quantity import (
       read_current,
       read_snapshots,
       read_increments,
   )

   # Bieżące wartości
   df = read_current(engine, "fct_metryka", keys=["operator_id"])

   # Z filtrem
   df = read_current(
       engine, "fct_metryka", keys=["operator_id"],
       filters={"region": "Warszawa", "typ": ["A", "B"]},
   )

   # Historia (snapshoty dzienne)
   df = read_snapshots(engine, "fct_metryka", keys=["operator_id"],
                       start=dt_od, end=dt_do, step="day")

----

.. toctree::
   :maxdepth: 2
   :caption: Dokumentacja API

   api/index

.. toctree::
   :maxdepth: 1
   :caption: Inne

   changelog
