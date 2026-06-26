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

Najprościej przez fasadę :class:`~zus_db_utils.core.AggReader` — analogiczną
do ``AggWriter``, ale do odczytu (SELECT). Zarządza backendem i credentialem,
a domyślne ``keys`` można podać raz w konstruktorze:

.. code-block:: python

   from zus_db_utils import AggReader

   reader = AggReader(
       backend="postgres",
       credential="postgres-dwh",
       keys=["operator_id"],
   )

   # Bieżące wartości
   df = reader.read_current(table="fct_metryka")

   # Z filtrem
   df = reader.read_current(
       table="fct_metryka",
       filters={"region": "Warszawa", "typ": ["A", "B"]},
   )

   # Historia (snapshoty dzienne) oraz przyrosty
   df = reader.read_snapshots(table="fct_metryka", start=dt_od, end=dt_do, step="day")
   df = reader.read_increments(table="fct_metryka", start=dt_od, end=dt_do, step="day")

   # Dowolne zapytanie SQL (escape hatch) — z bezpiecznym wiązaniem parametrów
   df = reader.read_sql(
       "SELECT a1, SUM(ilosc) AS suma FROM fct_metryka "
       "WHERE region = :region GROUP BY a1",
       params={"region": "Warszawa"},
   )

Alternatywnie można wołać funkcje niskopoziomowe z modułu
:mod:`zus_db_utils.queries.incremental_quantity`, podając własny ``engine``:

.. code-block:: python

   from zus_db_utils.queries.incremental_quantity import read_current

   df = read_current(engine, "fct_metryka", keys=["operator_id"])

----

.. toctree::
   :maxdepth: 2
   :caption: Dokumentacja API

   api/index

.. toctree::
   :maxdepth: 1
   :caption: Inne

   changelog
