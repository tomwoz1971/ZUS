Backendy — ``backends``
=======================

Backend deklaruje, jak się połączyć z danym systemem i jakie strategie obsługuje.
Każdy backend udostępnia właściwość :attr:`~zus_db_utils.backends.base.Backend.engine`
(silnik SQLAlchemy) używaną przez strategie i funkcje odczytu.

.. note::

   **MSSQL**: moduł wspiera wyłącznie **SQL Authentication** (``UID=…;PWD=…``).
   Integrated Security / Kerberos nie są obsługiwane — wymagałyby konfiguracji AD
   niemożliwej w środowiskach RHEL bez uprawnień root.

Backend (klasa bazowa)
----------------------

.. autoclass:: zus_db_utils.backends.base.Backend
   :members:
   :special-members: __init__

SQLiteBackend
-------------

.. autoclass:: zus_db_utils.backends.sqlite.SQLiteBackend
   :members:
   :special-members: __init__

PostgresBackend
---------------

.. autoclass:: zus_db_utils.backends.postgres.PostgresBackend
   :members:
   :special-members: __init__

MSSQLBackend
------------

.. autoclass:: zus_db_utils.backends.mssql.MSSQLBackend
   :members:
   :special-members: __init__
