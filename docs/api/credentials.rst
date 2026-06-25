Uprawnienia / credentiale — ``credentials``
===========================================

.. _credentials-overview:

Podsystem :mod:`zus_db_utils.credentials` odpowiada za bezpieczne
przechowywanie i pobieranie danych dostępowych (użytkownik, hasło,
metadane połączenia) używanych przez backendy sieciowe
(PostgreSQL, MSSQL). Backend SQLite nie wymaga credentiala.

.. rubric:: Architektura

* :class:`~zus_db_utils.credentials.models.Credential` — niemutowalny model
  danych (Pydantic). Hasło trzymane jako :class:`pydantic.SecretStr`, więc
  nigdy nie wycieka do ``repr()`` / ``str()`` / logów.
* :class:`~zus_db_utils.credentials.store.CredentialStore` — abstrakcyjny
  interfejs storu z metodami ``get`` / ``set`` / ``delete`` /
  ``list_entries`` / ``rotate``.
* Dwie implementacje pluginowe wybierane przez :meth:`CredentialStore.auto`:

  * :class:`~zus_db_utils.credentials.keyring_provider.KeyringStore`
    — Windows Credential Manager (przez bibliotekę ``keyring``);
  * :class:`~zus_db_utils.credentials.encrypted_file.EncryptedFileStore`
    — szyfrowany plik (Fernet, klucz z PBKDF2-HMAC-SHA256), domyślnie
    pod ``~/.zus_db_utils`` — przeznaczony dla RHEL/Linux bez root'a.

.. rubric:: Wybór providera

:meth:`CredentialStore.auto` dobiera implementację do platformy:

* Windows → :class:`KeyringStore`,
* pozostałe systemy → :class:`EncryptedFileStore`.

.. code-block:: python

   from zus_db_utils.credentials import CredentialStore, Credential

   store = CredentialStore.auto()
   store.set("postgres-dwh", Credential(
       username="etl_user",
       password="sekret",
       metadata={"host": "dwh.local", "port": 5432, "database": "warehouse"},
   ))

   cred = store.get("postgres-dwh")
   print(cred.username)              # etl_user
   print(cred.password.get_secret_value())  # sekret (jawnie, na żądanie)

.. rubric:: Użycie z fasadami

Zarówno :class:`~zus_db_utils.core.AggWriter`, jak i
:class:`~zus_db_utils.core.AggReader` przyjmują credential jako:

* nazwę (``str``) — rozwiązywaną przez ``credential_store`` (domyślnie
  :meth:`CredentialStore.auto`),
* gotowy obiekt :class:`Credential`,
* ``None`` (dozwolone tylko dla backendu SQLite).

.. code-block:: python

   from zus_db_utils import AggReader

   reader = AggReader(
       backend="postgres",
       credential="postgres-dwh",   # nazwa w storze
       keys=["operator_id"],
   )

.. rubric:: Rotacja hasła

:meth:`CredentialStore.rotate` zmienia wyłącznie hasło, zachowując
``username`` i ``metadata``:

.. code-block:: python

   store.rotate("postgres-dwh", "nowe-haslo")

.. rubric:: Bezpieczeństwo

* Hasła nigdy nie pojawiają się w ``repr()`` / logach (``SecretStr``).
* ``EncryptedFileStore`` weryfikuje uprawnienia pliku i zapisuje atomowo
  (plik tymczasowy + ``os.replace``) z kopią zapasową ``creds.enc.bak``.
* Master password można podać jawnie, przez zmienną środowiskową
  ``ZUS_DB_UTILS_MASTER_PASSWORD`` albo interaktywnie (``getpass``).

API
---

Model danych
^^^^^^^^^^^^

.. autoclass:: zus_db_utils.credentials.models.Credential
   :members:
   :show-inheritance:

Interfejs storu
^^^^^^^^^^^^^^^

.. autoclass:: zus_db_utils.credentials.store.CredentialStore
   :members:
   :show-inheritance:
   :member-order: bysource

Implementacje
^^^^^^^^^^^^^

.. autoclass:: zus_db_utils.credentials.encrypted_file.EncryptedFileStore
   :members:
   :show-inheritance:

.. autoclass:: zus_db_utils.credentials.keyring_provider.KeyringStore
   :members:
   :show-inheritance:
