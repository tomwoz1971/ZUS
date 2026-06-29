#!/home/ubuntu/ZUS/.venv/bin/python
"""Przykłady użycia loggera z zus_db_utils.

Ten skrypt pokazuje wszystkie sposoby logowania w kontekście pakietu.
"""
import logging
import sys
from pathlib import Path

# Dodaj katalog projektu do PYTHONPATH
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from zus_db_utils.logging_config import configure_file_logging


# ============================================================================
# METODA 1: Szybka konfiguracja - configure_file_logging()
# ============================================================================
print("=" * 70)
print("METODA 1: configure_file_logging() - najprostsza")
print("=" * 70)

# Skonfiguruj logging do pliku (tylko dla pakietu zus_db_utils)
log_dir = PROJECT_ROOT / "logs"
log_dir.mkdir(exist_ok=True)
log_file = log_dir / "example_method1.log"

handler = configure_file_logging(
    str(log_file),
    level=logging.INFO,  # Poziom: DEBUG, INFO, WARNING, ERROR, CRITICAL
    rotate=True,         # Automatyczna rotacja plików
    max_bytes=10 * 1024 * 1024,  # 10 MB przed rotacją
    backup_count=3,      # Zachowaj 3 ostatnie pliki
)

# Teraz możesz logować używając loggera zus_db_utils
logger = logging.getLogger("zus_db_utils")
logger.info("To jest log INFO")
logger.warning("To jest log WARNING")
logger.debug("To NIE zostanie zapisane (poziom DEBUG < INFO)")

# Możesz też użyć loggera potomnego (np. dla własnego modułu)
custom_logger = logging.getLogger("zus_db_utils.my_module")
custom_logger.info("Log z mojego modułu")

print(f"✓ Logi zapisane do: {log_file}")
print(f"  Zobacz: tail -f {log_file}\n")

# Usuń handler (opcjonalnie)
logging.getLogger("zus_db_utils").removeHandler(handler)
handler.close()


# ============================================================================
# METODA 2: Ręczna konfiguracja - standardowy logging
# ============================================================================
print("=" * 70)
print("METODA 2: Ręczna konfiguracja standardowym logging")
print("=" * 70)

# Skonfiguruj ręcznie (więcej kontroli)
log_file2 = log_dir / "example_method2.log"

file_handler = logging.FileHandler(log_file2, encoding="utf-8")
file_handler.setLevel(logging.DEBUG)  # Ten handler akceptuje DEBUG
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
file_handler.setFormatter(formatter)

logger2 = logging.getLogger("zus_db_utils")
logger2.setLevel(logging.DEBUG)  # Logger akceptuje DEBUG
logger2.addHandler(file_handler)

logger2.debug("Teraz DEBUG będzie zapisany")
logger2.info("Info również")
logger2.error("I błędy oczywiście")

print(f"✓ Logi zapisane do: {log_file2}\n")

logger2.removeHandler(file_handler)
file_handler.close()


# ============================================================================
# METODA 3: Własny logger + logi zus_db_utils do tego samego pliku
# ============================================================================
print("=" * 70)
print("METODA 3: Integracja z własnymi logami aplikacji")
print("=" * 70)

log_file3 = log_dir / "example_method3.log"

# Skonfiguruj root logger (dla całej aplikacji)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file3, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),  # Dodatkowo na ekran
    ]
)

# Teraz wszystkie loggery (także zus_db_utils) będą pisać do tego pliku
app_logger = logging.getLogger(__name__)  # Logger aplikacji
zus_logger = logging.getLogger("zus_db_utils")  # Logger pakietu

app_logger.info("Start aplikacji")
zus_logger.info("Logger zus_db_utils aktywny")
app_logger.warning("Ostrzeżenie z aplikacji")
zus_logger.error("Błąd z pakietu zus_db_utils")

print(f"✓ Logi zapisane do: {log_file3}")
print(f"  (i wyświetlone na ekranie)\n")


# ============================================================================
# METODA 4: Różne poziomy dla różnych loggerów
# ============================================================================
print("=" * 70)
print("METODA 4: Różne poziomy logowania dla różnych modułów")
print("=" * 70)

log_file4 = log_dir / "example_method4.log"

# Resetuj konfigurację
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

handler4 = logging.FileHandler(log_file4, encoding="utf-8")
handler4.setFormatter(logging.Formatter("%(levelname)s | %(name)s | %(message)s"))

# Logger główny pakietu - poziom WARNING (tylko ostrzeżenia i błędy)
zus_main = logging.getLogger("zus_db_utils")
zus_main.setLevel(logging.WARNING)
zus_main.addHandler(handler4)

# Logger konkretnego modułu - poziom DEBUG (wszystko)
zus_strategies = logging.getLogger("zus_db_utils.strategies")
zus_strategies.setLevel(logging.DEBUG)

zus_main.debug("To NIE zostanie zapisane (DEBUG < WARNING)")
zus_main.info("To też NIE (INFO < WARNING)")
zus_main.warning("To TAK - ostrzeżenie")
zus_main.error("To TAK - błąd")

zus_strategies.debug("To TAK - strategies ma DEBUG")
zus_strategies.info("To TAK - strategies ma DEBUG")

print(f"✓ Logi zapisane do: {log_file4}\n")


# ============================================================================
# PRAKTYCZNY PRZYKŁAD: Użycie w prawdziwym kodzie
# ============================================================================
print("=" * 70)
print("PRAKTYCZNY PRZYKŁAD: Operacje na danych z logowaniem")
print("=" * 70)

log_file5 = log_dir / "example_practical.log"

# Wyczyść poprzednie handlery
for h in logging.getLogger("zus_db_utils").handlers[:]:
    logging.getLogger("zus_db_utils").removeHandler(h)

configure_file_logging(str(log_file5), level=logging.INFO)

# Użyj loggera w swoim własnym kodzie ETL
# (AggWriter/AggReader automatycznie logują swoje operacje)

# Pobierz logger zus_db_utils
my_logger = logging.getLogger("zus_db_utils.my_etl_script")

# Loguj różne zdarzenia
my_logger.info("START procesu ETL")
my_logger.debug("Szczegóły debugowania - to nie zostanie zapisane (poziom INFO)")

# Symulacja operacji ETL
try:
    data_count = 1250
    my_logger.info(f"Załadowano {data_count} wierszy z źródła")
    
    # Symulacja transformacji
    my_logger.info("Rozpoczęto transformację danych")
    transformed = data_count * 0.95  # 5% odrzucone
    my_logger.warning(f"Odrzucono {int(data_count - transformed)} wierszy z błędami")
    
    # Symulacja zapisu
    my_logger.info(f"Zapisano {int(transformed)} wierszy do docelowej tabeli")
    my_logger.info("KONIEC procesu ETL - sukces")
    
    print(f"✓ Symulacja ETL zakończona")
    print(f"✓ Wszystkie logi zapisane automatycznie w: {log_file5}")
    print(f"  Zobacz: tail {log_file5}\n")
    
except Exception as e:
    my_logger.error(f"Błąd krytyczny w procesie ETL: {e}", exc_info=True)
    print(f"✗ Błąd: {e}")


# ============================================================================
# PODSUMOWANIE
# ============================================================================
print("=" * 70)
print("PODSUMOWANIE")
print("=" * 70)
print("""
1. NAJPROSTSZA (zalecana dla większości przypadków):
   from zus_db_utils.logging_config import configure_file_logging
   configure_file_logging("/path/to/logfile.log")

2. Użyj standardowego loggera:
   import logging
   logger = logging.getLogger("zus_db_utils")
   logger.info("Twoja wiadomość")

3. AggWriter i AggReader automatycznie logują swoje operacje
   (poziom INFO) - nie musisz nic robić, tylko skonfiguruj plik.

4. Poziomy logowania:
   - DEBUG: szczegółowe informacje diagnostyczne
   - INFO: normalne operacje (domyślny)
   - WARNING: ostrzeżenia
   - ERROR: błędy
   - CRITICAL: krytyczne błędy

5. Zobacz wygenerowane pliki logów w:
""")
for f in sorted(log_dir.glob("example_*.log")):
    print(f"   - {f}")

print("\nKomenda do podglądu:")
print(f"   tail -f {log_dir}/example_*.log")
print("=" * 70)
