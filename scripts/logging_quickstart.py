#!/home/ubuntu/ZUS/.venv/bin/python
"""Szybki start - logging w zus_db_utils (COPY-PASTE READY).

To jest minimalny przykład gotowy do skopiowania do własnego kodu.
"""
import logging
import sys
from pathlib import Path

# Dodaj katalog projektu do PYTHONPATH (jeśli nie zainstalowano pip install -e .)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from zus_db_utils.logging_config import configure_file_logging


# ============================================================================
# KROK 1: Skonfiguruj plik logu (TYLKO RAZ na początku skryptu)
# ============================================================================
log_file = PROJECT_ROOT / "logs" / "moj_skrypt.log"
log_file.parent.mkdir(exist_ok=True)  # Stwórz katalog jeśli nie istnieje

configure_file_logging(
    str(log_file),
    level=logging.INFO,  # DEBUG | INFO | WARNING | ERROR | CRITICAL
    rotate=True,         # Automatyczna rotacja (zalecane)
)


# ============================================================================
# KROK 2: Pobierz logger i używaj go w swoim kodzie
# ============================================================================
logger = logging.getLogger("zus_db_utils.moj_modul")

# Przykłady logowania:
logger.debug("To jest DEBUG - szczegółowe informacje dla developerów")
logger.info("To jest INFO - normalne operacje")
logger.warning("To jest WARNING - ostrzeżenie")
logger.error("To jest ERROR - błąd")
logger.critical("To jest CRITICAL - krytyczny błąd")

# Logowanie z parametrami (szybsze niż f-string)
user_id = 12345
logger.info("Użytkownik %s wykonał operację", user_id)

# Logowanie wyjątków ze stack trace
try:
    result = 10 / 0
except Exception as e:
    logger.error("Wystąpił błąd: %s", e, exc_info=True)  # exc_info=True doda stack trace


# ============================================================================
# KROK 3: Sprawdź logi
# ============================================================================
print(f"\n✓ Logi zapisane w: {log_file}")
print(f"  Komenda: tail -f {log_file}\n")

# Wyświetl zawartość
print("=" * 70)
print("ZAWARTOŚĆ LOGU:")
print("=" * 70)
with open(log_file) as f:
    print(f.read())
