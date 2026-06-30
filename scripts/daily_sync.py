#!/home/ubuntu/ZUS/.venv/bin/python
"""Przykładowy skrypt synchronizacji danych - uruchamiany przez cron.

Ten skrypt demonstruje najlepsze praktyki dla cron jobs:
- Pełne ścieżki absolutne
- Obsługa błędów i logowanie
- Wyraźne komunikaty o sukcesie/porażce
"""
import sys
from datetime import datetime
from pathlib import Path

# Dodaj katalog główny projektu do PYTHONPATH
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

# Teraz możemy importować z zus_db_utils
from zus_db_utils import AggWriter
from zus_db_utils.logging_config import configure_file_logging
import logging


def main() -> int:
    """Główna funkcja skryptu - zwraca kod wyjścia (0 = sukces, >0 = błąd)."""
    # Konfiguracja logowania - logi do pliku w /home/ubuntu/ZUS/logs/
    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"daily_sync_{datetime.now():%Y%m%d}.log"
    
    configure_file_logging(str(log_file), level=logging.INFO, rotate=True)
    
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] START: daily_sync.py", file=sys.stderr)
    print(f"Logi zapisywane do: {log_file}", file=sys.stderr)
    
    try:
        # === PRZYKŁADOWA OPERACJA ===
        # W prawdziwym skrypcie tutaj byłby kod synchronizacji danych
        
        # Upewnij się że katalog data/ istnieje
        data_dir = PROJECT_ROOT / "data"
        data_dir.mkdir(exist_ok=True)
        
        # Przykład: zapis heartbeat do pliku tekstowego
        heartbeat_file = data_dir / "cron_heartbeat.txt"
        timestamp = datetime.now().isoformat()
        
        with open(heartbeat_file, "a") as f:
            f.write(f"{timestamp} - daily_sync.py executed successfully\n")
        
        print(f"Heartbeat zapisany do {heartbeat_file}", file=sys.stderr)
        
        # Możesz tutaj dodać właściwą logikę synchronizacji, np.:
        # writer = AggWriter(backend="postgres", strategy="upsert", credential="dwh", keys=["id"])
        # result = writer.write(df, "target_table")
        # print(f"Zsynchronizowano {result.inserted + result.updated} wierszy")
        
        print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] SUKCES", file=sys.stderr)
        return 0
        
    except Exception as e:
        print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] BŁĄD: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
