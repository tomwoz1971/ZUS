# Skrypty cron dla `zus_db_utils`

Ten katalog zawiera przykładowe skrypty do uruchamiania przez **cron** w środowisku produkcyjnym.

## 📁 Pliki

| Plik | Opis |
|------|------|
| `daily_sync.py` | Przykładowy skrypt synchronizacji (Python) |
| `run_daily_sync.sh` | Wrapper bash dla `daily_sync.py` |
| `example_crontab.txt` | Przykładowa konfiguracja crontab |
| `README.md` | Ta dokumentacja |

---

## 🚀 3 metody uruchamiania Python z venv w cron

### **Metoda 1: Bezpośrednie wywołanie interpretera** ⭐ **ZALECANA**

Najprostrza i najbezpieczniejsza — bezpośrednie wywołanie `python` z venv.

**Wpis w crontab:**
```cron
0 2 * * * /home/ubuntu/ZUS/.venv/bin/python /home/ubuntu/ZUS/scripts/daily_sync.py >> /home/ubuntu/ZUS/logs/cron_stdout.log 2>&1
```

**Zalety:**
- ✅ Najprostsza — tylko jedna linia
- ✅ Bez dodatkowych plików
- ✅ Pełna kontrola nad ścieżkami
- ✅ Nie trzeba `chmod +x`

**Wady:**
- ⚠️ Długa linia w crontab

---

### **Metoda 2: Shebang w skrypcie Python**

Skrypt Python ma shebang `#!/home/ubuntu/ZUS/.venv/bin/python` i jest wykonywalny.

**Wpis w crontab:**
```cron
0 2 * * * /home/ubuntu/ZUS/scripts/daily_sync.py >> /home/ubuntu/ZUS/logs/cron_stdout.log 2>&1
```

**Zalety:**
- ✅ Krótsza linia w crontab
- ✅ Skrypt "wie" który Python użyć

**Wady:**
- ⚠️ Wymaga `chmod +x daily_sync.py`
- ⚠️ Shebang z pełną ścieżką do venv (trudniej przenosić między środowiskami)

**Konfiguracja:**
```bash
# Dodaj shebang na początku daily_sync.py:
#!/home/ubuntu/ZUS/.venv/bin/python

# Nadaj uprawnienia:
chmod +x /home/ubuntu/ZUS/scripts/daily_sync.py
```

---

### **Metoda 3: Wrapper bash aktywujący venv**

Bash script (`run_daily_sync.sh`) uruchamia Python z venv.

**Wpis w crontab:**
```cron
0 2 * * * /home/ubuntu/ZUS/scripts/run_daily_sync.sh >> /home/ubuntu/ZUS/logs/cron_stdout.log 2>&1
```

**Zalety:**
- ✅ Możliwość dodatkowej logiki (lock file, pre/post-processing)
- ✅ Walidacja środowiska przed uruchomieniem
- ✅ Łatwiejsze debugowanie

**Wady:**
- ⚠️ Dodatkowy plik do utrzymania
- ⚠️ Wymaga `chmod +x run_daily_sync.sh`

**Konfiguracja:**
```bash
chmod +x /home/ubuntu/ZUS/scripts/run_daily_sync.sh
```

---

## ⚙️ Instalacja w crontab

### 1. Edytuj crontab użytkownika
```bash
crontab -e
```

### 2. Dodaj wpis (wybierz metodę 1, 2 lub 3)
```cron
# Codziennie o 2:00 AM (metoda 1 - ZALECANA)
0 2 * * * /home/ubuntu/ZUS/.venv/bin/python /home/ubuntu/ZUS/scripts/daily_sync.py >> /home/ubuntu/ZUS/logs/cron_stdout.log 2>&1
```

### 3. Zapisz i wyjdź
- vim: `:wq`
- nano: `Ctrl+X`, `Y`, `Enter`

### 4. Weryfikacja
```bash
# Zobacz aktualny crontab
crontab -l

# Sprawdź czy cron działa
sudo systemctl status cron

# Monitoruj logi systemowe cron (Debian/Ubuntu)
grep CRON /var/log/syslog | tail -20

# Sprawdź logi skryptu
tail -f /home/ubuntu/ZUS/logs/daily_sync_*.log
```

---

## 📊 Przykłady harmonogramów

| Harmonogram | Składnia cron | Opis |
|-------------|---------------|------|
| Co 15 minut | `*/15 * * * *` | Każde :00, :15, :30, :45 |
| Co godzinę | `0 * * * *` | Każda pełna godzina (xx:00) |
| Codziennie o 2 AM | `0 2 * * *` | Każdego dnia o 2:00 |
| W dni powszednie o 6 AM | `0 6 * * 1-5` | Poniedziałek-piątek, 6:00 |
| 1. dnia miesiąca o 1 AM | `0 1 1 * *` | Pierwszy dzień miesiąca, 1:00 |
| W niedzielę o 3 AM | `0 3 * * 0` | Niedziela, 3:00 |
| Co 30 min (8-18, pn-pt) | `*/30 8-18 * * 1-5` | Business hours |

**Składnia:**
```
┌─── minuta (0-59)
│ ┌─── godzina (0-23)
│ │ ┌─── dzień miesiąca (1-31)
│ │ │ ┌─── miesiąc (1-12)
│ │ │ │ ┌─── dzień tygodnia (0-7, 0/7=niedziela)
│ │ │ │ │
* * * * * komenda
```

---

## 📝 Przekierowanie wyjścia

| Wzorzec | Opis |
|---------|------|
| `>> file.log 2>&1` | stdout + stderr do tego samego pliku (append) |
| `> file.log 2>&1` | stdout + stderr do tego samego pliku (nadpisanie) |
| `2>> errors.log` | tylko stderr do pliku (append) |
| `>> out.log 2>> err.log` | stdout i stderr do osobnych plików |
| `> /dev/null 2>&1` | wyciszenie (brak logów) |

**Zalecane:** `>> /home/ubuntu/ZUS/logs/cron_stdout.log 2>&1`

---

## 🧪 Testowanie przed dodaniem do cron

### 1. Uruchom skrypt ręcznie
```bash
# Metoda 1 - bezpośrednio
/home/ubuntu/ZUS/.venv/bin/python /home/ubuntu/ZUS/scripts/daily_sync.py

# Metoda 2 - jako executable
/home/ubuntu/ZUS/scripts/daily_sync.py

# Metoda 3 - przez wrapper
/home/ubuntu/ZUS/scripts/run_daily_sync.sh
```

### 2. Sprawdź kod wyjścia
```bash
/home/ubuntu/ZUS/.venv/bin/python /home/ubuntu/ZUS/scripts/daily_sync.py
echo $?  # Powinno być 0 (sukces)
```

### 3. Sprawdź logi
```bash
# Logi aplikacyjne (z configure_file_logging)
tail -f /home/ubuntu/ZUS/logs/daily_sync_*.log

# Heartbeat
tail -f /home/ubuntu/ZUS/data/cron_heartbeat.txt
```

### 4. Symulacja środowiska cron (bez zmiennych ENV)
```bash
# Cron ma minimalne ENV - testuj bez PATH itp.
env -i /home/ubuntu/ZUS/.venv/bin/python /home/ubuntu/ZUS/scripts/daily_sync.py
```

---

## 🔒 Zapobieganie nakładaniu się zadań (lock file)

Jeśli skrypt może działać dłużej niż okres cron, użyj lock file:

```python
# Na początku main() w daily_sync.py
import fcntl

lock_file = PROJECT_ROOT / "data" / "daily_sync.lock"
lock_fd = open(lock_file, "w")

try:
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
except BlockingIOError:
    print("Inna instancja już działa, wychodzimy", file=sys.stderr)
    sys.exit(0)

# ... reszta skryptu ...
```

Lub użyj `flock` w crontab:
```cron
0 2 * * * flock -n /tmp/daily_sync.lock /home/ubuntu/ZUS/.venv/bin/python /home/ubuntu/ZUS/scripts/daily_sync.py
```

---

## 📧 Email z wynikami cron

### Włącz email (domyślnie cron wysyła przy błędzie lub stdout)
```cron
MAILTO=admin@example.com
0 2 * * * /home/ubuntu/ZUS/.venv/bin/python /home/ubuntu/ZUS/scripts/daily_sync.py
```

### Wyłącz email
```cron
MAILTO=""
0 2 * * * /home/ubuntu/ZUS/.venv/bin/python /home/ubuntu/ZUS/scripts/daily_sync.py > /dev/null 2>&1
```

---

## 🛠️ Debugowanie problemów z cron

### Problem: Skrypt działa ręcznie, ale nie z cron

**Przyczyny:**
1. **Brak PATH** — cron ma minimal PATH (`/usr/bin:/bin`)
2. **Brak zmiennych ENV** — cron nie ładuje `.bashrc`, `.profile`
3. **Inny katalog roboczy** — cron domyślnie uruchamia w `$HOME`
4. **Brak uprawnień** — plik nie jest wykonywalny (`chmod +x`)

**Rozwiązanie:**
```bash
# ✅ ZAWSZE używaj pełnych ścieżek absolutnych
/home/ubuntu/ZUS/.venv/bin/python /home/ubuntu/ZUS/scripts/daily_sync.py

# ✅ Ustaw zmienne ENV w crontab (opcjonalnie)
SHELL=/bin/bash
PATH=/usr/local/bin:/usr/bin:/bin

# ✅ Przekierowuj stdout/stderr do pliku
>> /home/ubuntu/ZUS/logs/cron_debug.log 2>&1
```

### Sprawdź logi systemowe cron
```bash
# Debian/Ubuntu
grep CRON /var/log/syslog | tail -50

# CentOS/RHEL
grep CRON /var/log/cron | tail -50
```

### Sprawdź czy zadanie zostało zarejestrowane
```bash
crontab -l
```

### Sprawdź status demona cron
```bash
sudo systemctl status cron       # Debian/Ubuntu
sudo systemctl status crond      # CentOS/RHEL
```

---

## ✅ Checklist przed produkcją

- [ ] Skrypt działa poprawnie ręcznie
- [ ] Pełne ścieżki absolutne (Python, skrypt, logi)
- [ ] Przekierowanie stdout/stderr (`>> logfile 2>&1`)
- [ ] Logowanie do pliku (przez `configure_file_logging`)
- [ ] Obsługa błędów i zwracanie kodów wyjścia (0=OK, >0=błąd)
- [ ] Testowanie w środowisku bez ENV (`env -i ...`)
- [ ] Lock file jeśli skrypt może się nakładać
- [ ] Monitorowanie logów po pierwszym uruchomieniu
- [ ] Rotacja logów (logrotate lub `rotate=True` w `configure_file_logging`)
- [ ] Alert/monitoring jeśli skrypt się wywali

---

## 📚 Więcej informacji

- **Dokumentacja cron:** `man 5 crontab`
- **Generator crontab:** https://crontab.guru/
- **Projekt zus_db_utils:** `/home/ubuntu/ZUS/README.md`
