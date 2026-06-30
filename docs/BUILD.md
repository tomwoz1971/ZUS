# Budowanie dokumentacji

Ten katalog zawiera dokumentację projektu `zus_db_utils` w formacie Sphinx.

## 🚀 Szybki start

### Linux/Mac:
```bash
make html        # HTML z nawigacją
make singlehtml  # Pojedynczy plik HTML
make latexpdf    # PDF (wymaga LaTeX)
```

### Windows:
```cmd
make.bat html        # HTML z nawigacją
make.bat singlehtml  # Pojedynczy plik HTML
make.bat latexpdf    # PDF (wymaga LaTeX)
```

## 📋 Wszystkie dostępne formaty

| Format | Komenda | Opis | Wynik |
|--------|---------|------|-------|
| **HTML** | `make html` | Strony HTML z nawigacją | `_build/html/` |
| **SingleHTML** | `make singlehtml` | Cała dokumentacja w 1 pliku HTML | `_build/singlehtml/` |
| **LaTeX** | `make latex` | Pliki LaTeX | `_build/latex/` |
| **PDF** | `make latexpdf` | PDF przez LaTeX (wymaga `pdflatex`) | `_build/latex/*.pdf` |
| **Text** | `make text` | Zwykły tekst | `_build/text/` |
| **Man pages** | `make man` | Strony podręcznika Unix | `_build/man/` |
| **ePub** | `make epub` | E-book ePub | `_build/epub/*.epub` |
| **JSON** | `make json` | Dane strukturalne JSON | `_build/json/` |
| **XML** | `make xml` | Docutils XML | `_build/xml/` |

### Formaty specjalistyczne

| Format | Komenda | Opis |
|--------|---------|------|
| **dirhtml** | `make dirhtml` | HTML jako `index.html` w katalogach |
| **htmlhelp** | `make htmlhelp` | Windows HTML Help (.chm) |
| **qthelp** | `make qthelp` | Qt Help Collection |
| **devhelp** | `make devhelp` | Devhelp (GNOME) |
| **texinfo** | `make texinfo` | Texinfo (GNU) |
| **pickle** | `make pickle` | Pickle (serializacja Python) |

### Narzędzia diagnostyczne

| Komenda | Opis |
|---------|------|
| `make linkcheck` | Sprawdź poprawność zewnętrznych linków |
| `make doctest` | Uruchom doctesty w dokumentacji |
| `make coverage` | Sprawdź pokrycie dokumentacji |
| `make changes` | Przegląd zmian/deprecated |

### Inne

| Komenda | Opis |
|---------|------|
| `make clean` | Usuń katalog `_build/` |
| `make help` | Pokaż dostępne targety |

---

## 📦 Wymagania

### Podstawowe (wszystkie formaty poza PDF):
```bash
pip install -e ".[docs]"  # sphinx>=7.0, furo>=2024.1.29
```

### PDF (latexpdf):
```bash
# Ubuntu/Debian:
sudo apt-get install texlive-latex-recommended texlive-fonts-recommended texlive-latex-extra latexmk

# macOS:
brew install --cask mactex

# Windows:
# Pobierz i zainstaluj MiKTeX: https://miktex.org/download
```

---

## 🔧 Zaawansowane użycie

### Budowanie z ostrzeżeniami jako błędami:
```bash
make SPHINXOPTS="-W" html
```

### Ustawienie formatu papieru dla PDF:
```bash
make PAPER=a4 latexpdf     # A4 (domyślny)
make PAPER=letter latexpdf # Letter (US)
```

### Verbose build:
```bash
make SPHINXOPTS="-v" html
```

### Równoległe budowanie:
```bash
make SPHINXOPTS="-j auto" html
```

---

## 📂 Struktura wyjściowa

Po zbudowaniu dokumentacji:

```
_build/
├── html/           # make html
│   ├── index.html
│   ├── api/
│   └── ...
├── singlehtml/     # make singlehtml
│   └── index.html  (cała dokumentacja)
├── latex/          # make latex
│   ├── zus_db_utils.tex
│   └── zus_db_utils.pdf  (po make latexpdf)
├── text/           # make text
│   ├── index.txt
│   └── ...
├── epub/           # make epub
│   └── zus_db_utils.epub
└── man/            # make man
    └── zus_db_utils.1
```

---

## 🌐 Przeglądanie dokumentacji

### HTML (lokalnie):
```bash
# Linux/Mac:
make html && open _build/html/index.html

# Windows:
make.bat html
start _build\html\index.html
```

### Man page:
```bash
make man
man ./_build/man/zus_db_utils.1
```

### ePub:
```bash
make epub
# Otwórz plik _build/epub/zus_db_utils.epub w czytniiku e-booków
```

---

## ⚠️ Typowe problemy

### Problem: `latexpdf` kończy się błędem

**Rozwiązanie:**
1. Sprawdź czy LaTeX jest zainstalowany: `pdflatex --version`
2. Jeśli brak, zainstaluj (patrz sekcja "Wymagania")
3. Najpierw zbuduj `make latex`, potem ręcznie w katalogu `_build/latex/`:
   ```bash
   cd _build/latex
   make
   ```

### Problem: Brak polskich znaków w PDF

**Rozwiązanie:** Zainstaluj dodatkowe czcionki LaTeX:
```bash
sudo apt-get install texlive-lang-polish texlive-fonts-extra
```

### Problem: `linkcheck` zgłasza wiele broken links

To może być spowodowane rate limiting lub timeoutami. Uruchom ponownie:
```bash
make linkcheck
```

---

## 📚 Więcej informacji

- Oficjalna dokumentacja Sphinx: https://www.sphinx-doc.org/
- Motyw Furo: https://pradyunsg.me/furo/
- reStructuredText: https://docutils.sourceforge.io/rst.html
