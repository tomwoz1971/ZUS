@ECHO OFF
pushd %~dp0

REM Konfiguracja Sphinx
if "%SPHINXBUILD%" == "" (
    set SPHINXBUILD=sphinx-build
)
set SOURCEDIR=.
set BUILDDIR=_build

REM Sprawdź czy podano argument
if "%1" == "" goto help

REM Specjalne traktowanie dla clean (nie wywołuje Sphinx)
if "%1" == "clean" goto clean

REM Specjalne traktowanie dla html (niestandardowy komunikat)
if "%1" == "html" goto html

REM Wszystkie inne targety - uniwersalny handler
goto universal

:help
echo Uzycie: make.bat [target]
echo.
echo Dostepne targety (buildery Sphinx):
echo   html        - standalone HTML files
echo   dirhtml     - HTML files named index.html in directories
echo   singlehtml  - single large HTML file
echo   pickle      - pickle files
echo   json        - JSON files
echo   htmlhelp    - HTML files and HTML help project
echo   qthelp      - HTML files and qthelp project
echo   devhelp     - HTML files and Devhelp project
echo   epub        - epub ebook
echo   latex       - LaTeX files (PAPER=a4 lub PAPER=letter)
echo   latexpdf    - LaTeX and PDF files (via pdflatex)
echo   latexpdfja  - LaTeX files (via platex/dvipdfmx)
echo   text        - plain text files
echo   man         - manual pages
echo   texinfo     - Texinfo files
echo   info        - Texinfo files and run makeinfo
echo   gettext     - PO message catalogs
echo   changes     - overview of changes/added/deprecated
echo   xml         - Docutils-native XML files
echo   pseudoxml   - pseudoxml-XML files
echo   linkcheck   - check all external links
echo   doctest     - run doctests in documentation
echo   coverage    - check documentation coverage
echo.
echo Specjalne targety:
echo   clean       - remove everything in build directory
echo   help        - show this message
goto end

:html
%SPHINXBUILD% -M html %SOURCEDIR% %BUILDDIR% %SPHINXOPTS% %O%
if errorlevel 1 exit /b 1
echo.
echo Build gotowy: %BUILDDIR%\html\index.html
goto end

:clean
if exist %BUILDDIR% rmdir /s /q %BUILDDIR%
echo Katalog %BUILDDIR% usuniety.
goto end

:universal
REM Uniwersalny handler dla wszystkich innych targetów Sphinx
%SPHINXBUILD% -M %1 %SOURCEDIR% %BUILDDIR% %SPHINXOPTS% %O%
if errorlevel 1 exit /b 1
echo.
echo Build '%1' gotowy w: %BUILDDIR%\%1\
goto end

:end
popd
