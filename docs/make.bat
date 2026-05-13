@ECHO OFF
pushd %~dp0

if "%SPHINXBUILD%" == "" (
    set SPHINXBUILD=sphinx-build
)
set SOURCEDIR=.
set BUILDDIR=_build

if "%1" == "" goto help
if "%1" == "html" goto html
if "%1" == "clean" goto clean
if "%1" == "linkcheck" goto linkcheck

:help
%SPHINXBUILD% -M help %SOURCEDIR% %BUILDDIR% %SPHINXOPTS% %O%
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

:linkcheck
%SPHINXBUILD% -M linkcheck %SOURCEDIR% %BUILDDIR% %SPHINXOPTS% %O%
goto end

:end
popd
