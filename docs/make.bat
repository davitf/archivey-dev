@ECHO OFF

pushd %~dp0

REM Command file for Sphinx documentation

if "%SPHINXBUILD%" == "" (
	set SPHINXBUILD=sphinx-build
)
set SOURCEDIR=.
set BUILDDIR=_build
set SPHINXOPTS=%*

if "%1" == "help" (
	%SPHINXBUILD% -M help %SOURCEDIR% %BUILDDIR% %SPHINXOPTS%
	goto end
)
if "%1" == "html" (
	%SPHINXBUILD% -b html %SOURCEDIR% %BUILDDIR% %SPHINXOPTS%
	goto end
)
if "%1" == "clean" (
	%SPHINXBUILD% -M clean %SOURCEDIR% %BUILDDIR% %SPHINXOPTS%
	goto end
)
if "%1" == "latexpdf" (
	%SPHINXBUILD% -b latex %SOURCEDIR% %BUILDDIR% %SPHINXOPTS%
	%SPHINXBUILD% -b latex %SOURCEDIR% %BUILDDIR% %SPHINXOPTS%
	goto end
)

%SPHINXBUILD% -M %1 %SOURCEDIR% %BUILDDIR% %SPHINXOPTS%

:end
popd
