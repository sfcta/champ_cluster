@echo off

python Y:\champ\util\bin\cluster.py %1
if ERRORLEVEL 2 goto :error

goto :done

:error
echo Job failed; press CTRL-C to quit, or
pause

:done
echo.

