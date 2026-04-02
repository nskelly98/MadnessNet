@echo on
setlocal

set PIPE=C:\GoopNet\pipeline
set PY=python

if not exist C:\GoopNet\logs mkdir C:\GoopNet\logs
set LOG=C:\GoopNet\logs\night_%DATE:~10,4%%DATE:~4,2%%DATE:~7,2%.log
set LOCK=%PIPE%\90_runs\.night.lock

if exist "%LOCK%" (
  echo Night job already running. Exiting.
  exit /b 0
)
echo lock > "%LOCK%"

echo ===== NIGHT RUN START %DATE% %TIME% ===== > "%LOG%"

cd /d %PIPE%

%PY% "%PIPE%\10_ingest\ingest_ncaa_api_recent.py" --days 7 >> "%LOG%" 2>&1
if errorlevel 1 goto cleanup

%PY% "%PIPE%\10_ingest\oddsapi_pull_odds_today.py" >> "%LOG%" 2>&1
if errorlevel 1 goto cleanup

%PY% "%PIPE%\40_bets\grade_recommendations.py" >> "%LOG%" 2>&1
if errorlevel 1 goto cleanup

:cleanup
del "%LOCK%"
echo ===== NIGHT RUN END %DATE% %TIME% ===== >> "%LOG%"
endlocal
