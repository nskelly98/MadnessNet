@echo on
setlocal

set PIPE=C:\GoopNet\pipeline
set PY=python

if not exist C:\GoopNet\logs mkdir C:\GoopNet\logs
set LOG=C:\GoopNet\logs\morning_%DATE:~10,4%%DATE:~4,2%%DATE:~7,2%.log
set LOCK=%PIPE%\90_runs\.morning.lock

if exist "%LOCK%" (
  echo Morning job already running. Exiting.
  exit /b 0
)
echo lock > "%LOCK%"

echo ===== MORNING RUN START %DATE% %TIME% ===== > "%LOG%"

cd /d %PIPE%

%PY% "%PIPE%\10_ingest\ingest_ncaa_api_recent.py" --days 1 >> "%LOG%" 2>&1
if errorlevel 1 goto cleanup

%PY% "%PIPE%\20_features\compute_rolling_stats.py" >> "%LOG%" 2>&1
if errorlevel 1 goto cleanup

%PY% "%PIPE%\20_features\build_model_games_enriched.py" >> "%LOG%" 2>&1
if errorlevel 1 goto cleanup

%PY% "%PIPE%\10_ingest\oddsapi_pull_odds_today.py" >> "%LOG%" 2>&1
if errorlevel 1 goto cleanup

%PY% "%PIPE%\40_bets\build_team_map_oddsapi.py" >> "%LOG%" 2>&1
if errorlevel 1 goto cleanup

%PY% "%PIPE%\40_bets\generate_daily_bets_live_shadow.py" >> "%LOG%" 2>&1
if errorlevel 1 goto cleanup

%PY% "%PIPE%\40_bets\notify_daily_report.py" >> "%LOG%" 2>&1

:cleanup
del "%LOCK%"
echo ===== MORNING RUN END %DATE% %TIME% ===== >> "%LOG%"
endlocal
