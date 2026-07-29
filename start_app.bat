@echo off
cd /d "%~dp0"

echo ============================================
echo  Welcome to PIM QC Tool - Startup 
echo  In Case of Issue Contact Charles Kireki
echo ============================================
echo.

:: Check Python 3.11 is available
py -3.11 --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Python 3.11 not found. Please install it from python.org
    pause
    exit /b 1
)

echo [1/4] Installing / updating requirements...
echo "Please be patient, this may take a few ma while if running for first time"
py -3.11 -m pip install -r requirements.txt --quiet --disable-pip-version-check
if %errorlevel% neq 0 (
    echo.
    echo WARNING: Some packages may have failed to install.
    echo The app will still attempt to start.
)
echo Done.
echo.

:: Find a free port starting at 8505
echo [2/4] Finding available port...
set PORT=8505

:find_port
:: Only LISTENING sockets mean the port is taken. Without this filter an
:: ESTABLISHED connection *to* :8505 on another host also matched, so the
:: script would skip a port that was actually free.
netstat -ano -p TCP | findstr ":%PORT% " | findstr /I "LISTENING" >nul 2>&1
if %errorlevel%==0 (
    set /a PORT+=1
    goto find_port
)
echo Using port %PORT%
echo.

:: Start the validation API server if Redis is running (much faster:
:: validation runs out-of-process and results are cached in Redis, so
:: re-uploading the same file is instant and the UI never freezes).
echo [3/4] Checking for Redis / starting API server...
netstat -ano | findstr ":6379 " >nul 2>&1
if %errorlevel%==0 (
    netstat -ano | findstr ":8000 " >nul 2>&1
    if %errorlevel%==0 (
        echo API port 8000 already in use - assuming API server is running.
    ) else (
        echo Redis detected - starting validation API server on port 8000...
        set VALIDATOR_WORKERS=8
        start "PIM QC API Server" /min py -3.11 run_api.py
    )
) else (
    echo Redis not detected on port 6379 - the app will validate in direct mode.
    echo TIP: install and start Redis, then rerun this script for faster cached runs.
)
echo.

echo [4/4] Launching app...
echo.
echo     The app will open at:  http://localhost:%PORT%
echo     Loading the rules and the category model takes a few seconds.
echo     Leave this window open - closing it stops the app.
echo.

:: Wait for the server to actually answer before opening the browser.
:: This used to be "timeout /t 3" followed by an unconditional start, which
:: opened the browser on a dead port whenever the app took longer than three
:: seconds to boot - and it usually does, because startup loads a 25MB rules
:: database and a 27MB category model. The result was a connection-refused
:: page that the user had to refresh by hand.
:: Runs in the background so Streamlit itself can stay in the foreground.
start "" /min powershell -NoProfile -WindowStyle Hidden -Command ^
  "for($i=0; $i -lt 240; $i++){ try { $r = Invoke-WebRequest -Uri 'http://localhost:%PORT%/_stcore/health' -UseBasicParsing -TimeoutSec 2; if($r.StatusCode -eq 200){ Start-Process 'http://localhost:%PORT%'; break } } catch { Start-Sleep -Milliseconds 500 } }"

:: Start Streamlit with Python 3.11
py -3.11 -m streamlit run streamlit_app.py ^
--server.port %PORT% ^
--server.maxUploadSize 500 ^
--server.headless true

echo.
echo The app has stopped.
pause
