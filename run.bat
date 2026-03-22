@echo off
cd /d "%~dp0"

:: Всё пишем в лог-файл
set LOG=run_log.txt
echo. > %LOG%
echo [%DATE% %TIME%] Запуск run.bat >> %LOG%
echo [%DATE% %TIME%] Папка: %CD% >> %LOG%

where python >> %LOG% 2>&1
if errorlevel 1 (
    if exist "C:\Python314\python.exe" (
        set PYTHON=C:\Python314\python.exe
    ) else (
        echo [%DATE% %TIME%] ОШИБКА: python не найден >> %LOG%
        type %LOG%
        pause
        exit /b 1
    )
) else (
    set PYTHON=python
)

%PYTHON% --version >> %LOG% 2>&1
echo [%DATE% %TIME%] Python: OK >> %LOG%

%PYTHON% -c "import fastapi, uvicorn, httpx; print('deps OK')" >> %LOG% 2>&1
if errorlevel 1 (
    echo [%DATE% %TIME%] Зависимости не найдены, устанавливаем... >> %LOG%
    %PYTHON% -m pip install fastapi uvicorn httpx beautifulsoup4 lxml apscheduler aiofiles pydantic python-dotenv >> %LOG% 2>&1
)

for /f "tokens=5" %%a in ('netstat -ano 2^>nul ^| findstr ":8000 " ^| findstr LISTENING') do (
    echo [%DATE% %TIME%] Останавливаем старый сервер PID=%%a >> %LOG%
    taskkill /PID %%a /F >nul 2>&1
)

echo [%DATE% %TIME%] Запускаем main.py... >> %LOG%
echo Лог: %CD%\%LOG%
echo Дашборд: http://localhost:8000/app

cd backend
%PYTHON% main.py >> ..\%LOG% 2>&1

echo [%DATE% %TIME%] Сервер остановлен >> ..\%LOG%
