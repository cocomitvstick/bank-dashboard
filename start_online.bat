@echo off
chcp 65001 >nul
title ПСБ Analytics — Онлайн

echo ============================================
echo   ПСБ Analytics — Публичный доступ через ngrok
echo ============================================
echo.

:: Проверяем ngrok
where ngrok >nul 2>&1
if errorlevel 1 (
    echo [ОШИБКА] ngrok не найден в PATH.
    echo Скачайте с https://ngrok.com и добавьте в PATH.
    pause
    exit /b 1
)

:: Запускаем сервер в фоне
echo [1/2] Запуск сервера...
start "ПСБ Backend" /min cmd /c "cd /d %~dp0backend && python main.py"

:: Ждём пока сервер стартует
timeout /t 4 /nobreak >nul

:: Запускаем ngrok
echo [2/2] Запуск ngrok...
echo.
echo Дождитесь строки "Forwarding https://..." — это и есть публичный URL.
echo Нажмите Ctrl+C чтобы остановить.
echo.
ngrok http 8000
