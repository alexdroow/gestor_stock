@echo off
setlocal
cd /d "%~dp0"

REM Modo dev rapido para app.py
start "" "C:\Visual Studio Code\gestor_stock\venv\Scripts\python.exe" app.py
timeout /t 2 >nul
start "" http://127.0.0.1:5000
