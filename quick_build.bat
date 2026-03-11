@echo off
chcp 65001 >nul
echo ==========================================
echo   GESTOR DE STOCK PRO - BUILD RAPIDO
echo ==========================================
echo.

python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python no encontrado. Instala Python 3.10+
    pause
    exit /b 1
)

echo Ejecutando build.py (incluye .exe, instalador y paquete portable con bundle de datos)...
python build.py
if errorlevel 1 (
    echo.
    echo [ERROR] Build fallido.
    pause
    exit /b 1
)

echo.
echo [OK] Build completado.
echo Revisa la carpeta dist\ para usar GestionStockPro_Portable.zip en migracion a otro PC.
echo.
pause
