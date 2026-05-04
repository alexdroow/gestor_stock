# Backup y Recuperacion GestorStock (Local + PythonAnywhere)

## Objetivo
- Backup semanal completo de lo desplegado en servidor (codigo + base de datos + archivos).
- Respaldo local automatico en Windows cada domingo 23:00.
- Procedimiento claro de recuperacion.

## Requisitos previos
1. Tener `ssh` y `scp` disponibles en Windows (OpenSSH).
2. Tener acceso SSH a PythonAnywhere con clave (sin pedir password interactiva).
3. Validar usuario/host/ruta remota en `tools/backup_pythonanywhere_weekly.ps1`.

## Instalacion de tarea semanal
Ejecutar en PowerShell (como usuario que dejara la tarea activa):

```powershell
tools\install_weekly_backup_task.ps1
```

Esto crea la tarea `GestorStockWeeklyFullBackup` para domingo 23:00.

## Ejecutar backup manual
```powershell
tools\backup_pythonanywhere_weekly.ps1
```

Salida local esperada:
- `backups/pythonanywhere_full/YYYY-Www/gestor_stock_full_YYYYMMDD_HHMMSS.tar.gz`
- `manifest_*.json`
- `*.sha256`

## Restauracion local (preview)
```powershell
tools\restore_from_full_backup.ps1 -BackupArchive "C:\ruta\gestor_stock_full_YYYYMMDD_HHMMSS.tar.gz"
```

Se extrae en `_restore_preview` para revisar antes de sobreescribir.

## Recuperacion recomendada si hay corrupcion
1. Detener cambios/escrituras.
2. Extraer ultimo backup valido con `restore_from_full_backup.ps1`.
3. Verificar contenido clave:
   - `stock.db`
   - `templates/`, `static/`, `app.py`, `database.py`
   - `facturas/`
4. Reemplazar en entorno afectado.
5. Reiniciar servicio web y validar rutas criticas (`/agenda`, `/ventas`, `/insumos`, `/facturas`).

## Respaldo local de facturas por cliente/mes/anio
Adicionalmente, cada subida de factura crea espejo en:

`DATA_DIR/facturas_respaldo_local/<cliente>/<YYYY>/<MM>/`

Con un `.json` por archivo para trazabilidad.
