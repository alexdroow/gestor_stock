# Migracion a Otro PC (HACCP + Smart Life)

Este proyecto ya genera un paquete portable con datos para mover todo a otro equipo.

## Archivo recomendado para copiar

- `dist/GestionStockPro_Portable.zip`

Tambien puedes copiar la carpeta:

- `dist/GestionStockPro_Portable/`

## En el otro PC

1. Descomprime `GestionStockPro_Portable.zip`.
2. Abre `GestionStockPro_Portable`.
3. Ejecuta `Abrir_GestionStockPro.bat`.

## Que migra automaticamente

- Base de datos `stock.db` (incluye HACCP y configuracion Tuya/Smart Life).
- Carpeta `backups/`.
- Carpeta `facturas/`.

## Verificacion rapida post-migracion

1. Entra a `HACCP` y confirma que existan puntos/controles historicos.
2. Abre `Configurar sensor Tuya`:
   - Debe mostrar estado de SDK disponible.
   - Deben aparecer endpoint, terminal y vinculaciones guardadas.
3. Ejecuta `Cargar dispositivos` y `Probar lectura`.

## Si Smart Life no conecta en el otro PC

1. Verifica fecha/hora del sistema (debe ser correcta) y conexion a internet.
2. Si sale que no hay SDK Tuya, el ejecutable fue generado sin dependencias Tuya:
   - reconstruye con `python build.py` (usa `GestionStock.spec`).
3. Si el token no responde, usa `Generar QR` y `Verificar escaneo` para reloguear la cuenta.

## Build recomendado (origen)

```powershell
python build.py
```

Esto genera:

- `dist/GestionStockPro.exe`
- `dist/GestionStockPro_data_bundle.zip`
- `dist/GestionStockPro_Portable/`
- `dist/GestionStockPro_Portable.zip`

## Comandos utiles (manual)

Exportar bundle manual:

```powershell
python tools/data_bundle.py export --output dist/GestionStockPro_data_bundle.zip
```

Importar bundle manual en AppData del PC destino:

```powershell
python tools/data_bundle.py import --bundle dist/GestionStockPro_data_bundle.zip --dest "%LOCALAPPDATA%\\GestionStockPro" --force
```
