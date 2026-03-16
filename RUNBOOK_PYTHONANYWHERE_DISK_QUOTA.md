# Runbook: PythonAnywhere `Disk quota exceeded` / `disk I/O error`

## Contexto del incidente
- Sintoma en `git pull`:
  - `fatal: error when closing loose object file: Disk quota exceeded`
  - `fatal: unpack-objects failed`
- Sintoma en la app Flask:
  - Tienda/agenda/admin con errores tipo `disk I/O error`.
  - Cargas parciales, paneles vacios o endpoints publicos fallando.

## Causa raiz
- Cuota de disco agotada en la cuenta de PythonAnywhere.
- SQLite/Git fallan al escribir archivos temporales/WAL/objetos.

## Diagnostico rapido (Bash console)
```bash
quota -s
du -h --max-depth=1 ~ | sort -h
ls -lah /home/alexdroow/gestor_stock/*.db*
```

## Mitigacion inmediata
```bash
truncate -s 0 /var/log/alexdroow.pythonanywhere.com.error.log
truncate -s 0 /var/log/alexdroow.pythonanywhere.com.server.log
rm -rf ~/.cache/pip
rm -rf ~/.cache/pypoetry
```

Opcional (limpiar backups viejos del proyecto):
```bash
find /home/alexdroow/gestor_stock -type f -name "*.zip" -mtime +14 -delete
find /home/alexdroow/gestor_stock -type f -name "*.bak" -mtime +14 -delete
```

## Recuperacion deploy
```bash
cd /home/alexdroow/gestor_stock
git pull
```

Luego:
1. Web tab -> Reload
2. Navegador -> Ctrl + F5

## Verificacion SQLite
```bash
sqlite3 /home/alexdroow/gestor_stock/stock.db "PRAGMA integrity_check;"
```
- Esperado: `ok`

## Prevencion
- Mantener al menos 300-500 MB libres.
- Revisar periodicamente:
```bash
quota -s && du -h --max-depth=1 ~ | sort -h
```

