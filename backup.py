import os
import sqlite3
import time
from datetime import datetime

from config import BACKUP_DIR, DB_PATH


MAX_BACKUPS = 120
RETENTION_DAYS = 180
MIN_SECONDS_BETWEEN_BACKUPS = 45
_last_backup_ts = 0.0


def _ensure_dirs():
    os.makedirs(BACKUP_DIR, exist_ok=True)


def _sqlite_safe_copy(src_path, dst_path):
    src = sqlite3.connect(src_path, timeout=30)
    dst = sqlite3.connect(dst_path, timeout=30)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()


def crear_backup(force=False):
    """Crea un backup consistente de SQLite con control anti-saturación."""
    global _last_backup_ts
    try:
        if not os.path.exists(DB_PATH):
            return None

        now_ts = time.time()
        if not force and (now_ts - _last_backup_ts) < MIN_SECONDS_BETWEEN_BACKUPS:
            return None

        _ensure_dirs()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(BACKUP_DIR, f"stock_backup_{timestamp}.db")
        _sqlite_safe_copy(DB_PATH, backup_path)
        _last_backup_ts = now_ts

        limpiar_backups_antiguos()
        print(f"[OK] Backup creado: {backup_path}")
        return backup_path
    except Exception as e:
        print(f"[ERROR] Error al crear backup: {e}")
        return None


def limpiar_backups_antiguos():
    """Mantiene backups por cantidad máxima y antigüedad."""
    try:
        _ensure_dirs()
        backups = sorted(
            [f for f in os.listdir(BACKUP_DIR) if f.startswith("stock_backup_") and f.endswith(".db")]
        )
        if not backups:
            return

        now = datetime.now()
        valid = []
        for name in backups:
            try:
                ts = name.replace("stock_backup_", "").replace(".db", "")
                dt = datetime.strptime(ts, "%Y%m%d_%H%M%S")
                age_days = (now - dt).days
                if age_days > RETENTION_DAYS:
                    os.remove(os.path.join(BACKUP_DIR, name))
                    continue
                valid.append(name)
            except Exception:
                # Si no parsea, conservar por seguridad.
                valid.append(name)

        while len(valid) > MAX_BACKUPS:
            oldest = valid.pop(0)
            try:
                os.remove(os.path.join(BACKUP_DIR, oldest))
            except Exception:
                break
    except Exception:
        # El backup no debe fallar por errores de limpieza.
        pass


def obtener_ultimo_backup():
    """Retorna la fecha del último backup formateada."""
    try:
        if not os.path.exists(BACKUP_DIR):
            return None

        backups = sorted(
            f for f in os.listdir(BACKUP_DIR)
            if f.startswith("stock_backup_") and f.endswith(".db")
        )
        if not backups:
            return None

        ultimo = backups[-1]
        fecha_str = ultimo.replace("stock_backup_", "").replace(".db", "")
        fecha = datetime.strptime(fecha_str, "%Y%m%d_%H%M%S")
        return fecha.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return None
