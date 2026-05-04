import os
import re
import json
import shutil
import sqlite3
import sys
from datetime import datetime

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from config import DATA_DIR, DB_PATH, LEGACY_DATA_DIRS

FACTURAS_DIR = os.path.join(DATA_DIR, "facturas")
FACTURAS_RESPALDO_LOCAL_DIR = os.path.join(DATA_DIR, "facturas_respaldo_local")


def normalizar_nombre_carpeta(valor):
    limpio = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(valor or "").strip())
    limpio = limpio.strip("_")
    return limpio[:80] or "sin_proveedor"


def resolver_ruta_factura(ruta_relativa):
    rel = str(ruta_relativa or "").replace("\\", "/").strip("/")
    if not rel:
        raise ValueError("ruta relativa vacia")

    bases = [FACTURAS_DIR]
    for legacy_root in LEGACY_DATA_DIRS:
        candidate = os.path.abspath(os.path.join(legacy_root, "facturas"))
        if candidate not in bases and os.path.isdir(candidate):
            bases.append(candidate)

    for base in bases:
        abs_path = os.path.abspath(os.path.join(base, rel))
        if abs_path.startswith(os.path.abspath(base) + os.sep) and os.path.exists(abs_path):
            return abs_path
    return None


def main():
    os.makedirs(FACTURAS_RESPALDO_LOCAL_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute(
        """
        SELECT id, proveedor, fecha_factura, numero_factura,
               archivo_ruta_relativa, archivo_nombre_original
        FROM facturas_archivo
        ORDER BY id ASC
        """
    )
    rows = cur.fetchall()
    conn.close()

    total = len(rows)
    copiados = 0
    existentes = 0
    omitidos = 0
    errores = 0

    for r in rows:
        rid = int(r["id"] or 0)
        proveedor = str(r["proveedor"] or "").strip()
        fecha = str(r["fecha_factura"] or "").strip()
        numero = str(r["numero_factura"] or "").strip()
        ruta_rel = str(r["archivo_ruta_relativa"] or "").strip()
        original = str(r["archivo_nombre_original"] or "").strip()

        try:
            origen = resolver_ruta_factura(ruta_rel)
            if not origen or not os.path.exists(origen):
                omitidos += 1
                continue

            try:
                dt = datetime.strptime(fecha, "%Y-%m-%d")
            except Exception:
                dt = datetime.now()

            cliente_slug = normalizar_nombre_carpeta(proveedor or "sin_cliente")
            destino_dir = os.path.join(
                FACTURAS_RESPALDO_LOCAL_DIR,
                cliente_slug,
                f"{dt.year}",
                f"{dt.month:02d}",
            )
            os.makedirs(destino_dir, exist_ok=True)

            ext = os.path.splitext(original)[1].lower() or os.path.splitext(origen)[1].lower() or ".bin"
            base = re.sub(r"[^a-zA-Z0-9_-]+", "_", os.path.splitext(original or "factura")[0]).strip("_") or "factura"
            nombre_dest = f"factura_{rid}_{dt.strftime('%Y%m%d')}_{base}{ext}"
            destino = os.path.abspath(os.path.join(destino_dir, nombre_dest))
            meta_path = f"{destino}.json"

            if os.path.exists(destino) and os.path.exists(meta_path):
                existentes += 1
                continue

            shutil.copy2(origen, destino)
            meta = {
                "factura_id": rid,
                "proveedor_cliente": proveedor,
                "fecha_factura": fecha,
                "numero_factura": numero,
                "archivo_origen": origen,
                "archivo_respaldo": destino,
                "creado_en": datetime.now().isoformat(timespec="seconds"),
                "origen": "backfill",
            }
            with open(meta_path, "w", encoding="utf-8") as fh:
                json.dump(meta, fh, ensure_ascii=False, indent=2)
            copiados += 1
        except Exception:
            errores += 1

    print(
        json.dumps(
            {
                "success": True,
                "total_registros": total,
                "copiados": copiados,
                "ya_existentes": existentes,
                "omitidos_sin_archivo": omitidos,
                "errores": errores,
                "destino": FACTURAS_RESPALDO_LOCAL_DIR,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
