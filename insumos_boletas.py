"""Safe receipt-review helpers for updating ingredient purchase prices."""

import hashlib
import json
import os
import re
import unicodedata
import uuid
from datetime import datetime, timedelta

from database import (
    get_db,
    _factor_unidad_costo,
    _registrar_historial_precio_insumo_cursor,
)


def ensure_schema():
    conn = get_db()
    try:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS insumos_boletas (
                id TEXT PRIMARY KEY,
                sha256 TEXT NOT NULL UNIQUE,
                nombre_archivo TEXT NOT NULL,
                ruta_archivo TEXT NOT NULL,
                texto_lectura TEXT NOT NULL DEFAULT '',
                estado TEXT NOT NULL DEFAULT 'revision',
                usuario_id TEXT,
                usuario_nombre TEXT,
                lineas_json TEXT NOT NULL DEFAULT '[]',
                creado TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                confirmado TEXT,
                revertida_en TEXT
            )"""
        )
        columns = {row[1] for row in conn.execute("PRAGMA table_info(insumos_boletas)").fetchall()}
        if "lineas_json" not in columns:
            conn.execute("ALTER TABLE insumos_boletas ADD COLUMN lineas_json TEXT NOT NULL DEFAULT '[]'")
        if "revertida_en" not in columns:
            conn.execute("ALTER TABLE insumos_boletas ADD COLUMN revertida_en TEXT")
        conn.commit()
    finally:
        conn.close()


def normalize_text(value):
    value = unicodedata.normalize("NFKD", str(value or "").lower())
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    return " ".join(re.findall(r"[a-z0-9]+", value))


def receipt_digest(raw):
    return hashlib.sha256(raw).hexdigest()


def make_receipt_id():
    return uuid.uuid4().hex


def extract_receipt_text(path, extension):
    """Extract local text. OCR for raster images is optional and never remote."""
    if extension == ".pdf":
        try:
            from pypdf import PdfReader
            reader = PdfReader(path)
            return "\n".join((page.extract_text() or "") for page in reader.pages[:20]), "pdf-text"
        except Exception:
            return "", "ocr-unavailable"
    try:
        import pytesseract
        from PIL import Image, ImageOps
        image = ImageOps.exif_transpose(Image.open(path))
        return pytesseract.image_to_string(image, lang="spa"), "tesseract"
    except Exception:
        return "", "ocr-unavailable"


def candidate_matches(receipt_text, ingredients):
    """Conservative name matching; presentation must still be confirmed by a person."""
    source = normalize_text(receipt_text)
    if not source:
        return []
    tokens = set(source.split())
    ranked = []
    for item in ingredients:
        name = normalize_text(item.get("nombre"))
        name_tokens = set(name.split())
        if not name_tokens:
            continue
        if name == source:
            return [dict(item, coincidencia=1.0)]
        overlap = len(tokens & name_tokens) / max(len(tokens | name_tokens), 1)
        if overlap >= 0.5:
            ranked.append((overlap, item))
    ranked.sort(key=lambda pair: pair[0], reverse=True)
    if len(ranked) > 1 and ranked[0][0] - ranked[1][0] < 0.15:
        return []
    if not ranked or ranked[0][0] < 0.8:
        return []
    return [dict(ranked[0][1], coincidencia=round(ranked[0][0], 3))]


def create_receipt(receipt_id, digest, original_name, path, text, user_id=None, user_name=None):
    conn = get_db()
    try:
        conn.execute(
            """INSERT INTO insumos_boletas
               (id, sha256, nombre_archivo, ruta_archivo, texto_lectura, usuario_id, usuario_nombre)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (receipt_id, digest, original_name, path, text or "", str(user_id or ""), str(user_name or "")),
        )
        conn.commit()
    finally:
        conn.close()


def get_receipt(receipt_id):
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM insumos_boletas WHERE id = ?", (receipt_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def purge_expired_files(retention_days=None):
    """Remove old private document files while preserving their audit rows."""
    if retention_days is None:
        try:
            retention_days = int(os.environ.get("GESTIONSTOCK_INSUMOS_BOLETA_RETENTION_DAYS", "180"))
        except (TypeError, ValueError):
            retention_days = 180
    retention_days = min(max(int(retention_days), 30), 3650)
    cutoff = (datetime.utcnow() - timedelta(days=retention_days)).strftime("%Y-%m-%d %H:%M:%S")
    conn = get_db()
    removed = 0
    try:
        rows = conn.execute(
            "SELECT id, ruta_archivo FROM insumos_boletas WHERE creado < ? AND COALESCE(ruta_archivo,'') <> ''",
            (cutoff,),
        ).fetchall()
        for row in rows:
            path = os.path.abspath(str(row["ruta_archivo"] or ""))
            if os.path.basename(os.path.dirname(path)) != "insumos_boletas" or os.path.basename(path) != str(row["id"]) + os.path.splitext(path)[1]:
                continue
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
            except OSError:
                continue
            conn.execute("UPDATE insumos_boletas SET ruta_archivo='' WHERE id=?", (row["id"],))
            removed += 1
        conn.commit()
        return removed
    finally:
        conn.close()


def confirm_receipt(receipt_id, lines, user_name=None):
    """Atomically apply explicitly selected lines and record price history."""
    if not isinstance(lines, list) or not lines:
        raise ValueError("Selecciona al menos un cambio de precio")
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        receipt = conn.execute(
            "SELECT * FROM insumos_boletas WHERE id = ?", (receipt_id,)
        ).fetchone()
        if not receipt:
            raise ValueError("Boleta no encontrada")
        if receipt["estado"] == "confirmada":
            raise ValueError("Esta boleta ya fue confirmada")

        validated = []
        for line in lines:
            if not isinstance(line, dict) or not line.get("confirmar"):
                continue
            try:
                item_id = int(line.get("insumo_id"))
                price = float(line.get("precio_unitario"))
                quantity = float(line.get("cantidad_comprada"))
            except (TypeError, ValueError):
                raise ValueError("Revisa producto, precio y presentación de cada línea")
            unit = str(line.get("unidad_compra") or "").strip().lower()
            if price < 0 or quantity <= 0 or not unit:
                raise ValueError("Precio y presentación deben ser válidos")
            item = conn.execute(
                "SELECT id, nombre, stock, stock_minimo, unidad, precio_unitario, cantidad_comprada, unidad_compra, precio_incluye_iva FROM insumos WHERE id = ?",
                (item_id,),
            ).fetchone()
            if not item:
                raise ValueError("Uno de los insumos seleccionados ya no existe")
            old_unit = str(item["unidad_compra"] or item["unidad"] or "unidad").strip().lower()
            _, old_dimension = _factor_unidad_costo(old_unit)
            _, new_dimension = _factor_unidad_costo(unit)
            known_dimensions = {"g", "ml", "unidad"}
            if old_dimension in known_dimensions and new_dimension in known_dimensions and old_dimension != new_dimension:
                raise ValueError(f"La unidad {unit} no es compatible con la presentación actual de {item['nombre']}")
            expected = line.get("precio_actual")
            if expected is None or round(float(expected), 6) != round(float(item["precio_unitario"] or 0), 6):
                raise ValueError(f"El precio de {item['nombre']} cambió mientras revisabas. Vuelve a revisar la boleta.")
            expected_quantity = line.get("cantidad_actual")
            expected_unit = str(line.get("unidad_actual") or "").strip().lower()
            expected_tax = line.get("iva_actual")
            if (
                expected_quantity is None
                or round(float(expected_quantity), 6) != round(float(item["cantidad_comprada"] or 1), 6)
                or expected_unit != str(item["unidad_compra"] or item["unidad"] or "unidad").strip().lower()
                or expected_tax is None
                or int(bool(expected_tax)) != int(item["precio_incluye_iva"] if item["precio_incluye_iva"] is not None else 1)
            ):
                raise ValueError(f"La presentación o el IVA de {item['nombre']} cambió mientras revisabas. Revisa la boleta otra vez.")
            validated.append((item, price, quantity, unit, bool(line.get("precio_incluye_iva", True))))

        if not validated:
            raise ValueError("No hay cambios seleccionados para confirmar")

        updated = []
        for item, price, quantity, unit, includes_tax in validated:
            cursor = conn.execute(
                """UPDATE insumos SET precio_unitario=?, cantidad_comprada=?, unidad_compra=?,
                   precio_incluye_iva=?, precio_unitario_anterior=?, cantidad_comprada_anterior=?,
                   unidad_compra_anterior=?, precio_incluye_iva_anterior=?
                   WHERE id=? AND precio_unitario=?""",
                (price, quantity, unit, int(includes_tax), float(item["precio_unitario"] or 0),
                 float(item["cantidad_comprada"] or 1), item["unidad_compra"] or item["unidad"],
                 int(item["precio_incluye_iva"] if item["precio_incluye_iva"] is not None else 1),
                 item["id"], float(item["precio_unitario"] or 0)),
            )
            if cursor.rowcount != 1:
                raise ValueError(f"El precio de {item['nombre']} cambió; no se aplicó ningún cambio")
            _registrar_historial_precio_insumo_cursor(
                conn.cursor(), item["id"], price, quantity, unit, includes_tax,
                origen_modulo="boleta_insumos", codigo_operacion=f"BOL-{receipt_id[:12]}"
            )
            updated.append(item["nombre"])
        conn.execute(
            "UPDATE insumos_boletas SET estado='confirmada', confirmado=CURRENT_TIMESTAMP, usuario_nombre=COALESCE(?, usuario_nombre), lineas_json=? WHERE id=?",
            (user_name, json.dumps(lines, ensure_ascii=False), receipt_id),
        )
        conn.commit()
        return updated
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def reverse_receipt(receipt_id, user_name=None):
    """Restore pre-receipt prices only when no later edit has superseded them."""
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        receipt = conn.execute("SELECT * FROM insumos_boletas WHERE id=?", (receipt_id,)).fetchone()
        if not receipt or receipt["estado"] != "confirmada":
            raise ValueError("Solo se puede revertir una boleta confirmada")
        lines = json.loads(receipt["lineas_json"] or "[]")
        selected = [line for line in lines if isinstance(line, dict) and line.get("confirmar")]
        if not selected:
            raise ValueError("No hay líneas confirmadas para revertir")

        validated = []
        for line in selected:
            item_id = int(line.get("insumo_id") or 0)
            item = conn.execute(
                "SELECT id, nombre, precio_unitario, cantidad_comprada, unidad_compra, unidad, precio_incluye_iva FROM insumos WHERE id=?",
                (item_id,),
            ).fetchone()
            if not item:
                raise ValueError("Uno de los insumos ya no existe; no se revirtió ningún cambio")
            if (
                round(float(item["precio_unitario"] or 0), 6) != round(float(line["precio_unitario"]), 6)
                or round(float(item["cantidad_comprada"] or 1), 6) != round(float(line["cantidad_comprada"]), 6)
                or str(item["unidad_compra"] or item["unidad"] or "unidad").strip().lower() != str(line["unidad_compra"]).strip().lower()
                or int(item["precio_incluye_iva"] if item["precio_incluye_iva"] is not None else 1) != int(bool(line["precio_incluye_iva"]))
            ):
                raise ValueError(f"{item['nombre']} fue modificado después de esta boleta. No se sobrescribieron cambios posteriores.")
            validated.append((line, item))

        for line, item in validated:
            old_price = float(line["precio_actual"])
            old_quantity = float(line["cantidad_actual"])
            old_unit = str(line["unidad_actual"])
            old_tax = int(bool(line["iva_actual"]))
            conn.execute(
                """UPDATE insumos SET precio_unitario=?, cantidad_comprada=?, unidad_compra=?, precio_incluye_iva=?,
                   precio_unitario_anterior=?, cantidad_comprada_anterior=?, unidad_compra_anterior=?, precio_incluye_iva_anterior=?
                   WHERE id=?""",
                (old_price, old_quantity, old_unit, old_tax, float(item["precio_unitario"]),
                 float(item["cantidad_comprada"] or 1), item["unidad_compra"] or item["unidad"],
                 int(item["precio_incluye_iva"] if item["precio_incluye_iva"] is not None else 1), item["id"]),
            )
            _registrar_historial_precio_insumo_cursor(
                conn.cursor(), item["id"], old_price, old_quantity, old_unit, old_tax,
                origen_modulo="reversion_boleta_insumos", codigo_operacion=f"REV-BOL-{receipt_id[:12]}"
            )
        conn.execute(
            "UPDATE insumos_boletas SET estado='revertida', revertida_en=CURRENT_TIMESTAMP, usuario_nombre=COALESCE(?, usuario_nombre) WHERE id=?",
            (user_name, receipt_id),
        )
        conn.commit()
        return [item["nombre"] for _line, item in validated]
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

