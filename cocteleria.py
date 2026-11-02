"""Independent public and admin catalog for sweet and savory catering."""
from __future__ import annotations

import json
import re
import sqlite3
import unicodedata
import uuid
from pathlib import Path

from flask import Blueprint, current_app, jsonify, redirect, render_template, request, session, url_for
from werkzeug.utils import secure_filename

from config import DB_PATH

cocteleria = Blueprint("cocteleria", __name__)
ADMIN_SESSION_KEY = "admin_autenticado"
IMAGE_EXTENSIONS = {"jpg", "jpeg", "png", "webp"}


def _db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def ensure_cocteleria_schema(conn=None):
    own = conn is None
    conn = conn or _db()
    try:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS tienda_cocteleria (
            id INTEGER PRIMARY KEY AUTOINCREMENT, slug TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL, category TEXT NOT NULL CHECK(category IN ('dulce','salado')),
            short_description TEXT NOT NULL DEFAULT '', description TEXT NOT NULL DEFAULT '',
            ingredients TEXT NOT NULL DEFAULT '', allergens TEXT NOT NULL DEFAULT '',
            care_text TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'draft',
            min_lead_hours INTEGER NOT NULL DEFAULT 24, min_quantity INTEGER NOT NULL DEFAULT 1,
            max_quantity INTEGER NOT NULL DEFAULT 99, sort_order INTEGER NOT NULL DEFAULT 0,
            created_by TEXT NOT NULL DEFAULT '', updated_by TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS tienda_cocteleria_presentaciones (
            id INTEGER PRIMARY KEY AUTOINCREMENT, model_id INTEGER NOT NULL, label TEXT NOT NULL,
            units INTEGER NOT NULL DEFAULT 1, unit_label TEXT NOT NULL DEFAULT 'unidades',
            price_clp INTEGER NOT NULL, active INTEGER NOT NULL DEFAULT 1, sort_order INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(model_id) REFERENCES tienda_cocteleria(id)
        );
        CREATE TABLE IF NOT EXISTS tienda_cocteleria_imagenes (
            id INTEGER PRIMARY KEY AUTOINCREMENT, model_id INTEGER NOT NULL, path TEXT NOT NULL,
            alt TEXT NOT NULL DEFAULT '', sort_order INTEGER NOT NULL DEFAULT 0,
            is_cover INTEGER NOT NULL DEFAULT 0, active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY(model_id) REFERENCES tienda_cocteleria(id)
        );
        CREATE TABLE IF NOT EXISTS tienda_cocteleria_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT, model_id INTEGER NOT NULL, actor TEXT NOT NULL DEFAULT '',
            action TEXT NOT NULL, before_json TEXT NOT NULL DEFAULT '', after_json TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS tienda_cocteleria_reservas (
            id INTEGER PRIMARY KEY AUTOINCREMENT, request_key TEXT NOT NULL UNIQUE, event_id INTEGER NOT NULL UNIQUE,
            model_id INTEGER NOT NULL, presentation_id INTEGER NOT NULL, snapshot_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS tienda_cocteleria_reserva_lineas (
            id INTEGER PRIMARY KEY AUTOINCREMENT, event_id INTEGER NOT NULL, model_id INTEGER NOT NULL,
            presentation_id INTEGER NOT NULL, snapshot_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(event_id,model_id,presentation_id)
        );
        CREATE INDEX IF NOT EXISTS idx_cocteleria_status_order ON tienda_cocteleria(status, sort_order);
        CREATE INDEX IF NOT EXISTS idx_cocteleria_presentations_model ON tienda_cocteleria_presentaciones(model_id, active, sort_order);
        """)
        if own:
            conn.commit()
    finally:
        if own:
            conn.close()


def _admin():
    return bool(session.get(ADMIN_SESSION_KEY))


def _slug(value):
    text = unicodedata.normalize("NFKD", str(value or "")).encode("ascii", "ignore").decode().lower()
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", text)).strip("-")[:120]


def _product(conn, model_id, public=False):
    row = conn.execute("SELECT * FROM tienda_cocteleria WHERE id=?", (model_id,)).fetchone()
    if not row:
        return None
    data = dict(row)
    presentations = conn.execute("SELECT id,label,units,unit_label,price_clp,active,sort_order FROM tienda_cocteleria_presentaciones WHERE model_id=? ORDER BY sort_order,id", (model_id,)).fetchall()
    images = conn.execute("SELECT id,path,alt,sort_order,is_cover FROM tienda_cocteleria_imagenes WHERE model_id=? AND active=1 ORDER BY is_cover DESC,sort_order,id", (model_id,)).fetchall()
    data["presentations"] = [dict(x) for x in presentations if not public or int(x["active"])]
    data["images"] = [dict(x) for x in images]
    data["cover"] = next((x["path"] for x in data["images"] if x["is_cover"]), data["images"][0]["path"] if data["images"] else "")
    data["from_price"] = min((int(x["price_clp"]) for x in data["presentations"] if int(x["active"])), default=None)
    if public:
        keys = ("id", "slug", "name", "category", "short_description", "description", "ingredients", "allergens", "care_text", "min_lead_hours", "min_quantity", "max_quantity", "presentations", "images", "cover", "from_price")
        data = {k: data[k] for k in keys}
    return data


def get_published_cocktail(model_id, presentation_id):
    ensure_cocteleria_schema()
    conn = _db()
    try:
        row = conn.execute("""SELECT m.id model_id,m.slug,m.name,m.category,m.description,m.ingredients,m.allergens,m.care_text,
            m.min_lead_hours,m.min_quantity,m.max_quantity,p.id presentation_id,p.label presentation_label,
            p.units,p.unit_label,p.price_clp,i.path photo FROM tienda_cocteleria m
            JOIN tienda_cocteleria_presentaciones p ON p.model_id=m.id AND p.active=1
            LEFT JOIN tienda_cocteleria_imagenes i ON i.model_id=m.id AND i.active=1 AND i.is_cover=1
            WHERE m.id=? AND p.id=? AND m.status='published' LIMIT 1""", (int(model_id), int(presentation_id))).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _audit(conn, model_id, actor, action, before, after):
    conn.execute("INSERT INTO tienda_cocteleria_audit(model_id,actor,action,before_json,after_json) VALUES(?,?,?,?,?)",
                 (model_id, actor, action, json.dumps(before or {}, ensure_ascii=False), json.dumps(after or {}, ensure_ascii=False)))


def _save_presentations(conn, model_id, items):
    if len(items) > 30:
        raise ValueError("Cada producto admite hasta 30 presentaciones")
    conn.execute("DELETE FROM tienda_cocteleria_presentaciones WHERE model_id=?", (model_id,))
    for order, item in enumerate(items):
        label = str(item.get("label") or "").strip()[:80]
        try:
            units, price = int(item.get("units") or 0), int(item.get("price_clp"))
        except (TypeError, ValueError):
            units, price = 0, -1
        if not label or units < 1 or price < 0:
            raise ValueError("Cada presentación necesita nombre, unidades y precio CLP válido")
        conn.execute("INSERT INTO tienda_cocteleria_presentaciones(model_id,label,units,unit_label,price_clp,active,sort_order) VALUES(?,?,?,?,?,?,?)",
                     (model_id, label, units, str(item.get("unit_label") or "unidades")[:30], price, int(bool(item.get("active", True))), order))


def _payload(data):
    name = str(data.get("name") or "").strip()[:120]
    slug = _slug(data.get("slug") or name)
    category = str(data.get("category") or "").strip().lower()
    if not name or not slug or category not in {"dulce", "salado"}:
        raise ValueError("Nombre, URL y categoría dulce o salado son obligatorios")
    try:
        min_q = max(1, int(data.get("min_quantity") or 1))
        max_q = max(min_q, min(999, int(data.get("max_quantity") or 99)))
        lead = max(1, min(720, int(data.get("min_lead_hours") or 24)))
        order = int(data.get("sort_order") or 0)
    except (TypeError, ValueError):
        raise ValueError("Cantidad, anticipación y orden deben ser números válidos")
    return {"name": name, "slug": slug, "category": category,
            "short_description": str(data.get("short_description") or "")[:180],
            "description": str(data.get("description") or "")[:2000],
            "ingredients": str(data.get("ingredients") or "")[:1000],
            "allergens": str(data.get("allergens") or "")[:500], "care_text": str(data.get("care_text") or "")[:1000],
            "min_quantity": min_q, "max_quantity": max_q, "min_lead_hours": lead, "sort_order": order,
            "presentations": data.get("presentations") if isinstance(data.get("presentations"), list) else []}


@cocteleria.get("/tienda/cocteleria")
def public_page():
    ensure_cocteleria_schema()
    conn = _db()
    try:
        ids = [r[0] for r in conn.execute("SELECT id FROM tienda_cocteleria WHERE status='published' ORDER BY sort_order,id")]
        items = [_product(conn, mid, public=True) for mid in ids]
    finally:
        conn.close()
    try:
        from app import _obtener_tienda_personalizacion
        cfg = _obtener_tienda_personalizacion() or {}
    except Exception:
        cfg = {}
    return render_template("tienda_cocteleria.html", items=items,
                           delivery_origin_lat=float(cfg.get("agenda_delivery_origin_lat") or -33.5191105),
                           delivery_origin_lng=float(cfg.get("agenda_delivery_origin_lng") or -70.7849094))


@cocteleria.get("/ventas/cocteleria")
def admin_page():
    if not _admin():
        return redirect(url_for("admin_login", next=request.path))
    ensure_cocteleria_schema()
    return render_template("tienda_cocteleria_admin.html")


@cocteleria.get("/api/tienda/cocteleria")
def api_public_list():
    ensure_cocteleria_schema()
    conn = _db()
    try:
        ids = [r[0] for r in conn.execute("SELECT id FROM tienda_cocteleria WHERE status='published' ORDER BY sort_order,id")]
        return jsonify(success=True, items=[_product(conn, mid, public=True) for mid in ids])
    finally:
        conn.close()


@cocteleria.route("/api/tienda/admin/cocteleria", methods=["GET", "POST"])
def api_admin_list():
    if not _admin():
        return jsonify(success=False, error="Acceso no autorizado"), 401
    ensure_cocteleria_schema()
    conn = _db()
    try:
        if request.method == "GET":
            ids = [r[0] for r in conn.execute("SELECT id FROM tienda_cocteleria WHERE status!='archived' ORDER BY sort_order,id")]
            return jsonify(success=True, items=[_product(conn, mid) for mid in ids])
        data = _payload(request.get_json(silent=True) or {})
        conn.execute("BEGIN IMMEDIATE")
        if conn.execute("SELECT 1 FROM tienda_cocteleria WHERE slug=?", (data["slug"],)).fetchone():
            return jsonify(success=False, error="Ese slug ya está en uso"), 409
        actor = str(session.get("admin_user_name") or session.get("admin_user_id") or "admin")
        cur = conn.execute("""INSERT INTO tienda_cocteleria(slug,name,category,short_description,description,ingredients,allergens,care_text,min_lead_hours,min_quantity,max_quantity,sort_order,created_by,updated_by)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (data["slug"], data["name"], data["category"], data["short_description"], data["description"], data["ingredients"], data["allergens"], data["care_text"], data["min_lead_hours"], data["min_quantity"], data["max_quantity"], data["sort_order"], actor, actor))
        mid = int(cur.lastrowid)
        _save_presentations(conn, mid, data["presentations"])
        _audit(conn, mid, actor, "create", {}, data)
        conn.commit()
        return jsonify(success=True, item=_product(conn, mid)), 201
    except (ValueError, sqlite3.Error) as exc:
        conn.rollback()
        return jsonify(success=False, error=str(exc)), 400
    finally:
        conn.close()


@cocteleria.route("/api/tienda/admin/cocteleria/<int:model_id>", methods=["PUT", "DELETE"])
def api_admin_detail(model_id):
    if not _admin():
        return jsonify(success=False, error="Acceso no autorizado"), 401
    ensure_cocteleria_schema()
    conn = _db()
    try:
        before = _product(conn, model_id)
        if not before:
            return jsonify(success=False, error="Producto no encontrado"), 404
        actor = str(session.get("admin_user_name") or session.get("admin_user_id") or "admin")
        if request.method == "DELETE":
            conn.execute("UPDATE tienda_cocteleria SET status='archived',updated_by=?,updated_at=CURRENT_TIMESTAMP WHERE id=?", (actor, model_id))
            _audit(conn, model_id, actor, "archive", before, {"status": "archived"})
            conn.commit()
            return jsonify(success=True)
        data = _payload(request.get_json(silent=True) or {})
        if conn.execute("SELECT 1 FROM tienda_cocteleria WHERE slug=? AND id<>?", (data["slug"], model_id)).fetchone():
            return jsonify(success=False, error="Ese slug ya está en uso"), 409
        if before["status"] == "published" and (not data["description"].strip() or not before["cover"] or not any(x.get("active", True) and int(x.get("price_clp", -1)) >= 0 for x in data["presentations"])):
            return jsonify(success=False, error="Un producto publicado necesita descripción, portada y una presentación activa con precio"), 400
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("""UPDATE tienda_cocteleria SET slug=?,name=?,category=?,short_description=?,description=?,ingredients=?,allergens=?,care_text=?,min_lead_hours=?,min_quantity=?,max_quantity=?,sort_order=?,updated_by=?,updated_at=CURRENT_TIMESTAMP WHERE id=?""",
                     (data["slug"], data["name"], data["category"], data["short_description"], data["description"], data["ingredients"], data["allergens"], data["care_text"], data["min_lead_hours"], data["min_quantity"], data["max_quantity"], data["sort_order"], actor, model_id))
        _save_presentations(conn, model_id, data["presentations"])
        after = _product(conn, model_id)
        _audit(conn, model_id, actor, "update", before, after)
        conn.commit()
        return jsonify(success=True, item=_product(conn, model_id))
    except (ValueError, sqlite3.Error) as exc:
        conn.rollback()
        return jsonify(success=False, error=str(exc)), 400
    finally:
        conn.close()


@cocteleria.post("/api/tienda/admin/cocteleria/<int:model_id>/status")
def api_admin_status(model_id):
    if not _admin():
        return jsonify(success=False, error="Acceso no autorizado"), 401
    ensure_cocteleria_schema()
    conn = _db()
    try:
        before = _product(conn, model_id)
        if not before:
            return jsonify(success=False, error="Producto no encontrado"), 404
        status = str((request.get_json(silent=True) or {}).get("status") or "")
        if status not in {"published", "hidden"}:
            return jsonify(success=False, error="Estado no válido"), 400
        if status == "published":
            missing = []
            if not before["description"].strip(): missing.append("descripción")
            if not before["cover"] or not before["images"]: missing.append("foto de portada")
            if not any(int(p["active"]) and int(p["price_clp"]) >= 0 for p in before["presentations"]): missing.append("presentación activa con precio")
            if missing: return jsonify(success=False, error="Falta: " + ", ".join(missing)), 400
        actor = str(session.get("admin_user_name") or "admin")
        conn.execute("UPDATE tienda_cocteleria SET status=?,updated_by=?,updated_at=CURRENT_TIMESTAMP WHERE id=?", (status, actor, model_id))
        _audit(conn, model_id, actor, "status", before, {"status": status})
        conn.commit()
        return jsonify(success=True, item=_product(conn, model_id))
    finally:
        conn.close()


@cocteleria.post("/api/tienda/admin/cocteleria/<int:model_id>/duplicate")
def api_admin_duplicate(model_id):
    if not _admin(): return jsonify(success=False, error="Acceso no autorizado"), 401
    ensure_cocteleria_schema(); conn = _db()
    try:
        src = _product(conn, model_id)
        if not src: return jsonify(success=False, error="Producto no encontrado"), 404
        slug = _slug(src["slug"] + "-copia"); base = slug; n = 2
        while conn.execute("SELECT 1 FROM tienda_cocteleria WHERE slug=?", (slug,)).fetchone():
            slug = f"{base}-{n}"; n += 1
        actor = str(session.get("admin_user_name") or "admin")
        cur = conn.execute("""INSERT INTO tienda_cocteleria(slug,name,category,short_description,description,ingredients,allergens,care_text,min_lead_hours,min_quantity,max_quantity,sort_order,created_by,updated_by)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (slug, (src["name"] + " (copia)")[:120], src["category"], src["short_description"], src["description"], src["ingredients"], src["allergens"], src["care_text"], src["min_lead_hours"], src["min_quantity"], src["max_quantity"], src["sort_order"], actor, actor))
        mid = int(cur.lastrowid)
        for p in src["presentations"]:
            conn.execute("INSERT INTO tienda_cocteleria_presentaciones(model_id,label,units,unit_label,price_clp,active,sort_order) VALUES(?,?,?,?,?,?,?)", (mid, p["label"], p["units"], p["unit_label"], p["price_clp"], p["active"], p["sort_order"]))
        for im in src["images"]:
            conn.execute("INSERT INTO tienda_cocteleria_imagenes(model_id,path,alt,sort_order,is_cover,active) VALUES(?,?,?,?,?,1)", (mid, im["path"], im["alt"], im["sort_order"], im["is_cover"]))
        _audit(conn, mid, actor, "duplicate", {}, src); conn.commit()
        return jsonify(success=True, item=_product(conn, mid)), 201
    finally:
        conn.close()


@cocteleria.post("/api/tienda/admin/cocteleria/<int:model_id>/images")
def api_admin_image(model_id):
    if not _admin(): return jsonify(success=False, error="Acceso no autorizado"), 401
    ensure_cocteleria_schema(); conn = _db()
    try:
        if not conn.execute("SELECT 1 FROM tienda_cocteleria WHERE id=?", (model_id,)).fetchone(): return jsonify(success=False, error="Producto no encontrado"), 404
        upload = request.files.get("image")
        if not upload: return jsonify(success=False, error="Selecciona una imagen"), 400
        ext = secure_filename(upload.filename or "").rsplit(".", 1)[-1].lower(); raw = upload.read(5 * 1024 * 1024 + 1)
        valid = raw.startswith(b"\xff\xd8\xff") if ext in {"jpg", "jpeg"} else raw.startswith(b"\x89PNG\r\n\x1a\n") if ext == "png" else raw[:4] == b"RIFF" and raw[8:12] == b"WEBP"
        if ext not in IMAGE_EXTENSIONS or not raw or len(raw) > 5 * 1024 * 1024 or not valid:
            return jsonify(success=False, error="Imagen inválida (JPG, PNG o WebP; máximo 5 MB)"), 400
        filename = f"{uuid.uuid4().hex}.{ext}"; folder = Path(current_app.static_folder) / "cocteleria"; folder.mkdir(parents=True, exist_ok=True); (folder / filename).write_bytes(raw)
        order = conn.execute("SELECT COALESCE(MAX(sort_order),-1)+1 FROM tienda_cocteleria_imagenes WHERE model_id=?", (model_id,)).fetchone()[0]
        cover = not bool(conn.execute("SELECT 1 FROM tienda_cocteleria_imagenes WHERE model_id=? AND is_cover=1 AND active=1", (model_id,)).fetchone())
        conn.execute("INSERT INTO tienda_cocteleria_imagenes(model_id,path,alt,sort_order,is_cover,active) VALUES(?,?,?,?,?,1)", (model_id, f"/static/cocteleria/{filename}", str(request.form.get("alt") or "")[:180], order, int(cover)))
        _audit(conn, model_id, str(session.get("admin_user_name") or "admin"), "image_add", {}, _product(conn, model_id)); conn.commit()
        return jsonify(success=True, item=_product(conn, model_id))
    finally:
        conn.close()


@cocteleria.post("/api/tienda/admin/cocteleria/<int:model_id>/cover/<int:image_id>")
def api_admin_cover(model_id, image_id):
    if not _admin(): return jsonify(success=False, error="Acceso no autorizado"), 401
    ensure_cocteleria_schema(); conn = _db()
    try:
        if not conn.execute("SELECT 1 FROM tienda_cocteleria_imagenes WHERE id=? AND model_id=? AND active=1", (image_id, model_id)).fetchone(): return jsonify(success=False, error="Imagen no encontrada"), 404
        conn.execute("UPDATE tienda_cocteleria_imagenes SET is_cover=CASE WHEN id=? THEN 1 ELSE 0 END WHERE model_id=?", (image_id, model_id)); conn.commit()
        return jsonify(success=True, item=_product(conn, model_id))
    finally:
        conn.close()


@cocteleria.get("/api/tienda/admin/cocteleria/<int:model_id>/reservas")
def api_admin_reservations(model_id):
    if not _admin(): return jsonify(success=False, error="Acceso no autorizado"), 401
    ensure_cocteleria_schema(); conn = _db()
    try:
        rows = conn.execute("""SELECT event_id,snapshot_json,created_at FROM tienda_cocteleria_reserva_lineas
            WHERE model_id=? ORDER BY id DESC LIMIT 100""", (model_id,)).fetchall()
        if not rows:
            rows = conn.execute("SELECT event_id,snapshot_json,created_at FROM tienda_cocteleria_reservas WHERE model_id=? ORDER BY id DESC LIMIT 100", (model_id,)).fetchall()
        return jsonify(success=True, items=[dict(row) for row in rows])
    finally:
        conn.close()
