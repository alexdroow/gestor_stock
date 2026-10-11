"""Ready-to-book cake catalogue, kept separate from the custom cake builder."""
from __future__ import annotations

import json
import os
import re
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path

from flask import Blueprint, current_app, jsonify, render_template, request, session
from werkzeug.utils import secure_filename

from config import DB_PATH

ready_cakes = Blueprint("ready_cakes", __name__)
ADMIN_SESSION_KEY = "admin_autenticado"
STATUSES = {"draft", "published", "hidden", "archived"}
IMAGE_EXTENSIONS = {"jpg", "jpeg", "png", "webp"}


def _db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def ensure_ready_cakes_schema(conn=None):
    own = conn is None
    conn = conn or _db()
    try:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS tienda_tortas_listas (
            id INTEGER PRIMARY KEY AUTOINCREMENT, slug TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL, short_description TEXT NOT NULL DEFAULT '',
            description TEXT NOT NULL DEFAULT '', ingredients TEXT NOT NULL DEFAULT '',
            allergens TEXT NOT NULL DEFAULT '', care_text TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'draft', allow_message INTEGER NOT NULL DEFAULT 0,
            message_max_chars INTEGER NOT NULL DEFAULT 60, message_instructions TEXT NOT NULL DEFAULT '',
            sort_order INTEGER NOT NULL DEFAULT 0, created_by TEXT NOT NULL DEFAULT '',
            updated_by TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS tienda_tortas_listas_tamanos (
            id INTEGER PRIMARY KEY AUTOINCREMENT, model_id INTEGER NOT NULL,
            label TEXT NOT NULL, servings TEXT NOT NULL DEFAULT '', price_clp INTEGER NOT NULL,
            active INTEGER NOT NULL DEFAULT 1, sort_order INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(model_id) REFERENCES tienda_tortas_listas(id)
        );
        CREATE TABLE IF NOT EXISTS tienda_tortas_listas_imagenes (
            id INTEGER PRIMARY KEY AUTOINCREMENT, model_id INTEGER NOT NULL,
            path TEXT NOT NULL, alt TEXT NOT NULL DEFAULT '', sort_order INTEGER NOT NULL DEFAULT 0,
            is_cover INTEGER NOT NULL DEFAULT 0, active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(model_id) REFERENCES tienda_tortas_listas(id)
        );
        CREATE TABLE IF NOT EXISTS tienda_tortas_listas_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT, model_id INTEGER NOT NULL,
            actor TEXT NOT NULL DEFAULT '', action TEXT NOT NULL,
            before_json TEXT NOT NULL DEFAULT '', after_json TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS tienda_tortas_listas_reservas (
            id INTEGER PRIMARY KEY AUTOINCREMENT, request_key TEXT NOT NULL UNIQUE,
            event_id INTEGER NOT NULL, model_id INTEGER NOT NULL, size_id INTEGER NOT NULL,
            snapshot_json TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_ready_cakes_status_order ON tienda_tortas_listas(status, sort_order);
        CREATE INDEX IF NOT EXISTS idx_ready_cakes_sizes_model ON tienda_tortas_listas_tamanos(model_id, active, sort_order);
        """)
        if own:
            conn.commit()
    finally:
        if own:
            conn.close()


def _admin():
    return bool(session.get(ADMIN_SESSION_KEY))


def _slug(value):
    import unicodedata
    text = unicodedata.normalize("NFKD", str(value or "")).encode("ascii", "ignore").decode().lower()
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", text)).strip("-")[:120]


def _model(conn, model_id, public=False):
    row = conn.execute("SELECT * FROM tienda_tortas_listas WHERE id=?", (model_id,)).fetchone()
    if not row:
        return None
    data = dict(row)
    sizes = conn.execute("SELECT id,label,servings,price_clp,active,sort_order FROM tienda_tortas_listas_tamanos WHERE model_id=? ORDER BY sort_order,id", (model_id,)).fetchall()
    images = conn.execute("SELECT id,path,alt,sort_order,is_cover FROM tienda_tortas_listas_imagenes WHERE model_id=? AND active=1 ORDER BY is_cover DESC,sort_order,id", (model_id,)).fetchall()
    data["sizes"] = [dict(x) for x in sizes if not public or int(x["active"])]
    data["images"] = [dict(x) for x in images]
    data["cover"] = next((x["path"] for x in data["images"] if x["is_cover"]), data["images"][0]["path"] if data["images"] else "")
    data["has_cover"] = any(int(x["is_cover"]) for x in data["images"])
    data["allow_message"] = bool(data["allow_message"])
    data["active_sizes"] = [x for x in data["sizes"] if int(x["active"])]
    data["from_price"] = min((int(x["price_clp"]) for x in data["active_sizes"]), default=None)
    if public:
        data = {k: data[k] for k in ("id", "slug", "name", "short_description", "description", "ingredients", "allergens", "care_text", "allow_message", "message_max_chars", "message_instructions", "sizes", "images", "cover", "from_price")}
    return data


def get_published_ready_cake(model_id, size_id):
    """Authoritative reservation-time lookup; prices and labels come from SQLite."""
    ensure_ready_cakes_schema()
    conn = _db()
    try:
        row = conn.execute("""SELECT m.id model_id,m.slug,m.name,m.description,m.allow_message,
            m.message_max_chars,m.message_instructions,s.id size_id,s.label size_label,
            s.servings,s.price_clp,i.path photo
            FROM tienda_tortas_listas m JOIN tienda_tortas_listas_tamanos s ON s.model_id=m.id AND s.active=1
            LEFT JOIN tienda_tortas_listas_imagenes i ON i.model_id=m.id AND i.active=1 AND i.is_cover=1
            WHERE m.id=? AND s.id=? AND m.status='published' LIMIT 1""", (int(model_id), int(size_id))).fetchone()
        if not row:
            return None
        return dict(row)
    finally:
        conn.close()


def _write_audit(conn, model_id, actor, action, before, after):
    conn.execute("INSERT INTO tienda_tortas_listas_audit(model_id,actor,action,before_json,after_json) VALUES(?,?,?,?,?)",
                 (model_id, actor, action, json.dumps(before or {}, ensure_ascii=False), json.dumps(after or {}, ensure_ascii=False)))


@ready_cakes.get("/tienda/tortas-listas")
def public_catalog():
    ensure_ready_cakes_schema()
    conn = _db()
    try:
        ids = [r[0] for r in conn.execute("SELECT id FROM tienda_tortas_listas WHERE status='published' ORDER BY sort_order,id")]
        items = [_model(conn, i, public=True) for i in ids]
    finally:
        conn.close()
    try:
        from app import _obtener_tienda_personalizacion
        store_cfg = _obtener_tienda_personalizacion() or {}
    except Exception:
        store_cfg = {}
    return render_template(
        "tienda_tortas_listas.html", cakes=items,
        delivery_origin_lat=float(store_cfg.get("agenda_delivery_origin_lat") or -33.5191105),
        delivery_origin_lng=float(store_cfg.get("agenda_delivery_origin_lng") or -70.7849094),
    )


@ready_cakes.get("/ventas/tortas-listas")
def admin_page():
    if not _admin():
        from flask import redirect, url_for
        return redirect(url_for("admin_login", next=request.path))
    ensure_ready_cakes_schema()
    return render_template("tienda_tortas_listas_admin.html")


@ready_cakes.get("/api/tienda/tortas-listas")
def api_public_list():
    ensure_ready_cakes_schema()
    conn = _db()
    try:
        ids = [r[0] for r in conn.execute("SELECT id FROM tienda_tortas_listas WHERE status='published' ORDER BY sort_order,id")]
        return jsonify(success=True, items=[_model(conn, i, public=True) for i in ids])
    finally:
        conn.close()


@ready_cakes.route("/api/tienda/admin/tortas-listas", methods=["GET", "POST"])
def api_admin_list():
    if not _admin():
        return jsonify(success=False, error="Acceso no autorizado"), 401
    ensure_ready_cakes_schema()
    conn = _db()
    try:
        if request.method == "GET":
            ids = [r[0] for r in conn.execute("SELECT id FROM tienda_tortas_listas WHERE status!='archived' ORDER BY sort_order,id")]
            return jsonify(success=True, items=[_model(conn, i) for i in ids])
        data = request.get_json(silent=True) or {}
        name = str(data.get("name") or "").strip()[:120]
        slug = _slug(data.get("slug") or name)
        sizes = data.get("sizes") if isinstance(data.get("sizes"), list) else []
        if not name or not slug:
            return jsonify(success=False, error="Ingresa un nombre y un slug válido"), 400
        if len(sizes) > 30:
            return jsonify(success=False, error="El modelo admite hasta 30 tamaños"), 400
        conn.execute("BEGIN IMMEDIATE")
        if conn.execute("SELECT 1 FROM tienda_tortas_listas WHERE slug=?", (slug,)).fetchone():
            return jsonify(success=False, error="Ese slug ya está en uso"), 409
        actor = str(session.get("admin_user_name") or session.get("admin_user_id") or "admin")
        cur = conn.execute("""INSERT INTO tienda_tortas_listas(slug,name,short_description,description,ingredients,allergens,care_text,status,allow_message,message_max_chars,message_instructions,sort_order,created_by,updated_by)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (slug,name,str(data.get("short_description") or "")[:180],str(data.get("description") or "")[:2000],str(data.get("ingredients") or "")[:1000],str(data.get("allergens") or "")[:500],str(data.get("care_text") or "")[:1000],"draft",int(bool(data.get("allow_message"))),max(1,min(120,int(data.get("message_max_chars") or 60))),str(data.get("message_instructions") or "")[:240],int(data.get("sort_order") or 0),actor,actor))
        mid = int(cur.lastrowid)
        for order, size in enumerate(sizes):
            label = str(size.get("label") or "").strip()[:80]
            try:
                price = int(size.get("price_clp"))
            except (TypeError, ValueError):
                price = -1
            if not label or price < 0:
                raise ValueError("Cada tamaño debe tener nombre y precio CLP válido")
            conn.execute("INSERT INTO tienda_tortas_listas_tamanos(model_id,label,servings,price_clp,active,sort_order) VALUES(?,?,?,?,?,?)",
                         (mid, label, str(size.get("servings") or "")[:80], price, int(bool(size.get("active", True))), order))
        _write_audit(conn, mid, actor, "create", {}, data)
        conn.commit()
        return jsonify(success=True, item=_model(conn, mid)), 201
    except (ValueError, sqlite3.Error) as exc:
        conn.rollback()
        return jsonify(success=False, error=str(exc)), 400
    finally:
        conn.close()


@ready_cakes.route("/api/tienda/admin/tortas-listas/<int:model_id>", methods=["PUT", "DELETE"])
def api_admin_detail(model_id):
    if not _admin():
        return jsonify(success=False, error="Acceso no autorizado"), 401
    ensure_ready_cakes_schema()
    conn = _db()
    try:
        before = _model(conn, model_id)
        if not before:
            return jsonify(success=False, error="Modelo no encontrado"), 404
        actor = str(session.get("admin_user_name") or session.get("admin_user_id") or "admin")
        if request.method == "DELETE":
            conn.execute("UPDATE tienda_tortas_listas SET status='archived',updated_by=?,updated_at=CURRENT_TIMESTAMP WHERE id=?", (actor,model_id))
            _write_audit(conn,model_id,actor,"archive",before,{"status":"archived"})
            conn.commit()
            return jsonify(success=True)
        data=request.get_json(silent=True) or {}
        name=str(data.get("name") or "").strip()[:120]
        slug=_slug(data.get("slug") or name)
        if not name or not slug:
            return jsonify(success=False,error="Nombre y slug son obligatorios"),400
        if conn.execute("SELECT 1 FROM tienda_tortas_listas WHERE slug=? AND id<>?",(slug,model_id)).fetchone():
            return jsonify(success=False,error="Ese slug ya está en uso"),409
        sizes=data.get("sizes") if isinstance(data.get("sizes"),list) else []
        if before.get("status") == "published":
            live_sizes=[s for s in sizes if isinstance(s,dict) and bool(s.get("active",True))]
            if not name or not str(data.get("description") or "").strip() or not before.get("has_cover") or not live_sizes:
                return jsonify(success=False,error="Un modelo publicado debe conservar nombre, descripción, portada y al menos un tamaño activo"),400
            for size in live_sizes:
                try: price=int(size.get("price_clp"))
                except (TypeError,ValueError): price=-1
                if not str(size.get("label") or "").strip() or price<0:
                    return jsonify(success=False,error="Cada tamaño activo debe conservar nombre y precio válido"),400
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("""UPDATE tienda_tortas_listas SET slug=?,name=?,short_description=?,description=?,ingredients=?,allergens=?,care_text=?,allow_message=?,message_max_chars=?,message_instructions=?,sort_order=?,updated_by=?,updated_at=CURRENT_TIMESTAMP WHERE id=?""",
            (slug,name,str(data.get("short_description") or "")[:180],str(data.get("description") or "")[:2000],str(data.get("ingredients") or "")[:1000],str(data.get("allergens") or "")[:500],str(data.get("care_text") or "")[:1000],int(bool(data.get("allow_message"))),max(1,min(120,int(data.get("message_max_chars") or 60))),str(data.get("message_instructions") or "")[:240],int(data.get("sort_order") or 0),actor,model_id))
        conn.execute("DELETE FROM tienda_tortas_listas_tamanos WHERE model_id=?",(model_id,))
        for order, size in enumerate(sizes[:30]):
            label=str(size.get("label") or "").strip()[:80]
            try: price=int(size.get("price_clp"))
            except (TypeError,ValueError): price=-1
            if not label or price < 0: raise ValueError("Cada tamaño debe tener nombre y precio CLP válido")
            conn.execute("INSERT INTO tienda_tortas_listas_tamanos(model_id,label,servings,price_clp,active,sort_order) VALUES(?,?,?,?,?,?)",(model_id,label,str(size.get("servings") or "")[:80],price,int(bool(size.get("active",True))),order))
        after=_model(conn,model_id)
        _write_audit(conn,model_id,actor,"update",before,after)
        conn.commit()
        return jsonify(success=True,item=_model(conn,model_id))
    except (ValueError,sqlite3.Error) as exc:
        conn.rollback(); return jsonify(success=False,error=str(exc)),400
    finally: conn.close()


@ready_cakes.post("/api/tienda/admin/tortas-listas/<int:model_id>/status")
def api_admin_status(model_id):
    if not _admin(): return jsonify(success=False,error="Acceso no autorizado"),401
    ensure_ready_cakes_schema(); data=request.get_json(silent=True) or {}; status=str(data.get("status") or "")
    if status not in STATUSES-{"archived"}: return jsonify(success=False,error="Estado inválido"),400
    conn=_db()
    try:
        before=_model(conn,model_id)
        if not before: return jsonify(success=False,error="Modelo no encontrado"),404
        if status=="published":
            if not before["name"].strip() or not before["description"].strip() or not before["has_cover"] or not any(int(s["active"]) and int(s["price_clp"])>=0 for s in before["sizes"]):
                return jsonify(success=False,error="Para publicar se requiere nombre, descripción, foto de portada y al menos un tamaño activo con precio válido"),400
        actor=str(session.get("admin_user_name") or session.get("admin_user_id") or "admin")
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("UPDATE tienda_tortas_listas SET status=?,updated_by=?,updated_at=CURRENT_TIMESTAMP WHERE id=?",(status,actor,model_id))
        _write_audit(conn,model_id,actor,"status",before,{"status":status}); conn.commit()
        return jsonify(success=True,item=_model(conn,model_id))
    finally: conn.close()


@ready_cakes.post("/api/tienda/admin/tortas-listas/<int:model_id>/duplicate")
def api_admin_duplicate(model_id):
    if not _admin(): return jsonify(success=False,error="Acceso no autorizado"),401
    ensure_ready_cakes_schema(); conn=_db()
    try:
        src=_model(conn,model_id)
        if not src:return jsonify(success=False,error="Modelo no encontrado"),404
        name=(src["name"]+" (copia)")[:120]; slug=_slug(src["slug"]+"-copia")
        base=slug; n=2
        while conn.execute("SELECT 1 FROM tienda_tortas_listas WHERE slug=?",(slug,)).fetchone(): slug=f"{base}-{n}"; n+=1
        actor=str(session.get("admin_user_name") or "admin")
        cur=conn.execute("INSERT INTO tienda_tortas_listas(slug,name,short_description,description,ingredients,allergens,care_text,status,allow_message,message_max_chars,message_instructions,created_by,updated_by) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",(slug,name,src["short_description"],src["description"],src["ingredients"],src["allergens"],src["care_text"],"draft",int(src["allow_message"]),src["message_max_chars"],src["message_instructions"],actor,actor))
        mid=int(cur.lastrowid)
        for x in src["sizes"]: conn.execute("INSERT INTO tienda_tortas_listas_tamanos(model_id,label,servings,price_clp,active,sort_order) VALUES(?,?,?,?,?,?)",(mid,x["label"],x["servings"],x["price_clp"],x["active"],x["sort_order"]))
        for x in src["images"]: conn.execute("INSERT INTO tienda_tortas_listas_imagenes(model_id,path,alt,sort_order,is_cover,active) VALUES(?,?,?,?,?,1)",(mid,x["path"],x["alt"],x["sort_order"],x["is_cover"]))
        _write_audit(conn,mid,actor,"duplicate",{},src);conn.commit()
        return jsonify(success=True,item=_model(conn,mid)),201
    finally:conn.close()


@ready_cakes.post("/api/tienda/admin/tortas-listas/<int:model_id>/images")
def api_admin_image(model_id):
    if not _admin():return jsonify(success=False,error="Acceso no autorizado"),401
    ensure_ready_cakes_schema(); conn=_db()
    try:
        if not conn.execute("SELECT 1 FROM tienda_tortas_listas WHERE id=?",(model_id,)).fetchone():return jsonify(success=False,error="Modelo no encontrado"),404
        before = _model(conn, model_id)
        upload=request.files.get("image")
        if not upload:return jsonify(success=False,error="Selecciona una imagen"),400
        ext=secure_filename(upload.filename or "").rsplit(".",1)[-1].lower()
        raw=upload.read(5*1024*1024+1)
        if ext not in IMAGE_EXTENSIONS or not raw or len(raw)>5*1024*1024:return jsonify(success=False,error="Imagen inválida (JPG, PNG o WebP; máximo 5 MB)"),400
        valid=(raw.startswith(b"\xff\xd8\xff") if ext in {"jpg","jpeg"} else raw.startswith(b"\x89PNG\r\n\x1a\n") if ext=="png" else raw[:4]==b"RIFF" and raw[8:12]==b"WEBP")
        if not valid:return jsonify(success=False,error="El contenido no corresponde al formato de imagen declarado"),400
        filename=f"{uuid.uuid4().hex}.{ext}"; folder=Path(current_app.static_folder)/"tortas_listas";folder.mkdir(parents=True,exist_ok=True)
        path=folder/filename;path.write_bytes(raw)
        order=conn.execute("SELECT COALESCE(MAX(sort_order),-1)+1 FROM tienda_tortas_listas_imagenes WHERE model_id=?",(model_id,)).fetchone()[0]
        cover=not bool(conn.execute("SELECT 1 FROM tienda_tortas_listas_imagenes WHERE model_id=? AND is_cover=1 AND active=1",(model_id,)).fetchone())
        conn.execute("INSERT INTO tienda_tortas_listas_imagenes(model_id,path,alt,sort_order,is_cover) VALUES(?,?,?,?,?)",(model_id,f"/static/tortas_listas/{filename}",str(request.form.get("alt") or "")[:180],order,int(cover)))
        _write_audit(conn, model_id, str(session.get("admin_user_name") or "admin"), "image_add", before, _model(conn, model_id))
        conn.commit();return jsonify(success=True,item=_model(conn,model_id))
    finally:conn.close()


@ready_cakes.post("/api/tienda/admin/tortas-listas/<int:model_id>/cover/<int:image_id>")
def api_admin_cover(model_id,image_id):
    if not _admin():return jsonify(success=False,error="Acceso no autorizado"),401
    ensure_ready_cakes_schema();conn=_db()
    try:
        if not conn.execute("SELECT 1 FROM tienda_tortas_listas_imagenes WHERE id=? AND model_id=? AND active=1",(image_id,model_id)).fetchone():return jsonify(success=False,error="Imagen no encontrada"),404
        before = _model(conn, model_id)
        conn.execute("UPDATE tienda_tortas_listas_imagenes SET is_cover=CASE WHEN id=? THEN 1 ELSE 0 END WHERE model_id=?",(image_id,model_id))
        _write_audit(conn, model_id, str(session.get("admin_user_name") or "admin"), "cover_change", before, _model(conn, model_id));conn.commit()
        return jsonify(success=True,item=_model(conn,model_id))
    finally:conn.close()


@ready_cakes.patch("/api/tienda/admin/tortas-listas/<int:model_id>/images/<int:image_id>")
def api_admin_image_update(model_id, image_id):
    if not _admin(): return jsonify(success=False,error="Acceso no autorizado"),401
    ensure_ready_cakes_schema(); conn=_db()
    try:
        before=_model(conn,model_id)
        image=conn.execute("SELECT * FROM tienda_tortas_listas_imagenes WHERE id=? AND model_id=?",(image_id,model_id)).fetchone()
        if not before or not image:return jsonify(success=False,error="Modelo o imagen no encontrado"),404
        data=request.get_json(silent=True) or {}; actor=str(session.get("admin_user_name") or "admin")
        if "sort_order" in data:
            try: order=max(0,min(10000,int(data["sort_order"])))
            except (TypeError,ValueError): return jsonify(success=False,error="Orden de foto inválido"),400
            conn.execute("UPDATE tienda_tortas_listas_imagenes SET sort_order=? WHERE id=?",(order,image_id))
        if "alt" in data: conn.execute("UPDATE tienda_tortas_listas_imagenes SET alt=? WHERE id=?",(str(data.get("alt") or "")[:180],image_id))
        if "active" in data:
            if not bool(data["active"]) and int(image["is_cover"]):
                replacement=conn.execute("SELECT id FROM tienda_tortas_listas_imagenes WHERE model_id=? AND active=1 AND id<>? ORDER BY sort_order,id LIMIT 1",(model_id,image_id)).fetchone()
                model_status=conn.execute("SELECT status FROM tienda_tortas_listas WHERE id=?",(model_id,)).fetchone()
                if not replacement and model_status and model_status[0]=="published":
                    return jsonify(success=False,error="Agrega otra foto antes de retirar la portada de un modelo publicado"),409
                if replacement: conn.execute("UPDATE tienda_tortas_listas_imagenes SET is_cover=1 WHERE id=?",(replacement[0],))
                conn.execute("UPDATE tienda_tortas_listas_imagenes SET is_cover=0 WHERE id=?",(image_id,))
            conn.execute("UPDATE tienda_tortas_listas_imagenes SET active=? WHERE id=?",(int(bool(data["active"])),image_id))
        after=_model(conn,model_id);_write_audit(conn,model_id,actor,"image_update",before,after);conn.commit()
        return jsonify(success=True,item=after)
    finally:conn.close()


@ready_cakes.get("/api/tienda/admin/tortas-listas/<int:model_id>/reservas")
def api_admin_reservations(model_id):
    if not _admin():return jsonify(success=False,error="Acceso no autorizado"),401
    ensure_ready_cakes_schema();conn=_db()
    try:
        rows=conn.execute("SELECT event_id,snapshot_json,created_at FROM tienda_tortas_listas_reservas WHERE model_id=? ORDER BY id DESC LIMIT 100",(model_id,)).fetchall()
        return jsonify(success=True,items=[dict(r) for r in rows])
    finally:conn.close()
