import csv
import json
import os
import re
import secrets
import sqlite3
import threading
import time
import uuid
from collections import Counter
from datetime import datetime, time as day_time, timedelta, timezone
from io import BytesIO
from io import StringIO
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from flask import Blueprint, abort, current_app, jsonify, make_response, redirect, render_template, request, session, url_for
from werkzeug.utils import secure_filename

from database import get_db


redes_blueprint = Blueprint("redes", __name__)
_ADMIN_SESSION_KEY = "admin_autenticado"
_ALLOWED_LAYOUTS = {"featured", "quick", "social", "standard"}
_ALLOWED_STATES = {"active", "hidden"}
_MAX_IMAGE_BYTES = 5 * 1024 * 1024
_EVENT_RATE_LOCK = threading.Lock()
_EVENT_RATE_BUCKETS = {}
_PROMO_RATE_LOCK = threading.Lock()
_PROMO_RATE_BUCKETS = {}
_BRAND_PROFILE = {
    "title": "Todo el universo Sucrée en un lugar",
    "description": "Descubre la tienda, agenda tu torta y encuentra todos nuestros canales oficiales.",
    "hero_title": "Un momento dulce merece algo inolvidable",
    "hero_subtitle": "Tortas, celebraciones y detalles hechos con cariño.",
    "cover_image": "",
    "cover_alt": "",
}


def _db():
    conn = get_db()
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS redes_profile (
            id INTEGER PRIMARY KEY CHECK(id=1),
            draft_json TEXT NOT NULL DEFAULT '{}',
            published_json TEXT NOT NULL DEFAULT '{}',
            draft_version INTEGER NOT NULL DEFAULT 1,
            published_at TEXT,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS redes_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            draft_json TEXT NOT NULL,
            published_json TEXT,
            position INTEGER NOT NULL DEFAULT 0,
            draft_archived INTEGER NOT NULL DEFAULT 0,
            published_archived INTEGER NOT NULL DEFAULT 0,
            archived_at TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS redes_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL CHECK(event_type IN ('view','click')),
            link_id INTEGER,
            source TEXT NOT NULL DEFAULT 'desconocido',
            campaign TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_redes_events_time ON redes_events(event_type, created_at);
        CREATE INDEX IF NOT EXISTS idx_redes_events_link_time ON redes_events(link_id, event_type, created_at);
        CREATE TABLE IF NOT EXISTS redes_revisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_json TEXT NOT NULL,
            links_json TEXT NOT NULL,
            author TEXT NOT NULL DEFAULT 'Administrador',
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS redes_promo_config (
            id INTEGER PRIMARY KEY CHECK(id=1),
            active INTEGER NOT NULL DEFAULT 0,
            discount_pct REAL NOT NULL DEFAULT 10,
            validity_days INTEGER NOT NULL DEFAULT 30,
            starts_on TEXT NOT NULL DEFAULT '',
            ends_on TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS redes_promo_codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cliente_id INTEGER,
            email TEXT NOT NULL COLLATE NOCASE UNIQUE,
            telefono TEXT NOT NULL UNIQUE,
            cliente_ref TEXT NOT NULL,
            codigo TEXT NOT NULL UNIQUE,
            cupon_id INTEGER NOT NULL UNIQUE,
            discount_pct REAL NOT NULL,
            created_at TEXT NOT NULL,
            expires_on TEXT NOT NULL,
            consented_at TEXT NOT NULL,
            used_sale_id INTEGER,
            used_at TEXT,
            tipo TEXT NOT NULL DEFAULT 'welcome',
            alcance TEXT NOT NULL DEFAULT 'tienda',
            usos_max INTEGER NOT NULL DEFAULT 1
        );
        CREATE INDEX IF NOT EXISTS idx_redes_promo_codes_created ON redes_promo_codes(created_at DESC);
        """
    )
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(redes_links)").fetchall()}
    if "draft_archived" not in columns:
        conn.execute("ALTER TABLE redes_links ADD COLUMN draft_archived INTEGER NOT NULL DEFAULT 0")
    if "published_archived" not in columns:
        conn.execute("ALTER TABLE redes_links ADD COLUMN published_archived INTEGER NOT NULL DEFAULT 0")
    promo_columns = {row["name"] for row in conn.execute("PRAGMA table_info(redes_promo_codes)").fetchall()}
    for name, ddl in (("tipo", "TEXT NOT NULL DEFAULT 'welcome'"), ("alcance", "TEXT NOT NULL DEFAULT 'tienda'"), ("usos_max", "INTEGER NOT NULL DEFAULT 1")):
        if name not in promo_columns:
            conn.execute(f"ALTER TABLE redes_promo_codes ADD COLUMN {name} {ddl}")
    conn.execute(
        "INSERT OR IGNORE INTO redes_profile(id, draft_json, published_json) VALUES(1, ?, ?)",
        (json.dumps(_BRAND_PROFILE, ensure_ascii=False), json.dumps({}, ensure_ascii=False)),
    )
    conn.execute("INSERT OR IGNORE INTO redes_promo_config(id) VALUES(1)")
    conn.commit()
    return conn


def _now_utc():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _promo_config(conn):
    row = conn.execute("SELECT * FROM redes_promo_config WHERE id=1").fetchone()
    data = dict(row) if row else {}
    return {
        "active": bool(data.get("active")),
        "discount_pct": max(1.0, min(100.0, float(data.get("discount_pct") or 10))),
        "validity_days": max(1, min(365, int(data.get("validity_days") or 30))),
        "starts_on": str(data.get("starts_on") or ""),
        "ends_on": str(data.get("ends_on") or ""),
        "updated_at": str(data.get("updated_at") or ""),
    }


def _promo_window_open(config, today=None):
    today = today or datetime.now(ZoneInfo("America/Santiago")).date().isoformat()
    return bool(config.get("active")) and (not config.get("starts_on") or today >= config["starts_on"]) and (not config.get("ends_on") or today <= config["ends_on"])


def _promo_admin_codes(conn, limit=150):
    today = datetime.now(ZoneInfo("America/Santiago")).date().isoformat()
    assignment_table = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='tienda_cliente_cupones'"
    ).fetchone()
    if assignment_table:
        rows = conn.execute(
            """SELECT p.id,p.email,p.telefono,p.codigo,p.discount_pct,p.created_at,p.expires_on,p.used_sale_id,p.used_at,
                      p.tipo,p.alcance,p.usos_max,COALESCE(cc.usos_realizados,0) AS usos_realizados,
                      COALESCE(cc.usos_max,p.usos_max,1) AS asignacion_usos_max
               FROM redes_promo_codes p
               LEFT JOIN tienda_cliente_cupones cc ON cc.cupon_id=p.cupon_id
               ORDER BY p.id DESC LIMIT ?""",
            (int(limit),),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT id,email,telefono,codigo,discount_pct,created_at,expires_on,used_sale_id,used_at,
                      tipo,alcance,usos_max,0 AS usos_realizados,usos_max AS asignacion_usos_max
               FROM redes_promo_codes ORDER BY id DESC LIMIT ?""",
            (int(limit),),
        ).fetchall()
    config = _promo_config(conn)
    result = []
    for row in rows:
        item = dict(row)
        item["usos_realizados"] = int(item.get("usos_realizados") or 0)
        item["usos_max"] = max(1, int(item.get("asignacion_usos_max") or item.get("usos_max") or 1))
        item["alcance"] = str(item.get("alcance") or "tienda")
        item["tipo"] = str(item.get("tipo") or "welcome")
        if item["usos_realizados"] >= item["usos_max"] or (item["tipo"] != "manual" and item.get("used_sale_id")):
            item["status"] = "Usado"
        elif item.get("expires_on", "") < today:
            item["status"] = "Vencido"
        elif item["tipo"] == "welcome" and not _promo_window_open(config, today):
            item["status"] = "Promoción inactiva"
        else:
            item["status"] = "Disponible"
        result.append(item)
    return result


def _normalise_promo_email(value):
    email = str(value or "").strip().lower()
    if len(email) > 254 or not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email):
        raise ValueError("Ingresa un correo electrónico válido.")
    return email


def _normalise_promo_phone(value):
    digits = re.sub(r"\D+", "", str(value or ""))
    if digits.startswith("56"):
        digits = digits[2:]
    if digits.startswith("9") and len(digits) == 9:
        digits = digits[1:]
    if len(digits) != 8:
        raise ValueError("El teléfono debe incluir +569 y 8 dígitos.")
    return "+569" + digits


def _promo_rate_allowed():
    key = str(request.remote_addr or "unknown")
    now = time.monotonic()
    with _PROMO_RATE_LOCK:
        start, count = _PROMO_RATE_BUCKETS.get(key, (now, 0))
        if now - start >= 600:
            start, count = now, 0
        count += 1
        _PROMO_RATE_BUCKETS[key] = (start, count)
        if len(_PROMO_RATE_BUCKETS) > 4000:
            for address, bucket in list(_PROMO_RATE_BUCKETS.items()):
                if now - bucket[0] > 600:
                    _PROMO_RATE_BUCKETS.pop(address, None)
        return count <= 8


def redes_promo_registration_contact(registration_id, code):
    """Resolve the current browser's registration for coupon validation without storing PII in its cookie."""
    if not registration_id or not code:
        return None
    conn = get_db()
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute("SELECT email,telefono,codigo FROM redes_promo_codes WHERE id=?", (int(registration_id),)).fetchone()
        if not row or str(row["codigo"]).upper() != str(code).strip().upper():
            return None
        return {"email": row["email"], "telefono": row["telefono"]}
    except (TypeError, ValueError):
        return None
    finally:
        conn.close()


def redes_promo_validate_checkout(code, cliente_ref):
    """Return None for ordinary coupons or a validation result for a Redes code."""
    if not code:
        return None
    conn = get_db()
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            """SELECT p.*,c.activo AS coupon_active,
                      cc.activo AS assignment_active,COALESCE(cc.usos_realizados,0) AS usos_realizados,
                      COALESCE(cc.usos_max,p.usos_max,1) AS assignment_uses_max
               FROM redes_promo_codes p
               LEFT JOIN tienda_cupones c ON c.id=p.cupon_id
               LEFT JOIN tienda_cliente_cupones cc ON cc.cupon_id=p.cupon_id
               WHERE p.codigo=? COLLATE NOCASE""",
            (str(code).strip().upper(),),
        ).fetchone()
        if not row:
            return None
        today = datetime.now(ZoneInfo("America/Santiago")).date().isoformat()
        is_manual = str(row["tipo"] or "welcome") == "manual"
        if not is_manual and not _promo_window_open(_promo_config(conn), today):
            return {"ok": False, "error": "La promoci\u00f3n de bienvenida no est\u00e1 activa."}
        if str(row["expires_on"] or "") < today:
            return {"ok": False, "error": "El c\u00f3digo de descuento venci\u00f3."}
        if is_manual:
            if not int(row["coupon_active"] or 0) or not int(row["assignment_active"] or 0):
                return {"ok": False, "error": "Este c\u00f3digo ya no est\u00e1 disponible."}
            if int(row["usos_realizados"] or 0) >= int(row["assignment_uses_max"] or 1):
                return {"ok": False, "error": "Este c\u00f3digo ya agot\u00f3 sus usos disponibles."}
        elif row["used_sale_id"]:
            return {"ok": False, "error": "Este c\u00f3digo ya fue utilizado."}
        if not cliente_ref or not secrets.compare_digest(str(row["cliente_ref"]), str(cliente_ref)):
            return {"ok": False, "error": "Este c\u00f3digo pertenece a otro correo o tel\u00e9fono."}
        return {"ok": True, "id": int(row["id"]), "coupon_id": int(row["cupon_id"])}
    except sqlite3.OperationalError as exc:
        message = str(exc).lower()
        if "no such column" in message:
            legacy = conn.execute(
                "SELECT * FROM redes_promo_codes WHERE codigo=? COLLATE NOCASE",
                (str(code).strip().upper(),),
            ).fetchone()
            if not legacy:
                return None
            today = datetime.now(ZoneInfo("America/Santiago")).date().isoformat()
            if not _promo_window_open(_promo_config(conn), today):
                return {"ok": False, "error": "La promocion de bienvenida no esta activa."}
            if str(legacy["expires_on"] or "") < today:
                return {"ok": False, "error": "El codigo de bienvenida vencio."}
            if legacy["used_sale_id"]:
                return {"ok": False, "error": "Este codigo ya fue utilizado."}
            if not cliente_ref or not secrets.compare_digest(str(legacy["cliente_ref"]), str(cliente_ref)):
                return {"ok": False, "error": "Este codigo pertenece a otro correo o telefono."}
            return {"ok": True, "id": int(legacy["id"]), "coupon_id": int(legacy["cupon_id"])}
        if "no such table" in message:
            return None
        raise
    finally:
        conn.close()


def _json(value, fallback=None):
    try:
        parsed = json.loads(value or "")
        return parsed if isinstance(parsed, dict) else (fallback or {})
    except (TypeError, ValueError):
        return fallback or {}


def _clean_text(value, limit, default=""):
    text = re.sub(r"[\x00-\x1f\x7f]", "", str(value or "")).strip()
    return text[:limit] if text else default


def _validate_destination(value):
    raw = str(value or "").strip()
    if not raw or len(raw) > 1500 or re.search(r"[\x00-\x20\x7f\\]", raw):
        raise ValueError("Ingresa una URL o ruta válida, sin espacios ni caracteres de control.")
    parsed = urlparse(raw)
    if parsed.scheme:
        if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
            raise ValueError("Solo se permiten enlaces http/https con un dominio válido.")
        try:
            parsed.port
        except ValueError as exc:
            raise ValueError("El puerto de la URL no es válido.") from exc
        if parsed.username or parsed.password:
            raise ValueError("No se permiten credenciales dentro de la URL.")
        return raw
    if parsed.netloc or not raw.startswith("/") or raw.startswith("//"):
        raise ValueError("Las rutas internas deben comenzar con una sola barra (/).")
    return raw


def _normalise_link(payload):
    if not isinstance(payload, dict):
        raise ValueError("Los datos del enlace no tienen un formato válido.")
    title = _clean_text(payload.get("title"), 90)
    if not title:
        raise ValueError("El título es obligatorio.")
    layout = str(payload.get("layout") or "standard").strip().lower()
    state = str(payload.get("status") or "active").strip().lower()
    if layout not in _ALLOWED_LAYOUTS:
        raise ValueError("Selecciona una presentación válida.")
    if state not in _ALLOWED_STATES:
        raise ValueError("Selecciona un estado válido.")
    image = _clean_text(payload.get("image"), 240)
    if image and not re.fullmatch(r"/static/uploads/redes/[a-f0-9]{32}\.jpg", image):
        raise ValueError("La imagen del enlace debe subirse desde este panel.")
    return {
        "title": title,
        "url": _validate_destination(payload.get("url")),
        "subtitle": _clean_text(payload.get("subtitle"), 180),
        "layout": layout,
        "group": _clean_text(payload.get("group"), 60),
        "status": state,
        "icon": _clean_text(payload.get("icon"), 16),
        "image": image,
        "image_alt": _clean_text(payload.get("image_alt"), 140),
    }


def _normalise_profile(payload):
    if not isinstance(payload, dict):
        raise ValueError("Los datos de la página no tienen un formato válido.")
    profile = dict(_BRAND_PROFILE)
    for key, maximum in (("title", 100), ("description", 220), ("hero_title", 100), ("hero_subtitle", 180), ("cover_alt", 140)):
        profile[key] = _clean_text(payload.get(key), maximum, _BRAND_PROFILE[key])
    profile["cover_image"] = _clean_text(payload.get("cover_image"), 240)
    if profile["cover_image"] and not re.fullmatch(r"/static/uploads/redes/[a-f0-9]{32}\.jpg", profile["cover_image"]):
        raise ValueError("La foto de portada debe subirse desde este panel.")
    return profile


def _admin_required(mutating=False):
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "Acceso no autorizado."}), 401
    if mutating:
        expected = str(session.get("redes_csrf") or "")
        supplied = str(request.headers.get("X-CSRF-Token") or "")
        if not expected or not supplied or not secrets.compare_digest(expected, supplied):
            return jsonify({"success": False, "error": "La sesión de edición venció. Recarga el panel y vuelve a intentar."}), 403
    return None


def _all_links(conn, published=False, include_archived=False):
    if published:
        where = "" if include_archived else "WHERE published_archived=0"
    else:
        where = "" if include_archived else "WHERE draft_archived=0"
    rows = conn.execute(f"SELECT * FROM redes_links {where} ORDER BY position, id").fetchall()
    out = []
    for row in rows:
        raw = row["published_json"] if published else row["draft_json"]
        if not raw:
            continue
        item = _json(raw)
        item.update({"id": row["id"], "position": row["position"], "updated_at": row["updated_at"], "archived": bool(row["draft_archived"] if not published else row["published_archived"])})
        out.append(item)
    return out


def _published_state(conn):
    row = conn.execute("SELECT published_json,published_at FROM redes_profile WHERE id=1").fetchone()
    profile = _json(row["published_json"] if row else "")
    if not row or not row["published_at"]:
        profile = dict(_BRAND_PROFILE)
    links = [link for link in _all_links(conn, published=True) if link.get("status") == "active"]
    return profile, links


def _period_bounds(period, start_raw="", end_raw=""):
    tz = ZoneInfo("America/Santiago")
    now_local = datetime.now(tz)
    if period == "today":
        start_local = datetime.combine(now_local.date(), day_time.min, tzinfo=tz)
        end_local = now_local + timedelta(seconds=1)
    elif period == "range":
        try:
            first = datetime.strptime(start_raw, "%Y-%m-%d").date()
            last = datetime.strptime(end_raw, "%Y-%m-%d").date()
        except ValueError:
            raise ValueError("Selecciona un rango de fechas válido.")
        if last < first or (last - first).days > 366:
            raise ValueError("El rango debe ser válido y no superar 366 días.")
        start_local = datetime.combine(first, day_time.min, tzinfo=tz)
        end_local = datetime.combine(last + timedelta(days=1), day_time.min, tzinfo=tz)
    else:
        days = 7 if period == "7" else 30
        start_local = datetime.combine((now_local - timedelta(days=days - 1)).date(), day_time.min, tzinfo=tz)
        end_local = now_local + timedelta(seconds=1)
    return start_local.astimezone(timezone.utc).isoformat(), end_local.astimezone(timezone.utc).isoformat()


def _stats(conn, start_utc, end_utc):
    totals = conn.execute(
        "SELECT event_type, COUNT(*) AS n FROM redes_events WHERE created_at>=? AND created_at<? GROUP BY event_type",
        (start_utc, end_utc),
    ).fetchall()
    counts = {row["event_type"]: int(row["n"]) for row in totals}
    views, clicks = counts.get("view", 0), counts.get("click", 0)
    links = conn.execute(
        """SELECT e.link_id, COUNT(*) AS clicks, COALESCE(l.draft_json,l.published_json,'{}') AS link_json
           FROM redes_events e LEFT JOIN redes_links l ON l.id=e.link_id
           WHERE e.event_type='click' AND e.created_at>=? AND e.created_at<?
           GROUP BY e.link_id ORDER BY clicks DESC""",
        (start_utc, end_utc),
    ).fetchall()
    daily_rows = conn.execute(
        "SELECT created_at,event_type FROM redes_events WHERE created_at>=? AND created_at<? ORDER BY created_at",
        (start_utc, end_utc),
    ).fetchall()
    daily_counts = Counter()
    santiago = ZoneInfo("America/Santiago")
    for row in daily_rows:
        try:
            local_day = datetime.fromisoformat(row["created_at"]).astimezone(santiago).date().isoformat()
            daily_counts[(local_day, row["event_type"])] += 1
        except ValueError:
            continue
    origins = conn.execute(
        "SELECT source, COUNT(*) AS n FROM redes_events WHERE event_type='view' AND created_at>=? AND created_at<? GROUP BY source ORDER BY n DESC LIMIT 10",
        (start_utc, end_utc),
    ).fetchall()
    return {
        "views": views,
        "clicks": clicks,
        "rate": round(clicks * 100 / views, 1) if views else 0,
        "by_link": [{"id": row["link_id"], "title": _json(row["link_json"]).get("title") or "Enlace archivado", "clicks": row["clicks"]} for row in links],
        "daily": [{"day": day, "event_type": event_type, "n": count} for (day, event_type), count in sorted(daily_counts.items())],
        "origins": [dict(row) for row in origins],
    }


def _csv_safe(value):
    text = str(value or "")
    if text.lstrip(" \t\r\n").startswith(("=", "+", "-", "@")):
        return "'" + text
    return text


def _is_bot_or_prefetch():
    ua = (request.user_agent.string or "").lower()
    purpose = " ".join((request.headers.get("Purpose", ""), request.headers.get("Sec-Purpose", ""))).lower()
    return bool(session.get("admin_autenticado")) or any(token in ua for token in ("bot", "spider", "crawler", "preview", "facebookexternalhit", "whatsapp")) or any(token in purpose for token in ("prefetch", "prerender"))


def _allow_analytics_event():
    key = str(request.remote_addr or "unknown")
    now = time.monotonic()
    with _EVENT_RATE_LOCK:
        if len(_EVENT_RATE_BUCKETS) > 4000:
            for address, bucket in list(_EVENT_RATE_BUCKETS.items()):
                if now - bucket[0] > 60:
                    _EVENT_RATE_BUCKETS.pop(address, None)
        start, count = _EVENT_RATE_BUCKETS.get(key, (now, 0))
        if now - start >= 60:
            start, count = now, 0
        count += 1
        _EVENT_RATE_BUCKETS[key] = (start, count)
        return count <= 60


def _source_from_referrer(value):
    try:
        parsed = urlparse(str(value or ""))
        host = (parsed.hostname or "").lower()
        if not host or host == request.host.split(":")[0].lower():
            return "desconocido"
        return host[:100]
    except Exception:
        return "desconocido"


def _attribution_from_request():
    payload = request.get_json(silent=True) or {}
    old = session.get("redes_attribution") or {}
    source = _source_from_referrer(payload.get("referrer"))
    campaign = ""
    for key in ("utm_source", "utm_medium", "utm_campaign"):
        value = re.sub(r"[^A-Za-z0-9._-]", "", str(payload.get(key) or ""))[:80]
        if value:
            campaign = f"{campaign}&{key}={value}".strip("&")
    if source == "desconocido" and old.get("source"):
        source = _clean_text(old.get("source"), 100, "desconocido")
        campaign = _clean_text(old.get("campaign"), 240)
    else:
        session["redes_attribution"] = {"source": source, "campaign": campaign, "captured_at": _now_utc()}
    return source, campaign


@redes_blueprint.get("/redes")
def public_page():
    conn = _db()
    try:
        profile, links = _published_state(conn)
        promo = _promo_config(conn)
        promo_token = session.setdefault("redes_promo_csrf", secrets.token_urlsafe(32))
        response = make_response(render_template(
            "redes_publica.html",
            profile=profile,
            links=links,
            preview=False,
            promo=promo,
            promo_available=_promo_window_open(promo),
            promo_csrf=promo_token,
        ))
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        return response
    finally:
        conn.close()


@redes_blueprint.post("/api/redes/promo/register")
def register_welcome_promo():
    expected = str(session.get("redes_promo_csrf") or "")
    supplied = str(request.headers.get("X-CSRF-Token") or "")
    if not expected or not supplied or not secrets.compare_digest(expected, supplied):
        return jsonify({"success": False, "error": "Recarga la página e intenta nuevamente."}), 403
    if not _promo_rate_allowed():
        return jsonify({"success": False, "error": "Demasiados intentos. Vuelve a probar más tarde."}), 429
    data = request.get_json(silent=True) or {}
    if str(data.get("website") or "").strip():
        return jsonify({"success": True})
    if not bool(data.get("consent")):
        return jsonify({"success": False, "error": "Debes aceptar el uso de tus datos para participar en la promoción."}), 400
    try:
        email = _normalise_promo_email(data.get("email"))
        phone = _normalise_promo_phone(data.get("telefono"))
    except ValueError as exc:
        return jsonify({"success": False, "error": str(exc)}), 400

    conn = _db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        promo = _promo_config(conn)
        now_local = datetime.now(ZoneInfo("America/Santiago"))
        today = now_local.date().isoformat()
        if not _promo_window_open(promo, today):
            conn.rollback()
            return jsonify({"success": False, "error": "La promoción de bienvenida no está disponible ahora."}), 410
        # Reuse SucreeStock's customer and coupon tables so the code works in the existing cart.
        from app import _buscar_cliente_por_contacto_cursor, _ensure_adm_pagina_tables, _nombre_desde_email, _normalizar_cliente_ref, _upsert_cliente_tienda_cursor

        cursor = conn.cursor()
        _ensure_adm_pagina_tables(cursor)
        existing = conn.execute(
            """SELECT p.*,c.activo AS coupon_active
               FROM redes_promo_codes p LEFT JOIN tienda_cupones c ON c.id=p.cupon_id
               WHERE p.email=? COLLATE NOCASE OR p.telefono=? LIMIT 1""",
            (email, phone),
        ).fetchone()
        if existing:
            same_account = str(existing["email"]).strip().lower() == email and str(existing["telefono"]).strip() == phone
            expiry_date = datetime.strptime(str(existing["expires_on"]), "%Y-%m-%d").date()
            code_is_active = (
                same_account
                and not existing["used_sale_id"]
                and int(existing["coupon_active"] or 0) == 1
                and expiry_date >= now_local.date()
                and _promo_window_open(promo, today)
            )
            if code_is_active:
                conn.commit()
                session["redes_promo_registration_id"] = int(existing["id"])
                remaining_days = (expiry_date - now_local.date()).days
                return jsonify({
                    "success": True,
                    "already_registered": True,
                    "active_code": True,
                    "discount_pct": float(existing["discount_pct"]),
                    "expires_on": str(existing["expires_on"]),
                    "remaining_days": remaining_days,
                })
            conn.rollback()
            return jsonify({"success": False, "error": "El correo o telefono ya esta asociado a una cuenta. Inicia sesion con tus datos registrados; no se creara otra cuenta ni otro codigo."}), 409

        cliente_existente = _buscar_cliente_por_contacto_cursor(cursor, email, phone)
        if cliente_existente:
            conn.rollback()
            return jsonify({"success": False, "error": "El correo o telefono ya esta asociado a una cuenta. Inicia sesion con tus datos registrados; no se creara otra cuenta ni otro codigo."}), 409

        cliente = _upsert_cliente_tienda_cursor(
            cursor,
            nombre=_nombre_desde_email(email),
            email=email,
            telefono=phone,
            email_confirmado=0,
        )
        if not cliente:
            conn.rollback()
            return jsonify({"success": False, "error": "No se pudo registrar el contacto. Intenta nuevamente."}), 500

        expiry = now_local.date() + timedelta(days=promo["validity_days"])
        if promo["ends_on"]:
            campaign_end = datetime.strptime(promo["ends_on"], "%Y-%m-%d").date()
            expiry = min(expiry, campaign_end)
        expiry_text = expiry.isoformat()
        code = ""
        coupon_id = 0
        for _ in range(8):
            candidate = "SU" + secrets.token_hex(6).upper()
            try:
                cursor.execute(
                    """INSERT INTO tienda_cupones
                       (codigo,nombre,tipo_descuento,valor_descuento,activo,fecha_inicio,fecha_fin,
                        usos_max_total,usos_max_por_cliente,monto_minimo,solo_sin_oferta,alcance)
                       VALUES (?,?, 'porcentaje', ?,1,?,?,1,1,0,0,'tienda')""",
                    (candidate, "Descuento bienvenida Redes", promo["discount_pct"], today, expiry_text),
                )
                coupon_id = int(cursor.lastrowid or 0)
                code = candidate
                break
            except sqlite3.IntegrityError:
                continue
        if not code:
            conn.rollback()
            return jsonify({"success": False, "error": "No se pudo crear el código. Intenta nuevamente."}), 500

        client_ref = _normalizar_cliente_ref(email, phone)
        cursor.execute(
            """INSERT INTO tienda_cliente_cupones
               (cliente_id,cliente_ref,cupon_id,activo,usado,usos_realizados,usos_max,nota,asignado_por,fecha_vencimiento,fecha_asignado)
               VALUES (?,?,?,1,0,0,1,?,'redes_promo',?,CURRENT_TIMESTAMP)""",
            (int(cliente.get("id") or 0), client_ref, coupon_id, f"Promoción bienvenida Redes: {promo['discount_pct']:g}%", expiry_text),
        )
        cursor.execute(
            """INSERT INTO redes_promo_codes
               (cliente_id,email,telefono,cliente_ref,codigo,cupon_id,discount_pct,created_at,expires_on,consented_at)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (int(cliente.get("id") or 0), email, phone, client_ref, code, coupon_id, promo["discount_pct"], _now_utc(), expiry_text, _now_utc()),
        )
        registration_id = int(cursor.lastrowid or 0)
        conn.commit()
        session["redes_promo_registration_id"] = registration_id
        return jsonify({"success": True, "codigo": code, "discount_pct": promo["discount_pct"], "expires_on": expiry_text})
    except sqlite3.IntegrityError:
        conn.rollback()
        return jsonify({"success": False, "error": "Ya existe un registro promocional para uno de estos datos."}), 409
    except Exception:
        conn.rollback()
        current_app.logger.exception("No se pudo crear el código de bienvenida de Redes.")
        return jsonify({"success": False, "error": "No se pudo crear el código en este momento. Intenta nuevamente."}), 500
    finally:
        conn.close()


@redes_blueprint.post("/api/redes/promo/pending")
def pending_welcome_promo():
    data = request.get_json(silent=True) or {}
    registration_id = session.get("redes_promo_registration_id")
    email = str(data.get("email") or "").strip().lower()
    try:
        phone = _normalise_promo_phone(data.get("telefono")) if data.get("telefono") else ""
    except ValueError:
        phone = ""
    conn = _db()
    try:
        if email and phone:
            row = conn.execute(
                """SELECT p.*,c.activo AS coupon_active,
                          COALESCE(cc.usos_realizados,0) AS usos_realizados,
                          COALESCE(cc.usos_max,p.usos_max,1) AS asignacion_usos_max,
                          cc.activo AS assignment_active
                   FROM redes_promo_codes p
                   LEFT JOIN tienda_cupones c ON c.id=p.cupon_id
                   LEFT JOIN tienda_cliente_cupones cc ON cc.cupon_id=p.cupon_id
                   WHERE p.email=? COLLATE NOCASE AND p.telefono=? LIMIT 1""",
                (email, phone),
            ).fetchone()
        elif registration_id:
            row = conn.execute(
                """SELECT p.*,c.activo AS coupon_active,
                          COALESCE(cc.usos_realizados,0) AS usos_realizados,
                          COALESCE(cc.usos_max,p.usos_max,1) AS asignacion_usos_max,
                          cc.activo AS assignment_active
                   FROM redes_promo_codes p
                   LEFT JOIN tienda_cupones c ON c.id=p.cupon_id
                   LEFT JOIN tienda_cliente_cupones cc ON cc.cupon_id=p.cupon_id
                   WHERE p.id=? LIMIT 1""",
                (int(registration_id),),
            ).fetchone()
        else:
            row = None
        if not row:
            return jsonify({"success": True, "pending": False})

        today = datetime.now(ZoneInfo("America/Santiago")).date()
        expires = datetime.strptime(str(row["expires_on"]), "%Y-%m-%d").date()
        config = _promo_config(conn)
        is_manual = str(row["tipo"] or "welcome") == "manual"
        remaining = int(row["asignacion_usos_max"] or 1) - int(row["usos_realizados"] or 0)
        is_pending = (
            (is_manual or not row["used_sale_id"])
            and int(row["coupon_active"] or 0) == 1
            and int(row["assignment_active"] or 0) == 1
            and remaining > 0
            and expires >= today
            and (is_manual or _promo_window_open(config, today.isoformat()))
            and str(row["alcance"] or "tienda") in {"tienda", "ambos"}
        )
        if not is_pending:
            return jsonify({"success": True, "pending": False})
        session["redes_promo_registration_id"] = int(row["id"])
        return jsonify({
            "success": True,
            "pending": True,
            "codigo": str(row["codigo"]),
            "discount_pct": float(row["discount_pct"]),
            "expires_on": str(row["expires_on"]),
            "remaining_days": (expires - today).days,
            "alcance": str(row["alcance"] or "tienda"),
            "usos_restantes": remaining,
        })
    except (TypeError, ValueError):
        return jsonify({"success": True, "pending": False})
    finally:
        conn.close()


@redes_blueprint.get("/redes/go/<int:link_id>")
def go_link(link_id):
    conn = _db()
    try:
        row = conn.execute("SELECT published_json, published_archived FROM redes_links WHERE id=?", (link_id,)).fetchone()
        if not row or row["published_archived"] or not row["published_json"]:
            return render_template("redes_no_disponible.html"), 404
        item = _json(row["published_json"])
        if item.get("status") != "active":
            return render_template("redes_no_disponible.html"), 404
        try:
            destination = _validate_destination(item.get("url"))
        except ValueError:
            return render_template("redes_no_disponible.html"), 404
        if not _is_bot_or_prefetch() and _allow_analytics_event():
            try:
                attribution = session.get("redes_attribution") or {}
                conn.execute(
                    "INSERT INTO redes_events(event_type,link_id,source,campaign,created_at) VALUES('click',?,?,?,?)",
                    (link_id, _clean_text(attribution.get("source"), 100, "desconocido"), _clean_text(attribution.get("campaign"), 240), _now_utc()),
                )
                conn.commit()
            except Exception:
                current_app.logger.exception("No se pudo registrar el clic de Redes; se mantiene la redirección.")
        return redirect(destination, code=302)
    finally:
        conn.close()


@redes_blueprint.post("/api/redes/view")
def register_view():
    if _is_bot_or_prefetch() or not _allow_analytics_event():
        return jsonify({"success": True, "counted": False})
    source, campaign = _attribution_from_request()
    conn = _db()
    try:
        conn.execute(
            "INSERT INTO redes_events(event_type,source,campaign,created_at) VALUES('view',?,?,?)",
            (source, campaign, _now_utc()),
        )
        conn.commit()
        return jsonify({"success": True, "counted": True})
    finally:
        conn.close()


@redes_blueprint.get("/admin/redes")
def admin_page():
    if not session.get(_ADMIN_SESSION_KEY):
        return redirect(url_for("admin_login", next=request.path))
    csrf = session.setdefault("redes_csrf", secrets.token_urlsafe(32))
    return render_template("redes_admin.html", csrf_token=csrf, embedded=request.args.get("pos_embed") == "1")


@redes_blueprint.get("/admin/redes/preview")
def admin_preview():
    if not session.get(_ADMIN_SESSION_KEY):
        return redirect(url_for("admin_login", next=request.path))
    conn = _db()
    try:
        row = conn.execute("SELECT draft_json FROM redes_profile WHERE id=1").fetchone()
        profile = _json(row["draft_json"] if row else "", _BRAND_PROFILE)
        links = [link for link in _all_links(conn) if link.get("status") == "active"]
        return render_template("redes_publica.html", profile=profile, links=links, preview=True)
    finally:
        conn.close()


@redes_blueprint.get("/api/redes/admin/state")
def admin_state():
    denied = _admin_required()
    if denied:
        return denied
    conn = _db()
    try:
        row = conn.execute("SELECT draft_json,published_json,draft_version,published_at,updated_at FROM redes_profile WHERE id=1").fetchone()
        profile = _json(row["draft_json"], _BRAND_PROFILE)
        links = _all_links(conn)
        published_links = _all_links(conn, published=True)
        published_profile = _json(row["published_json"])
        draft_compact = [(item["id"], {key: value for key, value in item.items() if key not in {"id", "updated_at", "archived"}}) for item in links]
        live_compact = [(item["id"], {key: value for key, value in item.items() if key not in {"id", "updated_at", "archived"}}) for item in published_links]
        has_changes = json.dumps(profile, sort_keys=True, ensure_ascii=False) != json.dumps(published_profile, sort_keys=True, ensure_ascii=False) or json.dumps(draft_compact, sort_keys=True, ensure_ascii=False) != json.dumps(live_compact, sort_keys=True, ensure_ascii=False)
        period = request.args.get("period", "30")
        start, end = _period_bounds(period, request.args.get("start", ""), request.args.get("end", ""))
        # Per-link click totals are scoped to the selected interval and archived links remain visible.
        stats = _stats(conn, start, end)
        revisions = [dict(item) for item in conn.execute("SELECT id,author,created_at FROM redes_revisions ORDER BY id DESC LIMIT 8").fetchall()]
        for link in links:
            link["clicks"] = next((item["clicks"] for item in stats["by_link"] if item["id"] == link["id"]), 0)
        return jsonify({"success": True, "profile": profile, "published_profile": published_profile, "links": links, "version": row["draft_version"], "published_at": row["published_at"], "updated_at": row["updated_at"], "published": bool(row["published_at"]), "has_changes": has_changes, "stats": stats, "period": period, "revisions": revisions, "promo": _promo_config(conn), "promo_codes": _promo_admin_codes(conn)})
    except ValueError as exc:
        return jsonify({"success": False, "error": str(exc)}), 400
    finally:
        conn.close()


@redes_blueprint.get("/api/redes/admin/customers")
def admin_customers():
    denied = _admin_required()
    if denied:
        return denied
    try:
        page = max(1, int(request.args.get("page", "1")))
        per_page = min(100, max(1, int(request.args.get("per_page", "25"))))
    except (TypeError, ValueError):
        return jsonify({"success": False, "error": "Paginacion invalida."}), 400

    query = str(request.args.get("q") or "").strip()[:120].lower()
    where = ""
    params = []
    if query:
        where = "WHERE LOWER(COALESCE(c.nombre,'')) LIKE ? OR LOWER(COALESCE(c.email,'')) LIKE ? OR LOWER(COALESCE(c.telefono,'')) LIKE ?"
        term = f"%{query}%"
        params.extend((term, term, term))

    conn = _db()
    try:
        client_columns = {row["name"] for row in conn.execute("PRAGMA table_info(tienda_clientes)").fetchall()}
        created_expr = "c.creado_en" if "creado_en" in client_columns else "c.actualizado_en" if "actualizado_en" in client_columns else "NULL"
        login_expr = "c.ultimo_login" if "ultimo_login" in client_columns else "NULL"
        active_expr = "c.activo" if "activo" in client_columns else "1"
        total = int(conn.execute(
            f"SELECT COUNT(*) FROM tienda_clientes c {where}", tuple(params)
        ).fetchone()[0])
        rows = conn.execute(
            f"""SELECT c.id,c.nombre,c.email,c.telefono,{active_expr} AS activo,
                       {created_expr} AS creado_en,{login_expr} AS ultimo_login,
                       p.codigo AS promo_codigo,p.tipo AS promo_tipo,p.expires_on AS promo_vence
                FROM tienda_clientes c
                LEFT JOIN redes_promo_codes p
                  ON p.id=(SELECT MAX(p2.id) FROM redes_promo_codes p2 WHERE p2.cliente_id=c.id)
                {where}
                ORDER BY datetime(COALESCE({created_expr},'1970-01-01')) DESC,c.id DESC
                LIMIT ? OFFSET ?""",
            (*params, per_page, (page - 1) * per_page),
        ).fetchall()
        customers = []
        today = datetime.now(ZoneInfo("America/Santiago")).date().isoformat()
        for row in rows:
            item = dict(row)
            if item.get("promo_codigo"):
                item["origen"] = "Codigo manual" if item.get("promo_tipo") == "manual" else "Promocion Redes"
                item["promo_estado"] = "Vencido" if str(item.get("promo_vence") or "") < today else "Con codigo"
            else:
                item["origen"] = "Cuenta registrada"
                item["promo_estado"] = ""
            item["activo"] = bool(item.get("activo"))
            customers.append(item)
        return jsonify({
            "success": True,
            "clientes": customers,
            "total": total,
            "page": page,
            "per_page": per_page,
            "pages": max(1, (total + per_page - 1) // per_page),
        })
    finally:
        conn.close()


@redes_blueprint.post("/api/redes/admin/promo")
def save_promo_config():
    denied = _admin_required(mutating=True)
    if denied:
        return denied
    data = request.get_json(silent=True) or {}
    try:
        active = 1 if data.get("active") is True else 0
        discount_pct = float(data.get("discount_pct"))
        validity_days = int(data.get("validity_days"))
        starts_on = str(data.get("starts_on") or "").strip()
        ends_on = str(data.get("ends_on") or "").strip()
        if not 1 <= discount_pct <= 100:
            raise ValueError("El descuento debe estar entre 1% y 100%.")
        if not 1 <= validity_days <= 365:
            raise ValueError("La vigencia de cada código debe ser de 1 a 365 días.")
        for value in (starts_on, ends_on):
            if value:
                datetime.strptime(value, "%Y-%m-%d")
        if starts_on and ends_on and ends_on < starts_on:
            raise ValueError("La fecha de término no puede ser anterior a la fecha de inicio.")
    except (TypeError, ValueError) as exc:
        return jsonify({"success": False, "error": str(exc) or "Revisa la configuración de la promoción."}), 400
    conn = _db()
    try:
        conn.execute(
            """UPDATE redes_promo_config SET active=?,discount_pct=?,validity_days=?,starts_on=?,ends_on=?,updated_at=? WHERE id=1""",
            (active, discount_pct, validity_days, starts_on, ends_on, _now_utc()),
        )
        conn.commit()
        return jsonify({"success": True, "promo": _promo_config(conn)})
    finally:
        conn.close()


@redes_blueprint.post("/api/redes/admin/promo/create-code")
def create_manual_promo_code():
    denied = _admin_required(mutating=True)
    if denied:
        return denied
    data = request.get_json(silent=True) or {}
    try:
        email = _normalise_promo_email(data.get("email"))
        phone = _normalise_promo_phone(data.get("telefono"))
        discount_pct = float(data.get("discount_pct"))
        validity_days = int(data.get("validity_days"))
        uses_max = int(data.get("uses_max"))
        alcance = str(data.get("alcance") or "tienda").strip().lower()
        if not 1 <= discount_pct <= 100:
            raise ValueError("El descuento debe estar entre 1% y 100%.")
        if not 1 <= validity_days <= 3650:
            raise ValueError("La vigencia debe ser de 1 a 3650 dias.")
        if not 1 <= uses_max <= 1000:
            raise ValueError("Los usos deben ser de 1 a 1000.")
        if alcance not in {"tienda", "agenda_torta", "ambos"}:
            raise ValueError("Selecciona un alcance valido para el codigo.")
    except (TypeError, ValueError) as exc:
        return jsonify({"success": False, "error": str(exc) or "Revisa los datos del codigo."}), 400

    from app import (
        _buscar_cliente_por_contacto_cursor,
        _ensure_adm_pagina_tables,
        _nombre_desde_email,
        _normalizar_cliente_ref,
        _normalizar_email,
        _normalizar_telefono_cl,
        _obtener_cliente_por_contacto_cursor,
        _upsert_cliente_tienda_cursor,
    )

    conn = _db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        cursor = conn.cursor()
        _ensure_adm_pagina_tables(cursor)
        existing_code = conn.execute(
            "SELECT id FROM redes_promo_codes WHERE email=? COLLATE NOCASE OR telefono=? LIMIT 1",
            (email, phone),
        ).fetchone()
        if existing_code:
            conn.rollback()
            return jsonify({"success": False, "error": "Ya existe un codigo de Redes asociado a ese correo o telefono."}), 409

        existing_client = _buscar_cliente_por_contacto_cursor(cursor, email, phone)
        if existing_client:
            exact_email = _normalizar_email(existing_client.get("email")) == email
            exact_phone = _normalizar_telefono_cl(existing_client.get("telefono")) == phone
            if not (exact_email and exact_phone):
                conn.rollback()
                return jsonify({"success": False, "error": "El correo o telefono ya pertenece a una cuenta con otros datos."}), 409
            cliente = _obtener_cliente_por_contacto_cursor(cursor, email, phone)
        else:
            cliente = _upsert_cliente_tienda_cursor(
                cursor,
                nombre=_nombre_desde_email(email),
                email=email,
                telefono=phone,
                email_confirmado=0,
            )
        if not cliente:
            conn.rollback()
            return jsonify({"success": False, "error": "No se pudo crear o encontrar la cuenta del cliente."}), 500

        today = datetime.now(ZoneInfo("America/Santiago")).date()
        expires = today + timedelta(days=validity_days)
        expiry_text = expires.isoformat()
        code = ""
        coupon_id = 0
        for _ in range(8):
            candidate = "SU" + secrets.token_hex(6).upper()
            try:
                cursor.execute(
                    """INSERT INTO tienda_cupones
                       (codigo,nombre,tipo_descuento,valor_descuento,activo,fecha_inicio,fecha_fin,
                        usos_max_total,usos_max_por_cliente,monto_minimo,solo_sin_oferta,alcance)
                       VALUES (?,?,'porcentaje',?,1,?,?,?, ?,0,0,?)""",
                    (candidate, "Descuento manual Redes", discount_pct, today.isoformat(), expiry_text, uses_max, uses_max, alcance),
                )
                coupon_id = int(cursor.lastrowid or 0)
                code = candidate
                break
            except sqlite3.IntegrityError:
                continue
        if not code:
            conn.rollback()
            return jsonify({"success": False, "error": "No se pudo generar un codigo unico."}), 500

        cliente_id = int(cliente.get("id") or 0)
        client_ref = _normalizar_cliente_ref(email, phone)
        cursor.execute(
            """INSERT INTO tienda_cliente_cupones
               (cliente_id,cliente_ref,cupon_id,activo,usado,usos_realizados,usos_max,nota,asignado_por,fecha_vencimiento,fecha_asignado)
               VALUES (?,?,?,1,0,0,?,?,'redes_manual',?,CURRENT_TIMESTAMP)""",
            (cliente_id, client_ref, coupon_id, uses_max, f"Codigo manual Redes: {discount_pct:g}% - {alcance}", expiry_text),
        )
        cursor.execute(
            """INSERT INTO redes_promo_codes
               (cliente_id,email,telefono,cliente_ref,codigo,cupon_id,discount_pct,created_at,expires_on,consented_at,tipo,alcance,usos_max)
               VALUES (?,?,?,?,?,?,?,?,?,?, 'manual', ?, ?)""",
            (cliente_id, email, phone, client_ref, code, coupon_id, discount_pct, _now_utc(), expiry_text, _now_utc(), alcance, uses_max),
        )
        conn.commit()
        return jsonify({
            "success": True,
            "codigo": code,
            "email": email,
            "telefono": phone,
            "discount_pct": discount_pct,
            "expires_on": expiry_text,
            "alcance": alcance,
            "usos_max": uses_max,
        })
    except sqlite3.IntegrityError:
        conn.rollback()
        return jsonify({"success": False, "error": "Ese correo o telefono ya recibio un codigo de Redes."}), 409
    except Exception:
        conn.rollback()
        current_app.logger.exception("No se pudo crear un código manual de Redes.")
        return jsonify({"success": False, "error": "No se pudo generar el codigo manual."}), 500
    finally:
        conn.close()


@redes_blueprint.post("/api/redes/admin/profile")
def save_profile():
    denied = _admin_required(mutating=True)
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    try:
        profile = _normalise_profile(payload.get("profile"))
        expected = int(payload.get("version") or 0)
    except (ValueError, TypeError) as exc:
        return jsonify({"success": False, "error": str(exc)}), 400
    conn = _db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT draft_version FROM redes_profile WHERE id=1").fetchone()
        if int(row["draft_version"]) != expected:
            conn.rollback()
            return jsonify({"success": False, "error": "Otro administrador guardó cambios. Recarga para evitar sobrescribirlos.", "conflict": True}), 409
        conn.execute("UPDATE redes_profile SET draft_json=?,draft_version=draft_version+1,updated_at=? WHERE id=1", (json.dumps(profile, ensure_ascii=False), _now_utc()))
        conn.commit()
        return jsonify({"success": True, "profile": profile, "version": expected + 1})
    finally:
        conn.close()


@redes_blueprint.post("/api/redes/admin/links")
def create_link():
    denied = _admin_required(mutating=True)
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    try:
        item = _normalise_link(payload)
    except ValueError as exc:
        return jsonify({"success": False, "error": str(exc)}), 400
    conn = _db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        if item["layout"] == "featured" and item["status"] == "active":
            existing = conn.execute("SELECT id,draft_json FROM redes_links WHERE draft_archived=0").fetchall()
            if any((link := _json(row["draft_json"])).get("layout") == "featured" and link.get("status") == "active" for row in existing):
                return jsonify({"success": False, "error": "Ya existe un enlace destacado activo. Cambia el actual antes de guardar otro."}), 409
        position = conn.execute("SELECT COALESCE(MAX(position),0)+1 AS p FROM redes_links WHERE draft_archived=0").fetchone()["p"]
        now = _now_utc()
        cur = conn.execute("INSERT INTO redes_links(draft_json,position,created_at,updated_at) VALUES(?,?,?,?)", (json.dumps(item, ensure_ascii=False), position, now, now))
        conn.execute("UPDATE redes_profile SET draft_version=draft_version+1,updated_at=? WHERE id=1", (now,))
        conn.commit()
        item.update({"id": cur.lastrowid, "position": position, "clicks": 0, "updated_at": now})
        return jsonify({"success": True, "link": item}), 201
    finally:
        conn.close()


@redes_blueprint.patch("/api/redes/admin/links/<int:link_id>")
def update_link(link_id):
    denied = _admin_required(mutating=True)
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    try:
        item = _normalise_link(payload)
    except ValueError as exc:
        return jsonify({"success": False, "error": str(exc)}), 400
    conn = _db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT draft_json,updated_at FROM redes_links WHERE id=? AND draft_archived=0", (link_id,)).fetchone()
        if not row:
            return jsonify({"success": False, "error": "El enlace ya no existe o fue archivado."}), 404
        if payload.get("updated_at") and payload["updated_at"] != row["updated_at"]:
            return jsonify({"success": False, "error": "Este enlace cambió en otra sesión. Recarga antes de guardarlo.", "conflict": True}), 409
        if item["layout"] == "featured" and item["status"] == "active":
            others = conn.execute("SELECT id,draft_json FROM redes_links WHERE id<>? AND draft_archived=0", (link_id,)).fetchall()
            if any((existing := _json(other["draft_json"])).get("layout") == "featured" and existing.get("status") == "active" for other in others):
                return jsonify({"success": False, "error": "Ya existe otro enlace destacado activo. Edita primero el destacado actual."}), 409
        now = _now_utc()
        conn.execute("UPDATE redes_links SET draft_json=?,updated_at=? WHERE id=?", (json.dumps(item, ensure_ascii=False), now, link_id))
        conn.execute("UPDATE redes_profile SET draft_version=draft_version+1,updated_at=? WHERE id=1", (now,))
        conn.commit()
        item.update({"id": link_id, "position": conn.execute("SELECT position FROM redes_links WHERE id=?", (link_id,)).fetchone()["position"], "updated_at": now})
        return jsonify({"success": True, "link": item})
    finally:
        conn.close()


@redes_blueprint.post("/api/redes/admin/links/order")
def reorder_links():
    denied = _admin_required(mutating=True)
    if denied:
        return denied
    ids = (request.get_json(silent=True) or {}).get("ids")
    if not isinstance(ids, list) or len(ids) > 300 or any(not str(value).isdigit() for value in ids) or len(set(ids)) != len(ids):
        return jsonify({"success": False, "error": "La lista de orden no es válida."}), 400
    conn = _db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        active = {row["id"] for row in conn.execute("SELECT id FROM redes_links WHERE draft_archived=0").fetchall()}
        if set(int(value) for value in ids) != active:
            conn.rollback()
            return jsonify({"success": False, "error": "La lista cambió. Recarga antes de ordenar."}), 409
        for position, link_id in enumerate(ids, 1):
            conn.execute("UPDATE redes_links SET position=?,updated_at=? WHERE id=?", (position, _now_utc(), int(link_id)))
        conn.execute("UPDATE redes_profile SET draft_version=draft_version+1,updated_at=? WHERE id=1", (_now_utc(),))
        conn.commit()
        return jsonify({"success": True})
    finally:
        conn.close()


@redes_blueprint.post("/api/redes/admin/links/<int:link_id>/duplicate")
def duplicate_link(link_id):
    denied = _admin_required(mutating=True)
    if denied:
        return denied
    conn = _db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT draft_json,position FROM redes_links WHERE id=? AND draft_archived=0", (link_id,)).fetchone()
        if not row:
            return jsonify({"success": False, "error": "Enlace no encontrado."}), 404
        item = _json(row["draft_json"])
        item["title"] = _clean_text(item.get("title"), 90)[:78] + " (copia)"
        if item.get("layout") == "featured":
            item["layout"] = "standard"
        item["status"] = "hidden"
        now = _now_utc()
        conn.execute("UPDATE redes_links SET position=position+1 WHERE draft_archived=0 AND position>?", (row["position"],))
        cur = conn.execute("INSERT INTO redes_links(draft_json,position,created_at,updated_at) VALUES(?,?,?,?)", (json.dumps(item, ensure_ascii=False), row["position"] + 1, now, now))
        conn.execute("UPDATE redes_profile SET draft_version=draft_version+1,updated_at=? WHERE id=1", (now,))
        conn.commit()
        return jsonify({"success": True, "id": cur.lastrowid})
    finally:
        conn.close()


@redes_blueprint.post("/api/redes/admin/links/<int:link_id>/archive")
def archive_link(link_id):
    denied = _admin_required(mutating=True)
    if denied:
        return denied
    conn = _db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        now = _now_utc()
        result = conn.execute("UPDATE redes_links SET draft_archived=1,updated_at=? WHERE id=? AND draft_archived=0", (now, link_id))
        if not result.rowcount:
            return jsonify({"success": False, "error": "Enlace no encontrado."}), 404
        conn.execute("UPDATE redes_profile SET draft_version=draft_version+1,updated_at=? WHERE id=1", (now,))
        conn.commit()
        return jsonify({"success": True})
    finally:
        conn.close()


@redes_blueprint.post("/api/redes/admin/image")
def upload_cover():
    denied = _admin_required(mutating=True)
    if denied:
        return denied
    file = request.files.get("image")
    if not file or not file.filename:
        return jsonify({"success": False, "error": "Selecciona una imagen."}), 400
    raw = file.read(_MAX_IMAGE_BYTES + 1)
    if not raw or len(raw) > _MAX_IMAGE_BYTES:
        return jsonify({"success": False, "error": "La imagen debe pesar hasta 5 MB."}), 400
    signatures = ((b"\xff\xd8\xff", "image/jpeg"), (b"\x89PNG\r\n\x1a\n", "image/png"))
    valid_signature = any(raw.startswith(signature) and (not file.mimetype or file.mimetype in {mime, "application/octet-stream"}) for signature, mime in signatures)
    valid_webp = len(raw) > 12 and raw[:4] == b"RIFF" and raw[8:12] == b"WEBP" and (not file.mimetype or file.mimetype in {"image/webp", "application/octet-stream"})
    if not valid_signature and not valid_webp:
        return jsonify({"success": False, "error": "Usa una imagen JPG, PNG o WebP válida."}), 400
    try:
        from PIL import Image
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            image = Image.open(BytesIO(raw))
            image.verify()
        image = Image.open(BytesIO(raw))
        image.thumbnail((1800, 1800), Image.Resampling.LANCZOS)
        if image.mode not in {"RGB", "L"}:
            image = image.convert("RGB")
        output = BytesIO()
        image.save(output, format="JPEG", quality=84, optimize=True, progressive=True)
        raw = output.getvalue()
    except Exception:
        return jsonify({"success": False, "error": "No se pudo procesar esta imagen. Prueba con otra foto JPG, PNG o WebP."}), 400
    root = str(current_app.config.get("REDES_UPLOAD_DIR") or os.path.join(current_app.static_folder, "uploads", "redes"))
    os.makedirs(root, exist_ok=True)
    filename = f"{uuid.uuid4().hex}.jpg"
    path = os.path.join(root, secure_filename(filename))
    with open(path, "wb") as output:
        output.write(raw)
    return jsonify({"success": True, "url": url_for("static", filename=f"uploads/redes/{filename}")})


@redes_blueprint.post("/api/redes/admin/publish")
def publish():
    denied = _admin_required(mutating=True)
    if denied:
        return denied
    conn = _db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        profile_row = conn.execute("SELECT draft_json,published_json,draft_version FROM redes_profile WHERE id=1").fetchone()
        draft_profile = _json(profile_row["draft_json"], _BRAND_PROFILE)
        links = _all_links(conn)
        featured = [item for item in links if item.get("layout") == "featured" and item.get("status") == "active"]
        if len(featured) > 1:
            conn.rollback()
            return jsonify({"success": False, "error": "Solo se permite un enlace destacado activo."}), 409
        for item in links:
            if item.get("status") == "active":
                _validate_destination(item.get("url"))
        old_links = _all_links(conn, published=True, include_archived=True)
        conn.execute(
            "INSERT INTO redes_revisions(profile_json,links_json,author,created_at) VALUES(?,?,?,?)",
            (profile_row["published_json"] or "{}", json.dumps(old_links, ensure_ascii=False), _clean_text(session.get("admin_user_name"), 80, "Administrador"), _now_utc()),
        )
        now = _now_utc()
        conn.execute("UPDATE redes_links SET published_json=NULL,published_archived=1,archived_at=? WHERE draft_archived=1", (now,))
        for row in conn.execute("SELECT id,draft_json FROM redes_links WHERE draft_archived=0").fetchall():
            conn.execute("UPDATE redes_links SET published_json=?,published_archived=0,archived_at=NULL WHERE id=?", (row["draft_json"], row["id"]))
        conn.execute("UPDATE redes_profile SET published_json=?,published_at=?,updated_at=? WHERE id=1", (json.dumps(draft_profile, ensure_ascii=False), now, now))
        conn.commit()
        return jsonify({"success": True, "published_at": now, "version": profile_row["draft_version"]})
    except ValueError as exc:
        conn.rollback()
        return jsonify({"success": False, "error": str(exc)}), 400
    finally:
        conn.close()


@redes_blueprint.get("/api/redes/admin/metrics.csv")
def export_metrics():
    denied = _admin_required()
    if denied:
        return denied
    try:
        start, end = _period_bounds(request.args.get("period", "30"), request.args.get("start", ""), request.args.get("end", ""))
    except ValueError as exc:
        return jsonify({"success": False, "error": str(exc)}), 400
    conn = _db()
    try:
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(["fecha_utc", "tipo_evento", "id_enlace", "enlace", "origen", "campana"])
        rows = conn.execute(
            """SELECT e.created_at,e.event_type,e.link_id,e.source,e.campaign,l.draft_json,l.published_json
               FROM redes_events e LEFT JOIN redes_links l ON l.id=e.link_id
               WHERE e.created_at>=? AND e.created_at<? ORDER BY e.created_at""",
            (start, end),
        ).fetchall()
        for row in rows:
            title = _json(row["draft_json"] or row["published_json"]).get("title", "")
            writer.writerow([_csv_safe(row["created_at"]), _csv_safe(row["event_type"]), row["link_id"] or "", _csv_safe(title), _csv_safe(row["source"]), _csv_safe(row["campaign"])])
        response = make_response("\ufeff" + output.getvalue())
        response.headers["Content-Type"] = "text/csv; charset=utf-8"
        response.headers["Content-Disposition"] = "attachment; filename=metricas-redes.csv"
        response.headers["Cache-Control"] = "no-store"
        return response
    finally:
        conn.close()


@redes_blueprint.post("/api/redes/admin/metrics/reset")
def reset_metrics():
    denied = _admin_required(mutating=True)
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    if payload.get("confirm") != "BORRAR ESTADISTICAS":
        return jsonify({"success": False, "error": "La confirmación no coincide; las estadísticas no se borraron."}), 400
    conn = _db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        deleted = conn.execute("SELECT COUNT(*) FROM redes_events").fetchone()[0]
        conn.execute("DELETE FROM redes_events")
        conn.commit()
        return jsonify({"success": True, "deleted": deleted})
    finally:
        conn.close()


@redes_blueprint.post("/api/redes/admin/revisions/<int:revision_id>/restore")
def restore_revision(revision_id):
    denied = _admin_required(mutating=True)
    if denied:
        return denied
    conn = _db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        revision = conn.execute("SELECT profile_json,links_json FROM redes_revisions WHERE id=?", (revision_id,)).fetchone()
        if not revision:
            conn.rollback()
            return jsonify({"success": False, "error": "No se encontró la revisión."}), 404
        profile = _normalise_profile(_json(revision["profile_json"], _BRAND_PROFILE))
        try:
            restored_links = json.loads(revision["links_json"] or "[]")
            if not isinstance(restored_links, list):
                raise ValueError("Historial inválido")
            normalized = [(int(item.get("id") or 0), int(item.get("position") or 0), _normalise_link(item)) for item in restored_links]
        except (TypeError, ValueError, AttributeError):
            conn.rollback()
            return jsonify({"success": False, "error": "La revisión contiene datos que no se pueden restaurar."}), 400
        conn.execute("UPDATE redes_links SET draft_archived=1,updated_at=?", (_now_utc(),))
        for link_id, position, item in normalized:
            if not link_id:
                continue
            conn.execute(
                "UPDATE redes_links SET draft_json=?,draft_archived=0,position=?,updated_at=? WHERE id=?",
                (json.dumps(item, ensure_ascii=False), position, _now_utc(), link_id),
            )
        conn.execute("UPDATE redes_profile SET draft_json=?,draft_version=draft_version+1,updated_at=? WHERE id=1", (json.dumps(profile, ensure_ascii=False), _now_utc()))
        conn.commit()
        return jsonify({"success": True, "message": "Revisión restaurada al borrador. La página pública no cambia hasta publicar."})
    finally:
        conn.close()

