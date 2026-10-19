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
        """
    )
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(redes_links)").fetchall()}
    if "draft_archived" not in columns:
        conn.execute("ALTER TABLE redes_links ADD COLUMN draft_archived INTEGER NOT NULL DEFAULT 0")
    if "published_archived" not in columns:
        conn.execute("ALTER TABLE redes_links ADD COLUMN published_archived INTEGER NOT NULL DEFAULT 0")
    conn.execute(
        "INSERT OR IGNORE INTO redes_profile(id, draft_json, published_json) VALUES(1, ?, ?)",
        (json.dumps(_BRAND_PROFILE, ensure_ascii=False), json.dumps({}, ensure_ascii=False)),
    )
    conn.commit()
    return conn


def _now_utc():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


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
    return any(token in ua for token in ("bot", "spider", "crawler", "preview", "facebookexternalhit", "whatsapp")) or any(token in purpose for token in ("prefetch", "prerender"))


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
        return render_template("redes_publica.html", profile=profile, links=links, preview=False)
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
    return render_template("redes_admin.html", csrf_token=csrf)


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
        return jsonify({"success": True, "profile": profile, "published_profile": published_profile, "links": links, "version": row["draft_version"], "published_at": row["published_at"], "updated_at": row["updated_at"], "published": bool(row["published_at"]), "has_changes": has_changes, "stats": stats, "period": period, "revisions": revisions})
    except ValueError as exc:
        return jsonify({"success": False, "error": str(exc)}), 400
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

