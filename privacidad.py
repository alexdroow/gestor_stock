"""Privacy center, preference evidence and data-subject request workflow."""
from __future__ import annotations

import hashlib
import json
import secrets
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

from flask import Blueprint, current_app, jsonify, make_response, redirect, render_template, request, session, url_for
from itsdangerous import BadSignature, URLSafeSerializer

from database import get_db


privacidad_blueprint = Blueprint("privacidad", __name__)
_schema_lock = threading.Lock()
_schema_ready = False
_REQUEST_TYPES = {"acceso", "rectificacion", "supresion", "oposicion", "portabilidad", "bloqueo", "consulta"}
_REQUEST_STATES = {"recibida", "identidad_pendiente", "en_revision", "respondida", "cerrada"}
_INCIDENT_STATES = {"registrado", "evaluacion", "contencion", "notificacion_evaluada", "cerrado"}
_PREFERENCE_KEYS = ("marketing_email", "marketing_whatsapp", "analytics_internal")
_RIGHTS_RATE_LOCK = threading.Lock()
_RIGHTS_RATE = {}

_POLICY_FIELDS = (
    "responsible_name", "responsible_tax_id", "representative_name", "address", "contact_email", "privacy_contact",
    "prevention_contact", "data_categories", "data_subjects_sources", "purposes_bases",
    "legitimate_interests", "recipients", "international_transfers", "retention", "data_required_consequences",
    "security_summary", "rights_process", "withdrawal_process", "automated_decisions",
    "agency_complaint", "effective_date", "legal_reviewed",
)
_POLICY_LABELS = {
    "responsible_name": "Responsable de los datos (razon social o nombre)",
    "responsible_tax_id": "RUT del responsable (si corresponde)",
    "representative_name": "Representante legal",
    "address": "Domicilio del responsable",
    "contact_email": "Correo general de contacto",
    "privacy_contact": "Canal operativo para privacidad y derechos",
    "prevention_contact": "Encargado de prevencion (indicar No designado solo si se confirmo)",
    "data_categories": "Categorias de datos personales tratados",
    "data_subjects_sources": "Titulares y origen de los datos",
    "purposes_bases": "Finalidades y base de licitud para cada finalidad",
    "legitimate_interests": "Intereses legitimos y evaluacion (o No aplica validado)",
    "recipients": "Destinatarios y proveedores, rol y finalidad",
    "international_transfers": "Transferencias internacionales, pais y garantia (o No aplica validado)",
    "retention": "Plazos o criterios de conservacion por finalidad",
    "data_required_consequences": "Datos obligatorios/opcionales y consecuencias de no entregarlos",
    "security_summary": "Resumen de medidas de seguridad, sin revelar secretos",
    "rights_process": "Como ejercer derechos y canal de respuesta",
    "withdrawal_process": "Como retirar consentimiento",
    "automated_decisions": "Decisiones automatizadas/perfiles (o No aplica validado)",
    "agency_complaint": "Informacion de reclamacion ante la Agencia (validar antes de publicar)",
    "effective_date": "Fecha de vigencia de esta version (AAAA-MM-DD)",
}

_ACCOUNT_TERMS_VERSION = "2026-11-v1"
_ACCOUNT_PRIVACY_SCOPE = (
    "Gestionar la cuenta; autenticarla con correo y telefono; procesar compras y reservas; mostrar historial, estado, "
    "cupones y beneficios; coordinar entrega o retiro; atender consultas y cumplir obligaciones aplicables."
)
_ACCOUNT_MARKETING_SCOPE = (
    "Enviar ofertas y novedades por correo o WhatsApp y, cuando corresponda, adaptar promociones usando compras y "
    "reservas anteriores. Es opcional y no condiciona la creacion ni uso de la cuenta."
)


def _suggested_policy_draft():
    return {
        "responsible_name": "Pasteleria Sucree (razon social y RUT pendientes de validacion)",
        "address": "Rubidio 1815, 9271406 Maipu, Region Metropolitana (confirmar domicilio legal)",
        "contact_email": "pasteleria.sucree@outlook.com",
        "privacy_contact": "Solicitudes en /privacidad/derechos; tambien pasteleria.sucree@outlook.com y WhatsApp +56964330546 (confirmar canal formal)",
        "data_categories": "Identificacion y contacto: nombre, correo, telefono; fecha de nacimiento; domicilio y coordenadas si se ingresan; datos de compra, reserva, carrito, pagos, cupones, puntos y comunicaciones de atencion. Para el monitoreo operativo de la tienda se usa un identificador aleatorio temporal, cantidades, productos y total del carrito activo; no se asocian correo, telefono ni IP a ese pulso y sus filas se eliminan luego de 24 horas. El registro adicional de visitas y clics sigue siendo opcional y requiere habilitacion en Preferencias.",
        "data_subjects_sources": "Clientes y visitantes. Datos entregados directamente al crear cuenta, comprar, reservar o contactar; informacion de pedido y pago generada por el uso del servicio; estado de pago recibido del proveedor Flow; busqueda de direcciones mediante OpenStreetMap/Nominatim cuando se usa el mapa.",
        "purposes_bases": "Gestion de cuenta, compras, reservas, entrega/retiro, historial y atencion: tratamiento necesario para prestar el servicio solicitado (validar base legal aplicable). Monitoreo operativo en vivo: mantener actualizado el numero de sesiones y carritos activos para gestionar la tienda; usa identificador temporal, productos, cantidades y total del carrito, sin datos directos de contacto ni IP, y se elimina en 24 horas. Visitas, clics y analitica adicional: solo cuando se habilita en Preferencias. Promociones por correo/WhatsApp y personalizacion comercial: solo con autorizacion opcional, separada y revocable. Obligaciones contables y legales: conservar lo exigido por normativa (plazos por validar).",
        "recipients": "Personal autorizado de Pasteleria Sucree. El sitio integra Flow para pagos y OpenStreetMap/Nominatim para busqueda de direcciones; se usan enlaces externos de WhatsApp. Confirmar contratos, roles y proveedores tecnicos del hosting/CDN antes de publicar.",
        "international_transfers": "Pendiente confirmar ubicacion de hosting, CDN, Flow, servicios de mapas, correo y WhatsApp, y si hay transferencias internacionales.",
        "retention": "Los registros operativos temporales de sesiones y carritos en vivo se eliminan automaticamente despues de 24 horas. Otros plazos por finalidad deben definirse y validarse con asesoria y obligaciones contables; los registros de pedidos se conservan segun sus fines y obligaciones aplicables.",
        "data_required_consequences": "Nombre, correo y telefono son requeridos para crear y recuperar la cuenta. Datos de pedido se requieren para gestionar una compra o reserva; domicilio/coordenadas solo para coordinar despacho. Sin datos necesarios no se podra prestar el flujo solicitado.",
        "security_summary": "La aplicacion dispone de autenticacion administrativa, controles de sesion y proteccion CSRF en formularios de privacidad. Revisar y documentar controles de acceso, respaldos, cifrado, monitoreo y respuesta a incidentes antes de publicar.",
        "rights_process": "Enviar una solicitud desde /privacidad/derechos. Se entrega un numero de caso y codigo privado para seguimiento en /privacidad/seguimiento. La identidad se verifica proporcionalmente antes de revelar o modificar datos.",
        "withdrawal_process": "Las autorizaciones opcionales de marketing y metricas se pueden revocar desde /privacidad/preferencias. La revocacion no elimina tratamientos ya realizados ni registros que deban mantenerse por obligacion legal.",
        "automated_decisions": "El sistema muestra historial, puntos, nivel y cupones asociados a la cuenta. Confirmar si existe perfilamiento o personalizacion automatizada antes de publicar.",
        "agency_complaint": "Indicar mecanismo vigente de reclamacion ante la autoridad competente luego de validar su canal oficial y la fecha de entrada en vigor de la normativa.",
    }


def record_account_consent(cursor, customer_id, *, privacy_version, terms_version, marketing_email=False, marketing_whatsapp=False):
    """Record the required account notice acceptance and separate optional marketing choice."""
    cursor.execute(
        """INSERT INTO privacy_account_consents
           (customer_id,privacy_version,terms_version,required_scope,marketing_email_opt_in,marketing_whatsapp_opt_in,marketing_scope,source,accepted_at)
           VALUES(?,?,?,?,?,?,?,?,?)""",
        (
            int(customer_id), str(privacy_version or "operativa-v1")[:40], str(terms_version or _ACCOUNT_TERMS_VERSION)[:40],
            _ACCOUNT_PRIVACY_SCOPE, 1 if marketing_email else 0, 1 if marketing_whatsapp else 0,
            _ACCOUNT_MARKETING_SCOPE if marketing_email or marketing_whatsapp else "",
            "registro_tienda", _now(),
        ),
    )


def current_notice_version():
    published = _published_config()
    return str(published.get("version") or "operativa-v1")[:40]


def _now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _ensure_schema():
    with _schema_lock:
        conn = get_db()
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS privacy_policy_config (
                    id INTEGER PRIMARY KEY CHECK(id=1), draft_json TEXT NOT NULL DEFAULT '{}',
                    published_json TEXT NOT NULL DEFAULT '{}', draft_updated_at TEXT,
                    updated_by TEXT NOT NULL DEFAULT '', published_version TEXT NOT NULL DEFAULT '',
                    published_at TEXT NOT NULL DEFAULT ''
                );
                INSERT OR IGNORE INTO privacy_policy_config (id) VALUES (1);
                CREATE TABLE IF NOT EXISTS privacy_policy_versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, version TEXT NOT NULL UNIQUE,
                    snapshot_json TEXT NOT NULL, author TEXT NOT NULL DEFAULT '',
                    published_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS privacy_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, case_code TEXT NOT NULL UNIQUE,
                    tracking_hash TEXT NOT NULL, request_type TEXT NOT NULL, full_name TEXT NOT NULL DEFAULT '',
                    email TEXT NOT NULL DEFAULT '', phone TEXT NOT NULL DEFAULT '',
                    description TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'recibida',
                    identity_status TEXT NOT NULL DEFAULT 'pendiente', response_note TEXT NOT NULL DEFAULT '',
                    response_text TEXT NOT NULL DEFAULT '', response_due_at TEXT NOT NULL DEFAULT '', extension_until TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL, closed_at TEXT NOT NULL DEFAULT ''
                );
                CREATE INDEX IF NOT EXISTS idx_privacy_requests_status_date ON privacy_requests(status, created_at DESC);
                CREATE TABLE IF NOT EXISTS privacy_request_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, request_id INTEGER NOT NULL,
                    event_type TEXT NOT NULL, detail TEXT NOT NULL DEFAULT '', actor TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL, FOREIGN KEY(request_id) REFERENCES privacy_requests(id) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS privacy_visitors (
                    visitor_hash TEXT PRIMARY KEY, preferences_json TEXT NOT NULL,
                    notice_version TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS privacy_account_consents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, customer_id INTEGER NOT NULL,
                    privacy_version TEXT NOT NULL, terms_version TEXT NOT NULL,
                    required_scope TEXT NOT NULL, marketing_email_opt_in INTEGER NOT NULL DEFAULT 0,
                    marketing_whatsapp_opt_in INTEGER NOT NULL DEFAULT 0,
                    marketing_scope TEXT NOT NULL DEFAULT '', source TEXT NOT NULL,
                    accepted_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_privacy_account_consents_customer
                    ON privacy_account_consents(customer_id, accepted_at DESC);
                CREATE TABLE IF NOT EXISTS privacy_preference_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, visitor_hash TEXT NOT NULL,
                    preferences_json TEXT NOT NULL, notice_version TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'centro_privacidad', created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS privacy_incidents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, incident_code TEXT NOT NULL UNIQUE,
                    summary TEXT NOT NULL, discovered_at TEXT NOT NULL, affected_data TEXT NOT NULL DEFAULT '',
                    risk_assessment TEXT NOT NULL DEFAULT '', containment TEXT NOT NULL DEFAULT '',
                    notification_decision TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'registrado',
                    owner TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS privacy_retention_rules (
                    dataset_key TEXT PRIMARY KEY, retention_days INTEGER, legal_basis TEXT NOT NULL DEFAULT '',
                    approved INTEGER NOT NULL DEFAULT 0, updated_by TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL DEFAULT ''
                );
                """
            )
            conn.commit()
        finally:
            conn.close()


def _serializer():
    return URLSafeSerializer(current_app.secret_key, salt="sucree-privacy-preferences-v1")


def get_privacy_state():
    defaults = {key: False for key in _PREFERENCE_KEYS}
    result = {"chosen": False, "preferences": defaults, "version": "1.0"}
    try:
        payload = _serializer().loads(request.cookies.get("sucree_privacy", ""))
        preferences = payload.get("preferences") if isinstance(payload, dict) else None
        if isinstance(preferences, dict):
            result["chosen"] = True
            result["version"] = str(payload.get("version") or "1.0")
            result["preferences"] = {key: bool(preferences.get(key)) for key in _PREFERENCE_KEYS}
    except (BadSignature, TypeError, ValueError):
        pass
    return result


def privacy_csrf_token():
    return session.setdefault("privacy_csrf", secrets.token_urlsafe(24))


def _csrf_valid(data=None):
    expected = str(session.get("privacy_csrf") or "")
    supplied = str(request.headers.get("X-CSRF-Token") or ((data or {}).get("csrf_token") if isinstance(data, dict) else "") or "")
    return bool(expected and supplied and secrets.compare_digest(expected, supplied))


def tracking_allowed_from_request():
    return bool(get_privacy_state()["preferences"].get("analytics_internal"))


def _visitor_from_cookie():
    try:
        payload = _serializer().loads(request.cookies.get("sucree_privacy", ""))
        visitor = str(payload.get("visitor") or "") if isinstance(payload, dict) else ""
        return visitor if len(visitor) >= 32 else ""
    except (BadSignature, TypeError, ValueError):
        return ""


def _sha(value):
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def _rights_rate_allowed():
    key = _sha(request.remote_addr or "unknown")[:24]
    now = time.monotonic()
    with _RIGHTS_RATE_LOCK:
        for address, attempts in list(_RIGHTS_RATE.items()):
            recent = [stamp for stamp in attempts if now - stamp < 900]
            if recent:
                _RIGHTS_RATE[address] = recent
            else:
                _RIGHTS_RATE.pop(address, None)
        attempts = _RIGHTS_RATE.setdefault(key, [])
        if len(attempts) >= 5:
            return False
        attempts.append(now)
        return True


def _record_preferences(preferences, source="centro_privacidad"):
    _ensure_schema()
    visitor = _visitor_from_cookie() or secrets.token_urlsafe(36)
    visitor_hash = _sha(visitor)
    now = _now()
    clean = {key: bool(preferences.get(key)) for key in _PREFERENCE_KEYS}
    encoded = json.dumps(clean, ensure_ascii=False, separators=(",", ":"))
    conn = get_db()
    try:
        conn.execute(
            """INSERT INTO privacy_visitors(visitor_hash,preferences_json,notice_version,created_at,updated_at)
               VALUES(?,?,?,?,?) ON CONFLICT(visitor_hash) DO UPDATE SET
               preferences_json=excluded.preferences_json, notice_version=excluded.notice_version,
               updated_at=excluded.updated_at""",
            (visitor_hash, encoded, "1.0", now, now),
        )
        conn.execute(
            "INSERT INTO privacy_preference_events(visitor_hash,preferences_json,notice_version,source,created_at) VALUES(?,?,?,?,?)",
            (visitor_hash, encoded, "1.0", str(source)[:40], now),
        )
        conn.commit()
    finally:
        conn.close()
    token = _serializer().dumps({"visitor": visitor, "preferences": clean, "version": "1.0"})
    return token


def record_registration_preferences(email, phone, marketing_email=False, marketing_whatsapp=False):
    """Persist explicit optional channel choices attached to the signup contact."""
    _ensure_schema()
    subject_hash = _sha(f"{str(email or '').strip().lower()}|{str(phone or '').strip()}")
    prefs = {"marketing_email": bool(marketing_email), "marketing_whatsapp": bool(marketing_whatsapp), "analytics_internal": False}
    visitor_cookie = _record_preferences(prefs, "registro_promocion")
    now = _now()
    conn = get_db()
    try:
        conn.execute(
            """INSERT INTO privacy_visitors(visitor_hash,preferences_json,notice_version,created_at,updated_at)
               VALUES(?,?,?,?,?) ON CONFLICT(visitor_hash) DO UPDATE SET
               preferences_json=excluded.preferences_json, notice_version=excluded.notice_version, updated_at=excluded.updated_at""",
            (subject_hash, json.dumps(prefs, ensure_ascii=False), "1.0", now, now),
        )
        conn.execute(
            "INSERT INTO privacy_preference_events(visitor_hash,preferences_json,notice_version,source,created_at) VALUES(?,?,?,?,?)",
            (subject_hash, json.dumps(prefs, ensure_ascii=False), "1.0", "registro_promocion", now),
        )
        conn.commit()
    finally:
        conn.close()
    return visitor_cookie


def _admin_ok():
    return bool(session.get("admin_autenticado"))


def _admin_denied():
    if not _admin_ok():
        return jsonify({"success": False, "error": "Acceso no autorizado."}), 401
    if request.method not in {"GET", "HEAD", "OPTIONS"} and not _csrf_valid(request.get_json(silent=True) or {}):
        return jsonify({"success": False, "error": "La sesion vencio. Recarga la pagina e intenta nuevamente."}), 403
    return None


def _json_object(value):
    try:
        parsed = json.loads(value or "{}")
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, ValueError):
        return {}


def _published_config():
    _ensure_schema()
    conn = get_db()
    try:
        row = conn.execute("SELECT published_json,published_version,published_at FROM privacy_policy_config WHERE id=1").fetchone()
        return {"content": _json_object(row["published_json"]), "version": row["published_version"], "published_at": row["published_at"]} if row else {"content": {}, "version": "", "published_at": ""}
    finally:
        conn.close()


@privacidad_blueprint.get("/privacidad")
def center():
    return render_template("privacidad_publica.html", view="center", policy=_published_config())


@privacidad_blueprint.get("/privacidad/politica")
def policy_page():
    return render_template("privacidad_publica.html", view="policy", policy=_published_config(), operational=_suggested_policy_draft())


@privacidad_blueprint.get("/privacidad/preferencias")
def preferences_page():
    return render_template("privacidad_publica.html", view="preferences", policy=_published_config())


@privacidad_blueprint.get("/privacidad/derechos")
def rights_page():
    return render_template("privacidad_publica.html", view="rights", policy=_published_config())


@privacidad_blueprint.get("/privacidad/seguimiento")
def request_tracking_page():
    return render_template("privacidad_publica.html", view="tracking", policy=_published_config())


@privacidad_blueprint.get("/admin/privacidad")
def admin_page():
    if not _admin_ok():
        return redirect(url_for("admin_login", next=request.full_path))
    _ensure_schema()
    return render_template("privacidad_admin.html")


@privacidad_blueprint.get("/api/privacidad/preferencias")
def preferences_get():
    return jsonify({"success": True, **get_privacy_state(), "csrf_token": privacy_csrf_token()})


@privacidad_blueprint.post("/api/privacidad/preferencias")
def preferences_post():
    data = request.get_json(silent=True) or {}
    if not _csrf_valid(data):
        return jsonify({"success": False, "error": "Recarga la pagina para actualizar tus preferencias."}), 403
    allowed = {key: bool(data.get(key, False)) for key in _PREFERENCE_KEYS}
    if data.get("necessary") is False:
        return jsonify({"success": False, "error": "El almacenamiento necesario para seguridad y sesion no se puede desactivar desde aqui."}), 400
    email = str(data.get("email") or "").strip().lower()[:254]
    phone = str(data.get("phone") or "").strip()[:40]
    if allowed["marketing_email"] and ("@" not in email or "." not in email.rsplit("@", 1)[-1]):
        return jsonify({"success": False, "error": "Ingresa un correo para guardar la preferencia de promociones por correo."}), 400
    if allowed["marketing_whatsapp"] and len("".join(char for char in phone if char.isdigit())) < 8:
        return jsonify({"success": False, "error": "Ingresa un telefono para guardar la preferencia de WhatsApp."}), 400
    cookie_value = _record_preferences(allowed, str(data.get("source") or "centro_privacidad"))
    if allowed["marketing_email"] or allowed["marketing_whatsapp"]:
        record_registration_preferences(email, phone, allowed["marketing_email"], allowed["marketing_whatsapp"])
    response = jsonify({"success": True, "chosen": True, "preferences": allowed, "version": "1.0"})
    response.set_cookie("sucree_privacy", cookie_value, max_age=31536000, httponly=True,
                        secure=request.is_secure, samesite="Lax", path="/")
    response.headers["Cache-Control"] = "no-store"
    return response


@privacidad_blueprint.post("/api/privacidad/solicitudes")
def rights_submit():
    data = request.get_json(silent=True) or {}
    if not _csrf_valid(data):
        return jsonify({"success": False, "error": "Recarga la pagina e intenta nuevamente."}), 403
    if str(data.get("website") or "").strip():
        return jsonify({"success": True, "case_code": ""})
    if not _rights_rate_allowed():
        return jsonify({"success": False, "error": "Se alcanzo el limite de solicitudes por ahora. Intenta mas tarde."}), 429
    kind = str(data.get("request_type") or "").strip().lower()
    name = str(data.get("full_name") or "").strip()[:120]
    email = str(data.get("email") or "").strip().lower()[:254]
    phone = str(data.get("phone") or "").strip()[:40]
    description = str(data.get("description") or "").strip()[:1800]
    if kind not in _REQUEST_TYPES or len(name) < 2 or not (email or phone):
        return jsonify({"success": False, "error": "Indica tipo de solicitud, nombre y al menos un medio de contacto."}), 400
    if email and ("@" not in email or len(email) > 254):
        return jsonify({"success": False, "error": "Revisa el correo ingresado."}), 400
    _ensure_schema()
    case_code = "PRV-" + uuid.uuid4().hex[:10].upper()
    tracking_token = secrets.token_urlsafe(24)
    now = _now()
    response_due = (datetime.now(timezone.utc) + timedelta(days=30)).replace(microsecond=0).isoformat()
    conn = get_db()
    try:
        cur = conn.execute(
            """INSERT INTO privacy_requests(case_code,tracking_hash,request_type,full_name,email,phone,description,status,identity_status,response_due_at,created_at,updated_at)
               VALUES(?,?,?,?,?,?,?,'recibida','pendiente',?,?,?)""",
            (case_code, _sha(tracking_token), kind, name, email, phone, description, response_due, now, now),
        )
        conn.execute("INSERT INTO privacy_request_events(request_id,event_type,detail,actor,created_at) VALUES(?,'recibida','Solicitud ingresada por canal publico','titular',?)", (cur.lastrowid, now))
        conn.commit()
    finally:
        conn.close()
    return jsonify({"success": True, "case_code": case_code, "tracking_token": tracking_token, "status": "recibida"}), 201


@privacidad_blueprint.post("/api/privacidad/solicitudes/consultar")
def rights_track():
    data = request.get_json(silent=True) or {}
    code = str(data.get("case_code") or "").strip().upper()[:20]
    token = str(data.get("tracking_token") or "").strip()[:100]
    if not code or not token:
        return jsonify({"success": False, "error": "Ingresa el numero de caso y el codigo privado de seguimiento."}), 400
    _ensure_schema()
    conn = get_db()
    try:
        row = conn.execute("SELECT request_type,status,identity_status,created_at,updated_at FROM privacy_requests WHERE case_code=? AND tracking_hash=?", (code, _sha(token))).fetchone()
        if not row:
            return jsonify({"success": False, "error": "No encontramos ese caso con el codigo de seguimiento."}), 404
        return jsonify({"success": True, "request_type": row["request_type"], "status": row["status"], "identity_status": row["identity_status"], "created_at": row["created_at"], "updated_at": row["updated_at"]})
    finally:
        conn.close()


@privacidad_blueprint.get("/api/admin/privacidad/config")
def admin_config_get():
    denied = _admin_denied()
    if denied:
        return denied
    _ensure_schema()
    conn = get_db()
    try:
        row = conn.execute("SELECT draft_json,published_version,published_at FROM privacy_policy_config WHERE id=1").fetchone()
        draft = _suggested_policy_draft()
        draft.update(_json_object(row["draft_json"]))
        return jsonify({"success": True, "draft": draft, "published_version": row["published_version"], "published_at": row["published_at"], "fields": _POLICY_LABELS})
    finally:
        conn.close()


@privacidad_blueprint.get("/api/admin/privacidad/versiones")
def admin_policy_versions():
    denied = _admin_denied()
    if denied:
        return denied
    _ensure_schema()
    conn = get_db()
    try:
        rows = conn.execute("SELECT version,author,published_at FROM privacy_policy_versions ORDER BY id DESC LIMIT 100").fetchall()
        return jsonify({"success": True, "versions": [dict(row) for row in rows]})
    finally:
        conn.close()


@privacidad_blueprint.post("/api/admin/privacidad/config")
def admin_config_save():
    denied = _admin_denied()
    if denied:
        return denied
    data = request.get_json(silent=True) or {}
    draft = {key: str(data.get(key) or "").strip()[:8000] for key in _POLICY_FIELDS}
    draft["legal_reviewed"] = bool(data.get("legal_reviewed"))
    conn = get_db()
    try:
        conn.execute("UPDATE privacy_policy_config SET draft_json=?,draft_updated_at=?,updated_by=? WHERE id=1", (json.dumps(draft, ensure_ascii=False), _now(), str(session.get("admin_user_name") or "Administrador")[:100]))
        conn.commit()
        return jsonify({"success": True, "draft": draft})
    finally:
        conn.close()


def _validate_policy(policy):
    missing = []
    for key in _POLICY_FIELDS:
        if key == "legal_reviewed":
            continue
        value = str(policy.get(key) or "").strip()
        if not value or "[" in value or "]" in value or "pendiente" in value.lower() or "por definir" in value.lower():
            missing.append(_POLICY_LABELS[key])
    if not policy.get("legal_reviewed"):
        missing.append("Confirmacion de revision por asesoria juridica")
    if policy.get("contact_email") and "@" not in policy["contact_email"]:
        missing.append("Correo general con formato valido")
    if policy.get("effective_date"):
        try:
            datetime.strptime(policy["effective_date"], "%Y-%m-%d")
        except ValueError:
            missing.append("Fecha de vigencia valida (AAAA-MM-DD)")
    return missing


@privacidad_blueprint.post("/api/admin/privacidad/publicar")
def admin_policy_publish():
    denied = _admin_denied()
    if denied:
        return denied
    _ensure_schema()
    conn = get_db()
    try:
        row = conn.execute("SELECT draft_json,published_version FROM privacy_policy_config WHERE id=1").fetchone()
        policy = _json_object(row["draft_json"])
        missing = _validate_policy(policy)
        if missing:
            return jsonify({"success": False, "error": "No se puede publicar: faltan datos verificados.", "missing": missing}), 400
        prev = str(row["published_version"] or "0.0")
        try:
            major, minor = (int(part) for part in prev.split(".", 1))
        except (TypeError, ValueError):
            major, minor = (1, 0)
        version = f"{major}.{minor + 1}"
        now = _now()
        snapshot = json.dumps(policy, ensure_ascii=False)
        conn.execute("INSERT INTO privacy_policy_versions(version,snapshot_json,author,published_at) VALUES(?,?,?,?)", (version, snapshot, str(session.get("admin_user_name") or "Administrador")[:100], now))
        conn.execute("UPDATE privacy_policy_config SET published_json=?,published_version=?,published_at=?,updated_by=? WHERE id=1", (snapshot, version, now, str(session.get("admin_user_name") or "Administrador")[:100]))
        conn.commit()
        return jsonify({"success": True, "version": version, "published_at": now})
    finally:
        conn.close()


@privacidad_blueprint.get("/api/admin/privacidad/solicitudes")
def admin_requests_get():
    denied = _admin_denied()
    if denied:
        return denied
    _ensure_schema()
    state = str(request.args.get("status") or "").strip()
    conn = get_db()
    try:
        query = "SELECT id,case_code,request_type,full_name,email,phone,description,status,identity_status,response_note,response_text,response_due_at,extension_until,created_at,updated_at,closed_at FROM privacy_requests"
        params = ()
        if state in _REQUEST_STATES:
            query += " WHERE status=?"
            params = (state,)
        rows = conn.execute(query + " ORDER BY created_at DESC LIMIT 300", params).fetchall()
        return jsonify({"success": True, "requests": [dict(row) for row in rows]})
    finally:
        conn.close()


@privacidad_blueprint.get("/api/admin/privacidad/solicitudes/<int:request_id>/eventos")
def admin_request_events(request_id):
    denied = _admin_denied()
    if denied:
        return denied
    conn = get_db()
    try:
        rows = conn.execute("SELECT event_type,detail,actor,created_at FROM privacy_request_events WHERE request_id=? ORDER BY id", (request_id,)).fetchall()
        return jsonify({"success": True, "events": [dict(row) for row in rows]})
    finally:
        conn.close()


@privacidad_blueprint.get("/api/admin/privacidad/solicitudes/<int:request_id>/evidencia")
def admin_request_evidence(request_id):
    denied = _admin_denied()
    if denied:
        return denied
    _ensure_schema()
    conn = get_db()
    try:
        row = conn.execute("SELECT case_code,request_type,full_name,email,phone,description,status,identity_status,response_note,response_text,response_due_at,extension_until,created_at,updated_at,closed_at FROM privacy_requests WHERE id=?", (request_id,)).fetchone()
        if not row:
            return jsonify({"success": False, "error": "Caso no encontrado."}), 404
        events = conn.execute("SELECT event_type,detail,actor,created_at FROM privacy_request_events WHERE request_id=? ORDER BY id", (request_id,)).fetchall()
        response = jsonify({"success": True, "request": dict(row), "events": [dict(event) for event in events]})
        response.headers["Content-Disposition"] = f'attachment; filename="{row["case_code"]}-evidencia.json"'
        response.headers["Cache-Control"] = "no-store"
        return response
    finally:
        conn.close()


@privacidad_blueprint.post("/api/admin/privacidad/solicitudes/<int:request_id>")
def admin_request_update(request_id):
    denied = _admin_denied()
    if denied:
        return denied
    data = request.get_json(silent=True) or {}
    status = str(data.get("status") or "").strip()
    identity = str(data.get("identity_status") or "pendiente").strip()
    if status not in _REQUEST_STATES or identity not in {"pendiente", "verificada", "no_verificada"}:
        return jsonify({"success": False, "error": "Estado de caso invalido."}), 400
    note = str(data.get("response_note") or "").strip()[:500]
    response_text = str(data.get("response_text") or "").strip()[:8000]
    extension = str(data.get("extension_until") or "").strip()[:30]
    now = _now()
    actor = str(session.get("admin_user_name") or "Administrador")[:100]
    conn = get_db()
    try:
        exists = conn.execute("SELECT id FROM privacy_requests WHERE id=?", (request_id,)).fetchone()
        if not exists:
            return jsonify({"success": False, "error": "Caso no encontrado."}), 404
        conn.execute("UPDATE privacy_requests SET status=?,identity_status=?,response_note=?,response_text=?,extension_until=?,updated_at=?,closed_at=? WHERE id=?", (status, identity, note, response_text, extension, now, now if status == "cerrada" else "", request_id))
        conn.execute("INSERT INTO privacy_request_events(request_id,event_type,detail,actor,created_at) VALUES(?,?,?,?,?)", (request_id, "actualizacion", f"Estado: {status}; identidad: {identity}; {note}"[:1000], actor, now))
        conn.commit()
        return jsonify({"success": True})
    finally:
        conn.close()


@privacidad_blueprint.get("/api/admin/privacidad/incidentes")
def admin_incidents_get():
    denied = _admin_denied()
    if denied:
        return denied
    _ensure_schema()
    conn = get_db()
    try:
        rows = conn.execute("SELECT * FROM privacy_incidents ORDER BY created_at DESC LIMIT 200").fetchall()
        return jsonify({"success": True, "incidents": [dict(row) for row in rows]})
    finally:
        conn.close()


@privacidad_blueprint.post("/api/admin/privacidad/incidentes")
def admin_incident_create():
    denied = _admin_denied()
    if denied:
        return denied
    data = request.get_json(silent=True) or {}
    summary = str(data.get("summary") or "").strip()[:500]
    discovered = str(data.get("discovered_at") or "").strip()[:40]
    if len(summary) < 5 or not discovered:
        return jsonify({"success": False, "error": "Indica resumen y fecha/hora de deteccion."}), 400
    code = "INC-" + uuid.uuid4().hex[:10].upper()
    now = _now()
    conn = get_db()
    try:
        conn.execute("INSERT INTO privacy_incidents(incident_code,summary,discovered_at,affected_data,risk_assessment,containment,notification_decision,status,owner,created_at,updated_at) VALUES(?,?,?,?,?,?,?,'registrado',?,?,?)", (code, summary, discovered, str(data.get("affected_data") or "").strip()[:2000], str(data.get("risk_assessment") or "").strip()[:3000], str(data.get("containment") or "").strip()[:3000], str(data.get("notification_decision") or "").strip()[:3000], str(session.get("admin_user_name") or "Administrador")[:100], now, now))
        conn.commit()
        return jsonify({"success": True, "incident_code": code}), 201
    finally:
        conn.close()


@privacidad_blueprint.post("/api/admin/privacidad/incidentes/<int:incident_id>")
def admin_incident_update(incident_id):
    denied = _admin_denied()
    if denied:
        return denied
    data = request.get_json(silent=True) or {}
    status = str(data.get("status") or "").strip()
    if status not in _INCIDENT_STATES:
        return jsonify({"success": False, "error": "Estado de incidente invalido."}), 400
    conn = get_db()
    try:
        cur = conn.execute("UPDATE privacy_incidents SET status=?,risk_assessment=?,containment=?,notification_decision=?,owner=?,updated_at=? WHERE id=?", (status, str(data.get("risk_assessment") or "").strip()[:3000], str(data.get("containment") or "").strip()[:3000], str(data.get("notification_decision") or "").strip()[:3000], str(session.get("admin_user_name") or "Administrador")[:100], _now(), incident_id))
        conn.commit()
        return jsonify({"success": cur.rowcount > 0}), (200 if cur.rowcount else 404)
    finally:
        conn.close()


_RETENTION_DATASETS = {
    "tienda_clientes": ("Clientes tienda", "tienda_clientes", "creado_en"),
    "pedidos_tienda": ("Pedidos tienda y POS", "ventas", "fecha_hora"),
    "agenda": ("Reservas y agenda", "agenda_eventos", "fecha"),
    "metricas_redes": ("Metricas de redes", "redes_events", "created_at"),
    "metricas_qr": ("Eventos de QR", "redes_qr_events", "created_at"),
    "codigos_promocion": ("Codigos y promociones", "redes_promo_codes", "created_at"),
}


@privacidad_blueprint.get("/api/admin/privacidad/retencion")
def admin_retention_get():
    denied = _admin_denied()
    if denied:
        return denied
    _ensure_schema()
    conn = get_db()
    try:
        rules = {row["dataset_key"]: dict(row) for row in conn.execute("SELECT * FROM privacy_retention_rules").fetchall()}
        datasets = [{"key": key, "label": meta[0], "table": meta[1], "date_column": meta[2], **rules.get(key, {})} for key, meta in _RETENTION_DATASETS.items()]
        return jsonify({"success": True, "datasets": datasets, "mode": "dry_run_only"})
    finally:
        conn.close()


@privacidad_blueprint.post("/api/admin/privacidad/retencion")
def admin_retention_save():
    denied = _admin_denied()
    if denied:
        return denied
    data = request.get_json(silent=True) or {}
    key = str(data.get("dataset_key") or "").strip()
    if key not in _RETENTION_DATASETS:
        return jsonify({"success": False, "error": "Conjunto de datos invalido."}), 400
    try:
        days = int(data.get("retention_days"))
        if days < 1 or days > 36500:
            raise ValueError
    except (TypeError, ValueError):
        return jsonify({"success": False, "error": "El plazo debe estar entre 1 y 36500 dias."}), 400
    legal_basis = str(data.get("legal_basis") or "").strip()[:1000]
    approved = bool(data.get("approved"))
    conn = get_db()
    try:
        conn.execute("""INSERT INTO privacy_retention_rules(dataset_key,retention_days,legal_basis,approved,updated_by,updated_at) VALUES(?,?,?,?,?,?)
            ON CONFLICT(dataset_key) DO UPDATE SET retention_days=excluded.retention_days,legal_basis=excluded.legal_basis,approved=excluded.approved,updated_by=excluded.updated_by,updated_at=excluded.updated_at""", (key, days, legal_basis, int(approved), str(session.get("admin_user_name") or "Administrador")[:100], _now()))
        conn.commit()
        return jsonify({"success": True, "note": "Solo se guardo la regla; no se borro ni modifico informacion."})
    finally:
        conn.close()


@privacidad_blueprint.post("/api/admin/privacidad/retencion/simulacion")
def admin_retention_dry_run():
    denied = _admin_denied()
    if denied:
        return denied
    _ensure_schema()
    conn = get_db()
    try:
        results = []
        for key, (label, table, date_col) in _RETENTION_DATASETS.items():
            rule = conn.execute("SELECT retention_days,approved FROM privacy_retention_rules WHERE dataset_key=?", (key,)).fetchone()
            if not rule or not rule["retention_days"]:
                results.append({"key": key, "label": label, "status": "sin_plazo_aprobado", "count": None})
                continue
            cutoff = (datetime.now(timezone.utc) - timedelta(days=int(rule["retention_days"]))).isoformat()
            try:
                count = conn.execute(f'SELECT COUNT(*) FROM "{table}" WHERE "{date_col}" < ?', (cutoff,)).fetchone()[0]
                status = "solo_simulacion_aprobada" if rule["approved"] else "requiere_aprobacion_legal"
                results.append({"key": key, "label": label, "status": status, "before": cutoff, "count": int(count)})
            except Exception:
                results.append({"key": key, "label": label, "status": "fuente_no_disponible", "count": None})
        return jsonify({"success": True, "dry_run": True, "deleted": 0, "results": results})
    finally:
        conn.close()
