from flask import Flask, render_template, request, jsonify, redirect, url_for, make_response, send_file, Response, session
import os
import sys
import math
import ssl
import re
import time
import json
import base64
import socket
import unicodedata
import uuid
import csv
import shutil
import hashlib
import hmac
import subprocess
import threading
import sqlite3
try:
    import imghdr  # Python <= 3.12
except ModuleNotFoundError:
    imghdr = None
from urllib.parse import urlencode, quote, unquote, urlparse
from urllib.request import Request as UrlRequest, urlopen
from urllib.error import HTTPError
from datetime import datetime, timedelta
from io import BytesIO, StringIO
from zoneinfo import ZoneInfo
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash
from camera_hub import CameraHub
try:
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import A4
except ModuleNotFoundError:
    canvas = None
    A4 = (595.27, 841.89)

# Silenciar ruido nativo de FFmpeg/OpenCV cuando se ejecuta app.py directo.
# En servidores (Render/gunicorn) conviene dejar stderr visible para diagnostico.
# Se puede activar manualmente con: GESTIONSTOCK_SUPPRESS_STDERR=1
if os.environ.get("GESTIONSTOCK_SUPPRESS_STDERR", "0").strip().lower() not in ("0", "false", "no", "off"):
    try:
        _null_stderr = open(os.devnull, "w", encoding="utf-8", buffering=1)
        os.dup2(_null_stderr.fileno(), 2)
        sys.stderr = _null_stderr
    except Exception:
        pass

if getattr(sys, 'frozen', False):
    template_dir = os.path.join(sys._MEIPASS, 'templates')
    static_dir = os.path.join(sys._MEIPASS, 'static')
else:
    template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
    static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')

app = Flask(__name__, template_folder=template_dir, static_folder=static_dir)
CAMERA_HUB = CameraHub()
app.secret_key = os.environ.get("GESTIONSTOCK_SECRET_KEY", "gestor_stock_dev_secret_change_me")
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = str(os.environ.get("GESTIONSTOCK_SESSION_SECURE") or "0").strip().lower() in {"1", "true", "yes", "on"}
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=12)
PUBLIC_BASE_URL = str(os.environ.get("GESTIONSTOCK_PUBLIC_BASE_URL") or "https://pasteleriasucree.cl").strip().rstrip("/")

ADMIN_PIN_ENV = "GESTIONSTOCK_ADMIN_PIN"
DEFAULT_ADMIN_PIN = "1234"
ADMIN_LEGACY_USER_ENV = "GESTIONSTOCK_ADMIN_USER"
DEFAULT_ADMIN_LEGACY_USER = "admin"
_ADMIN_SESSION_KEY = "admin_autenticado"
_ADMIN_USER_ID_SESSION_KEY = "admin_user_id"
_ADMIN_USER_NAME_SESSION_KEY = "admin_user_name"

_ADMIN_LOGIN_ATTEMPTS = {}
_ADMIN_LOGIN_MAX_FAILS = 6
_ADMIN_LOGIN_BLOCK_SECONDS = 300

if not str(os.environ.get(ADMIN_PIN_ENV) or "").strip():
    print(
        f"[WARN] {ADMIN_PIN_ENV} no configurado. Usando PIN temporal por defecto ({DEFAULT_ADMIN_PIN}). "
        "Configuralo en variables de entorno para produccion."
    )


def _obtener_admin_pin():
    pin = str(os.environ.get(ADMIN_PIN_ENV) or "").strip()
    if pin:
        return pin
    return DEFAULT_ADMIN_PIN


def _obtener_admin_legacy_username():
    user = str(os.environ.get(ADMIN_LEGACY_USER_ENV) or "").strip()
    if user:
        return user
    return DEFAULT_ADMIN_LEGACY_USER


def _admin_client_ip():
    xff = str(request.headers.get("X-Forwarded-For") or "").strip()
    if xff:
        return xff.split(",")[0].strip()
    return str(request.remote_addr or "0.0.0.0").strip()


def _admin_login_state(ip):
    now = time.time()
    state = _ADMIN_LOGIN_ATTEMPTS.get(ip) or {"fails": 0, "blocked_until": 0}
    blocked_until = float(state.get("blocked_until") or 0)
    if blocked_until and now >= blocked_until:
        state = {"fails": 0, "blocked_until": 0}
        _ADMIN_LOGIN_ATTEMPTS[ip] = state
    return state


def _admin_login_is_blocked(ip):
    state = _admin_login_state(ip)
    blocked_until = float(state.get("blocked_until") or 0)
    if blocked_until > time.time():
        return True, int(max(1, blocked_until - time.time()))
    return False, 0


def _admin_login_register_fail(ip):
    state = _admin_login_state(ip)
    fails = int(state.get("fails") or 0) + 1
    state["fails"] = fails
    if fails >= _ADMIN_LOGIN_MAX_FAILS:
        state["blocked_until"] = time.time() + _ADMIN_LOGIN_BLOCK_SECONDS
    _ADMIN_LOGIN_ATTEMPTS[ip] = state
    return state


def _admin_login_register_success(ip):
    _ADMIN_LOGIN_ATTEMPTS.pop(ip, None)


def _admin_users_count(conn=None):
    propia = conn is None
    if propia:
        conn = get_db()
    try:
        row = conn.execute("SELECT COUNT(*) AS c FROM admin_users WHERE COALESCE(activo,1)=1").fetchone()
        return int((row["c"] if row else 0) or 0)
    except Exception:
        return 0
    finally:
        if propia and conn is not None:
            conn.close()


def _public_base_url(fallback=None):
    base = str(PUBLIC_BASE_URL or "").strip().rstrip("/")
    if base:
        return base
    return str(fallback or request.url_root or "").strip().rstrip("/")


def _admin_find_user(username, conn=None):
    uname = str(username or "").strip().lower()
    if not uname:
        return None
    propia = conn is None
    if propia:
        conn = get_db()
    try:
        row = conn.execute(
            "SELECT id, username, display_name, password_hash, activo FROM admin_users WHERE LOWER(username)=? LIMIT 1",
            (uname,),
        ).fetchone()
        return dict(row) if row else None
    except Exception:
        return None
    finally:
        if propia and conn is not None:
            conn.close()


def _admin_audit_login(username, success, reason="", ip_addr=None):
    try:
        conn = get_db()
        conn.execute(
            """
            INSERT INTO admin_login_audit (username, success, reason, ip_address)
            VALUES (?, ?, ?, ?)
            """,
            (str(username or "").strip()[:80], 1 if success else 0, str(reason or "").strip()[:180], str(ip_addr or _admin_client_ip())[:80]),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


def _admin_ensure_bootstrap_user():
    try:
        conn = get_db()
        cursor = conn.cursor()
        row = cursor.execute("SELECT COUNT(*) AS c FROM admin_users").fetchone()
        total = int((row["c"] if row else 0) or 0)
        if total <= 0:
            uname = _obtener_admin_legacy_username().strip().lower()
            if len(uname) < 3:
                uname = DEFAULT_ADMIN_LEGACY_USER
            cursor.execute(
                """
                INSERT INTO admin_users (username, display_name, password_hash, activo)
                VALUES (?, ?, ?, 1)
                """,
                (uname, "Administrador", generate_password_hash(_obtener_admin_pin())),
            )
            conn.commit()
        conn.close()
    except Exception as e:
        print(f"[WARN] No se pudo crear usuario admin bootstrap: {e}")


def _ruta_es_publica(path):
    ruta = str(path or "").strip()
    if not ruta:
        return False
    if ruta.startswith("/static/"):
        return True
    if ruta in {"/tienda", "/tienda/", "/tienda/agendar", "/tienda/agendar-beta", "/tienda/presencial", "/tienda/presencial/", "/admin/login", "/admin/logout", "/favicon.ico"}:
        return True
    if ruta.startswith("/tienda/flow/"):
        return True
    if ruta.startswith("/api/tienda/"):
        return True
    return False


def _normalizar_next_admin(destino):
    raw = str(destino or "").strip()
    if not raw:
        return url_for("agenda")
    parsed = urlparse(raw)
    if parsed.scheme or parsed.netloc:
        return url_for("agenda")
    path = parsed.path or "/"
    if not path.startswith("/"):
        path = f"/{path}"
    query = f"?{parsed.query}" if parsed.query else ""
    return f"{path}{query}"


def _detectar_tipo_imagen(data: bytes) -> str:
    raw = data if isinstance(data, (bytes, bytearray)) else b""
    if not raw:
        return ""
    try:
        if imghdr is not None:
            detected = imghdr.what(None, raw)
            if detected in {"jpeg", "png", "webp"}:
                return detected
    except Exception:
        pass
    if raw[:3] == b"\xff\xd8\xff":
        return "jpeg"
    if raw[:8] == b"\x89PNG\r\n\x1a\n":
        return "png"
    if len(raw) >= 12 and raw[:4] == b"RIFF" and raw[8:12] == b"WEBP":
        return "webp"
    return ""


@app.before_request
def _proteger_area_admin():
    path = request.path or "/"
    if _ruta_es_publica(path):
        return None
    if session.get(_ADMIN_SESSION_KEY):
        return None
    if path == "/":
        return redirect(url_for("tienda_publica"))
    if path.startswith("/api/"):
        return jsonify({"success": False, "error": "Acceso no autorizado."}), 401
    destino = request.full_path if request.query_string else path
    return redirect(url_for("admin_login", next=_normalizar_next_admin(destino)))

_GO2RTC_PROCESS = None

TUYA_IMPORT_ERROR = None
try:
    from tuya_sharing import LoginControl, Manager
except Exception as exc:
    TUYA_IMPORT_ERROR = f"{exc.__class__.__name__}: {exc}"
    LoginControl = None
    Manager = None

from database import (
    init_db, get_db, actualizar_stock_producto, actualizar_stock_insumo,
    procesar_venta_con_insumos, actualizar_stock_insumo_con_unidad,
    obtener_historial_ventas, obtener_detalle_venta, obtener_codigo_operacion_venta, obtener_timeline_operacion, eliminar_venta,
    obtener_reporte_ventas, obtener_recetas, guardar_receta, 
    producir_receta, eliminar_receta, obtener_productos_con_dias,
    revertir_produccion, agregar_lote_producto,
    obtener_lotes_por_producto, eliminar_lote,  # NUEVOS
    calcular_dias_restantes, obtener_estado_lote,  # NUEVOS
    obtener_receta_detalle, actualizar_receta,
    obtener_insumo_detalle, actualizar_insumo,
    limpiar_producciones_antiguas, obtener_historial_produccion_semanal,
    obtener_plan_produccion_semanal, obtener_agenda_produccion_semanal,
    agendar_produccion_manual, eliminar_produccion_agendada, completar_produccion_agendada,
    obtener_requerimientos_agenda_produccion,
    obtener_producto_detalle, actualizar_producto,
    procesar_lote_rapido_insumos, actualizar_preferencias_scan_insumo,
    registrar_movimiento_stock,
    registrar_lote_insumo, sincronizar_lotes_insumo_stock,
    obtener_config_alertas, guardar_config_alertas,
    obtener_config_camaras, guardar_config_camaras,
    obtener_config_clima_sidebar, guardar_config_clima_sidebar,
    obtener_config_updater, guardar_config_updater,
    obtener_config_tuya_haccp, guardar_config_tuya_haccp,
    guardar_auth_tuya_haccp, guardar_lectura_tuya_haccp,
    guardar_vinculaciones_tuya_haccp, obtener_vinculaciones_tuya_haccp,
    registrar_lectura_tuya_haccp, obtener_historial_tuya_haccp,
    obtener_recordatorios_agenda_pendientes, descartar_recordatorio_agenda,
    limpiar_recordatorios_descartados,
    obtener_reporte_produccion, obtener_reporte_insumos_agregados, obtener_reporte_productos_agregados,
    obtener_reporte_mermas_productos, obtener_resumen_mermas_por_fecha,
    obtener_kardex_movimientos, obtener_sugerencias_compra_insumos,
    obtener_resumen_margen_ventas,
    listar_haccp_puntos, obtener_haccp_registros, obtener_resumen_haccp,
    contar_haccp_vencidos, obtener_haccp_puntos_vencidos,
    crear_haccp_punto, actualizar_haccp_punto, cambiar_estado_haccp_punto, registrar_haccp_control,
    obtener_haccp_trazabilidad_insumos,
    registrar_merma_producto, revertir_merma_producto,
    obtener_compras_pendientes, agregar_compra_pendiente, agregar_lote_compras_pendientes,
    actualizar_compra_pendiente, eliminar_compra_pendiente, limpiar_compras_pendientes,
    marcar_compras_pendientes_completadas, previsualizar_finalizacion_compras_pendientes,
    finalizar_compras_pendientes_con_stock,
    obtener_calculadora_compras_draft, guardar_calculadora_compras_draft, limpiar_calculadora_compras_draft,
    registrar_historial_cambio, listar_historial_cambios, eliminar_historial_cambio,
    descartar_insumos_masivo,
    obtener_evento_agenda_por_id, actualizar_estado_evento_agenda,
    eliminar_eventos_agenda_pasados,
    obtener_notas_agenda, guardar_nota_agenda, eliminar_nota_agenda,
    guardar_factura_archivo, obtener_facturas_archivadas, obtener_factura_archivo,
    eliminar_factura_archivo, actualizar_factura_archivo, obtener_filtros_facturas, obtener_auditoria_factura,
    obtener_anios_tributarios_disponibles, obtener_resumen_sii_facturas,
    guardar_ajustes_sii_facturas, limpiar_ajustes_sii_facturas,
    guardar_venta_semanal, listar_ventas_semanales, eliminar_venta_semanal, obtener_resumen_ventas_vs_compras,
    convertir_a_base
)
from backup import crear_backup, obtener_ultimo_backup
from config import DATA_DIR, LEGACY_DATA_DIRS, BACKUP_DIR, APP_VERSION, APP_DISPLAY_NAME, DB_PATH
from unit_utils import (
    normalize_unit,
    unit_type,
    units_compatible,
    convert_amount,
    format_simple_number,
)


@app.context_processor
def inject_app_globals():
    return {"app_version": APP_VERSION}


RUN_BOOTSTRAP_DB_ON_START = str(os.getenv("GESTORSTOCK_BOOTSTRAP_DB", "0")).strip().lower() in {"1", "true", "yes", "on"}
if RUN_BOOTSTRAP_DB_ON_START:
    try:
        init_db()
    except Exception as _init_err:
        # Blindaje de arranque en hosting: no tumbar WSGI por errores transitorios de lock.
        _err_txt = str(_init_err).lower()
        if ("locking protocol" in _err_txt) or ("database is locked" in _err_txt):
            print(f"[WARN] init_db omitida temporalmente por lock sqlite: {_init_err}")
        else:
            raise
else:
    print(f"[INFO] init_db desactivada en bootstrap (DB: {DB_PATH})")

# Migrar base de datos (agregar columnas nuevas)
from database import migrar_db
RUN_BOOTSTRAP_MIGRATIONS_ON_START = str(os.getenv("GESTORSTOCK_BOOTSTRAP_MIGRATIONS", "0")).strip().lower() in {"1", "true", "yes", "on"}
if RUN_BOOTSTRAP_MIGRATIONS_ON_START:
    try:
        migrar_db()
    except Exception as _migrar_err:
        print(f"[WARN] migrar_db omitida por error: {_migrar_err}")
    try:
        _admin_ensure_bootstrap_user()
    except Exception as _admin_boot_err:
        print(f"[WARN] bootstrap admin omitido por error: {_admin_boot_err}")
else:
    print("[INFO] migrar_db/bootstrap admin desactivados en bootstrap")

FACTURAS_DIR = os.path.join(DATA_DIR, "facturas")
FACTURAS_RESPALDO_LOCAL_DIR = os.path.join(DATA_DIR, "facturas_respaldo_local")
BACKUP_ORQUESTACION_CONFIG_PATH = os.path.join(DATA_DIR, "backup_orquestacion_config.json")
ALLOWED_FACTURA_EXTENSIONS = {".pdf", ".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tif", ".tiff"}
os.makedirs(FACTURAS_DIR, exist_ok=True)
os.makedirs(FACTURAS_RESPALDO_LOCAL_DIR, exist_ok=True)
LEGACY_FACTURAS_DIRS = []
for legacy_root in LEGACY_DATA_DIRS:
    candidate = os.path.abspath(os.path.join(legacy_root, "facturas"))
    if candidate == os.path.abspath(FACTURAS_DIR):
        continue
    if candidate not in LEGACY_FACTURAS_DIRS and os.path.isdir(candidate):
        LEGACY_FACTURAS_DIRS.append(candidate)


def _normalizar_nombre_carpeta(valor):
    limpio = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(valor or "").strip())
    limpio = limpio.strip("_")
    return limpio[:80] or "sin_proveedor"


def _registrar_respaldo_local_factura(abs_path, proveedor, fecha_factura, numero_factura, original_name):
    """
    Copia espejo local para facilitar recuperacion y busqueda por cliente/mes/anio.
    Estructura:
      DATA_DIR/facturas_respaldo_local/<cliente>/<YYYY>/<MM>/*
    """
    try:
        if not abs_path or not os.path.exists(abs_path):
            return {"success": False, "error": "archivo_origen_no_existe"}
        cliente_slug = _normalizar_nombre_carpeta(proveedor or "sin_cliente")
        try:
            dt = datetime.strptime(str(fecha_factura or ""), "%Y-%m-%d")
        except Exception:
            dt = datetime.now()
        carpeta_destino = os.path.join(
            FACTURAS_RESPALDO_LOCAL_DIR,
            cliente_slug,
            f"{dt.year}",
            f"{dt.month:02d}",
        )
        os.makedirs(carpeta_destino, exist_ok=True)

        ext = os.path.splitext(str(original_name or ""))[1].lower() or os.path.splitext(abs_path)[1].lower() or ".bin"
        base = secure_filename(os.path.splitext(str(original_name or "factura"))[0]) or "factura"
        nombre = f"{dt.strftime('%Y%m%d')}_{uuid.uuid4().hex[:8]}_{base}{ext}"
        destino = os.path.abspath(os.path.join(carpeta_destino, nombre))
        shutil.copy2(abs_path, destino)

        meta = {
            "proveedor_cliente": str(proveedor or "").strip(),
            "fecha_factura": str(fecha_factura or ""),
            "numero_factura": str(numero_factura or "").strip(),
            "archivo_origen": str(abs_path),
            "archivo_respaldo": str(destino),
            "creado_en": datetime.now().isoformat(timespec="seconds"),
        }
        meta_path = f"{destino}.json"
        with open(meta_path, "w", encoding="utf-8") as fh:
            json.dump(meta, fh, ensure_ascii=False, indent=2)
        return {"success": True, "path": destino}
    except Exception as e:
        print(f"[WARN] No se pudo crear respaldo local de factura: {e}")
        return {"success": False, "error": str(e)}


def _normalizar_texto_busqueda(valor):
    texto = str(valor or "").strip().lower()
    if not texto:
        return ""
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(ch for ch in texto if not unicodedata.combining(ch))
    return " ".join(texto.split())


def _backup_orquestacion_default():
    return {
        "enabled": True,
        "local_backup_root": r"C:\Visual Studio Code\gestor_stock\backups\pythonanywhere_full",
        "schedule_day": "sunday",
        "schedule_time": "23:00",
        "pythonanywhere_user": "alexdroow",
        "pythonanywhere_host": "ssh.pythonanywhere.com",
        "remote_project_dir": "/home/alexdroow/gestor_stock",
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }


def _normalizar_backup_orquestacion(payload):
    base = _backup_orquestacion_default()
    data = dict(payload or {})
    out = dict(base)
    out["enabled"] = bool(data.get("enabled", base["enabled"]))
    out["local_backup_root"] = str(data.get("local_backup_root") or base["local_backup_root"]).strip()[:260] or base["local_backup_root"]
    out["schedule_day"] = str(data.get("schedule_day") or base["schedule_day"]).strip().lower()
    if out["schedule_day"] not in {"sunday"}:
        out["schedule_day"] = "sunday"
    hhmm = str(data.get("schedule_time") or base["schedule_time"]).strip()
    out["schedule_time"] = hhmm if _parse_hora_hhmm(hhmm) else base["schedule_time"]
    out["pythonanywhere_user"] = str(data.get("pythonanywhere_user") or base["pythonanywhere_user"]).strip()[:80] or base["pythonanywhere_user"]
    out["pythonanywhere_host"] = str(data.get("pythonanywhere_host") or base["pythonanywhere_host"]).strip()[:120] or base["pythonanywhere_host"]
    out["remote_project_dir"] = str(data.get("remote_project_dir") or base["remote_project_dir"]).strip()[:220] or base["remote_project_dir"]
    out["updated_at"] = datetime.now().isoformat(timespec="seconds")
    return out


def _leer_backup_orquestacion():
    base = _backup_orquestacion_default()
    try:
        if not os.path.exists(BACKUP_ORQUESTACION_CONFIG_PATH):
            return base
        with open(BACKUP_ORQUESTACION_CONFIG_PATH, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        return _normalizar_backup_orquestacion(raw)
    except Exception:
        return base


def _guardar_backup_orquestacion(payload):
    cfg = _normalizar_backup_orquestacion(payload)
    os.makedirs(os.path.dirname(BACKUP_ORQUESTACION_CONFIG_PATH), exist_ok=True)
    with open(BACKUP_ORQUESTACION_CONFIG_PATH, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh, ensure_ascii=False, indent=2)
    return cfg


def _buscar_insumo_por_nombre_cursor(cursor, nombre):
    objetivo = _normalizar_texto_busqueda(nombre)
    if not objetivo:
        return None
    cursor.execute("SELECT * FROM insumos ORDER BY id ASC")
    for row in cursor.fetchall():
        if _normalizar_texto_busqueda(row["nombre"]) == objetivo:
            return row
    return None


def _buscar_insumo_por_codigo_cursor(cursor, codigo_barra):
    codigo = str(codigo_barra or "").strip()
    if not codigo:
        return None, None

    cursor.execute("SELECT * FROM insumos WHERE codigo_barra = ? LIMIT 1", (codigo,))
    insumo = cursor.fetchone()
    if insumo:
        return insumo, "codigo"

    try:
        cursor.execute(
            """
            SELECT i.*
            FROM insumo_codigos ic
            JOIN insumos i ON i.id = ic.insumo_id
            WHERE ic.codigo_barra = ?
            ORDER BY i.id ASC
            LIMIT 1
            """,
            (codigo,),
        )
        insumo = cursor.fetchone()
        if insumo:
            return insumo, "codigo_alias"
    except sqlite3.OperationalError:
        return None, None

    return None, None


def _asociar_codigo_insumo_cursor(cursor, insumo_id, codigo_barra):
    codigo = str(codigo_barra or "").strip()
    if not codigo:
        return

    insumo_id_int = int(insumo_id)
    cursor.execute(
        "SELECT id FROM insumos WHERE codigo_barra = ? AND id <> ? LIMIT 1",
        (codigo, insumo_id_int),
    )
    if cursor.fetchone():
        raise ValueError(f"El código '{codigo}' ya está asociado a otro insumo")

    try:
        cursor.execute(
            "SELECT insumo_id FROM insumo_codigos WHERE codigo_barra = ? LIMIT 1",
            (codigo,),
        )
        existente = cursor.fetchone()
    except sqlite3.OperationalError:
        return

    if existente and int(existente["insumo_id"] or 0) != insumo_id_int:
        raise ValueError(f"El código '{codigo}' ya está asociado a otro insumo")

    if not existente:
        cursor.execute(
            "INSERT INTO insumo_codigos (insumo_id, codigo_barra) VALUES (?, ?)",
            (insumo_id_int, codigo),
        )


def _parse_fecha_factura(valor):
    raw = str(valor or "").strip()
    if not raw:
        raise ValueError("La fecha es obligatoria")
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError("Fecha inválida")


def _safe_join_under(base_dir, rel_path):
    rel = str(rel_path or "").replace("\\", "/").lstrip("/")
    base = os.path.abspath(base_dir)
    abs_path = os.path.abspath(os.path.join(base, rel))
    if abs_path == base or not abs_path.startswith(base + os.sep):
        raise ValueError("Ruta de archivo inválida")
    return abs_path


def _safe_join_facturas(rel_path):
    return _safe_join_under(FACTURAS_DIR, rel_path)


def _resolver_ruta_factura(rel_path, incluir_legadas=False):
    abs_path = _safe_join_facturas(rel_path)
    if os.path.exists(abs_path) or not incluir_legadas:
        return abs_path, FACTURAS_DIR

    for legacy_dir in LEGACY_FACTURAS_DIRS:
        try:
            legacy_path = _safe_join_under(legacy_dir, rel_path)
        except ValueError:
            continue
        if os.path.exists(legacy_path):
            return legacy_path, legacy_dir
    return abs_path, FACTURAS_DIR


def _wants_json_response():
    if request.headers.get("X-Requested-With") == "fetch":
        return True
    accept = str(request.headers.get("Accept") or "").lower()
    return request.is_json or "application/json" in accept


def _ok_or_redirect(payload, endpoint, **values):
    if _wants_json_response():
        return jsonify(payload)
    return redirect(url_for(endpoint, **values))


def _error_or_text(message, status_code=400):
    if _wants_json_response():
        return jsonify({"success": False, "error": str(message)}), status_code
    return str(message), status_code


def _as_float(value, field_name, min_value=None):
    try:
        num = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"Valor inválido para {field_name}")
    if min_value is not None and num < min_value:
        raise ValueError(f"{field_name} debe ser mayor o igual a {min_value}")
    return num


def _as_int(value, field_name, min_value=None):
    try:
        num = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"Valor inválido para {field_name}")
    if min_value is not None and num < min_value:
        raise ValueError(f"{field_name} debe ser mayor o igual a {min_value}")
    return num


def _as_optional_date(value, field_name):
    raw = str(value or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError(f"Fecha inválida para {field_name}")


def _normalizar_unidad_producto(unidad_raw):
    return normalize_unit(unidad_raw)


def _formatear_numero_simple(valor):
    return format_simple_number(valor)


def _tipo_unidad(unidad_raw):
    return unit_type(unidad_raw)


def _son_unidades_compatibles_porcion(unidad_1, unidad_2):
    return units_compatible(unidad_1, unidad_2)


def _convertir_cantidad_unidad(cantidad, unidad_origen, unidad_destino):
    return convert_amount(cantidad, unidad_origen, unidad_destino, convertir_a_base)


CATALOGO_ICONOS_PRODUCTO = (
    ("cupcake", "\U0001F9C1", "Cupcake"),
    ("cake", "\U0001F382", "Torta"),
    ("cookie", "\U0001F36A", "Galleta"),
    ("muffin", "\U0001F9C7", "Muffin"),
    ("donut", "\U0001F369", "Donut"),
    ("croissant", "\U0001F950", "Croissant"),
    ("bread", "\U0001F35E", "Pan"),
    ("pie", "\U0001F967", "Pie"),
    ("chocolate", "\U0001F36B", "Chocolate"),
    ("candy", "\U0001F36C", "Dulce"),
    ("icecream", "\U0001F366", "Helado"),
    ("coffee", "\u2615", "Cafe"),
    ("package", "\U0001F4E6", "Generico"),
)
ICONOS_PRODUCTO_KEYS = {item[0] for item in CATALOGO_ICONOS_PRODUCTO}


def _catalogo_iconos_producto():
    return [{"key": key, "emoji": emoji, "label": label} for key, emoji, label in CATALOGO_ICONOS_PRODUCTO]


def _normalizar_icono_producto(icono_raw):
    key = str(icono_raw or "").strip().lower()
    return key if key in ICONOS_PRODUCTO_KEYS else "cupcake"


def _esta_cerca_minimo(stock_actual, stock_minimo):
    try:
        stock = float(stock_actual or 0)
        minimo = float(stock_minimo or 0)
    except (TypeError, ValueError):
        return False

    if minimo <= 0:
        return False
    if stock <= minimo:
        return False

    margen_alerta = max(2.0, float(math.ceil(minimo * 0.5)))
    umbral_superior = minimo + margen_alerta
    return stock <= umbral_superior


def _calcular_info_porciones_producto(producto):
    unidad_stock = _normalizar_unidad_producto(producto.get("unidad") or "unidad")
    stock_actual = float(producto.get("stock") or 0)
    stock_minimo = float(producto.get("stock_minimo") or 0)
    porcion_cantidad = float(producto.get("porcion_cantidad") or 1)
    porcion_unidad = _normalizar_unidad_producto(producto.get("porcion_unidad") or unidad_stock)

    conversion = _convertir_cantidad_unidad(porcion_cantidad, porcion_unidad, unidad_stock)
    if not conversion["success"]:
        return {
            "success": False,
            "error": conversion["error"],
            "unidad_stock": unidad_stock,
            "stock_actual": stock_actual,
            "stock_minimo": stock_minimo,
            "porcion_cantidad": porcion_cantidad,
            "porcion_unidad": porcion_unidad,
            "porcion_stock_equivalente": None,
            "porciones_disponibles": 0,
            "sin_porcion_disponible": True,
            "baja_porcion": True,
            "bajo_minimo": stock_actual <= stock_minimo,
            "cerca_minimo": _esta_cerca_minimo(stock_actual, stock_minimo),
        }

    porcion_stock_equivalente = float(conversion["cantidad"] or 0)
    if porcion_stock_equivalente <= 0:
        porciones_disponibles = 0
    else:
        porciones_disponibles = max(0, int(math.floor((stock_actual + 1e-9) / porcion_stock_equivalente)))

    return {
        "success": True,
        "error": None,
        "unidad_stock": unidad_stock,
        "stock_actual": stock_actual,
        "stock_minimo": stock_minimo,
        "porcion_cantidad": porcion_cantidad,
        "porcion_unidad": porcion_unidad,
        "porcion_stock_equivalente": porcion_stock_equivalente,
        "porciones_disponibles": porciones_disponibles,
        "sin_porcion_disponible": porciones_disponibles < 1,
        "baja_porcion": porciones_disponibles <= 1,
        "bajo_minimo": stock_actual <= stock_minimo,
        "cerca_minimo": _esta_cerca_minimo(stock_actual, stock_minimo),
    }


def _obtener_alertas_productos(cursor):
    cursor.execute(
        """
        SELECT *
        FROM productos
        WHERE COALESCE(eliminado, 0) = 0
        ORDER BY stock ASC, nombre ASC
        """
    )
    filas = cursor.fetchall()
    productos = [_armar_producto_base_para_venta(dict(f)) for f in filas]
    _enriquecer_productos_con_dependencias_venta(cursor, productos)
    _anotar_estado_desactivacion_manual(cursor, productos, limpiar_resueltas=True)

    productos_stock_bajo = []
    productos_baja_porcion = []
    ids_stock_bajo = set()
    ids_baja_porcion = set()

    for item in productos:
        producto_id = int(item.get("id") or 0)
        stock = float(item.get("stock") or 0)
        stock_minimo = float(item.get("stock_minimo") or 0)
        unidad_stock = _normalizar_unidad_producto(item.get("unidad") or "unidad")
        estado_disp = _resolver_estado_disponibilidad_producto(item)
        item["estado_disponibilidad"] = estado_disp.get("estado_final")

        if bool(item.get("bajo_minimo")) or bool(item.get("dependencias_criticas")):
            ids_stock_bajo.add(producto_id)
            faltante_alerta = max(0.0, stock_minimo - stock)
            productos_stock_bajo.append(
                {
                    "id": producto_id,
                    "nombre": item.get("nombre"),
                    "stock": stock,
                    "stock_label": _formatear_numero_simple(stock),
                    "stock_minimo": stock_minimo,
                    "stock_minimo_label": _formatear_numero_simple(stock_minimo),
                    "unidad": unidad_stock,
                    "faltante_alerta": faltante_alerta,
                    "faltante_alerta_label": _formatear_numero_simple(faltante_alerta),
                    "dependencias_alerta_texto": item.get("dependencias_alerta_texto") or "",
                    "estado_disponibilidad": estado_disp.get("estado_final"),
                }
            )

        if bool(item.get("sin_porcion_disponible")) or bool(item.get("dependencias_criticas")):
            ids_baja_porcion.add(producto_id)
            porcion_stock_equivalente = item.get("porcion_stock_equivalente")
            faltante_para_porcion = 0.0
            if porcion_stock_equivalente is not None:
                faltante_para_porcion = max(
                    0.0,
                    float(porcion_stock_equivalente or 0) - float(item.get("stock") or 0),
                )
            productos_baja_porcion.append(
                {
                    "id": producto_id,
                    "nombre": item.get("nombre"),
                    "stock": float(item.get("stock") or 0),
                    "stock_label": _formatear_numero_simple(item.get("stock")),
                    "unidad": unidad_stock,
                    "porcion_cantidad": float(item.get("porcion_cantidad") or 1),
                    "porcion_cantidad_label": _formatear_numero_simple(item.get("porcion_cantidad")),
                    "porcion_unidad": item.get("porcion_unidad") or unidad_stock,
                    "faltante_para_porcion": faltante_para_porcion,
                    "faltante_para_porcion_label": _formatear_numero_simple(faltante_para_porcion),
                    "error_porcion": item.get("porcion_error"),
                    "dependencias_alerta_texto": item.get("dependencias_alerta_texto") or "",
                    "estado_disponibilidad": estado_disp.get("estado_final"),
                }
            )

    return {
        "productos_stock_bajo": productos_stock_bajo,
        "productos_baja_porcion": productos_baja_porcion,
        "ids_stock_bajo": ids_stock_bajo,
        "ids_baja_porcion": ids_baja_porcion,
        "ids_union": ids_stock_bajo | ids_baja_porcion,
    }


def _nivel_alerta_producto(item):
    stock = float(item.get("stock") or 0)
    stock_min = float(item.get("stock_minimo") or 0)
    if item.get("error_porcion"):
        return "alta"
    if item.get("faltante_para_porcion") is not None:
        # Productos sin porción disponible se consideran críticos.
        return "critica"
    if stock <= 0:
        return "critica"
    if stock_min > 0 and stock <= (stock_min * 0.5):
        return "alta"
    return "media"


def _nivel_alerta_insumo(item):
    stock = float(item.get("stock") or 0)
    stock_min = float(item.get("stock_minimo") or 0)
    if stock_min <= 0:
        return "baja"
    if stock <= 0:
        return "critica"
    if stock_min > 0 and stock <= (stock_min * 0.5):
        return "alta"
    if stock <= stock_min:
        return "media"
    return "baja"


def _resumen_criticidad_alertas(productos_alerta, productos_baja_porcion, insumos_bajos):
    niveles = {"critica": 0, "alta": 0, "media": 0, "baja": 0}
    for p in productos_alerta:
        nivel = _nivel_alerta_producto(p)
        niveles[nivel] = niveles.get(nivel, 0) + 1
    for p in productos_baja_porcion:
        nivel = _nivel_alerta_producto(p)
        niveles[nivel] = niveles.get(nivel, 0) + 1
    for i in insumos_bajos:
        nivel = _nivel_alerta_insumo(i)
        niveles[nivel] = niveles.get(nivel, 0) + 1
    return niveles


def _mapa_desactivaciones_manuales(cursor, producto_ids=None):
    ids = []
    if producto_ids:
        for raw in producto_ids:
            try:
                pid = int(raw or 0)
            except (TypeError, ValueError):
                pid = 0
            if pid > 0:
                ids.append(pid)
    if ids:
        placeholders = ",".join("?" for _ in ids)
        cursor.execute(
            f"""
            SELECT producto_id, confirmado_en
            FROM producto_desactivaciones_manuales
            WHERE producto_id IN ({placeholders})
            """,
            tuple(ids),
        )
    else:
        cursor.execute(
            """
            SELECT producto_id, confirmado_en
            FROM producto_desactivaciones_manuales
            """
        )
    return {int(row["producto_id"]): row["confirmado_en"] for row in cursor.fetchall()}


def _anotar_estado_desactivacion_manual(cursor, productos, limpiar_resueltas=True):
    if not isinstance(productos, list) or not productos:
        return productos

    ids = []
    for item in productos:
        try:
            pid = int(item.get("id") or 0)
        except (TypeError, ValueError):
            pid = 0
        if pid > 0:
            ids.append(pid)

    mapa = _mapa_desactivaciones_manuales(cursor, ids)
    limpiar_ids = []

    for item in productos:
        try:
            pid = int(item.get("id") or 0)
        except (TypeError, ValueError):
            pid = 0

        estado_disponibilidad = _resolver_estado_disponibilidad_producto(item)
        es_critico = bool(estado_disponibilidad.get("bloqueado"))
        confirmado = bool(pid and pid in mapa and es_critico)
        requiere_confirmacion = bool(es_critico and not confirmado)

        item["desactivacion_manual_confirmada"] = confirmado
        item["desactivacion_manual_requiere_confirmacion"] = requiere_confirmacion
        item["desactivacion_manual_confirmada_en"] = mapa.get(pid) if confirmado else None

        if pid and (pid in mapa) and not es_critico:
            limpiar_ids.append(pid)

    if limpiar_resueltas and limpiar_ids:
        placeholders = ",".join("?" for _ in limpiar_ids)
        cursor.execute(
            f"DELETE FROM producto_desactivaciones_manuales WHERE producto_id IN ({placeholders})",
            tuple(limpiar_ids),
        )

    return productos


def _resolver_estado_disponibilidad_producto(item):
    porciones = int(item.get("porciones_disponibles") or 0)
    bloqueado = (
        bool(item.get("sin_porcion_disponible"))
        or porciones < 1
        or bool(item.get("bajo_minimo"))
        or bool(item.get("dependencias_criticas"))
    )
    advertencia = (
        not bloqueado
        and (
            bool(item.get("baja_porcion"))
            or bool(item.get("cerca_minimo"))
            or bool(item.get("dependencias_baja_porcion"))
            or bool(item.get("dependencias_cerca_minimo"))
            or bool(item.get("dependencias_limita_porciones"))
        )
    )
    if bloqueado:
        estado_final = "bloqueado"
    elif advertencia:
        estado_final = "advertencia"
    else:
        estado_final = "disponible"
    return {
        "estado_final": estado_final,
        "bloqueado": bloqueado,
        "advertencia": advertencia,
        "disponible": not bloqueado and not advertencia,
    }


def calcular_disponibilidad_producto(producto_id, conn=None):
    propia = conn is None
    if propia:
        conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM productos WHERE id = ?", (producto_id,))
        row = cursor.fetchone()
        if not row:
            return None
        producto = _armar_producto_base_para_venta(dict(row))
        _enriquecer_productos_con_dependencias_venta(cursor, [producto])
        _anotar_estado_desactivacion_manual(cursor, [producto], limpiar_resueltas=True)
        estado = _resolver_estado_disponibilidad_producto(producto)
        producto["estado_disponibilidad"] = estado["estado_final"]
        producto["disponible"] = estado["disponible"]
        producto["advertencia"] = estado["advertencia"]
        producto["bloqueado"] = estado["bloqueado"]
        return producto
    finally:
        if propia:
            conn.close()


@app.route('/api/producto/<int:producto_id>/disponibilidad')
def api_disponibilidad_producto(producto_id):
    try:
        data = calcular_disponibilidad_producto(producto_id)
        if not data:
            return jsonify({"success": False, "error": "Producto no encontrado"}), 404
        return jsonify({"success": True, "data": data})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/')
def index():
    if session.get(_ADMIN_SESSION_KEY):
        return render_template('index.html')
    return redirect(url_for("tienda_publica"))


@app.route('/inicio')
def inicio_dashboard():
    return redirect(url_for('index'))


@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    if session.get(_ADMIN_SESSION_KEY):
        return redirect(url_for("agenda"))

    error = None
    blocked_seconds = 0
    if request.method == 'POST':
        ip_addr = _admin_client_ip()
        blocked, wait = _admin_login_is_blocked(ip_addr)
        if blocked:
            blocked_seconds = wait
            error = f"Demasiados intentos fallidos. Espera {wait}s."
            next_url = _normalizar_next_admin(request.form.get('next'))
            return render_template(
                'admin_login.html',
                error=error,
                next_url=next_url,
                blocked_seconds=blocked_seconds,
                legacy_user=_obtener_admin_legacy_username(),
            )

        username = str(request.form.get('username') or '').strip()
        pin = str(request.form.get('pin') or '').strip()
        next_url = _normalizar_next_admin(request.form.get('next'))
        ok = False
        reason = "invalid_credentials"
        user_row = _admin_find_user(username)
        if user_row and int(user_row.get("activo") or 0) == 1:
            if check_password_hash(str(user_row.get("password_hash") or ""), pin):
                ok = True
                reason = "ok_user_pass"
                session[_ADMIN_USER_ID_SESSION_KEY] = int(user_row.get("id") or 0)
                session[_ADMIN_USER_NAME_SESSION_KEY] = str(user_row.get("display_name") or user_row.get("username") or "admin")
                try:
                    conn = get_db()
                    conn.execute(
                        """
                        UPDATE admin_users
                        SET last_login_at=CURRENT_TIMESTAMP, last_login_ip=?, actualizado_en=CURRENT_TIMESTAMP
                        WHERE id=?
                        """,
                        (ip_addr, int(user_row.get("id") or 0)),
                    )
                    conn.commit()
                    conn.close()
                except Exception:
                    pass

        legacy_user = _obtener_admin_legacy_username()
        if (not ok) and pin == _obtener_admin_pin() and str(username or "").strip().lower() == legacy_user.lower():
            ok = True
            reason = "ok_legacy_pin"
            session[_ADMIN_USER_ID_SESSION_KEY] = 0
            session[_ADMIN_USER_NAME_SESSION_KEY] = legacy_user

        if ok:
            session.permanent = True
            session[_ADMIN_SESSION_KEY] = True
            _admin_login_register_success(ip_addr)
            _admin_audit_login(username=username, success=True, reason=reason, ip_addr=ip_addr)
            return redirect(next_url)
        _admin_login_register_fail(ip_addr)
        _admin_audit_login(username=username, success=False, reason=reason, ip_addr=ip_addr)
        error = "Usuario o clave incorrectos."

    next_url = _normalizar_next_admin(request.args.get('next'))
    return render_template(
        'admin_login.html',
        error=error,
        next_url=next_url,
        blocked_seconds=blocked_seconds,
        legacy_user=_obtener_admin_legacy_username(),
    )


@app.route('/admin/logout', methods=['GET', 'POST'])
def admin_logout():
    session.pop(_ADMIN_SESSION_KEY, None)
    session.pop(_ADMIN_USER_ID_SESSION_KEY, None)
    session.pop(_ADMIN_USER_NAME_SESSION_KEY, None)
    return redirect(url_for('admin_login'))


def _request_is_phone_mobile():
    ua = str(request.headers.get("User-Agent") or "").lower()
    if not ua:
        return False
    is_phone = bool(re.search(r"(iphone|ipod|windows phone|iemobile|opera mini|blackberry|bb10|mobile safari|android)", ua))
    is_tablet_or_desktop = bool(re.search(r"(ipad|tablet|macintosh|windows nt|linux x86_64|cros)", ua))
    return is_phone and not is_tablet_or_desktop


@app.route('/tienda')
def tienda_publica():
    try:
        personalizacion = _obtener_tienda_personalizacion()
    except Exception:
        personalizacion = _default_tienda_personalizacion()
    return render_template(
        'tienda.html',
        tienda_personalizacion=personalizacion,
        force_agenda=False,
        modo_presencial=False,
        agenda_beta=False,
        mobile_detected=_request_is_phone_mobile(),
    )


@app.route('/tienda/agendar')
def tienda_publica_agendar():
    try:
        personalizacion = _obtener_tienda_personalizacion()
    except Exception:
        personalizacion = _default_tienda_personalizacion()
    # Agenda beta pasa a ser la version oficial en la ruta publica de agenda.
    return render_template(
        'tienda.html',
        tienda_personalizacion=personalizacion,
        force_agenda=True,
        modo_presencial=False,
        agenda_beta=True,
        mobile_detected=_request_is_phone_mobile(),
    )


@app.route('/tienda/agendar-beta')
def tienda_publica_agendar_beta():
    # Compatibilidad: mantenemos la ruta antigua apuntando a la oficial.
    return redirect(url_for('tienda_publica_agendar'))


@app.route('/tienda/presencial')
def tienda_publica_presencial():
    try:
        personalizacion = _obtener_tienda_personalizacion()
    except Exception:
        personalizacion = _default_tienda_personalizacion()
    return render_template(
        'tienda.html',
        tienda_personalizacion=personalizacion,
        force_agenda=False,
        modo_presencial=True,
        agenda_beta=False,
        mobile_detected=_request_is_phone_mobile(),
    )


@app.route('/tienda/preview')
def tienda_preview_admin():
    if not session.get(_ADMIN_SESSION_KEY):
        return redirect(url_for("admin_login", next=request.full_path if request.query_string else request.path))
    mode = str(request.args.get("mode") or "live").strip().lower()
    force_agenda = str(request.args.get("agenda") or "0").strip() in {"1", "true", "yes", "on"}
    try:
        if mode == "draft":
            personalizacion = _obtener_tienda_personalizacion(apply_programacion=False, editor_mode="draft")
        else:
            personalizacion = _obtener_tienda_personalizacion(apply_programacion=True, editor_mode="live")
    except Exception:
        personalizacion = _default_tienda_personalizacion()
    return render_template(
        "tienda.html",
        tienda_personalizacion=personalizacion,
        force_agenda=force_agenda,
        modo_presencial=False,
        agenda_beta=False,
        mobile_detected=_request_is_phone_mobile(),
    )


def _parse_fecha_yyyy_mm_dd(valor):
    raw = str(valor or "").strip()
    if not raw:
        return None
    raw = raw[:10]
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        return None


def _serializar_producto_tienda(producto, categorias_map=None, now_local=None):
    item = dict(producto or {})
    max_compra = int(item.get("porciones_disponibles") or 0)
    if max_compra < 0:
        max_compra = 0
    foto_url = str(item.get("foto_url") or "").strip()
    if not foto_url:
        foto_rel = str(item.get("foto") or "").strip()
        if foto_rel:
            foto_url = url_for('static', filename=foto_rel)
    categoria = str(item.get("categoria_tienda") or "").strip() or "General"
    categoria_cfg = (categorias_map or {}).get(categoria.lower().strip()) if categorias_map else None
    categoria_descuento = 0.0
    categoria_activa = True
    if categoria_cfg:
        eval_cat = _evaluar_categoria_activa(categoria_cfg, now_local=now_local)
        categoria_activa = bool(eval_cat.get("activa"))
        categoria_descuento = float(categoria_cfg.get("descuento_pct") or 0) if categoria_activa else 0.0
    descripcion = str(item.get("descripcion_tienda") or "").strip()
    descuento_base_producto = float(item.get("descuento_tienda_pct") or 0)
    oferta_inicio_tienda = str(item.get("oferta_inicio_tienda") or "").strip()
    oferta_fin_tienda = str(item.get("oferta_fin_tienda") or "").strip()
    fecha_reposicion_tienda = str(item.get("fecha_reposicion_tienda") or "").strip()
    fecha_inicio = _parse_fecha_yyyy_mm_dd(oferta_inicio_tienda)
    fecha_fin = _parse_fecha_yyyy_mm_dd(oferta_fin_tienda)
    if not now_local:
        now_local = datetime.now(ZoneInfo("America/Santiago"))
    hoy = now_local.date()
    oferta_programada_activa = True
    if fecha_inicio and hoy < fecha_inicio:
        oferta_programada_activa = False
    if fecha_fin and hoy > fecha_fin:
        oferta_programada_activa = False
    fecha_reposicion_tienda_visible = fecha_reposicion_tienda
    fecha_reposicion_obj = _parse_fecha_yyyy_mm_dd(fecha_reposicion_tienda)
    # Si llego la fecha de reposicion y el producto sigue sin stock, ocultamos la fecha al cliente.
    if fecha_reposicion_obj and max_compra <= 0 and hoy >= fecha_reposicion_obj:
        fecha_reposicion_tienda_visible = ""
    descuento_producto_efectivo = descuento_base_producto if oferta_programada_activa else 0.0
    descuento = descuento_producto_efectivo
    if categoria_descuento > descuento:
        descuento = categoria_descuento
    if descuento < 0:
        descuento = 0.0
    if descuento > 100:
        descuento = 100.0
    precio_base = float(item.get("precio") or 0)
    precio_final = precio_base * (1 - (descuento / 100.0))
    if precio_final < 0:
        precio_final = 0
    foto_fit = str(item.get("foto_fit_tienda") or "cover").strip().lower()
    if foto_fit not in {"cover", "contain"}:
        foto_fit = "cover"
    foto_pos = str(item.get("foto_pos_tienda") or "center").strip().lower()
    if foto_pos not in {"center", "top", "bottom"}:
        foto_pos = "center"
    try:
        foto_pos_x = float(item.get("foto_pos_x_tienda") if item.get("foto_pos_x_tienda") is not None else 50)
    except (TypeError, ValueError):
        foto_pos_x = 50.0
    try:
        foto_pos_y = float(item.get("foto_pos_y_tienda") if item.get("foto_pos_y_tienda") is not None else 50)
    except (TypeError, ValueError):
        foto_pos_y = 50.0
    try:
        foto_zoom = float(item.get("foto_zoom_tienda") if item.get("foto_zoom_tienda") is not None else 100)
    except (TypeError, ValueError):
        foto_zoom = 100.0
    foto_pos_x = max(0.0, min(100.0, foto_pos_x))
    foto_pos_y = max(0.0, min(100.0, foto_pos_y))
    foto_zoom = max(50.0, min(220.0, foto_zoom))
    return {
        "id": int(item.get("id") or 0),
        "nombre": item.get("nombre") or "Producto",
        "precio_base": precio_base,
        "precio_final": round(precio_final, 2),
        "descuento_tienda_pct": descuento,
        "descuento_tienda_base_pct": round(float(descuento_base_producto or 0), 2),
        "stock_visual": float(item.get("stock_visual") or 0),
        "stock_visual_label": item.get("stock_visual_label") or _formatear_numero_simple(item.get("stock_visual")),
        "stock_visual_unidad": item.get("stock_visual_unidad") or item.get("unidad") or "unidad",
        "foto_url": foto_url,
        "foto": str(item.get("foto") or "").strip(),
        "foto_fit_tienda": foto_fit,
        "foto_pos_tienda": foto_pos,
        "foto_pos_x_tienda": round(foto_pos_x, 2),
        "foto_pos_y_tienda": round(foto_pos_y, 2),
        "foto_zoom_tienda": round(foto_zoom, 2),
        "categoria_tienda": categoria,
        "categoria_descuento_pct": round(float(categoria_descuento or 0), 2),
        "categoria_activa": bool(categoria_activa),
        "descripcion_tienda": descripcion,
        "oferta_inicio_tienda": oferta_inicio_tienda,
        "oferta_fin_tienda": oferta_fin_tienda,
        "fecha_reposicion_tienda": fecha_reposicion_tienda_visible,
        "oferta_programada_activa": bool(oferta_programada_activa),
        "destacado_tienda": bool(item.get("destacado_tienda")),
        "orden_tienda": int(item.get("orden_tienda") or 0),
        "activo_tienda": bool(item.get("activo_tienda") if item.get("activo_tienda") is not None else 1),
        "icono": item.get("icono") or "package",
        "max_compra": max_compra,
    }


def _normalizar_cupon_codigo(codigo):
    raw = str(codigo or "").strip().upper()
    raw = re.sub(r"[^A-Z0-9_-]+", "", raw)
    return raw[:40]


def _normalizar_cliente_ref(email, telefono):
    em = str(email or "").strip().lower()
    te = re.sub(r"\D+", "", str(telefono or ""))
    if em and te:
        return f"{em}|{te}"
    return em or te or ""


def _normalizar_email(raw):
    email = str(raw or "").strip().lower()
    if not email:
        return ""
    if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email):
        return ""
    return email


def _nombre_desde_email(email):
    base = str(email or "").split("@")[0].strip()
    if not base:
        return "Cliente tienda"
    base = base.replace(".", " ").replace("_", " ").replace("-", " ")
    return " ".join(part.capitalize() for part in base.split() if part)[:80] or "Cliente tienda"


def _normalizar_fecha_nacimiento(raw):
    txt = str(raw or "").strip()
    if not txt:
        return ""
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(txt, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def _slug_simple(raw):
    txt = str(raw or "").strip().lower()
    txt = re.sub(r"[^a-z0-9]+", "-", txt)
    txt = re.sub(r"-{2,}", "-", txt).strip("-")
    return txt[:80] or "nivel"


def _config_programa_clientes_default():
    return {
        "enabled": True,
        "purchase_amount_base": 1000,
        "purchase_points": 10,
        "agenda_points": 20,
    }


def _normalizar_config_programa_clientes(payload):
    base = _config_programa_clientes_default()
    data = dict(payload or {})
    cfg = dict(base)
    cfg["enabled"] = bool(data.get("enabled", base["enabled"]))
    cfg["purchase_amount_base"] = _clamp_int(data.get("purchase_amount_base"), default=1000, min_value=500, max_value=10000000)
    cfg["purchase_points"] = _clamp_int(data.get("purchase_points"), default=10, min_value=0, max_value=100000)
    cfg["agenda_points"] = _clamp_int(data.get("agenda_points"), default=20, min_value=0, max_value=100000)
    return cfg


def _obtener_config_programa_clientes(conn):
    cfg = _config_programa_clientes_default()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT config_json FROM tienda_clientes_programa WHERE id = 1 LIMIT 1")
        row = cursor.fetchone()
        if row and str(row["config_json"] or "").strip():
            raw = json.loads(str(row["config_json"] or "{}"))
            cfg = _normalizar_config_programa_clientes(raw)
    except Exception:
        cfg = _config_programa_clientes_default()
    return cfg


def _guardar_config_programa_clientes(conn, payload):
    cfg = _normalizar_config_programa_clientes(payload)
    conn.execute(
        """
        INSERT INTO tienda_clientes_programa (id, config_json, actualizado_en)
        VALUES (1, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(id) DO UPDATE SET
            config_json = excluded.config_json,
            actualizado_en = CURRENT_TIMESTAMP
        """,
        (json.dumps(cfg, ensure_ascii=False),),
    )
    return cfg


def _cargar_niveles_clientes(conn, solo_activos=False):
    cursor = conn.cursor()
    query = """
        SELECT id, nombre, slug, orden, puntos_minimos, beneficios_json, descuento_pct, activo
        FROM tienda_clientes_niveles
    """
    params = ()
    if solo_activos:
        query += " WHERE activo = 1"
    query += " ORDER BY orden ASC, puntos_minimos ASC, id ASC"
    cursor.execute(query, params)
    out = []
    for row in cursor.fetchall():
        r = dict(row)
        try:
            beneficios = json.loads(str(r.get("beneficios_json") or "{}"))
        except Exception:
            beneficios = {}
        if not isinstance(beneficios, dict):
            beneficios = {}
        out.append(
            {
                "id": int(r.get("id") or 0),
                "nombre": str(r.get("nombre") or "").strip(),
                "slug": str(r.get("slug") or "").strip(),
                "orden": int(r.get("orden") or 0),
                "puntos_minimos": int(r.get("puntos_minimos") or 0),
                "beneficios": beneficios,
                "descuento_pct": float(r.get("descuento_pct") or 0),
                "activo": bool(r.get("activo")),
            }
        )
    return out


def _normalizar_niveles_clientes(rows):
    niveles = []
    for idx, raw in enumerate(list(rows or []), start=1):
        item = dict(raw or {})
        nombre = str(item.get("nombre") or "").strip()[:60] or f"Nivel {idx}"
        slug = _slug_simple(item.get("slug") or nombre)
        nivel_id = int(item.get("id") or 0) if str(item.get("id") or "").strip() else 0
        beneficios = item.get("beneficios")
        if not isinstance(beneficios, dict):
            beneficios = {}
        b_items = beneficios.get("beneficios")
        if isinstance(b_items, list):
            beneficios["beneficios"] = [str(x).strip()[:120] for x in b_items if str(x).strip()][:15]
        else:
            beneficios["beneficios"] = []
        niveles.append(
            {
                "id": nivel_id,
                "nombre": nombre,
                "slug": slug,
                "orden": _clamp_int(item.get("orden"), default=idx, min_value=0, max_value=9999),
                "puntos_minimos": _clamp_int(item.get("puntos_minimos"), default=0, min_value=0, max_value=100000000),
                "beneficios_json": json.dumps(beneficios, ensure_ascii=False),
                "descuento_pct": max(0.0, min(100.0, float(item.get("descuento_pct") or 0))),
                "activo": 1 if bool(item.get("activo", True)) else 0,
            }
        )
    niveles.sort(key=lambda n: (int(n["orden"]), int(n["puntos_minimos"]), str(n["nombre"])))
    if not niveles:
        niveles = [
            {
                "id": 0,
                "nombre": "Bronce",
                "slug": "bronce",
                "orden": 1,
                "puntos_minimos": 0,
                "beneficios_json": json.dumps({"beneficios": ["Acceso base a promociones"]}, ensure_ascii=False),
                "descuento_pct": 0.0,
                "activo": 1,
            }
        ]
    return niveles


def _guardar_niveles_clientes(conn, niveles):
    rows = _normalizar_niveles_clientes(niveles)
    cursor = conn.cursor()
    cursor.execute("BEGIN IMMEDIATE")
    cursor.execute("DELETE FROM tienda_clientes_niveles")
    for row in rows:
        cursor.execute(
            """
            INSERT INTO tienda_clientes_niveles (id, nombre, slug, orden, puntos_minimos, beneficios_json, descuento_pct, activo, creado_en, actualizado_en)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (
                int(row["id"]) if int(row["id"]) > 0 else None,
                row["nombre"],
                row["slug"],
                int(row["orden"]),
                int(row["puntos_minimos"]),
                row["beneficios_json"],
                float(row["descuento_pct"]),
                int(row["activo"]),
            ),
        )
    conn.commit()


def _resolver_nivel_cliente(puntos_total, niveles):
    pts = int(max(0, int(puntos_total or 0)))
    activos = [n for n in (niveles or []) if bool(n.get("activo"))]
    if not activos:
        return None
    best = activos[0]
    for n in activos:
        if pts >= int(n.get("puntos_minimos") or 0):
            best = n
    return best


def _actualizar_nivel_cliente_cursor(cursor, cliente_id):
    cursor.execute("SELECT puntos_total FROM tienda_clientes WHERE id = ? LIMIT 1", (int(cliente_id),))
    row = cursor.fetchone()
    if not row:
        return None
    puntos_total = int(row["puntos_total"] or 0)
    niveles = _cargar_niveles_clientes(cursor.connection, solo_activos=True)
    nivel = _resolver_nivel_cliente(puntos_total, niveles)
    nivel_id = int(nivel.get("id") or 0) if nivel else None
    cursor.execute(
        """
        UPDATE tienda_clientes
        SET nivel_id = ?, actualizado_en = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (nivel_id, int(cliente_id)),
    )
    return nivel


def _obtener_cliente_por_contacto_cursor(cursor, email, telefono):
    cursor.execute(
        """
        SELECT c.*, n.nombre AS nivel_nombre, n.slug AS nivel_slug, n.puntos_minimos AS nivel_puntos_minimos,
               n.descuento_pct AS nivel_descuento_pct, n.beneficios_json AS nivel_beneficios_json
        FROM tienda_clientes c
        LEFT JOIN tienda_clientes_niveles n ON n.id = c.nivel_id
        WHERE c.email = ? AND c.telefono = ?
        LIMIT 1
        """,
        (str(email or "").strip().lower(), str(telefono or "").strip()),
    )
    row = cursor.fetchone()
    if not row:
        return None
    out = dict(row)
    try:
        beneficios = json.loads(str(out.get("nivel_beneficios_json") or "{}"))
    except Exception:
        beneficios = {}
    out["nivel"] = {
        "id": int(out.get("nivel_id") or 0) if out.get("nivel_id") is not None else None,
        "nombre": str(out.get("nivel_nombre") or "").strip(),
        "slug": str(out.get("nivel_slug") or "").strip(),
        "puntos_minimos": int(out.get("nivel_puntos_minimos") or 0),
        "descuento_pct": float(out.get("nivel_descuento_pct") or 0),
        "beneficios": beneficios if isinstance(beneficios, dict) else {},
    }
    return out


def _upsert_cliente_tienda_cursor(
    cursor,
    *,
    nombre,
    email,
    telefono,
    fecha_nacimiento="",
    email_confirmado=0,
    direccion_default="",
    direccion_lat=None,
    direccion_lng=None,
):
    em = str(email or "").strip().lower()
    te = str(telefono or "").strip()
    nom = str(nombre or "").strip()[:80] or _nombre_desde_email(em)
    fecha_nac = _normalizar_fecha_nacimiento(fecha_nacimiento)
    dir_txt = str(direccion_default or "").strip()[:240] or None
    lat = float(direccion_lat) if direccion_lat is not None and str(direccion_lat).strip() != "" else None
    lng = float(direccion_lng) if direccion_lng is not None and str(direccion_lng).strip() != "" else None
    cursor.execute(
        """
        INSERT INTO tienda_clientes (
            nombre, email, telefono, fecha_nacimiento, email_confirmado, direccion_default, direccion_lat, direccion_lng,
            activo, actualizado_en, ultimo_login
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(email, telefono) DO UPDATE SET
            nombre = excluded.nombre,
            fecha_nacimiento = COALESCE(NULLIF(excluded.fecha_nacimiento, ''), tienda_clientes.fecha_nacimiento),
            email_confirmado = CASE WHEN excluded.email_confirmado = 1 THEN 1 ELSE tienda_clientes.email_confirmado END,
            direccion_default = COALESCE(excluded.direccion_default, tienda_clientes.direccion_default),
            direccion_lat = COALESCE(excluded.direccion_lat, tienda_clientes.direccion_lat),
            direccion_lng = COALESCE(excluded.direccion_lng, tienda_clientes.direccion_lng),
            activo = 1,
            actualizado_en = CURRENT_TIMESTAMP,
            ultimo_login = CURRENT_TIMESTAMP
        """,
        (nom, em, te, (fecha_nac or None), 1 if email_confirmado else 0, dir_txt, lat, lng),
    )
    cli = _obtener_cliente_por_contacto_cursor(cursor, em, te)
    if cli and (not cli.get("nivel_id")):
        _actualizar_nivel_cliente_cursor(cursor, int(cli["id"]))
        cli = _obtener_cliente_por_contacto_cursor(cursor, em, te)
    return cli


def _registrar_puntos_cliente_cursor(cursor, *, cliente_id, puntos, tipo, origen_tipo, origen_id, detalle):
    pts = int(puntos or 0)
    if pts == 0:
        return False
    cid = int(cliente_id or 0)
    if cid <= 0:
        return False
    o_tipo = str(origen_tipo or "").strip()[:40] or "manual"
    o_id = int(origen_id or 0)
    try:
        cursor.execute(
            """
            INSERT INTO tienda_clientes_puntos_mov (cliente_id, tipo, origen_tipo, origen_id, puntos, detalle, creado_en)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (cid, str(tipo or "ajuste").strip()[:30], o_tipo, o_id, pts, str(detalle or "").strip()[:220]),
        )
    except sqlite3.IntegrityError:
        return False
    cursor.execute(
        """
        UPDATE tienda_clientes
        SET puntos_actual = CASE WHEN COALESCE(puntos_actual, 0) + ? < 0 THEN 0 ELSE COALESCE(puntos_actual, 0) + ? END,
            puntos_total = CASE WHEN ? > 0 THEN COALESCE(puntos_total, 0) + ? ELSE COALESCE(puntos_total, 0) END,
            actualizado_en = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (pts, pts, pts, pts, cid),
    )
    _actualizar_nivel_cliente_cursor(cursor, cid)
    return True


def _puntos_compra(total_monto, cfg_programa):
    cfg = _normalizar_config_programa_clientes(cfg_programa)
    if not cfg.get("enabled"):
        return 0
    base = int(cfg.get("purchase_amount_base") or 0)
    puntos_por_base = int(cfg.get("purchase_points") or 0)
    if base <= 0 or puntos_por_base <= 0:
        return 0
    monto = max(0.0, float(total_monto or 0))
    bloques = int(monto // float(base))
    return max(0, bloques * puntos_por_base)


def _puntos_agenda(cfg_programa):
    cfg = _normalizar_config_programa_clientes(cfg_programa)
    if not cfg.get("enabled"):
        return 0
    return max(0, int(cfg.get("agenda_points") or 0))


def _normalizar_pedido_estado(raw):
    v = str(raw or "").strip().lower()
    if v in {"recibido", "confirmado", "preparando", "listo", "entregado", "cancelado"}:
        return v
    return "recibido"


def _pedido_estado_label(estado):
    est = _normalizar_pedido_estado(estado)
    labels = {
        "recibido": "Recibido",
        "confirmado": "Confirmado",
        "preparando": "En preparacion",
        "listo": "Listo para entregar",
        "entregado": "Entregado",
        "cancelado": "Cancelado",
    }
    return labels.get(est, "Recibido")


def _pedido_pago_flow_pendiente(metodo_pago_raw):
    metodo = str(metodo_pago_raw or "").strip().lower()
    return metodo in {"flow_pendiente", "flow_pending", "flow"}


PEDIDO_TIMER_OPCIONES_MIN = (10, 15, 25, 30, 45)


def _normalizar_pedido_timer_minutos(raw):
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None
    return value if value in PEDIDO_TIMER_OPCIONES_MIN else None


def _pedido_timer_label(minutos):
    mins = _normalizar_pedido_timer_minutos(minutos)
    if mins is None:
        return ""
    if mins >= 45:
        return "+30 min"
    return f"{mins} min"


def _pedido_timer_restante_segundos(estado_raw, minutos_raw, inicio_raw):
    estado = _normalizar_pedido_estado(estado_raw)
    if estado not in {"confirmado", "preparando"}:
        return None
    mins = _normalizar_pedido_timer_minutos(minutos_raw)
    if mins is None:
        return None
    inicio_dt = _parse_sqlite_datetime_safe(inicio_raw)
    if not inicio_dt:
        return None
    limite = inicio_dt + timedelta(minutes=mins)
    restante = int((limite - datetime.now()).total_seconds())
    return max(0, restante)


def _pedido_timer_payload(estado_raw, minutos_raw, inicio_raw):
    mins = _normalizar_pedido_timer_minutos(minutos_raw)
    restante = _pedido_timer_restante_segundos(estado_raw, mins, inicio_raw)
    return {
        "timer_minutos": mins,
        "timer_label": _pedido_timer_label(mins),
        "timer_inicio": str(inicio_raw or "").strip(),
        "timer_restante_segundos": restante,
        "timer_activo": restante is not None,
        "timer_opciones": [
            {"value": int(v), "label": _pedido_timer_label(v), "warning": bool(int(v) >= 45)}
            for v in PEDIDO_TIMER_OPCIONES_MIN
        ],
    }


def _chat_origen_tipo(raw):
    v = str(raw or "").strip().lower()
    if v in {"venta", "agenda"}:
        return v
    return "venta"


def _parse_sqlite_datetime_safe(raw):
    txt = str(raw or "").strip()
    if not txt:
        return None
    try:
        return datetime.fromisoformat(txt.replace("Z", "").replace(" ", "T"))
    except Exception:
        return None


def _chat_cierre_restante_segundos(origen_tipo, estado_raw, estado_actualizado_raw=None):
    estado = str(estado_raw or "").strip().lower()
    if str(origen_tipo or "").strip().lower() != "venta":
        return None
    if estado != "entregado":
        return None
    dt = _parse_sqlite_datetime_safe(estado_actualizado_raw)
    if not dt:
        return 0
    limite = dt + timedelta(minutes=15)
    restante = int((limite - datetime.now()).total_seconds())
    return max(0, restante)


def _chat_estado_activo(origen_tipo, estado_raw, estado_actualizado_raw=None):
    estado = str(estado_raw or "").strip().lower()
    if origen_tipo == "agenda":
        return estado in {"pendiente", "confirmado", "preparando"}
    if estado in {"recibido", "confirmado", "preparando", "listo"}:
        return True
    if estado == "entregado":
        return (_chat_cierre_restante_segundos(origen_tipo, estado, estado_actualizado_raw) or 0) > 0
    return False


def _chat_info_origen_cursor(cursor, origen_tipo, origen_id):
    origen = _chat_origen_tipo(origen_tipo)
    oid = int(origen_id or 0)
    if oid <= 0:
        return None
    if origen == "agenda":
        cursor.execute(
            """
            SELECT id, estado, cliente_email, cliente_telefono, cliente
            FROM agenda_eventos
            WHERE id = ?
            LIMIT 1
            """,
            (oid,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        data = dict(row)
        estado = str(data.get("estado") or "").strip().lower() or "pendiente"
        return {
            "origen_tipo": "agenda",
            "origen_id": oid,
            "estado": estado,
            "chat_activo": _chat_estado_activo("agenda", estado),
            "chat_cierre_restante_segundos": _chat_cierre_restante_segundos("agenda", estado, None),
            "cliente_email": str(data.get("cliente_email") or "").strip().lower(),
            "cliente_telefono": _normalizar_telefono_cl(data.get("cliente_telefono")),
            "cliente_nombre": str(data.get("cliente") or "").strip(),
        }
    cursor.execute(
        """
        SELECT id,
               COALESCE(NULLIF(TRIM(pedido_estado), ''), 'recibido') AS estado,
               pedido_estado_actualizado,
               cliente_email, cliente_telefono, cliente_nombre
        FROM ventas
        WHERE id = ? AND canal_venta IN ('tienda_online', 'tienda_online_flow_pendiente')
        LIMIT 1
        """,
        (oid,),
    )
    row = cursor.fetchone()
    if not row:
        return None
    data = dict(row)
    estado = _normalizar_pedido_estado(data.get("estado"))
    return {
        "origen_tipo": "venta",
        "origen_id": oid,
        "estado": estado,
        "chat_activo": _chat_estado_activo("venta", estado, data.get("pedido_estado_actualizado")),
        "chat_cierre_restante_segundos": _chat_cierre_restante_segundos("venta", estado, data.get("pedido_estado_actualizado")),
        "cliente_email": str(data.get("cliente_email") or "").strip().lower(),
        "cliente_telefono": _normalizar_telefono_cl(data.get("cliente_telefono")),
        "cliente_nombre": str(data.get("cliente_nombre") or "").strip(),
    }


def _chat_cliente_autorizado(info_origen, email, telefono):
    if not info_origen:
        return False
    email_in = str(email or "").strip().lower()
    tel_in = _normalizar_telefono_cl(telefono)
    return bool(email_in) and bool(tel_in) and email_in == str(info_origen.get("cliente_email") or "").strip().lower() and tel_in == _normalizar_telefono_cl(info_origen.get("cliente_telefono"))


_TRANSFER_PROOF_PREFIX = "/static/tienda_comprobantes_transferencia/"
_RE_TRANSFER_PROOF_URL = re.compile(r"/static/tienda_comprobantes_transferencia/([A-Za-z0-9._-]+)")


def _transfer_proof_abs_path(filename):
    safe = secure_filename(os.path.basename(str(filename or "").strip()))
    if not safe:
        return ""
    return os.path.join(static_dir, "tienda_comprobantes_transferencia", safe)


def _purge_transfer_proofs_for_venta_cursor(cursor, venta_id):
    try:
        oid = int(venta_id or 0)
    except (TypeError, ValueError):
        return 0
    if oid <= 0:
        return 0
    cursor.execute(
        """
        SELECT id, mensaje
        FROM tienda_pedido_chat
        WHERE origen_tipo = 'venta' AND origen_id = ?
        ORDER BY id ASC
        """,
        (oid,),
    )
    rows = cursor.fetchall() or []
    deleted = 0
    for row in rows:
        msg_id = int(row["id"] or 0)
        raw = str(row["mensaje"] or "")
        found = _RE_TRANSFER_PROOF_URL.findall(raw)
        if not found:
            continue
        for fname in found:
            abs_path = _transfer_proof_abs_path(fname)
            if abs_path and os.path.isfile(abs_path):
                try:
                    os.remove(abs_path)
                    deleted += 1
                except Exception:
                    pass
        cleaned = _RE_TRANSFER_PROOF_URL.sub("[Comprobante eliminado]", raw)
        if cleaned != raw and msg_id > 0:
            cursor.execute(
                "UPDATE tienda_pedido_chat SET mensaje = ? WHERE id = ?",
                (cleaned, msg_id),
            )
    return deleted


def _clamp_int(value, default=0, min_value=0, max_value=9999):
    try:
        out = int(value)
    except (TypeError, ValueError):
        out = int(default)
    if out < min_value:
        out = min_value
    if out > max_value:
        out = max_value
    return out


def _hora_hhmm_o_default(raw, default):
    hora = str(raw or "").strip()
    if _parse_hora_hhmm(hora):
        return hora
    return default


def _hhmm_a_minutos(hora):
    parsed = _parse_hora_hhmm(hora)
    if not parsed:
        return None
    hh, mm = parsed
    return (int(hh) * 60) + int(mm)


def _minutos_a_hhmm(total_minutos):
    total = int(total_minutos or 0)
    h = max(0, min(23, total // 60))
    m = max(0, min(59, total % 60))
    return f"{h:02d}:{m:02d}"


def _obtener_cfg_agenda_tienda(cfg_tienda=None):
    cfg = dict(cfg_tienda or _obtener_tienda_personalizacion() or {})
    enabled = bool(cfg.get("agenda_enabled", True))
    days_ahead = _clamp_int(cfg.get("agenda_days_ahead"), default=14, min_value=3, max_value=60)
    slot_minutes = _clamp_int(cfg.get("agenda_slot_minutes"), default=60, min_value=30, max_value=120)
    if slot_minutes not in {30, 60, 90, 120}:
        slot_minutes = 60
    slot_capacity = _clamp_int(cfg.get("agenda_slot_capacity"), default=1, min_value=1, max_value=20)
    day_close_min_orders = _clamp_int(cfg.get("agenda_day_close_min_orders"), default=3, min_value=1, max_value=20)
    buffer_minutes = _clamp_int(cfg.get("agenda_event_buffer_minutes"), default=120, min_value=0, max_value=360)
    start_h = _hora_hhmm_o_default(cfg.get("agenda_hour_start"), "09:00")
    end_h = _hora_hhmm_o_default(cfg.get("agenda_hour_end"), "19:00")
    start_m = _hhmm_a_minutos(start_h) or 9 * 60
    end_m = _hhmm_a_minutos(end_h) or 19 * 60
    min_end = start_m + slot_minutes
    if end_m <= start_m:
        end_m = min(24 * 60, min_end)
    if end_m < min_end:
        end_m = min(24 * 60, min_end)
    return {
        "enabled": enabled,
        "days_ahead": days_ahead,
        "slot_minutes": slot_minutes,
        "slot_capacity": slot_capacity,
        "day_close_min_orders": day_close_min_orders,
        "event_buffer_minutes": buffer_minutes,
        "hour_start": _minutos_a_hhmm(start_m),
        "hour_end": _minutos_a_hhmm(end_m),
        "start_minutes": start_m,
        "end_minutes": end_m,
    }


_MAIPU_POLIGONO = [
    (-33.5660, -70.8970),
    (-33.5420, -70.8670),
    (-33.5100, -70.8490),
    (-33.4850, -70.8390),
    (-33.4620, -70.8140),
    (-33.4520, -70.7770),
    (-33.4600, -70.7380),
    (-33.4870, -70.7020),
    (-33.5220, -70.6780),
    (-33.5570, -70.6870),
    (-33.5790, -70.7230),
    (-33.5860, -70.7770),
    (-33.5760, -70.8310),
]


def _haversine_km(lat1, lon1, lat2, lon2):
    try:
        lat1f = float(lat1)
        lon1f = float(lon1)
        lat2f = float(lat2)
        lon2f = float(lon2)
    except (TypeError, ValueError):
        return 0.0
    r = 6371.0
    p1 = math.radians(lat1f)
    p2 = math.radians(lat2f)
    dp = math.radians(lat2f - lat1f)
    dl = math.radians(lon2f - lon1f)
    a = (math.sin(dp / 2) ** 2) + (math.cos(p1) * math.cos(p2) * (math.sin(dl / 2) ** 2))
    c = 2 * math.atan2(math.sqrt(max(0.0, a)), math.sqrt(max(0.0, 1 - a)))
    return max(0.0, r * c)


def _punto_en_poligono(lat, lon, polygon):
    try:
        y = float(lat)
        x = float(lon)
    except (TypeError, ValueError):
        return False
    if not polygon or len(polygon) < 3:
        return False
    inside = False
    j = len(polygon) - 1
    for i in range(len(polygon)):
        yi, xi = polygon[i]
        yj, xj = polygon[j]
        intersect = ((xi > x) != (xj > x)) and (
            y < ((yj - yi) * (x - xi) / ((xj - xi) if (xj - xi) else 1e-12)) + yi
        )
        if intersect:
            inside = not inside
        j = i
    return inside


def _obtener_cfg_envios_tienda(cfg_tienda=None):
    cfg = dict(cfg_tienda or _obtener_tienda_personalizacion() or {})
    def _num(v, d=0, mn=0, mx=999999):
        try:
            out = float(v)
        except (TypeError, ValueError):
            out = float(d)
        return max(float(mn), min(float(mx), out))

    # Punto base interno de calculo: Rubidio 1815, Maipu, Santiago.
    base_lat = -33.5191105
    base_lng = -70.7849094
    # Coordenada historica previa (menos precisa) para autoajuste.
    legacy_lat = -33.510910
    legacy_lng = -70.784909
    o_lat = _num(cfg.get("agenda_delivery_origin_lat"), base_lat, -90, 90)
    o_lng = _num(cfg.get("agenda_delivery_origin_lng"), base_lng, -180, 180)
    # Si aun esta el valor legacy, migrar en caliente al punto exacto.
    if abs(o_lat - legacy_lat) < 0.00001 and abs(o_lng - legacy_lng) < 0.00001:
        o_lat, o_lng = base_lat, base_lng
    f_0_3 = int(round(_num(cfg.get("agenda_delivery_fee_0_3"), 2500, 0, 300000)))
    f_3_6 = int(round(_num(cfg.get("agenda_delivery_fee_3_6"), 3500, 0, 300000)))
    f_6_9 = int(round(_num(cfg.get("agenda_delivery_fee_6_9"), 4500, 0, 300000)))
    f_9p = int(round(_num(cfg.get("agenda_delivery_fee_9_plus"), 5500, 0, 300000)))
    outside_msg = str(cfg.get("agenda_delivery_outside_warning") or "").strip()[:260]
    if not outside_msg:
        outside_msg = "Direccion fuera de Maipu: el valor de despacho no se puede mostrar. Confirma el PIN y realiza la reserva de horario y te contactaremos para cotizarlo internamente."
    included_msg = str(cfg.get("agenda_delivery_note_text") or "").strip()[:220]
    if not included_msg:
        included_msg = "El valor de despacho se calcula automaticamente para direcciones dentro de Maipu."
    bands = []
    for idx in (1, 2, 3):
        start = str(cfg.get(f"agenda_delivery_band_{idx}_start") or "").strip()
        end = str(cfg.get(f"agenda_delivery_band_{idx}_end") or "").strip()
        if not (_parse_hora_hhmm(start) and _parse_hora_hhmm(end)):
            continue
        try:
            extra = int(round(_num(cfg.get(f"agenda_delivery_band_{idx}_extra"), 0, 0, 300000)))
        except Exception:
            extra = 0
        bands.append(
            {
                "start": start,
                "end": end,
                "extra": max(0, int(extra)),
                "label": f"{start} a {end}",
            }
        )
    return {
        "origin_lat": o_lat,
        "origin_lng": o_lng,
        "fee_0_3": f_0_3,
        "fee_3_6": f_3_6,
        "fee_6_9": f_6_9,
        "fee_9_plus": f_9p,
        "outside_warning": outside_msg,
        "note_text": included_msg,
        "commune_name": "Maipu",
        "time_bands": bands,
        "zones": _normalizar_agenda_delivery_zones(cfg.get("agenda_delivery_zones")),
    }


def _normalizar_agenda_delivery_zones(raw):
    zonas = []
    data = raw if isinstance(raw, list) else []
    for idx, z in enumerate(data[:120]):
        if not isinstance(z, dict):
            continue
        nombre = str(z.get("name") or z.get("nombre") or f"Sector {idx + 1}").strip()[:90]
        if not nombre:
            nombre = f"Sector {idx + 1}"
        zid = str(z.get("id") or f"zone-{idx + 1}").strip().lower()[:60]
        try:
            tarifa = int(float(z.get("fee") or z.get("tarifa") or 0))
        except (TypeError, ValueError):
            tarifa = 0
        tarifa = max(0, min(300000, tarifa))
        try:
            prioridad = int(float(z.get("priority") or z.get("prioridad") or 0))
        except (TypeError, ValueError):
            prioridad = 0
        prioridad = max(-999, min(999, prioridad))
        color = str(z.get("color") or "#2563eb").strip()
        if not re.match(r"^#([0-9a-fA-F]{6})$", color):
            color = "#2563eb"
        activo = bool(z.get("active", True))
        poly_raw = z.get("polygon") if isinstance(z.get("polygon"), list) else []
        poly = []
        for p in poly_raw[:200]:
            lat = lng = None
            if isinstance(p, (list, tuple)) and len(p) >= 2:
                lat, lng = p[0], p[1]
            elif isinstance(p, dict):
                lat = p.get("lat")
                lng = p.get("lng")
            try:
                latf = float(lat)
                lngf = float(lng)
            except (TypeError, ValueError):
                continue
            if -90 <= latf <= 90 and -180 <= lngf <= 180:
                poly.append([latf, lngf])
        if len(poly) < 3:
            continue
        zonas.append(
            {
                "id": zid,
                "name": nombre,
                "fee": tarifa,
                "priority": prioridad,
                "color": color,
                "active": activo,
                "polygon": poly,
            }
        )
    return zonas


def _buscar_sector_envio(lat, lng, zones):
    activos = [z for z in (zones or []) if bool(z.get("active", True))]
    if not activos:
        return None
    activos.sort(key=lambda z: (int(z.get("priority") or 0), -int(z.get("fee") or 0)), reverse=True)
    for z in activos:
        poly = z.get("polygon") if isinstance(z.get("polygon"), list) else []
        if len(poly) < 3:
            continue
        if _punto_en_poligono(lat, lng, poly):
            return z
    return None


def _extra_horario_envio(cfg_envios, hora_inicio=None):
    hora_txt = str(hora_inicio or "").strip()
    minutos = _hhmm_a_minutos(hora_txt)
    if minutos is None:
        return {"extra": 0, "label": ""}
    for b in (cfg_envios.get("time_bands") or []):
        ini = _hhmm_a_minutos(b.get("start"))
        fin = _hhmm_a_minutos(b.get("end"))
        if ini is None or fin is None:
            continue
        if ini <= fin:
            active = ini <= minutos <= fin
        else:
            active = minutos >= ini or minutos <= fin
        if active:
            return {"extra": int(b.get("extra") or 0), "label": str(b.get("label") or "").strip()}
    return {"extra": 0, "label": ""}


def _cotizar_envio_agenda(lat, lng, cfg_tienda=None, hora_inicio=None):
    cfg = _obtener_cfg_envios_tienda(cfg_tienda)
    max_km_cotizacion = 6.0
    warning_fuera = "Direccion fuera de Maipu: el valor de despacho no se puede mostrar. Confirma el PIN y realiza la reserva de horario y te contactaremos para cotizarlo internamente."
    inside = _punto_en_poligono(lat, lng, _MAIPU_POLIGONO)
    distance_km = round(_haversine_km(cfg["origin_lat"], cfg["origin_lng"], lat, lng), 2)
    extra_horario = _extra_horario_envio(cfg, hora_inicio=hora_inicio)
    quote = {
        "inside_maipu": bool(inside),
        "distance_km": float(distance_km),
        "range_label": "",
        "shipping_fee": None,
        "shipping_fee_base": None,
        "shipping_fee_extra": int(extra_horario.get("extra") or 0),
        "time_band_label": str(extra_horario.get("label") or ""),
        "hora_inicio": str(hora_inicio or ""),
        "visible_to_client": False,
        "warning": "",
        "note": str(cfg["note_text"]),
        "commune_name": str(cfg["commune_name"]),
    }
    if not inside:
        quote["inside_maipu"] = False
        quote["warning"] = warning_fuera
        return quote
    if distance_km > max_km_cotizacion:
        quote["inside_maipu"] = False
        quote["warning"] = warning_fuera
        return quote

    fee = None
    range_label = ""
    quote["zone_name"] = ""
    quote["zone_id"] = ""
    zones = cfg.get("zones") or []
    if zones:
        sector = _buscar_sector_envio(lat, lng, zones)
        if not sector:
            quote["inside_maipu"] = False
            quote["warning"] = warning_fuera
            return quote
        fee = int(sector.get("fee") or 0)
        quote["zone_name"] = str(sector.get("name") or "")
        quote["zone_id"] = str(sector.get("id") or "")
        range_label = f"Sector: {quote['zone_name']}"
    elif distance_km <= 3:
        fee = int(cfg["fee_0_3"])
        range_label = "0 a 3 km"
    elif distance_km <= 6:
        fee = int(cfg["fee_3_6"])
        range_label = "3 a 6 km"
    else:
        quote["inside_maipu"] = False
        quote["warning"] = warning_fuera
        return quote

    fee_base = int(max(0, fee))
    fee_extra = int(max(0, extra_horario.get("extra") or 0))
    quote["range_label"] = range_label
    quote["shipping_fee_base"] = fee_base
    quote["shipping_fee"] = int(fee_base + fee_extra)
    quote["visible_to_client"] = True
    return quote


def _cotizar_envio_checkout_tienda(lat, lng, cfg_tienda=None, hora_inicio=None):
    quote = dict(_cotizar_envio_agenda(lat, lng, cfg_tienda=cfg_tienda, hora_inicio=hora_inicio) or {})
    recargo_pct = 30.0
    quote["service_provider"] = "uber_entregas"
    quote["service_markup_pct"] = recargo_pct
    quote["shipping_fee_store_base"] = quote.get("shipping_fee")
    quote["shipping_fee_markup"] = 0
    if bool(quote.get("inside_maipu")) and bool(quote.get("visible_to_client")) and quote.get("shipping_fee") is not None:
        base = float(quote.get("shipping_fee") or 0)
        markup = int(round(base * (recargo_pct / 100.0)))
        final_fee = int(round(base + markup))
        quote["shipping_fee_markup"] = int(max(0, markup))
        quote["shipping_fee"] = int(max(0, final_fee))
        quote["visible_to_client"] = True
    return quote


def _flow_cfg():
    api_key = str(os.environ.get("FLOW_API_KEY") or "").strip()
    secret_key = str(os.environ.get("FLOW_SECRET_KEY") or "").strip()
    api_url = str(os.environ.get("FLOW_API_URL") or "https://www.flow.cl/api").strip().rstrip("/")
    enabled = bool(api_key and secret_key)
    return {
        "enabled": enabled,
        "api_key": api_key,
        "secret_key": secret_key,
        "api_url": api_url,
    }


def _flow_fee_cfg():
    try:
        rate_pct = float(os.environ.get("GESTIONSTOCK_FLOW_FEE_RATE_PCT") or 3.19)
    except Exception:
        rate_pct = 3.19
    try:
        iva_pct = float(os.environ.get("GESTIONSTOCK_FLOW_FEE_IVA_PCT") or 19.0)
    except Exception:
        iva_pct = 19.0
    try:
        fixed_clp = float(os.environ.get("GESTIONSTOCK_FLOW_FEE_FIXED_CLP") or 0)
    except Exception:
        fixed_clp = 0
    rate = max(0.0, rate_pct / 100.0)
    iva = max(0.0, iva_pct / 100.0)
    fixed = max(0.0, fixed_clp)
    return {"rate": rate, "iva": iva, "fixed": fixed}


def _flow_gross_from_net(net_amount, fee_cfg=None, apply_fixed=True):
    fee = fee_cfg or _flow_fee_cfg()
    net = max(0.0, float(net_amount or 0))
    if net <= 0:
        return 0.0
    rate = max(0.0, float(fee.get("rate") or 0))
    iva = max(0.0, float(fee.get("iva") or 0))
    fixed = max(0.0, float(fee.get("fixed") or 0)) if apply_fixed else 0.0
    factor = 1.0 - (rate * (1.0 + iva))
    if factor <= 0:
        return float(net)
    gross = (net + fixed) / factor
    return float(max(0.0, math.ceil(gross)))


def _flow_sign(params, secret_key):
    keys = sorted([k for k in params.keys() if k != "s"])
    to_sign = "".join([f"{k}{params[k]}" for k in keys])
    return hmac.new(str(secret_key).encode("utf-8"), to_sign.encode("utf-8"), hashlib.sha256).hexdigest()


def _flow_post(endpoint, params, cfg):
    payload = dict(params or {})
    payload["apiKey"] = str(cfg.get("api_key") or "")
    payload["s"] = _flow_sign(payload, cfg.get("secret_key") or "")
    body = urlencode(payload).encode("utf-8")
    req = UrlRequest(
        f"{str(cfg.get('api_url') or '').rstrip('/')}/{str(endpoint).lstrip('/')}",
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        raw = urlopen(req, timeout=25).read().decode("utf-8", errors="replace")
    except HTTPError as http_err:
        raw_err = ""
        try:
            raw_err = http_err.read().decode("utf-8", errors="replace")
        except Exception:
            raw_err = ""
        try:
            data_err = json.loads(raw_err or "{}")
        except Exception:
            data_err = {}
        msg = ""
        if isinstance(data_err, dict):
            msg = str(data_err.get("message") or data_err.get("error") or "").strip()
        if not msg:
            msg = f"Flow HTTP {int(getattr(http_err, 'code', 400) or 400)}"
        raise RuntimeError(msg)
    data = json.loads(raw or "{}")
    if isinstance(data, dict) and data.get("code"):
        raise RuntimeError(str(data.get("message") or f"Flow error {data.get('code')}"))
    return data


def _flow_subject_safe(txt):
    base = str(txt or "").strip() or "Compra tienda Sucree"
    try:
        base = unicodedata.normalize("NFKD", base).encode("ascii", "ignore").decode("ascii")
    except Exception:
        pass
    base = " ".join(base.split())
    return base[:78]


def _ensure_flow_pago_table(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS tienda_flow_pagos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            venta_id INTEGER UNIQUE NOT NULL,
            commerce_order TEXT UNIQUE NOT NULL,
            flow_token TEXT UNIQUE,
            flow_order TEXT,
            estado TEXT DEFAULT 'pendiente',
            amount REAL DEFAULT 0,
            payment_data_json TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute("PRAGMA table_info(tienda_flow_pagos)")
    cols = {str(r["name"]).strip().lower() for r in (cursor.fetchall() or [])}
    if "flow_redirect_url" not in cols:
        cursor.execute("ALTER TABLE tienda_flow_pagos ADD COLUMN flow_redirect_url TEXT")
    if "checkout_backup_json" not in cols:
        cursor.execute("ALTER TABLE tienda_flow_pagos ADD COLUMN checkout_backup_json TEXT")
    if "notified_admin" not in cols:
        cursor.execute("ALTER TABLE tienda_flow_pagos ADD COLUMN notified_admin INTEGER DEFAULT 0")
    if "notified_at" not in cols:
        cursor.execute("ALTER TABLE tienda_flow_pagos ADD COLUMN notified_at TEXT")
    if "confirm_attempts" not in cols:
        cursor.execute("ALTER TABLE tienda_flow_pagos ADD COLUMN confirm_attempts INTEGER DEFAULT 0")
    if "last_error" not in cols:
        cursor.execute("ALTER TABLE tienda_flow_pagos ADD COLUMN last_error TEXT")


def _ensure_ventas_metodo_pago_column():
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(ventas)")
        cols = {str(r["name"]).strip().lower() for r in (cur.fetchall() or [])}
        if "metodo_pago" in cols:
            return
        cur.execute("ALTER TABLE ventas ADD COLUMN metodo_pago TEXT DEFAULT 'efectivo'")
        conn.commit()
    except sqlite3.OperationalError as e:
        # Compatibilidad ante condiciones de carrera o esquemas ya migrados.
        if "duplicate column name" not in str(e or "").lower():
            raise
    finally:
        if conn:
            conn.close()


def _ensure_ventas_flow_admin_alert_column():
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(ventas)")
        cols = {str(r["name"]).strip().lower() for r in (cur.fetchall() or [])}
        if "flow_admin_alertado" in cols:
            return
        # Default 1 para evitar alertar pedidos historicos ya existentes.
        cur.execute("ALTER TABLE ventas ADD COLUMN flow_admin_alertado INTEGER DEFAULT 1")
        conn.commit()
    except sqlite3.OperationalError as e:
        if "duplicate column name" not in str(e or "").lower():
            raise
    finally:
        if conn:
            conn.close()


def _ensure_ventas_flow_return_column():
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(ventas)")
        cols = {str(r["name"]).strip().lower() for r in (cur.fetchall() or [])}
        if "flow_cliente_regreso" in cols:
            return
        cur.execute("ALTER TABLE ventas ADD COLUMN flow_cliente_regreso INTEGER DEFAULT 1")
        conn.commit()
    except sqlite3.OperationalError as e:
        if "duplicate column name" not in str(e or "").lower():
            raise
    finally:
        if conn:
            conn.close()


def _normalizar_metodo_pago_flow_por_canal(cursor):
    try:
        cursor.execute(
            """
            UPDATE ventas
            SET metodo_pago = 'flow_pendiente'
            WHERE canal_venta = 'tienda_online_flow_pendiente'
              AND LOWER(TRIM(COALESCE(metodo_pago, ''))) NOT IN ('flow_pendiente', 'flow_pending', 'flow_pagado')
            """
        )
    except Exception:
        pass


def _guardar_flow_pago(venta_id, commerce_order, flow_token, amount, flow_redirect_url=None, checkout_backup=None):
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()
        _ensure_flow_pago_table(cur)
        cur.execute(
            """
            INSERT INTO tienda_flow_pagos (venta_id, commerce_order, flow_token, amount, flow_redirect_url, checkout_backup_json, estado, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, 'pendiente', CURRENT_TIMESTAMP)
            ON CONFLICT(venta_id) DO UPDATE SET
                commerce_order = excluded.commerce_order,
                flow_token = excluded.flow_token,
                amount = excluded.amount,
                flow_redirect_url = COALESCE(excluded.flow_redirect_url, tienda_flow_pagos.flow_redirect_url),
                checkout_backup_json = COALESCE(excluded.checkout_backup_json, tienda_flow_pagos.checkout_backup_json),
                estado = 'pendiente',
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                int(venta_id),
                str(commerce_order or "").strip(),
                str(flow_token or "").strip(),
                float(amount or 0),
                (str(flow_redirect_url or "").strip() or None),
                (json.dumps(checkout_backup, ensure_ascii=False) if isinstance(checkout_backup, dict) else None),
            ),
        )
        conn.commit()
    finally:
        if conn:
            conn.close()


def _obtener_flow_pago_por_token(token):
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()
        _ensure_flow_pago_table(cur)
        cur.execute(
            "SELECT * FROM tienda_flow_pagos WHERE flow_token = ? LIMIT 1",
            (str(token or "").strip(),),
        )
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        if conn:
            conn.close()


def _actualizar_flow_pago(venta_id, estado, flow_order=None, payment_data=None):
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()
        _ensure_flow_pago_table(cur)
        cur.execute(
            """
            UPDATE tienda_flow_pagos
            SET estado = ?,
                flow_order = COALESCE(?, flow_order),
                payment_data_json = COALESCE(?, payment_data_json),
                confirm_attempts = COALESCE(confirm_attempts, 0) + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE venta_id = ?
            """,
            (
                str(estado or "pendiente").strip().lower(),
                (str(flow_order).strip() if flow_order is not None else None),
                (json.dumps(payment_data, ensure_ascii=False) if isinstance(payment_data, dict) else None),
                int(venta_id),
            ),
        )
        conn.commit()
    finally:
        if conn:
            conn.close()


def _set_flow_error(venta_id, error_msg):
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()
        _ensure_flow_pago_table(cur)
        cur.execute(
            """
            UPDATE tienda_flow_pagos
            SET confirm_attempts = COALESCE(confirm_attempts, 0) + 1,
                last_error = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE venta_id = ?
            """,
            (str(error_msg or "")[:500], int(venta_id)),
        )
        conn.commit()
    finally:
        if conn:
            conn.close()


def _finalizar_venta_flow_pagada(venta_id, status_payload=None):
    venta_id = int(venta_id or 0)
    if venta_id <= 0:
        return
    conn = None
    notify_payload = None
    try:
        _ensure_ventas_metodo_pago_column()
        _ensure_ventas_flow_admin_alert_column()
        conn = get_db()
        cur = conn.cursor()
        _ensure_flow_pago_table(cur)
        cur.execute(
            """
            UPDATE ventas
            SET metodo_pago = 'flow_pagado',
                canal_venta = 'tienda_online',
                flow_admin_alertado = 0
            WHERE id = ?
            """,
            (venta_id,),
        )
        cur.execute(
            """
            UPDATE tienda_flow_pagos
            SET estado = 'pagado',
                payment_data_json = COALESCE(?, payment_data_json),
                updated_at = CURRENT_TIMESTAMP
            WHERE venta_id = ?
            """,
            (
                (json.dumps(status_payload, ensure_ascii=False) if isinstance(status_payload, dict) else None),
                venta_id,
            ),
        )
        cur.execute("SELECT notified_admin FROM tienda_flow_pagos WHERE venta_id = ? LIMIT 1", (venta_id,))
        row_n = cur.fetchone()
        notified_admin = int((dict(row_n).get("notified_admin") if row_n else 0) or 0)
        if notified_admin == 0:
            cur.execute(
                """
                SELECT id, total_monto, cliente_nombre, cliente_email, cliente_telefono,
                       COALESCE(NULLIF(TRIM(entrega_tipo), ''), 'retiro') AS entrega_tipo,
                       COALESCE(NULLIF(TRIM(hora_retiro), ''), '') AS hora_retiro,
                       COALESCE(NULLIF(TRIM(direccion_entrega), ''), '') AS direccion_entrega,
                       COALESCE(despacho_monto, 0) AS despacho_monto,
                       COALESCE(descuento_monto, 0) AS descuento_monto
                FROM ventas
                WHERE id = ?
                LIMIT 1
                """,
                (venta_id,),
            )
            row_v = cur.fetchone()
            if row_v:
                venta = dict(row_v)
                cur.execute("PRAGMA table_info(venta_items)")
                vi_cols = {str(r["name"]).strip().lower() for r in (cur.fetchall() or [])}
                select_precio = "precio_unitario" if "precio_unitario" in vi_cols else "0 AS precio_unitario"
                select_subtotal = "subtotal" if "subtotal" in vi_cols else "0 AS subtotal"
                cur.execute(
                    f"""
                    SELECT producto_id, producto_nombre, cantidad, {select_precio}, {select_subtotal}
                    FROM venta_items
                    WHERE venta_id = ?
                    ORDER BY id ASC
                    """,
                    (venta_id,),
                )
                items = []
                subtotal = 0.0
                for rr in (cur.fetchall() or []):
                    d = dict(rr)
                    qty = int(d.get("cantidad") or 0)
                    pu = float(d.get("precio_unitario") or 0)
                    sub = float(d.get("subtotal") or 0)
                    if sub <= 0 and qty > 0:
                        sub = pu * qty
                    if pu <= 0 and qty > 0 and sub > 0:
                        pu = sub / qty
                    subtotal += sub
                    items.append(
                        {
                            "id": int(d.get("producto_id") or 0),
                            "nombre": str(d.get("producto_nombre") or "").strip() or "Producto",
                            "cantidad": qty,
                            "precio_unitario": pu,
                            "subtotal": sub,
                        }
                    )
                notify_payload = {
                    "venta_id": venta_id,
                    "cliente_nombre": str(venta.get("cliente_nombre") or ""),
                    "cliente_email": str(venta.get("cliente_email") or ""),
                    "cliente_telefono": str(venta.get("cliente_telefono") or ""),
                    "items": items,
                    "subtotal": float(subtotal or 0),
                    "descuento": float(venta.get("descuento_monto") or 0),
                    "total": float(venta.get("total_monto") or 0),
                    "entrega_tipo": str(venta.get("entrega_tipo") or "retiro"),
                    "hora_retiro": str(venta.get("hora_retiro") or ""),
                    "direccion_entrega": str(venta.get("direccion_entrega") or ""),
                    "despacho_monto": float(venta.get("despacho_monto") or 0),
                }
                cur.execute(
                    """
                    UPDATE tienda_flow_pagos
                    SET notified_admin = 1,
                        notified_at = CURRENT_TIMESTAMP
                    WHERE venta_id = ?
                    """,
                    (venta_id,),
                )
        conn.commit()
    finally:
        if conn:
            conn.close()
    if notify_payload:
        try:
            _notificar_whatsapp_pedido_tienda_async(
                venta_id=notify_payload["venta_id"],
                cliente_nombre=notify_payload["cliente_nombre"],
                cliente_email=notify_payload["cliente_email"],
                cliente_telefono=notify_payload["cliente_telefono"],
                items=notify_payload["items"],
                subtotal=notify_payload["subtotal"],
                descuento=notify_payload["descuento"],
                total=notify_payload["total"],
                host_url=_public_base_url(request.url_root),
                entrega_tipo=notify_payload["entrega_tipo"],
                hora_retiro=notify_payload["hora_retiro"],
                direccion_entrega=notify_payload["direccion_entrega"],
                despacho_monto=notify_payload["despacho_monto"],
            )
        except Exception:
            pass


def _flow_reconciliar_pendientes(limit=25, horas=72):
    conn = None
    reconciliados = 0
    try:
        cfg = _flow_cfg()
        if not cfg.get("enabled"):
            return {"ok": True, "reconciliados": 0}
        conn = get_db()
        cur = conn.cursor()
        _ensure_flow_pago_table(cur)
        cur.execute(
            """
            SELECT flow_token
            FROM tienda_flow_pagos
            WHERE estado = 'pendiente'
              AND flow_token IS NOT NULL
              AND TRIM(flow_token) <> ''
              AND datetime(created_at) >= datetime('now', ?)
            ORDER BY id DESC
            LIMIT ?
            """,
            (f"-{int(max(1, horas))} hours", int(max(1, limit))),
        )
        tokens = [str(dict(r).get("flow_token") or "").strip() for r in (cur.fetchall() or [])]
        conn.close()
        conn = None
        for tk in tokens:
            if not tk:
                continue
            try:
                rr = _flow_confirmar_token_y_actualizar(tk)
                if rr.get("success") and rr.get("paid"):
                    reconciliados += 1
            except Exception:
                continue
        return {"ok": True, "reconciliados": reconciliados}
    finally:
        if conn:
            conn.close()


def _flow_confirmar_token_y_actualizar(token):
    token = str(token or "").strip()
    if not token:
        return {"success": False, "error": "Token Flow invalido"}
    row = _obtener_flow_pago_por_token(token)
    if not row:
        return {"success": False, "error": "Token Flow no registrado"}

    cfg = _flow_cfg()
    if not cfg.get("enabled"):
        return {"success": False, "error": "Flow no configurado"}

    try:
        status = _flow_post("/payment/getStatus", {"token": token}, cfg)
    except Exception as e:
        _set_flow_error(int(row.get("venta_id") or 0), str(e))
        raise
    def _safe_int(v, default=0):
        try:
            return int(v)
        except Exception:
            return default

    def _txt(v):
        return str(v or "").strip().lower()

    def _flow_pago_aprobado(payload):
        data = payload if isinstance(payload, dict) else {}
        status_code_local = _safe_int(data.get("status"), 0)
        if status_code_local == 2:
            return True
        if status_code_local in {3, 4}:
            return False

        positivos = {"paid", "approved", "success", "successful", "completed", "authorized", "authorised", "pagado", "aprobado"}
        negativos = {"pending", "rejected", "cancelled", "canceled", "failed", "error", "declined", "voided", "anulado", "rechazado"}

        candidatos = []
        status_nums = []
        def _collect_status_like(obj):
            if not isinstance(obj, dict):
                return
            for k in ("status", "statusCode", "paymentStatusCode", "payment_status_code"):
                vv = obj.get(k)
                if vv is None:
                    continue
                try:
                    status_nums.append(int(vv))
                except Exception:
                    pass
            for k in ("status_text", "statusText", "paymentStatus", "payment_status", "detailStatus", "message"):
                vv = _txt(obj.get(k))
                if vv:
                    candidatos.append(vv)

        # Formato Flow legacy/directo
        _collect_status_like(data)
        for k in ("status_text", "statusText", "paymentStatus", "payment_status", "detailStatus"):
            vv = _txt(data.get(k))
            if vv:
                candidatos.append(vv)

        # Formatos extendidos Flow (API v2/v6)
        for root_key in ("paymenResult", "paymentResult", "payment_result", "lastPayment", "last_payment", "result"):
            sub = data.get(root_key)
            if isinstance(sub, dict):
                _collect_status_like(sub)

        payment_data = data.get("paymentData")
        payment_items = []
        if isinstance(payment_data, dict):
            payment_items = [payment_data]
        elif isinstance(payment_data, list):
            payment_items = [x for x in payment_data if isinstance(x, dict)]

        for it in payment_items:
            _collect_status_like(it)
            for k in ("type",):
                vv = _txt(it.get(k))
                if vv:
                    candidatos.append(vv)

        # Si cualquier status numérico anidado indica pagado, aprobamos.
        if any(int(x) == 2 for x in status_nums):
            return True
        if any(int(x) in {3, 4} for x in status_nums):
            return False

        # Señal adicional: si hay flowOrder y fecha de pago en paymentData,
        # tratamos como aprobado aunque status venga transitoriamente distinto.
        has_flow_order = bool(str(data.get("flowOrder") or "").strip())
        has_payment_date = any(bool(str((it.get("date") or it.get("paymentDate") or it.get("transferDate") or "")).strip()) for it in payment_items)
        if has_flow_order and has_payment_date:
            return True

        if any(any(pos in c for pos in positivos) for c in candidatos):
            if not any(any(neg in c for neg in negativos) for c in candidatos):
                return True
        return False

    venta_id = int(row.get("venta_id") or 0)
    status_code = _safe_int(status.get("status"), 0)
    paid = bool(_flow_pago_aprobado(status))
    estado_flow = "pagado" if paid else ("rechazado" if status_code in {3, 4} else "pendiente")
    _actualizar_flow_pago(venta_id=venta_id, estado=estado_flow, flow_order=status.get("flowOrder"), payment_data=status)

    # metodo_pago visible en historial/clientes
    conn = None
    try:
        _ensure_ventas_metodo_pago_column()
        conn = get_db()
        cur = conn.cursor()
        if paid:
            cur.execute(
                "UPDATE ventas SET metodo_pago = ?, canal_venta = 'tienda_online' WHERE id = ?",
                ("flow_pagado", venta_id),
            )
        else:
            cur.execute(
                "UPDATE ventas SET metodo_pago = ?, canal_venta = 'tienda_online_flow_pendiente' WHERE id = ?",
                ("flow_pendiente", venta_id),
            )
        conn.commit()
    finally:
        if conn:
            conn.close()
    if paid:
        _finalizar_venta_flow_pagada(venta_id, status_payload=status)
    return {"success": True, "venta_id": venta_id, "paid": paid, "status": status}


def _marcar_flow_cliente_regreso(venta_id):
    vid = int(venta_id or 0)
    if vid <= 0:
        return
    conn = None
    try:
        _ensure_ventas_flow_return_column()
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE ventas
            SET flow_cliente_regreso = 1
            WHERE id = ? AND canal_venta IN ('tienda_online', 'tienda_online_flow_pendiente')
            """,
            (vid,),
        )
        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def _rangos_ocupados_evento_agenda(evento, slot_minutes, buffer_minutes=0):
    tipo = str(evento.get("tipo") or "").strip().lower()
    hora_inicio = _hhmm_a_minutos(evento.get("hora_inicio"))
    hora_fin = _hhmm_a_minutos(evento.get("hora_fin"))
    if tipo == "bloqueo":
        # En agenda publica, cualquier bloqueo del dia se interpreta como
        # dia completo sin cupos, independiente del rango horario.
        return {"bloqueo_dia": True, "rangos": []}

    if hora_inicio is None:
        hora_inicio = _hhmm_a_minutos(evento.get("hora_entrega"))
    if hora_inicio is None:
        return {"bloqueo_dia": True, "rangos": []}
    if hora_fin is None or hora_fin <= hora_inicio:
        hora_fin = min(24 * 60, hora_inicio + slot_minutes)
    try:
        extra = max(0, int(buffer_minutes or 0))
    except (TypeError, ValueError):
        extra = 0
    inicio_final = max(0, hora_inicio - extra)
    fin_final = min(24 * 60, hora_fin + extra)
    return {"bloqueo_dia": False, "rangos": [(inicio_final, fin_final, False)]}


def _calcular_disponibilidad_agenda_tienda(cursor, cfg_agenda, fecha_desde, fecha_hasta):
    slot_minutes = int(cfg_agenda["slot_minutes"])
    slot_capacity = int(cfg_agenda["slot_capacity"])
    day_close_min_orders = max(1, int(cfg_agenda.get("day_close_min_orders") or 3))
    event_buffer_minutes = max(0, int(cfg_agenda.get("event_buffer_minutes") or 120))
    start_m = int(cfg_agenda["start_minutes"])
    end_m = int(cfg_agenda["end_minutes"])
    days_ahead = int(cfg_agenda["days_ahead"])

    fecha_inicio_dt = datetime.strptime(fecha_desde, "%Y-%m-%d")
    fecha_hasta_dt = datetime.strptime(fecha_hasta, "%Y-%m-%d")
    total_days = min(days_ahead, max(1, (fecha_hasta_dt - fecha_inicio_dt).days + 1))
    dias_semana_es = ["Lun", "Mar", "Mie", "Jue", "Vie", "Sab", "Dom"]

    cursor.execute(
        """
        SELECT id, tipo, fecha, hora_inicio, hora_fin, hora_entrega, estado
        FROM agenda_eventos
        WHERE fecha >= ? AND fecha <= ?
          AND COALESCE(NULLIF(TRIM(estado), ''), 'pendiente') <> 'cancelado'
        ORDER BY fecha ASC, hora_inicio ASC, id ASC
        """,
        (fecha_desde, fecha_hasta),
    )
    eventos_rows = [dict(r) for r in cursor.fetchall()]
    eventos_por_fecha = {}
    for ev in eventos_rows:
        fecha_ev = str(ev.get("fecha") or "").strip()
        if not fecha_ev:
            continue
        eventos_por_fecha.setdefault(fecha_ev, []).append(ev)

    dias = []
    disponibilidad_mapa = {}
    for offset in range(total_days):
        dia_dt = fecha_inicio_dt + timedelta(days=offset)
        fecha_iso = dia_dt.strftime("%Y-%m-%d")
        slots = []
        minute_cursor = start_m
        while minute_cursor + slot_minutes <= end_m:
            slots.append(
                {
                    "hora_inicio": _minutos_a_hhmm(minute_cursor),
                    "hora_fin": _minutos_a_hhmm(minute_cursor + slot_minutes),
                    "ini": minute_cursor,
                    "fin": minute_cursor + slot_minutes,
                    "ocupados": 0,
                    "bloqueado": False,
                }
            )
            minute_cursor += slot_minutes

        eventos_dia = list(eventos_por_fecha.get(fecha_iso, []))
        pedidos_activos_dia = sum(1 for ev in eventos_dia if str(ev.get("tipo") or "").strip().lower() != "bloqueo")
        cierre_por_cupos_dia = pedidos_activos_dia >= day_close_min_orders
        bloqueo_dia = False
        for ev in eventos_dia:
            occ = _rangos_ocupados_evento_agenda(ev, slot_minutes, buffer_minutes=event_buffer_minutes)
            if occ.get("bloqueo_dia"):
                bloqueo_dia = True
                break
            for ini_ev, fin_ev, is_block in occ.get("rangos") or []:
                for slot in slots:
                    if max(slot["ini"], ini_ev) < min(slot["fin"], fin_ev):
                        if is_block:
                            slot["bloqueado"] = True
                        else:
                            slot["ocupados"] += 1

        horas_payload = []
        sin_cupos_total = True
        for slot in slots:
            if cierre_por_cupos_dia or bloqueo_dia or slot["bloqueado"]:
                disponible = False
                ocupados = slot_capacity
                cupos_disponibles = 0
            else:
                ocupados = int(slot["ocupados"])
                cupos_disponibles = max(0, slot_capacity - ocupados)
                disponible = cupos_disponibles > 0
            if disponible:
                sin_cupos_total = False
            horas_payload.append(
                {
                    "hora_inicio": slot["hora_inicio"],
                    "hora_fin": slot["hora_fin"],
                    "label": f"{slot['hora_inicio']} - {slot['hora_fin']}",
                    "disponible": bool(disponible),
                    "sin_cupos": not bool(disponible),
                    "cupos_disponibles": int(cupos_disponibles),
                    "ocupados": int(ocupados),
                    "capacidad": int(slot_capacity),
                }
            )

        dias.append(
            {
                "fecha": fecha_iso,
                "label": f"{dias_semana_es[dia_dt.weekday()]} {dia_dt.strftime('%d/%m')}",
                "sin_cupos": bool(sin_cupos_total),
                "bloqueado_dia": bool(bloqueo_dia),
                "cierre_por_cupos_dia": bool(cierre_por_cupos_dia),
                "pedidos_activos_dia": int(pedidos_activos_dia),
                "horas": horas_payload,
            }
        )
        disponibilidad_mapa[fecha_iso] = {h["hora_inicio"]: h for h in horas_payload}

    return {"dias": dias, "mapa": disponibilidad_mapa}


def _normalizar_tipo_reserva_tienda(raw):
    tipo = str(raw or "").strip().lower()
    if tipo in {"torta", "tortas"}:
        return "torta"
    if tipo in {"pastel", "pasteles"}:
        return "pastel"
    return ""


def _topper_requiere_96h(topper_id=None, topper_nombre=None):
    tid = str(topper_id or "").strip().lower()
    tname = str(topper_nombre or "").strip().lower()
    if not tid and not tname:
        return False
    texto = f"{tid} {tname}".strip()
    if "sin-topper" in texto or "sin topper" in texto:
        return False
    if re.search(r"\bsin\b", texto) and "topper" in texto:
        return False
    return True


def _minutos_anticipacion_reserva(tipo, topper_requiere_96h=False, pastel_fuera_lista=False, min_horas_categoria=None):
    t = _normalizar_tipo_reserva_tienda(tipo)
    if t == "torta":
        base_min = 96 * 60 if bool(topper_requiere_96h) else 48 * 60
        try:
            cat_min = int(float(min_horas_categoria or 0))
        except (TypeError, ValueError):
            cat_min = 0
        if cat_min > 0:
            base_min = max(base_min, cat_min * 60)
        return base_min
    if t == "pastel" and bool(pastel_fuera_lista):
        return 36 * 60
    # Pastel: 24h.
    return 24 * 60


def _min_datetime_anticipacion_reserva(
    tipo,
    cfg_agenda=None,
    now_local=None,
    topper_requiere_96h=False,
    pastel_fuera_lista=False,
    min_horas_categoria=None,
):
    tz = ZoneInfo("America/Santiago")
    now_dt = now_local or datetime.now(tz)
    t = _normalizar_tipo_reserva_tienda(tipo)
    # Regla horaria exacta por tipo:
    # - torta: 48h (96h si incluye topper distinto a "sin topper")
    # - pastel: 24h
    return now_dt + timedelta(
        minutes=_minutos_anticipacion_reserva(
            t,
            topper_requiere_96h=topper_requiere_96h,
            pastel_fuera_lista=pastel_fuera_lista,
            min_horas_categoria=min_horas_categoria,
        )
    )


def _cumple_anticipacion_reserva(
    fecha_iso,
    hora_inicio,
    tipo,
    cfg_agenda=None,
    now_local=None,
    topper_requiere_96h=False,
    pastel_fuera_lista=False,
    min_horas_categoria=None,
):
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", str(fecha_iso or "").strip()):
        return False
    hora = str(hora_inicio or "").strip()
    if not _parse_hora_hhmm(hora):
        return False
    now_dt = now_local or datetime.now(ZoneInfo("America/Santiago"))
    slot_dt = datetime.strptime(f"{fecha_iso} {hora}", "%Y-%m-%d %H:%M").replace(tzinfo=ZoneInfo("America/Santiago"))
    minimo_dt = _min_datetime_anticipacion_reserva(
        tipo,
        cfg_agenda=cfg_agenda,
        now_local=now_dt,
        topper_requiere_96h=topper_requiere_96h,
        pastel_fuera_lista=pastel_fuera_lista,
        min_horas_categoria=min_horas_categoria,
    )
    return slot_dt >= minimo_dt


def _normalizar_telefono_cl(raw):
    dig = re.sub(r"\D+", "", str(raw or ""))
    if dig.startswith("56"):
        dig = dig[2:]
    if dig.startswith("9") and len(dig) == 9:
        dig = dig[1:]
    if len(dig) != 8:
        return None
    return f"+569{dig}"


def _obtener_ip_cliente():
    xff = str(request.headers.get("X-Forwarded-For") or "").strip()
    if xff:
        ip = xff.split(",")[0].strip()
        if ip:
            return ip[:64]
    xr = str(request.headers.get("X-Real-IP") or "").strip()
    if xr:
        return xr[:64]
    return str(request.remote_addr or "")[:64]


def _bool_env(name, default=False):
    raw = str(os.environ.get(name, "1" if default else "0")).strip().lower()
    return raw in {"1", "true", "yes", "on", "si"}


def _normalizar_numero_whatsapp(raw):
    texto = str(raw or "").strip()
    if not texto:
        return ""
    if texto.lower().startswith("whatsapp:"):
        return texto
    dig = re.sub(r"\D+", "", texto)
    if not dig:
        return ""
    if not dig.startswith("56"):
        dig = f"56{dig}"
    return f"whatsapp:+{dig}"


def _crear_pdf_resumen_pedido_tienda(
    venta_id,
    cliente_nombre,
    cliente_email,
    cliente_telefono,
    items,
    subtotal,
    descuento,
    total,
    entrega_tipo="retiro",
    hora_retiro=None,
    direccion_entrega=None,
    despacho_monto=0,
):
    if canvas is None:
        raise RuntimeError("ReportLab no esta instalado en el entorno.")
    base_dir = os.path.join(static_dir, "tienda_pedidos_pdf")
    os.makedirs(base_dir, exist_ok=True)
    filename = f"pedido_{int(venta_id)}_{uuid.uuid4().hex[:10]}.pdf"
    abs_path = os.path.join(base_dir, filename)

    c = canvas.Canvas(abs_path, pagesize=A4)
    width, height = A4
    y = height - 52
    c.setFont("Helvetica-Bold", 16)
    c.drawString(40, y, f"Pedido tienda online #{int(venta_id)}")
    y -= 20
    c.setFont("Helvetica", 10)
    c.drawString(40, y, f"Fecha: {datetime.now(ZoneInfo('America/Santiago')).strftime('%d-%m-%Y %H:%M:%S')}")
    y -= 20
    c.drawString(40, y, f"Cliente: {cliente_nombre}")
    y -= 14
    c.drawString(40, y, f"Correo: {cliente_email}")
    y -= 14
    c.drawString(40, y, f"Telefono: {cliente_telefono}")
    y -= 24
    entrega_txt = "Despacho" if str(entrega_tipo or "").strip().lower() == "despacho" else "Retiro en tienda"
    c.drawString(40, y, f"Entrega: {entrega_txt}")
    y -= 14
    if hora_retiro:
        c.drawString(40, y, f"Hora retiro/entrega: {str(hora_retiro)}")
        y -= 14
    if str(entrega_txt).lower().startswith("despacho"):
        c.drawString(40, y, f"Direccion: {str(direccion_entrega or '-').strip()[:150]}")
        y -= 14
        c.drawString(40, y, f"Despacho estimado: ${float(despacho_monto or 0):,.0f}".replace(",", "."))
        y -= 20

    c.setFont("Helvetica-Bold", 11)
    c.drawString(40, y, "Productos")
    y -= 16
    c.setFont("Helvetica", 10)
    for it in (items or []):
        nombre = str(it.get("nombre") or f"Producto #{it.get('id')}").strip()
        cantidad = int(it.get("cantidad") or 0)
        precio_u = float(it.get("precio_unitario") or 0)
        linea_total = cantidad * precio_u
        c.drawString(44, y, f"- {nombre} x{cantidad} | ${linea_total:,.0f}".replace(",", "."))
        y -= 14
        if y < 88:
            c.showPage()
            y = height - 52
            c.setFont("Helvetica", 10)

    y -= 8
    c.setFont("Helvetica-Bold", 11)
    c.drawString(40, y, f"Subtotal: ${subtotal:,.0f}".replace(",", "."))
    y -= 14
    c.drawString(40, y, f"Descuento: -${descuento:,.0f}".replace(",", "."))
    y -= 14
    c.drawString(40, y, f"Total: ${total:,.0f}".replace(",", "."))
    c.save()
    return filename


def _crear_pdf_reserva_agenda_tienda(reserva):
    if canvas is None:
        raise RuntimeError("ReportLab no esta instalado en el entorno.")
    rid = int(reserva.get("id") or 0)
    base_dir = os.path.join(static_dir, "tienda_pedidos_pdf")
    os.makedirs(base_dir, exist_ok=True)
    filename = f"reserva_agenda_{rid}_{uuid.uuid4().hex[:10]}.pdf"
    abs_path = os.path.join(base_dir, filename)

    def _wrap(txt, max_len=92):
        text = str(txt or "").strip()
        if not text:
            return [""]
        out = []
        raw_lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
        for ln in raw_lines:
            line = ln.strip()
            if not line:
                out.append("")
                continue
            while len(line) > max_len:
                cut = line.rfind(" ", 0, max_len + 1)
                if cut <= 0:
                    cut = max_len
                out.append(line[:cut].strip())
                line = line[cut:].strip()
            out.append(line)
        return out or [""]

    def _split_sections(txt, default_title="Detalle pedido"):
        lines = [str(ln or "").strip() for ln in str(txt or "").replace("\r\n", "\n").replace("\r", "\n").split("\n")]
        lines = [ln for ln in lines if ln]
        if not lines:
            return [{"title": default_title, "items": ["-"]}]
        sections = []
        current = {"title": default_title, "items": []}
        for line in lines:
            if line.startswith("---") and line.endswith("---") and len(line) > 6:
                title = str(line.strip("- ").strip() or default_title)
                if current["items"] and str(current.get("title") or "").strip().lower() != "builder json":
                    sections.append(current)
                if title.lower() == "builder json":
                    current = {"title": title, "items": []}
                    continue
                current = {"title": title, "items": []}
                continue
            current["items"].append(line)
        if current["items"] and str(current.get("title") or "").strip().lower() != "builder json":
            sections.append(current)
        return sections or [{"title": default_title, "items": ["-"]}]

    def _parse_builder_json(txt):
        try:
            raw_lines = str(txt or "").replace("\r\n", "\n").replace("\r", "\n").split("\n")
            in_builder = False
            json_line = ""
            for raw in raw_lines:
                line = str(raw or "").strip()
                if not line:
                    continue
                if line.startswith("---") and line.endswith("---") and len(line) > 6:
                    title = str(line.strip("- ").strip() or "").lower()
                    in_builder = (title == "builder json")
                    continue
                if in_builder and line.startswith("{") and line.endswith("}"):
                    json_line = line
                    break
            if not json_line:
                return None
            data = json.loads(json_line)
            return data if isinstance(data, dict) else None
        except Exception:
            return None

    def _fmt_clp_pdf(value):
        try:
            n = int(round(float(value or 0)))
        except (TypeError, ValueError):
            n = 0
        return f"${n:,}".replace(",", ".")

    def _norm_text(value):
        txt = str(value or "").strip().lower()
        txt = txt.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u").replace("ü", "u")
        return " ".join(txt.split())

    def _parse_resumen_cliente_catalogo(txt):
        lines = [str(ln or "").strip() for ln in str(txt or "").replace("\r\n", "\n").replace("\r", "\n").split("\n")]
        lines = [ln for ln in lines if ln]
        if not lines:
            return None
        start = -1
        for i, ln in enumerate(lines):
            if _norm_text(ln.strip("- ").strip()) == "resumen de cotizacion (cliente)":
                start = i + 1
                break
        if start < 0:
            return None
        block = []
        for ln in lines[start:]:
            if ln.startswith("---") and ln.endswith("---") and len(ln) > 6:
                break
            block.append(ln)
        if not block:
            return None
        data = {"categoria": "", "tamano": "", "sabores": [], "extras": [], "topper": "", "nota": "", "referencias": []}
        mode = ""
        for raw in block:
            ln = str(raw or "").strip()
            low = _norm_text(ln)
            if low.startswith("categoria:"):
                data["categoria"] = ln.split(":", 1)[1].strip()
                mode = ""
                continue
            if low.startswith("tamano:"):
                data["tamano"] = ln.split(":", 1)[1].strip()
                mode = ""
                continue
            if low == "sabores:":
                mode = "sabores"
                continue
            if low == "extras:":
                mode = "extras"
                continue
            if low == "topper:":
                mode = "topper"
                continue
            if low.startswith("nota catalogo:"):
                data["nota"] = ln.split(":", 1)[1].strip()
                mode = ""
                continue
            if low.startswith("referencias:"):
                val = ln.split(":", 1)[1].strip()
                if val and val != "-":
                    data["referencias"].append(val)
                mode = "referencias"
                continue
            if mode in {"sabores", "extras", "topper", "referencias"} and ln.startswith("-"):
                val = ln[1:].strip()
                if not val or val == "-":
                    continue
                if mode == "sabores":
                    data["sabores"].append(val)
                elif mode == "extras":
                    data["extras"].append(val)
                elif mode == "topper":
                    data["topper"] = val
                else:
                    data["referencias"].append(val)
        return data

    def _builder_from_resumen_cliente(txt):
        parsed = _parse_resumen_cliente_catalogo(txt)
        if not parsed:
            return None
        try:
            cfg_tienda = _obtener_tienda_personalizacion(apply_programacion=True, editor_mode="live")
            catalogo = _catalogo_torta_publico_desde_personalizacion(cfg_tienda)
        except Exception:
            catalogo = {"categorias": [], "sizes": [], "sabores": [], "extras": [], "toppers": []}

        def _id_by_name(rows, query_name):
            want = _norm_text(query_name)
            if not want:
                return ""
            for r in (rows or []):
                rid = str((r or {}).get("id") or "").strip()
                nm = _norm_text((r or {}).get("nombre"))
                if rid and nm and (nm == want or nm in want or want in nm):
                    return rid
            return ""

        def _name_qty(row_txt):
            txt_row = str(row_txt or "").strip()
            qty = 1
            m = re.search(r"\bx\s*(\d+)\b", txt_row, re.IGNORECASE)
            if m:
                try:
                    qty = max(1, int(m.group(1)))
                except (TypeError, ValueError):
                    qty = 1
            txt_row = re.sub(r"\bx\s*\d+\b", "", txt_row, flags=re.IGNORECASE).strip()
            txt_row = re.sub(r"\(.*?\)", "", txt_row).strip(" -")
            return txt_row, qty

        categoria_id = _id_by_name(catalogo.get("categorias"), parsed.get("categoria"))
        tamano_name = re.sub(r"\(.*?\)", "", str(parsed.get("tamano") or "")).strip()
        size_id = _id_by_name(catalogo.get("sizes"), tamano_name or parsed.get("tamano"))

        sabor_ids = []
        for s in (parsed.get("sabores") or []):
            s_name, _ = _name_qty(s)
            sid = _id_by_name(catalogo.get("sabores"), s_name)
            if sid and sid not in sabor_ids:
                sabor_ids.append(sid)

        extra_items = []
        for ex in (parsed.get("extras") or []):
            ex_name, qty = _name_qty(ex)
            ex_id = _id_by_name(catalogo.get("extras"), ex_name)
            if ex_id:
                extra_items.append({"id": ex_id, "qty": int(max(1, qty))})

        topper_name, _ = _name_qty(parsed.get("topper"))
        topper_id = _id_by_name(catalogo.get("toppers"), topper_name) if topper_name and _norm_text(topper_name) != "sin topper" else ""

        builder = {
            "categoria_id": categoria_id,
            "size_id": size_id,
            "sabor_ids": sabor_ids,
            "extra_items": extra_items,
            "topper_id": topper_id,
            "referencia_urls": [str(x).strip() for x in (parsed.get("referencias") or []) if str(x).strip()],
            "nota": str(parsed.get("nota") or "").strip(),
        }
        if not any([builder["categoria_id"], builder["size_id"], builder["sabor_ids"], builder["extra_items"], builder["topper_id"], builder["referencia_urls"], builder["nota"]]):
            return None
        return builder

    def _catalogo_section_from_builder(builder):
        if not isinstance(builder, dict):
            return None
        try:
            cfg_tienda = _obtener_tienda_personalizacion(apply_programacion=True, editor_mode="live")
            catalogo = _catalogo_torta_publico_desde_personalizacion(cfg_tienda)
        except Exception:
            catalogo = {"categorias": [], "sizes": [], "sabores": [], "extras": [], "toppers": []}

        def _idx(rows):
            out = {}
            for r in (rows or []):
                key = str((r or {}).get("id") or "").strip().lower()
                if key:
                    out[key] = dict(r or {})
            return out

        categorias = _idx(catalogo.get("categorias"))
        sizes = _idx(catalogo.get("sizes"))
        sabores = _idx(catalogo.get("sabores"))
        extras = _idx(catalogo.get("extras"))
        toppers = _idx(catalogo.get("toppers"))

        categoria_id = str(builder.get("categoria_id") or "").strip().lower()
        size_id = str(builder.get("size_id") or "").strip().lower()
        sabor_ids = builder.get("sabor_ids") if isinstance(builder.get("sabor_ids"), list) else []
        extra_items = builder.get("extra_items") if isinstance(builder.get("extra_items"), list) else []
        topper_id = str(builder.get("topper_id") or "").strip().lower()
        refs = builder.get("referencia_urls") if isinstance(builder.get("referencia_urls"), list) else []
        nota = str(builder.get("nota") or "").strip()
        otros_cargos = builder.get("otros_cargos") if isinstance(builder.get("otros_cargos"), list) else []
        descuento_tipo = str(builder.get("descuento_tipo") or "ninguno").strip().lower()
        if descuento_tipo not in {"ninguno", "monto", "porcentaje"}:
            descuento_tipo = "ninguno"
        try:
            descuento_valor = float(builder.get("descuento_valor") or 0)
        except (TypeError, ValueError):
            descuento_valor = 0.0
        descuento_valor = max(0.0, descuento_valor)

        categoria = categorias.get(categoria_id) or {}
        size = sizes.get(size_id) or {}

        sabores_rows = []
        seen = set()
        for raw_sid in sabor_ids:
            sid = str(raw_sid or "").strip().lower()
            if not sid or sid in seen:
                continue
            row = sabores.get(sid)
            if not row:
                continue
            sabores_rows.append(row)
            seen.add(sid)

        extras_rows = []
        for raw_item in extra_items:
            item = dict(raw_item or {})
            eid = str(item.get("id") or "").strip().lower()
            row = extras.get(eid)
            if not row:
                continue
            try:
                qty = int(item.get("qty") or 0)
            except (TypeError, ValueError):
                qty = 0
            if qty <= 0:
                continue
            extras_rows.append({"nombre": row.get("nombre"), "precio": float(row.get("precio") or 0), "qty": qty})

        topper = toppers.get(topper_id) if topper_id else None

        subtotal = float(size.get("precio") or 0)
        subtotal += sum(float(s.get("precio") or 0) for s in sabores_rows)
        subtotal += sum(float(x.get("precio") or 0) * int(x.get("qty") or 0) for x in extras_rows)
        if topper:
            subtotal += float(topper.get("precio") or 0)
        subtotal += sum(max(0.0, float((dict(x or {})).get("valor") or 0)) for x in otros_cargos)
        descuento_monto = 0.0
        if descuento_tipo == "monto":
            descuento_monto = descuento_valor
        elif descuento_tipo == "porcentaje":
            descuento_monto = subtotal * (max(0.0, min(100.0, descuento_valor)) / 100.0)
        descuento_monto = max(0.0, min(subtotal, descuento_monto))
        total_catalogo = max(0.0, subtotal - descuento_monto)

        items = [
            f"Categoria: {categoria.get('nombre') or '-'}",
            f"Tamano: {(size.get('nombre') or '-')} ({_fmt_clp_pdf(size.get('precio') or 0)})",
            "Sabores:",
        ]
        if sabores_rows:
            for s in sabores_rows:
                items.append(f"- {s.get('nombre') or '-'} ({_fmt_clp_pdf(s.get('precio') or 0)})")
        else:
            items.append("- -")
        items.append("Extras:")
        if extras_rows:
            for ex in extras_rows:
                items.append(f"- {ex.get('nombre') or '-'} x{int(ex.get('qty') or 0)} ({_fmt_clp_pdf(float(ex.get('precio') or 0) * int(ex.get('qty') or 0))})")
        else:
            items.append("- -")
        items.append("Topper:")
        if topper:
            items.append(f"- {topper.get('nombre') or '-'} ({_fmt_clp_pdf(topper.get('precio') or 0)})")
        else:
            items.append("- Sin topper")
        items.append("Otros cargos:")
        if otros_cargos:
            printed = False
            for cargo in otros_cargos:
                c = dict(cargo or {})
                desc = str(c.get("descripcion") or "").strip() or "Cargo manual"
                try:
                    val = float(c.get("valor") or 0)
                except (TypeError, ValueError):
                    val = 0.0
                if val <= 0:
                    continue
                items.append(f"- {desc} ({_fmt_clp_pdf(val)})")
                printed = True
            if not printed:
                items.append("- -")
        else:
            items.append("- -")
        refs_ok = [str(r or "").strip() for r in refs if str(r or "").strip()]
        items.append(f"Referencias: {' | '.join(refs_ok) if refs_ok else '-'}")
        items.append(f"Nota catalogo: {nota or '-'}")
        items.append(f"Subtotal estimado productos: {_fmt_clp_pdf(subtotal)}")
        if descuento_monto > 0:
            if descuento_tipo == "porcentaje":
                items.append(f"Descuento aplicado: {int(round(max(0.0, min(100.0, descuento_valor))))}% (-{_fmt_clp_pdf(descuento_monto)})")
            else:
                items.append(f"Descuento aplicado: -{_fmt_clp_pdf(descuento_monto)}")
        else:
            items.append("Descuento aplicado: -")
        items.append(f"Total estimado productos: {_fmt_clp_pdf(total_catalogo)}")
        return {"title": "Catalogo torta", "items": items}

    c = canvas.Canvas(abs_path, pagesize=A4)
    _, height = A4
    y = height - 44
    margin = 40
    right = 560
    motivo_raw = str(reserva.get("motivo") or "").strip().lower()
    es_reserva_pendiente = "reserva cliente tienda online" in motivo_raw

    def _ensure_space(needed=20, reset_font=True):
        nonlocal y
        if y >= 62 + needed:
            return
        c.showPage()
        y = height - 44
        if reset_font:
            c.setFont("Helvetica", 10)

    def _draw_wrapped(lines, x=margin + 2, font="Helvetica", size=10, step=12):
        nonlocal y
        c.setFont(font, size)
        for ln in lines:
            _ensure_space(step + 2, reset_font=False)
            c.drawString(x, y, str(ln or "")[:170])
            y -= step

    c.setFont("Helvetica-Bold", 17)
    c.drawString(margin, y, f"Reserva agenda tienda #{rid}")
    y -= 16
    c.setFont("Helvetica", 10)
    c.drawString(margin, y, f"Generado: {datetime.now(ZoneInfo('America/Santiago')).strftime('%d-%m-%Y %H:%M:%S')}")
    y -= 18
    if es_reserva_pendiente:
        _ensure_space(34, reset_font=False)
        c.setFillColorRGB(1.0, 0.95, 0.86)
        c.roundRect(margin, y - 11, right - margin, 18, 4, stroke=0, fill=1)
        c.setFillColorRGB(0.62, 0.20, 0.04)
        c.setFont("Helvetica-Bold", 11)
        c.drawString(margin + 8, y + 1, "RESERVACION PENDIENTE")
        c.setFillColorRGB(0, 0, 0)
        y -= 22
    c.setStrokeColorRGB(0.87, 0.90, 0.95)
    c.line(margin, y, right, y)
    y -= 16

    c.setFont("Helvetica-Bold", 11)
    c.drawString(margin, y, "Datos de Reserva")
    y -= 14
    _draw_wrapped(_wrap(f"Codigo pedido: {reserva.get('codigo_pedido') or '-'}", max_len=92))
    _draw_wrapped(_wrap(f"Tipo: {str(reserva.get('tipo') or '').capitalize()}", max_len=92))
    _draw_wrapped(_wrap(f"Fecha: {reserva.get('fecha') or '-'} {reserva.get('hora_inicio') or '-'}", max_len=92))
    _draw_wrapped(_wrap(f"Cliente: {reserva.get('cliente') or '-'}", max_len=92))
    _draw_wrapped(_wrap(f"Telefono: {reserva.get('telefono') or '-'}", max_len=92))
    _draw_wrapped(_wrap(f"Direccion: {reserva.get('direccion') or 'Retiro en tienda'}", max_len=92))
    y -= 6

    c.setFont("Helvetica-Bold", 11)
    _ensure_space(24, reset_font=False)
    c.drawString(margin, y, "Detalle del Pedido")
    y -= 16

    ingredientes_txt = reserva.get("ingredientes") or ""
    sections = _split_sections(ingredientes_txt, "Ingredientes / Detalles")
    builder_data = _parse_builder_json(ingredientes_txt)
    if not builder_data:
        builder_data = _builder_from_resumen_cliente(ingredientes_txt)
    builder_catalog_section = _catalogo_section_from_builder(builder_data) if builder_data else None
    if builder_catalog_section:
        filtered = [sec for sec in sections if str(sec.get("title") or "").strip().lower() != "catalogo torta"]
        inserted = False
        merged = []
        for sec in filtered:
            merged.append(sec)
            t = str(sec.get("title") or "").strip().lower()
            if not inserted and (t in {"ingredientes / detalles", "detalle pedido"}):
                merged.append(builder_catalog_section)
                inserted = True
        if not inserted:
            merged.insert(0, builder_catalog_section)
        sections = merged

    for section in sections:
        _ensure_space(26, reset_font=False)
        c.setFillColorRGB(0.96, 0.97, 0.99)
        c.roundRect(margin, y - 11, right - margin, 16, 3, stroke=0, fill=1)
        c.setFillColorRGB(0, 0, 0)
        c.setFont("Helvetica-Bold", 10)
        c.drawString(margin + 6, y, str(section.get("title") or "Detalle")[:90])
        y -= 18
        items = section.get("items") or ["-"]
        for item in items:
            bullet = str(item or "-").strip()
            if not bullet:
                bullet = "-"
            if not bullet.startswith("-"):
                bullet = f"- {bullet}"
            wrapped = _wrap(bullet, max_len=96)
            _draw_wrapped(wrapped, x=margin + 8, font="Helvetica", size=9.8, step=11)
        y -= 4
    if es_reserva_pendiente:
        _ensure_space(56, reset_font=False)
        c.setStrokeColorRGB(0.97, 0.69, 0.28)
        c.setFillColorRGB(1.0, 0.98, 0.94)
        c.roundRect(margin, y - 36, right - margin, 42, 4, stroke=1, fill=1)
        c.setFillColorRGB(0.55, 0.23, 0.06)
        c.setFont("Helvetica-Bold", 10)
        c.drawString(margin + 8, y - 4, "Importante")
        c.setFillColorRGB(0.20, 0.15, 0.10)
        y -= 16
        _draw_wrapped(
            _wrap(
                "Esta reserva se encuentra pendiente de confirmacion. "
                "Cuando tu pedido sea confirmado por Pasteleria, se enviara "
                "la cotizacion final en PDF con toda la informacion adicional registrada.",
                max_len=92,
            ),
            x=margin + 8,
            font="Helvetica",
            size=9.2,
            step=10,
        )
        y -= 6
    c.save()
    return filename


def _enviar_whatsapp_twilio(body_text, media_url=None, to_number=None):
    account_sid = str(os.environ.get("TWILIO_ACCOUNT_SID") or "").strip()
    auth_token = str(os.environ.get("TWILIO_AUTH_TOKEN") or "").strip()
    from_number = _normalizar_numero_whatsapp(os.environ.get("TWILIO_WHATSAPP_FROM"))
    destino = _normalizar_numero_whatsapp(to_number or os.environ.get("GESTIONSTOCK_WHATSAPP_TO", "+56964330546"))
    if not account_sid or not auth_token or not from_number or not destino:
        return False, "Twilio no configurado"

    payload = {
        "To": destino,
        "From": from_number,
        "Body": str(body_text or "").strip()[:1500],
    }
    if media_url:
        payload["MediaUrl"] = media_url

    auth_b64 = base64.b64encode(f"{account_sid}:{auth_token}".encode("utf-8")).decode("ascii")
    req = UrlRequest(
        f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json",
        data=urlencode(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {auth_b64}",
        },
        method="POST",
    )
    try:
        with urlopen(req, timeout=20) as res:
            _ = res.read()
        return True, ""
    except HTTPError as e:
        try:
            detail = e.read().decode("utf-8", errors="ignore")
        except Exception:
            detail = str(e)
        return False, f"HTTP {e.code}: {detail[:240]}"
    except Exception as e:
        return False, str(e)


def _notificar_whatsapp_pedido_tienda_async(
    venta_id,
    cliente_nombre,
    cliente_email,
    cliente_telefono,
    items,
    subtotal,
    descuento,
    total,
    host_url,
    entrega_tipo="retiro",
    hora_retiro=None,
    direccion_entrega=None,
    despacho_monto=0,
):
    if not _bool_env("GESTIONSTOCK_WHATSAPP_ENABLED", default=False):
        return

    def _run():
        try:
            filename = _crear_pdf_resumen_pedido_tienda(
                venta_id=venta_id,
                cliente_nombre=cliente_nombre,
                cliente_email=cliente_email,
                cliente_telefono=cliente_telefono,
                items=items,
                subtotal=subtotal,
                descuento=descuento,
                total=total,
                entrega_tipo=entrega_tipo,
                hora_retiro=hora_retiro,
                direccion_entrega=direccion_entrega,
                despacho_monto=despacho_monto,
            )
            media_url = f"{str(host_url or '').rstrip('/')}/static/tienda_pedidos_pdf/{quote(filename)}"
            resumen_items = ", ".join(
                f"{str(it.get('nombre') or '').strip() or ('#' + str(it.get('id') or ''))} x{int(it.get('cantidad') or 0)}"
                for it in (items or [])
            )[:700]
            body = (
                f"Nuevo pedido tienda online #{int(venta_id)}\n"
                f"Cliente: {cliente_nombre}\n"
                f"Correo: {cliente_email}\n"
                f"Telefono: {cliente_telefono}\n"
                f"Entrega: {'Despacho' if str(entrega_tipo or '').strip().lower() == 'despacho' else 'Retiro en tienda'}\n"
                f"Hora: {str(hora_retiro or '-')}\n"
                f"Direccion: {str(direccion_entrega or '-').strip()[:140]}\n"
                f"Despacho: ${float(despacho_monto or 0):,.0f}\n"
                f"Total: ${total:,.0f}\n"
                f"Items: {resumen_items}\n"
                "Adjunto PDF de respaldo."
            ).replace(",", ".")
            ok, err = _enviar_whatsapp_twilio(body, media_url=media_url)
            if not ok:
                print(f"[WARN] No se pudo enviar WhatsApp de pedido #{venta_id}: {err}")
        except Exception as e:
            print(f"[WARN] Error en notificacion WhatsApp pedido #{venta_id}: {e}")

    threading.Thread(target=_run, daemon=True).start()


def _twilio_whatsapp_configurado():
    account_sid = str(os.environ.get("TWILIO_ACCOUNT_SID") or "").strip()
    auth_token = str(os.environ.get("TWILIO_AUTH_TOKEN") or "").strip()
    from_number = _normalizar_numero_whatsapp(os.environ.get("TWILIO_WHATSAPP_FROM"))
    return bool(account_sid and auth_token and from_number)


def _parse_hora_hhmm(valor):
    v = str(valor or "").strip()
    if not v:
        return None
    m = re.match(r"^(\d{1,2}):(\d{2})$", v)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2))
    if hh < 0 or hh > 23 or mm < 0 or mm > 59:
        return None
    return hh, mm


def _obtener_tienda_config():
    conn = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT modo_manual, horario_habilitado, hora_apertura, hora_cierre, mensaje_post_pedido, actualizado_en
            FROM tienda_config
            WHERE id = 1
            LIMIT 1
            """
        )
        row = cursor.fetchone()
        if row:
            item = dict(row)
        else:
            item = {}
        modo_raw = str(item.get("modo_manual") or "auto").strip().lower()
        if modo_raw not in {"auto", "abierta", "cerrada"}:
            modo_raw = "auto"
        hora_apertura = str(item.get("hora_apertura") or "09:00").strip()
        hora_cierre = str(item.get("hora_cierre") or "19:00").strip()
        if not _parse_hora_hhmm(hora_apertura):
            hora_apertura = "09:00"
        if not _parse_hora_hhmm(hora_cierre):
            hora_cierre = "19:00"
        return {
            "modo_manual": modo_raw,
            "horario_habilitado": bool(item.get("horario_habilitado")),
            "hora_apertura": hora_apertura,
            "hora_cierre": hora_cierre,
            "mensaje_post_pedido": str(item.get("mensaje_post_pedido") or "").strip() or "Tu pedido fue ingresado correctamente y sera contactado a la brevedad.",
            "actualizado_en": item.get("actualizado_en"),
        }
    finally:
        if conn:
            conn.close()


def _default_tienda_personalizacion():
    catalogo_torta_base = {
        "enabled": True,
        "show_prices": True,
        "max_extra_items": 8,
        "max_reference_images": 3,
        "categorias": [
            {
                "id": "bizcocho",
                "nombre": "Tortas Bizcocho",
                "activo": True,
                "descripcion": "",
                "badge": "",
                "imagen_url": "",
                "min_lead_hours": 48,
                "use_category_ingredients": False,
                "sabores_ids": [],
                "extras_ids": [],
                "toppers_ids": [],
            },
            {
                "id": "panqueque",
                "nombre": "Tortas Panqueque",
                "activo": True,
                "descripcion": "",
                "badge": "",
                "imagen_url": "",
                "min_lead_hours": 48,
                "use_category_ingredients": False,
                "sabores_ids": [],
                "extras_ids": [],
                "toppers_ids": [],
            },
        ],
        "sizes": [
            {"id": "torta-15-bizcocho", "categoria_id": "bizcocho", "nombre": "15 personas (Bizcocho)", "precio": 25990, "max_sabores": 3, "activo": True},
            {"id": "torta-20-bizcocho", "categoria_id": "bizcocho", "nombre": "20 personas (Bizcocho)", "precio": 31990, "max_sabores": 3, "activo": True},
        ],
        "sabores": [
            {"id": "manjar", "nombre": "Manjar", "precio": 0, "activo": True},
            {"id": "frambuesa", "nombre": "Frambuesa", "precio": 0, "activo": True},
            {"id": "chocolate", "nombre": "Chocolate", "precio": 0, "activo": True},
            {"id": "crema-pastelera", "nombre": "Crema pastelera", "precio": 0, "activo": True},
        ],
        "extras": [
            {"id": "extra-fruta", "nombre": "Fruta adicional", "precio": 2500, "max_cantidad": 3, "activo": True},
            {"id": "extra-relleno", "nombre": "Relleno adicional", "precio": 3000, "max_cantidad": 3, "activo": True},
        ],
        "toppers": [
            {"id": "sin-topper", "nombre": "Sin topper", "precio": 0, "activo": True},
            {"id": "topper-personalizado", "nombre": "Topper personalizado", "precio": 4500, "activo": True},
        ],
    }
    return {
        "brand_text": "Tienda en linea",
        "search_placeholder": "Buscar productos...",
        "menu_title": "Menu",
        "sec_ofertas_title": "Ofertas",
        "sec_destacados_title": "Articulos destacados",
        "cart_title": "Tu compra",
        "empty_cart_text": "El carrito esta vacio.",
        "whatsapp_url": "https://wa.me/56964330546?text=Hola%20Pasteleria%20Sucree%2C%20tengo%20una%20consulta%20sobre%20la%20tienda.",
        "whatsapp_title": "Escribenos por WhatsApp",
        "whatsapp_icon_url": "/static/whatsapp_icon.png",
        "show_whatsapp_button": True,
        "hero_enabled": False,
        "hero_badge": "Tienda online",
        "hero_title": "Pasteleria Sucree",
        "hero_subtitle": "Haz tu pedido online y revisa tu estado en vivo.",
        "hero_cta_text": "Pedir por WhatsApp",
        "hero_cta_url": "",
        "hero_image_url": "",
        "banner_enabled": False,
        "banner_text": "",
        "banner_bg": "#f0fdf4",
        "banner_text_color": "#166534",
        "color_bg": "#f5f5f5",
        "color_panel": "#ffffff",
        "color_line": "#e5e7eb",
        "color_text": "#111827",
        "color_muted": "#6b7280",
        "color_accent": "#f45d08",
        "color_accent_dark": "#cc4a03",
        "offer_card_border_color": "#fdba74",
        "offer_card_glow_color": "#fb923c",
        "offer_price_color": "#9a3412",
        "offer_discount_chip_bg": "#ef4444",
        "offer_discount_chip_text": "#ffffff",
        "offer_badge_enabled": True,
        "offer_badge_text": "Oferta",
        "offer_badge_icon": "🔥",
        "offer_badge_bg": "#f97316",
        "offer_badge_text_color": "#ffffff",
        "offer_float_icon": "✨",
        "offer_float_image_url": "",
        "offer_float_image_size": 46,
        "agenda_enabled": True,
        "agenda_menu_label": "Agendar pedido",
        "agenda_type_label": "Tipo de pedido",
        "agenda_type_torta_text": "Torta (48h)",
        "agenda_type_pastel_text": "Pasteles (24h)",
        "agenda_pastel_catalog_title": "Catalogo de pasteles disponibles",
        "agenda_pastel_catalog_button_text": "Abrir catalogo de pasteles",
        "agenda_pastel_catalog_empty_text": "No hay pasteles disponibles en este momento.",
        "agenda_pastel_mode_catalog_text": "Productos en lista (24h)",
        "agenda_pastel_mode_special_text": "Solicitud fuera de lista (36h)",
        "agenda_pastel_special_title": "Solicitar producto fuera de lista",
        "agenda_pastel_special_placeholder": "Describe el pastel o producto que necesitas para esa fecha.",
        "agenda_pastel_special_help_text": "Para productos fuera de lista se requiere minimo 36 horas.",
        "agenda_pastel_category_filter": "Pasteles",
        "agenda_section_title": "Agenda tu pedido",
        "agenda_section_subtitle": "Selecciona dia y hora disponible para reservar tu torta o pastel.",
        "agenda_builder_title": "Arma tu torta",
        "agenda_summary_title": "Resumen de cotizacion",
        "agenda_total_label": "Total estimado",
        "agenda_delivery_retiro_text": "Retiro en tienda",
        "agenda_delivery_despacho_text": "Despacho",
        "agenda_placeholder_name": "Nombre completo",
        "agenda_placeholder_email": "Correo electronico",
        "agenda_placeholder_phone": "12345678",
        "agenda_placeholder_detail": "Detalle rapido del pedido (opcional)",
        "agenda_placeholder_address": "Direccion de despacho (buscar y confirmar pin)",
        "agenda_map_search_text": "Buscar",
        "agenda_map_confirm_text": "Confirmar pin",
        "agenda_map_help_text": "Mueve el pin al punto exacto y confirma.",
        "agenda_delivery_origin_lat": -33.5191105,
        "agenda_delivery_origin_lng": -70.7849094,
        "agenda_delivery_fee_0_3": 2500,
        "agenda_delivery_fee_3_6": 3500,
        "agenda_delivery_fee_6_9": 4500,
        "agenda_delivery_fee_9_plus": 5500,
        "agenda_delivery_band_1_start": "10:00",
        "agenda_delivery_band_1_end": "13:00",
        "agenda_delivery_band_1_extra": 0,
        "agenda_delivery_band_2_start": "14:00",
        "agenda_delivery_band_2_end": "17:00",
        "agenda_delivery_band_2_extra": 1000,
        "agenda_delivery_band_3_start": "18:00",
        "agenda_delivery_band_3_end": "20:00",
        "agenda_delivery_band_3_extra": 1800,
        "agenda_delivery_note_text": "El valor de despacho se calcula automaticamente para direcciones dentro de Maipu.",
        "agenda_delivery_outside_warning": "Direccion fuera de Maipu: el valor de despacho no se puede mostrar. Confirma el PIN y realiza la reserva de horario y te contactaremos para cotizarlo internamente.",
        "agenda_delivery_zones": [],
        "agenda_days_ahead": 14,
        "agenda_hour_start": "09:00",
        "agenda_hour_end": "19:00",
        "agenda_slot_minutes": 60,
        "agenda_slot_capacity": 1,
        "agenda_day_close_min_orders": 3,
        "agenda_event_buffer_minutes": 120,
        "agenda_form_button_text": "Reservar horario",
        "agenda_confirm_title": "Confirmar reserva",
        "agenda_confirm_warning": "Verifica muy bien tu telefono: sera el medio principal de contacto para tu reserva.",
        "agenda_confirm_pdf_text": "Descargar comprobante",
        "agenda_confirm_cancel_text": "Volver",
        "agenda_confirm_accept_text": "Confirmar y generar orden PDF",
        "agenda_card_bg": "#f8fafc",
        "agenda_card_border": "#cbd5e1",
        "agenda_slot_available_bg": "#ecfeff",
        "agenda_slot_unavailable_bg": "#e5e7eb",
        "agenda_slot_unavailable_text": "#64748b",
        "visual_layout_max_width": 1400,
        "visual_grid_min_desktop": 240,
        "visual_grid_min_mobile": 190,
        "visual_card_radius": 12,
        "visual_card_shadow": 0,
        "visual_spacing_scale": 100,
        "visual_font_scale_desktop": 100,
        "visual_font_scale_mobile": 100,
        "visual_mobile_ui_scale": 100,
        "visual_fab_bottom_offset_mobile": 0,
        "visual_whatsapp_bottom_offset_mobile": 0,
        "visual_sections_order_desktop": "ofertas,destacados,categorias,agenda",
        "visual_sections_order_mobile": "agenda,ofertas,destacados,categorias",
        "visual_sections_visibility": {
            "ofertas": True,
            "destacados": True,
            "categorias": True,
            "agenda": True,
        },
        "visual_element_overrides": [],
        "visual_text_overrides": [],
        "catalogo_torta": catalogo_torta_base,
        "custom_css": "",
    }


def _normalizar_color_hex(raw, default):
    color = str(raw or "").strip().lower()
    if re.match(r"^#[0-9a-f]{6}$", color):
        return color
    if re.match(r"^#[0-9a-f]{3}$", color):
        return color
    return default


def _normalizar_url_personalizacion(raw):
    url = str(raw or "").strip()
    if not url:
        return ""
    if url.startswith("/static/"):
        return url
    if re.match(r"^https?://", url, re.IGNORECASE):
        return url
    return ""


def _escape_html_basico(texto):
    return (
        str(texto or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def _normalizar_html_liviano(raw, max_len=3000):
    txt = str(raw or "").replace("\r\n", "\n").replace("\r", "\n")
    txt = txt[: max(0, int(max_len or 0))]
    esc = _escape_html_basico(txt)

    whitelist = [
        ("&lt;b&gt;", "<b>"), ("&lt;/b&gt;", "</b>"),
        ("&lt;strong&gt;", "<strong>"), ("&lt;/strong&gt;", "</strong>"),
        ("&lt;i&gt;", "<i>"), ("&lt;/i&gt;", "</i>"),
        ("&lt;em&gt;", "<em>"), ("&lt;/em&gt;", "</em>"),
        ("&lt;u&gt;", "<u>"), ("&lt;/u&gt;", "</u>"),
        ("&lt;br&gt;", "<br>"), ("&lt;br/&gt;", "<br>"), ("&lt;br /&gt;", "<br>"),
    ]
    for src, dst in whitelist:
        esc = esc.replace(src, dst)

    return esc.replace("\n", "<br>")


def _normalizar_tienda_personalizacion(payload):
    base = _default_tienda_personalizacion()
    data = dict(payload or {})
    clean = dict(base)
    clean["brand_text"] = str(data.get("brand_text") or base["brand_text"]).strip()[:90] or base["brand_text"]
    clean["search_placeholder"] = str(data.get("search_placeholder") or base["search_placeholder"]).strip()[:120] or base["search_placeholder"]
    clean["menu_title"] = str(data.get("menu_title") or base["menu_title"]).strip()[:80] or base["menu_title"]
    clean["sec_ofertas_title"] = str(data.get("sec_ofertas_title") or base["sec_ofertas_title"]).strip()[:80] or base["sec_ofertas_title"]
    clean["sec_destacados_title"] = str(data.get("sec_destacados_title") or base["sec_destacados_title"]).strip()[:80] or base["sec_destacados_title"]
    clean["cart_title"] = str(data.get("cart_title") or base["cart_title"]).strip()[:80] or base["cart_title"]
    clean["empty_cart_text"] = str(data.get("empty_cart_text") or base["empty_cart_text"]).strip()[:140] or base["empty_cart_text"]

    clean["whatsapp_url"] = _normalizar_url_personalizacion(data.get("whatsapp_url")) or base["whatsapp_url"]
    clean["whatsapp_title"] = str(data.get("whatsapp_title") or base["whatsapp_title"]).strip()[:90] or base["whatsapp_title"]
    clean["whatsapp_icon_url"] = _normalizar_url_personalizacion(data.get("whatsapp_icon_url")) or base["whatsapp_icon_url"]
    clean["show_whatsapp_button"] = bool(data.get("show_whatsapp_button", base["show_whatsapp_button"]))

    clean["hero_enabled"] = bool(data.get("hero_enabled", base["hero_enabled"]))
    clean["hero_badge"] = str(data.get("hero_badge") or "").strip()[:60]
    clean["hero_title"] = str(data.get("hero_title") or "").strip()[:120]
    clean["hero_subtitle"] = str(data.get("hero_subtitle") or "").strip()[:260]
    clean["hero_cta_text"] = str(data.get("hero_cta_text") or "").strip()[:80]
    clean["hero_cta_url"] = _normalizar_url_personalizacion(data.get("hero_cta_url"))
    clean["hero_image_url"] = _normalizar_url_personalizacion(data.get("hero_image_url"))

    clean["banner_enabled"] = bool(data.get("banner_enabled", base["banner_enabled"]))
    clean["banner_text"] = str(data.get("banner_text") or "").strip()[:220]
    clean["banner_bg"] = _normalizar_color_hex(data.get("banner_bg"), base["banner_bg"])
    clean["banner_text_color"] = _normalizar_color_hex(data.get("banner_text_color"), base["banner_text_color"])

    clean["color_bg"] = _normalizar_color_hex(data.get("color_bg"), base["color_bg"])
    clean["color_panel"] = _normalizar_color_hex(data.get("color_panel"), base["color_panel"])
    clean["color_line"] = _normalizar_color_hex(data.get("color_line"), base["color_line"])
    clean["color_text"] = _normalizar_color_hex(data.get("color_text"), base["color_text"])
    clean["color_muted"] = _normalizar_color_hex(data.get("color_muted"), base["color_muted"])
    clean["color_accent"] = _normalizar_color_hex(data.get("color_accent"), base["color_accent"])
    clean["color_accent_dark"] = _normalizar_color_hex(data.get("color_accent_dark"), base["color_accent_dark"])
    clean["offer_card_border_color"] = _normalizar_color_hex(data.get("offer_card_border_color"), base["offer_card_border_color"])
    clean["offer_card_glow_color"] = _normalizar_color_hex(data.get("offer_card_glow_color"), base["offer_card_glow_color"])
    clean["offer_price_color"] = _normalizar_color_hex(data.get("offer_price_color"), base["offer_price_color"])
    clean["offer_discount_chip_bg"] = _normalizar_color_hex(data.get("offer_discount_chip_bg"), base["offer_discount_chip_bg"])
    clean["offer_discount_chip_text"] = _normalizar_color_hex(data.get("offer_discount_chip_text"), base["offer_discount_chip_text"])
    clean["offer_badge_enabled"] = bool(data.get("offer_badge_enabled", base["offer_badge_enabled"]))
    clean["offer_badge_text"] = str(data.get("offer_badge_text") or "").strip()[:28]
    clean["offer_badge_icon"] = str(data.get("offer_badge_icon") or "").strip()[:8]
    clean["offer_badge_bg"] = _normalizar_color_hex(data.get("offer_badge_bg"), base["offer_badge_bg"])
    clean["offer_badge_text_color"] = _normalizar_color_hex(data.get("offer_badge_text_color"), base["offer_badge_text_color"])
    clean["offer_float_icon"] = str(data.get("offer_float_icon") or "").strip()[:8]
    clean["offer_float_image_url"] = _normalizar_url_personalizacion(data.get("offer_float_image_url"))
    try:
        offer_float_image_size = int(data.get("offer_float_image_size") or base["offer_float_image_size"])
    except (TypeError, ValueError):
        offer_float_image_size = int(base["offer_float_image_size"])
    clean["offer_float_image_size"] = max(24, min(120, offer_float_image_size))

    clean["agenda_enabled"] = bool(data.get("agenda_enabled", base["agenda_enabled"]))
    clean["agenda_menu_label"] = str(data.get("agenda_menu_label") or base["agenda_menu_label"]).strip()[:60] or base["agenda_menu_label"]
    clean["agenda_type_label"] = str(data.get("agenda_type_label") or base["agenda_type_label"]).strip()[:60] or base["agenda_type_label"]
    clean["agenda_type_torta_text"] = str(data.get("agenda_type_torta_text") or base["agenda_type_torta_text"]).strip()[:60] or base["agenda_type_torta_text"]
    clean["agenda_type_pastel_text"] = str(data.get("agenda_type_pastel_text") or base["agenda_type_pastel_text"]).strip()[:60] or base["agenda_type_pastel_text"]
    clean["agenda_pastel_catalog_title"] = str(data.get("agenda_pastel_catalog_title") or base["agenda_pastel_catalog_title"]).strip()[:90] or base["agenda_pastel_catalog_title"]
    clean["agenda_pastel_catalog_button_text"] = str(data.get("agenda_pastel_catalog_button_text") or base["agenda_pastel_catalog_button_text"]).strip()[:70] or base["agenda_pastel_catalog_button_text"]
    clean["agenda_pastel_catalog_empty_text"] = str(data.get("agenda_pastel_catalog_empty_text") or base["agenda_pastel_catalog_empty_text"]).strip()[:180] or base["agenda_pastel_catalog_empty_text"]
    clean["agenda_pastel_mode_catalog_text"] = str(data.get("agenda_pastel_mode_catalog_text") or base["agenda_pastel_mode_catalog_text"]).strip()[:70] or base["agenda_pastel_mode_catalog_text"]
    clean["agenda_pastel_mode_special_text"] = str(data.get("agenda_pastel_mode_special_text") or base["agenda_pastel_mode_special_text"]).strip()[:80] or base["agenda_pastel_mode_special_text"]
    clean["agenda_pastel_special_title"] = str(data.get("agenda_pastel_special_title") or base["agenda_pastel_special_title"]).strip()[:90] or base["agenda_pastel_special_title"]
    clean["agenda_pastel_special_placeholder"] = str(data.get("agenda_pastel_special_placeholder") or base["agenda_pastel_special_placeholder"]).strip()[:260] or base["agenda_pastel_special_placeholder"]
    clean["agenda_pastel_special_help_text"] = str(data.get("agenda_pastel_special_help_text") or base["agenda_pastel_special_help_text"]).strip()[:220] or base["agenda_pastel_special_help_text"]
    clean["agenda_pastel_category_filter"] = str(data.get("agenda_pastel_category_filter") or base["agenda_pastel_category_filter"]).strip()[:220] or base["agenda_pastel_category_filter"]
    clean["agenda_section_title"] = str(data.get("agenda_section_title") or base["agenda_section_title"]).strip()[:90] or base["agenda_section_title"]
    clean["agenda_section_subtitle"] = _normalizar_html_liviano(
        data.get("agenda_section_subtitle") or base["agenda_section_subtitle"],
        max_len=3000,
    ) or base["agenda_section_subtitle"]
    clean["agenda_builder_title"] = str(data.get("agenda_builder_title") or base["agenda_builder_title"]).strip()[:80] or base["agenda_builder_title"]
    clean["agenda_summary_title"] = str(data.get("agenda_summary_title") or base["agenda_summary_title"]).strip()[:80] or base["agenda_summary_title"]
    clean["agenda_total_label"] = str(data.get("agenda_total_label") or base["agenda_total_label"]).strip()[:60] or base["agenda_total_label"]
    clean["agenda_delivery_retiro_text"] = str(data.get("agenda_delivery_retiro_text") or base["agenda_delivery_retiro_text"]).strip()[:50] or base["agenda_delivery_retiro_text"]
    clean["agenda_delivery_despacho_text"] = str(data.get("agenda_delivery_despacho_text") or base["agenda_delivery_despacho_text"]).strip()[:50] or base["agenda_delivery_despacho_text"]
    clean["agenda_placeholder_name"] = str(data.get("agenda_placeholder_name") or base["agenda_placeholder_name"]).strip()[:80] or base["agenda_placeholder_name"]
    clean["agenda_placeholder_email"] = str(data.get("agenda_placeholder_email") or base["agenda_placeholder_email"]).strip()[:120] or base["agenda_placeholder_email"]
    clean["agenda_placeholder_phone"] = str(data.get("agenda_placeholder_phone") or base["agenda_placeholder_phone"]).strip()[:20] or base["agenda_placeholder_phone"]
    clean["agenda_placeholder_detail"] = str(data.get("agenda_placeholder_detail") or base["agenda_placeholder_detail"]).strip()[:180] or base["agenda_placeholder_detail"]
    clean["agenda_placeholder_address"] = str(data.get("agenda_placeholder_address") or base["agenda_placeholder_address"]).strip()[:180] or base["agenda_placeholder_address"]
    clean["agenda_map_search_text"] = str(data.get("agenda_map_search_text") or base["agenda_map_search_text"]).strip()[:40] or base["agenda_map_search_text"]
    clean["agenda_map_confirm_text"] = str(data.get("agenda_map_confirm_text") or base["agenda_map_confirm_text"]).strip()[:40] or base["agenda_map_confirm_text"]
    clean["agenda_map_help_text"] = str(data.get("agenda_map_help_text") or base["agenda_map_help_text"]).strip()[:180] or base["agenda_map_help_text"]
    try:
        delivery_origin_lat = float(data.get("agenda_delivery_origin_lat") if data.get("agenda_delivery_origin_lat") is not None else base["agenda_delivery_origin_lat"])
    except (TypeError, ValueError):
        delivery_origin_lat = float(base["agenda_delivery_origin_lat"])
    try:
        delivery_origin_lng = float(data.get("agenda_delivery_origin_lng") if data.get("agenda_delivery_origin_lng") is not None else base["agenda_delivery_origin_lng"])
    except (TypeError, ValueError):
        delivery_origin_lng = float(base["agenda_delivery_origin_lng"])
    clean["agenda_delivery_origin_lat"] = max(-90.0, min(90.0, delivery_origin_lat))
    clean["agenda_delivery_origin_lng"] = max(-180.0, min(180.0, delivery_origin_lng))
    for key in ("agenda_delivery_fee_0_3", "agenda_delivery_fee_3_6", "agenda_delivery_fee_6_9", "agenda_delivery_fee_9_plus"):
        try:
            fee = int(float(data.get(key) if data.get(key) is not None else base[key]))
        except (TypeError, ValueError):
            fee = int(base[key])
        clean[key] = max(0, min(300000, fee))
    for idx in (1, 2, 3):
        key_start = f"agenda_delivery_band_{idx}_start"
        key_end = f"agenda_delivery_band_{idx}_end"
        key_extra = f"agenda_delivery_band_{idx}_extra"
        start_raw = str(data.get(key_start) if data.get(key_start) is not None else base.get(key_start) or "").strip()
        end_raw = str(data.get(key_end) if data.get(key_end) is not None else base.get(key_end) or "").strip()
        clean[key_start] = start_raw if _parse_hora_hhmm(start_raw) else str(base.get(key_start) or "")
        clean[key_end] = end_raw if _parse_hora_hhmm(end_raw) else str(base.get(key_end) or "")
        try:
            extra = int(float(data.get(key_extra) if data.get(key_extra) is not None else base.get(key_extra) or 0))
        except (TypeError, ValueError):
            extra = int(base.get(key_extra) or 0)
        clean[key_extra] = max(0, min(300000, extra))
    clean["agenda_delivery_note_text"] = str(data.get("agenda_delivery_note_text") or base["agenda_delivery_note_text"]).strip()[:220] or base["agenda_delivery_note_text"]
    clean["agenda_delivery_outside_warning"] = str(data.get("agenda_delivery_outside_warning") or base["agenda_delivery_outside_warning"]).strip()[:260] or base["agenda_delivery_outside_warning"]
    clean["agenda_delivery_zones"] = _normalizar_agenda_delivery_zones(data.get("agenda_delivery_zones") if data.get("agenda_delivery_zones") is not None else base.get("agenda_delivery_zones"))
    clean["agenda_form_button_text"] = str(data.get("agenda_form_button_text") or base["agenda_form_button_text"]).strip()[:50] or base["agenda_form_button_text"]
    clean["agenda_confirm_title"] = str(data.get("agenda_confirm_title") or base["agenda_confirm_title"]).strip()[:70] or base["agenda_confirm_title"]
    clean["agenda_confirm_warning"] = str(data.get("agenda_confirm_warning") or base["agenda_confirm_warning"]).strip()[:220] or base["agenda_confirm_warning"]
    clean["agenda_confirm_pdf_text"] = str(data.get("agenda_confirm_pdf_text") or base["agenda_confirm_pdf_text"]).strip()[:60] or base["agenda_confirm_pdf_text"]
    clean["agenda_confirm_cancel_text"] = str(data.get("agenda_confirm_cancel_text") or base["agenda_confirm_cancel_text"]).strip()[:40] or base["agenda_confirm_cancel_text"]
    clean["agenda_confirm_accept_text"] = str(data.get("agenda_confirm_accept_text") or base["agenda_confirm_accept_text"]).strip()[:80] or base["agenda_confirm_accept_text"]

    try:
        agenda_days = int(data.get("agenda_days_ahead") or base["agenda_days_ahead"])
    except (TypeError, ValueError):
        agenda_days = int(base["agenda_days_ahead"])
    clean["agenda_days_ahead"] = max(3, min(60, agenda_days))

    agenda_hora_inicio = str(data.get("agenda_hour_start") or base["agenda_hour_start"]).strip()
    agenda_hora_fin = str(data.get("agenda_hour_end") or base["agenda_hour_end"]).strip()
    clean["agenda_hour_start"] = agenda_hora_inicio if _parse_hora_hhmm(agenda_hora_inicio) else base["agenda_hour_start"]
    clean["agenda_hour_end"] = agenda_hora_fin if _parse_hora_hhmm(agenda_hora_fin) else base["agenda_hour_end"]

    try:
        slot_minutes = int(data.get("agenda_slot_minutes") or base["agenda_slot_minutes"])
    except (TypeError, ValueError):
        slot_minutes = int(base["agenda_slot_minutes"])
    if slot_minutes not in {30, 60, 90, 120}:
        slot_minutes = int(base["agenda_slot_minutes"])
    clean["agenda_slot_minutes"] = slot_minutes

    try:
        slot_capacity = int(data.get("agenda_slot_capacity") or base["agenda_slot_capacity"])
    except (TypeError, ValueError):
        slot_capacity = int(base["agenda_slot_capacity"])
    clean["agenda_slot_capacity"] = max(1, min(20, slot_capacity))

    try:
        day_close = int(data.get("agenda_day_close_min_orders") or base["agenda_day_close_min_orders"])
    except (TypeError, ValueError):
        day_close = int(base["agenda_day_close_min_orders"])
    clean["agenda_day_close_min_orders"] = max(1, min(20, day_close))

    try:
        event_buffer = int(data.get("agenda_event_buffer_minutes") or base["agenda_event_buffer_minutes"])
    except (TypeError, ValueError):
        event_buffer = int(base["agenda_event_buffer_minutes"])
    clean["agenda_event_buffer_minutes"] = max(0, min(360, event_buffer))

    clean["agenda_card_bg"] = _normalizar_color_hex(data.get("agenda_card_bg"), base["agenda_card_bg"])
    clean["agenda_card_border"] = _normalizar_color_hex(data.get("agenda_card_border"), base["agenda_card_border"])
    clean["agenda_slot_available_bg"] = _normalizar_color_hex(data.get("agenda_slot_available_bg"), base["agenda_slot_available_bg"])
    clean["agenda_slot_unavailable_bg"] = _normalizar_color_hex(data.get("agenda_slot_unavailable_bg"), base["agenda_slot_unavailable_bg"])
    clean["agenda_slot_unavailable_text"] = _normalizar_color_hex(data.get("agenda_slot_unavailable_text"), base["agenda_slot_unavailable_text"])

    try:
        clean["visual_layout_max_width"] = max(960, min(1920, int(data.get("visual_layout_max_width") or base["visual_layout_max_width"])))
    except (TypeError, ValueError):
        clean["visual_layout_max_width"] = int(base["visual_layout_max_width"])
    try:
        clean["visual_grid_min_desktop"] = max(160, min(380, int(data.get("visual_grid_min_desktop") or base["visual_grid_min_desktop"])))
    except (TypeError, ValueError):
        clean["visual_grid_min_desktop"] = int(base["visual_grid_min_desktop"])
    try:
        clean["visual_grid_min_mobile"] = max(130, min(280, int(data.get("visual_grid_min_mobile") or base["visual_grid_min_mobile"])))
    except (TypeError, ValueError):
        clean["visual_grid_min_mobile"] = int(base["visual_grid_min_mobile"])
    try:
        clean["visual_card_radius"] = max(6, min(28, int(data.get("visual_card_radius") or base["visual_card_radius"])))
    except (TypeError, ValueError):
        clean["visual_card_radius"] = int(base["visual_card_radius"])
    try:
        clean["visual_card_shadow"] = max(0, min(36, int(data.get("visual_card_shadow") or base["visual_card_shadow"])))
    except (TypeError, ValueError):
        clean["visual_card_shadow"] = int(base["visual_card_shadow"])
    try:
        clean["visual_spacing_scale"] = max(80, min(140, int(data.get("visual_spacing_scale") or base["visual_spacing_scale"])))
    except (TypeError, ValueError):
        clean["visual_spacing_scale"] = int(base["visual_spacing_scale"])
    try:
        clean["visual_font_scale_desktop"] = max(80, min(140, int(data.get("visual_font_scale_desktop") or base["visual_font_scale_desktop"])))
    except (TypeError, ValueError):
        clean["visual_font_scale_desktop"] = int(base["visual_font_scale_desktop"])
    try:
        clean["visual_font_scale_mobile"] = max(70, min(130, int(data.get("visual_font_scale_mobile") or base["visual_font_scale_mobile"])))
    except (TypeError, ValueError):
        clean["visual_font_scale_mobile"] = int(base["visual_font_scale_mobile"])
    try:
        clean["visual_mobile_ui_scale"] = max(50, min(140, int(data.get("visual_mobile_ui_scale") or base["visual_mobile_ui_scale"])))
    except (TypeError, ValueError):
        clean["visual_mobile_ui_scale"] = int(base["visual_mobile_ui_scale"])
    try:
        clean["visual_fab_bottom_offset_mobile"] = max(-120, min(200, int(data.get("visual_fab_bottom_offset_mobile") or base["visual_fab_bottom_offset_mobile"])))
    except (TypeError, ValueError):
        clean["visual_fab_bottom_offset_mobile"] = int(base["visual_fab_bottom_offset_mobile"])
    try:
        clean["visual_whatsapp_bottom_offset_mobile"] = max(-120, min(220, int(data.get("visual_whatsapp_bottom_offset_mobile") or base["visual_whatsapp_bottom_offset_mobile"])))
    except (TypeError, ValueError):
        clean["visual_whatsapp_bottom_offset_mobile"] = int(base["visual_whatsapp_bottom_offset_mobile"])
    clean["visual_sections_order_desktop"] = str(data.get("visual_sections_order_desktop") or base["visual_sections_order_desktop"]).strip()[:120] or base["visual_sections_order_desktop"]
    clean["visual_sections_order_mobile"] = str(data.get("visual_sections_order_mobile") or base["visual_sections_order_mobile"]).strip()[:120] or base["visual_sections_order_mobile"]
    vis_data = data.get("visual_sections_visibility")
    vis_base = dict(base.get("visual_sections_visibility") or {})
    vis_cfg = {}
    for key in ("ofertas", "destacados", "categorias", "agenda"):
        if isinstance(vis_data, dict) and key in vis_data:
            vis_cfg[key] = bool(vis_data.get(key))
        else:
            vis_cfg[key] = bool(vis_base.get(key, True))
    clean["visual_sections_visibility"] = vis_cfg
    raw_overrides = data.get("visual_element_overrides")
    overrides = []
    if isinstance(raw_overrides, list):
        for row in raw_overrides[:220]:
            if not isinstance(row, dict):
                continue
            selector = str(row.get("selector") or "").strip()[:180]
            prop = str(row.get("property") or "").strip().lower()[:60]
            value = str(row.get("value") or "").strip()[:220]
            target = str(row.get("target") or "both").strip().lower()
            enabled = bool(row.get("enabled", True))
            if not selector or not prop:
                continue
            if not re.match(r"^[a-z][a-z0-9\-]*$", prop):
                continue
            if target not in {"desktop", "mobile", "both"}:
                target = "both"
            if not re.match(r"^[#.\[:*a-zA-Z0-9_\-\s>,+~=\"'()]+$", selector):
                continue
            overrides.append({
                "selector": selector,
                "property": prop,
                "value": value,
                "target": target,
                "enabled": enabled,
            })
    clean["visual_element_overrides"] = overrides
    raw_text_overrides = data.get("visual_text_overrides")
    text_overrides = []
    if isinstance(raw_text_overrides, list):
        for row in raw_text_overrides[:220]:
            if not isinstance(row, dict):
                continue
            selector = str(row.get("selector") or "").strip()[:180]
            text = str(row.get("text") or "").strip()[:350]
            target = str(row.get("target") or "both").strip().lower()
            enabled = bool(row.get("enabled", True))
            if not selector:
                continue
            if target not in {"desktop", "mobile", "both"}:
                target = "both"
            if not re.match(r"^[#.\[:*a-zA-Z0-9_\-\s>,+~=\"'()]+$", selector):
                continue
            text_overrides.append({
                "selector": selector,
                "text": text,
                "target": target,
                "enabled": enabled,
            })
    clean["visual_text_overrides"] = text_overrides

    clean["catalogo_torta"] = _normalizar_catalogo_torta_cfg(data.get("catalogo_torta") or base.get("catalogo_torta"))

    clean["custom_css"] = str(data.get("custom_css") or "").strip()[:5000]
    return clean


def _normalizar_catalogo_torta_item(item, defaults, allow_max_sabores=False, allow_max_cantidad=False):
    row = dict(defaults)
    src = dict(item or {})
    item_id = str(src.get("id") or "").strip().lower()
    if not item_id:
        item_id = _slug_simple(src.get("nombre") or defaults.get("nombre") or "item")
    row["id"] = re.sub(r"[^a-z0-9\-]+", "-", item_id).strip("-")[:60] or _slug_simple(defaults.get("nombre") or "item")
    row["nombre"] = str(src.get("nombre") or defaults.get("nombre") or "Item").strip()[:80] or str(defaults.get("nombre") or "Item")
    try:
        precio = float(src.get("precio") if src.get("precio") is not None else defaults.get("precio") or 0)
    except (TypeError, ValueError):
        precio = float(defaults.get("precio") or 0)
    row["precio"] = max(0, round(precio, 2))
    row["activo"] = bool(src.get("activo", defaults.get("activo", True)))
    row["categoria_id"] = re.sub(r"[^a-z0-9\-]+", "-", str(src.get("categoria_id") or defaults.get("categoria_id") or "").strip().lower()).strip("-")[:60]
    if allow_max_sabores:
        try:
            max_sabores = int(src.get("max_sabores") if src.get("max_sabores") is not None else defaults.get("max_sabores") or 3)
        except (TypeError, ValueError):
            max_sabores = int(defaults.get("max_sabores") or 3)
        row["max_sabores"] = max(1, min(8, max_sabores))
    if allow_max_cantidad:
        try:
            max_cantidad = int(src.get("max_cantidad") if src.get("max_cantidad") is not None else defaults.get("max_cantidad") or 1)
        except (TypeError, ValueError):
            max_cantidad = int(defaults.get("max_cantidad") or 1)
        row["max_cantidad"] = max(1, min(20, max_cantidad))
    return row


def _normalizar_catalogo_torta_cfg(raw):
    base = _default_tienda_personalizacion().get("catalogo_torta") or {}
    data = dict(raw or {})
    out = {
        "enabled": bool(data.get("enabled", base.get("enabled", True))),
        "show_prices": bool(data.get("show_prices", base.get("show_prices", True))),
        "categorias": [],
        "sizes": [],
        "sabores": [],
        "extras": [],
        "toppers": [],
    }
    try:
        max_extra_items = int(data.get("max_extra_items") or base.get("max_extra_items") or 8)
    except (TypeError, ValueError):
        max_extra_items = int(base.get("max_extra_items") or 8)
    out["max_extra_items"] = max(1, min(20, max_extra_items))
    try:
        max_reference_images = int(data.get("max_reference_images") or base.get("max_reference_images") or 3)
    except (TypeError, ValueError):
        max_reference_images = int(base.get("max_reference_images") or 3)
    out["max_reference_images"] = max(0, min(10, max_reference_images))

    cats_in = data.get("categorias")
    if not isinstance(cats_in, list):
        cats_in = list(base.get("categorias") or [])
    used_cat_ids = set()
    used_cat_keys = set()
    for item in cats_in:
        cat = dict(item or {})
        cat_key = re.sub(r"[^a-z0-9_\-]+", "-", str(cat.get("key") or "").strip().lower()).strip("-_")[:40]
        if not cat_key:
            cat_key = f"cat-{uuid.uuid4().hex[:12]}"
        while cat_key in used_cat_keys:
            cat_key = f"cat-{uuid.uuid4().hex[:12]}"
        used_cat_keys.add(cat_key)
        cid_base = re.sub(r"[^a-z0-9\-]+", "-", str(cat.get("id") or "").strip().lower()).strip("-")[:60] or _slug_simple(cat.get("nombre") or "categoria")
        cid = cid_base
        n_dup = 2
        while cid in used_cat_ids:
            suf = f"-{n_dup}"
            cid = f"{cid_base[: max(1, 60 - len(suf))]}{suf}"
            n_dup += 1
        used_cat_ids.add(cid)
        nombre = str(cat.get("nombre") or "").strip()[:80] or "Categoria"
        descripcion = str(cat.get("descripcion") or "").strip()[:180]
        badge = str(cat.get("badge") or "").strip()[:32]
        imagen_url = _normalizar_url_personalizacion(cat.get("imagen_url"))
        try:
            min_lead_hours = int(cat.get("min_lead_hours") if cat.get("min_lead_hours") is not None else 48)
        except (TypeError, ValueError):
            min_lead_hours = 48
        min_lead_hours = max(1, min(336, min_lead_hours))
        use_category_ingredients = bool(cat.get("use_category_ingredients", False))
        sabores_ids = [
            re.sub(r"[^a-z0-9\-]+", "-", str(x or "").strip().lower()).strip("-")[:60]
            for x in (cat.get("sabores_ids") if isinstance(cat.get("sabores_ids"), list) else [])
            if str(x or "").strip()
        ]
        extras_ids = [
            re.sub(r"[^a-z0-9\-]+", "-", str(x or "").strip().lower()).strip("-")[:60]
            for x in (cat.get("extras_ids") if isinstance(cat.get("extras_ids"), list) else [])
            if str(x or "").strip()
        ]
        toppers_ids = [
            re.sub(r"[^a-z0-9\-]+", "-", str(x or "").strip().lower()).strip("-")[:60]
            for x in (cat.get("toppers_ids") if isinstance(cat.get("toppers_ids"), list) else [])
            if str(x or "").strip()
        ]
        out["categorias"].append(
            {
                "key": cat_key,
                "id": cid,
                "nombre": nombre,
                "activo": bool(cat.get("activo", True)),
                "descripcion": descripcion,
                "badge": badge,
                "imagen_url": imagen_url,
                "min_lead_hours": min_lead_hours,
                "use_category_ingredients": use_category_ingredients,
                "sabores_ids": list(dict.fromkeys(sabores_ids)),
                "extras_ids": list(dict.fromkeys(extras_ids)),
                "toppers_ids": list(dict.fromkeys(toppers_ids)),
            }
        )
    if not out["categorias"]:
        out["categorias"] = list(base.get("categorias") or [{"id": "general", "nombre": "General", "activo": True}])

    categorias_validas = {str(c.get("id") or "") for c in out["categorias"]}

    sizes_in = data.get("sizes")
    if not isinstance(sizes_in, list):
        sizes_in = list(base.get("sizes") or [])
    for item in sizes_in:
        norm = _normalizar_catalogo_torta_item(
            item,
            {"id": "size", "categoria_id": next(iter(categorias_validas), "general"), "nombre": "Tamano", "precio": 0, "max_sabores": 3, "activo": True},
            allow_max_sabores=True,
        )
        if not norm.get("categoria_id") or norm.get("categoria_id") not in categorias_validas:
            norm["categoria_id"] = next(iter(categorias_validas), "general")
        out["sizes"].append(norm)

    sabores_in = data.get("sabores")
    if not isinstance(sabores_in, list):
        sabores_in = list(base.get("sabores") or [])
    for item in sabores_in:
        out["sabores"].append(
            _normalizar_catalogo_torta_item(
                item,
                {"id": "sabor", "nombre": "Sabor", "precio": 0, "activo": True},
            )
        )

    extras_in = data.get("extras")
    if not isinstance(extras_in, list):
        extras_in = list(base.get("extras") or [])
    for item in extras_in:
        out["extras"].append(
            _normalizar_catalogo_torta_item(
                item,
                {"id": "extra", "nombre": "Extra", "precio": 0, "max_cantidad": 1, "activo": True},
                allow_max_cantidad=True,
            )
        )

    toppers_in = data.get("toppers")
    if not isinstance(toppers_in, list):
        toppers_in = list(base.get("toppers") or [])
    for item in toppers_in:
        out["toppers"].append(
            _normalizar_catalogo_torta_item(
                item,
                {"id": "topper", "nombre": "Topper", "precio": 0, "activo": True},
            )
        )

    if not out["sizes"]:
        out["sizes"] = list(base.get("sizes") or [])
    if not out["sabores"]:
        out["sabores"] = list(base.get("sabores") or [])
    if not out["extras"]:
        out["extras"] = list(base.get("extras") or [])
    if not out["toppers"]:
        out["toppers"] = list(base.get("toppers") or [])

    # Sincronizacion defensiva de listas por categoria:
    # si una categoria usa ingredientes por categoria, autocompleta con todos los activos
    # para evitar que nuevos items queden ocultos en tienda/agenda.
    sabores_activos_ids = [str(x.get("id") or "").strip() for x in (out.get("sabores") or []) if bool(x.get("activo")) and str(x.get("id") or "").strip()]
    extras_activos_ids = [str(x.get("id") or "").strip() for x in (out.get("extras") or []) if bool(x.get("activo")) and str(x.get("id") or "").strip()]
    toppers_activos_ids = [str(x.get("id") or "").strip() for x in (out.get("toppers") or []) if bool(x.get("activo")) and str(x.get("id") or "").strip()]

    sabores_set = set(sabores_activos_ids)
    extras_set = set(extras_activos_ids)
    toppers_set = set(toppers_activos_ids)

    def _sync_ids(ids_raw, ids_set, ids_ordered, autocompletar=False):
        out_ids = []
        seen = set()
        for rid in (ids_raw or []):
            sid = str(rid or "").strip()
            if not sid or sid in seen or sid not in ids_set:
                continue
            seen.add(sid)
            out_ids.append(sid)
        if autocompletar:
            for sid in ids_ordered:
                if sid in seen:
                    continue
                seen.add(sid)
                out_ids.append(sid)
        return out_ids

    for cat in (out.get("categorias") or []):
        use_cat = bool(cat.get("use_category_ingredients", False))
        cat["sabores_ids"] = _sync_ids(cat.get("sabores_ids") or [], sabores_set, sabores_activos_ids, autocompletar=use_cat)
        cat["extras_ids"] = _sync_ids(cat.get("extras_ids") or [], extras_set, extras_activos_ids, autocompletar=use_cat)
        cat["toppers_ids"] = _sync_ids(cat.get("toppers_ids") or [], toppers_set, toppers_activos_ids, autocompletar=use_cat)

    return out


def _catalogo_torta_publico(cfg):
    cat = _normalizar_catalogo_torta_cfg(cfg)
    sabores_activos = [x for x in (cat.get("sabores") or []) if bool(x.get("activo"))]
    extras_activos = [x for x in (cat.get("extras") or []) if bool(x.get("activo"))]
    toppers_activos = [x for x in (cat.get("toppers") or []) if bool(x.get("activo"))]
    sabores_ids = {str(x.get("id") or "") for x in sabores_activos}
    extras_ids = {str(x.get("id") or "") for x in extras_activos}
    toppers_ids = {str(x.get("id") or "") for x in toppers_activos}
    sabores_ids_orden = [str(x.get("id") or "") for x in sabores_activos if str(x.get("id") or "")]
    extras_ids_orden = [str(x.get("id") or "") for x in extras_activos if str(x.get("id") or "")]
    toppers_ids_orden = [str(x.get("id") or "") for x in toppers_activos if str(x.get("id") or "")]

    def _sync_ids_categoria(ids_raw, activos_set, activos_orden, autocompletar=True):
        ids_limpios = []
        vistos = set()
        for rid in (ids_raw or []):
            sid = str(rid or "").strip()
            if not sid or sid in vistos or sid not in activos_set:
                continue
            vistos.add(sid)
            ids_limpios.append(sid)
        if autocompletar:
            for sid in activos_orden:
                if sid in vistos:
                    continue
                vistos.add(sid)
                ids_limpios.append(sid)
        return ids_limpios

    categorias_activas = [x for x in (cat.get("categorias") or []) if bool(x.get("activo"))]
    categorias_activas = [
        {
            **x,
            "min_lead_hours": max(1, int(x.get("min_lead_hours") or 48)),
            "use_category_ingredients": bool(x.get("use_category_ingredients", False)),
            # Sincronizacion automatica: nuevos activos agregados en admin
            # se reflejan tambien en tienda y agenda manual.
            "sabores_ids": _sync_ids_categoria(
                x.get("sabores_ids") or [],
                sabores_ids,
                sabores_ids_orden,
                autocompletar=bool(x.get("use_category_ingredients", False)),
            ),
            "extras_ids": _sync_ids_categoria(
                x.get("extras_ids") or [],
                extras_ids,
                extras_ids_orden,
                autocompletar=bool(x.get("use_category_ingredients", False)),
            ),
            "toppers_ids": _sync_ids_categoria(
                x.get("toppers_ids") or [],
                toppers_ids,
                toppers_ids_orden,
                autocompletar=bool(x.get("use_category_ingredients", False)),
            ),
        }
        for x in categorias_activas
    ]
    sizes_activas = [x for x in (cat.get("sizes") or []) if bool(x.get("activo"))]
    if not categorias_activas:
        ids_detectadas = []
        seen = set()
        for s in sizes_activas:
            cid = str(s.get("categoria_id") or "").strip()
            if not cid or cid in seen:
                continue
            seen.add(cid)
            ids_detectadas.append(cid)
        if ids_detectadas:
            categorias_activas = [
                {"id": cid, "nombre": str(cid).replace("-", " ").title(), "activo": True}
                for cid in ids_detectadas
            ]
        else:
            categorias_activas = [{"id": "general", "nombre": "General", "activo": True}]
    cat_ids = {str(x.get("id") or "") for x in categorias_activas}
    sizes_publicas = []
    default_cat = str((categorias_activas[0] or {}).get("id") or "general")
    for s in sizes_activas:
        row = dict(s)
        cid = str(row.get("categoria_id") or "").strip()
        if not cid or cid not in cat_ids:
            row["categoria_id"] = default_cat
        sizes_publicas.append(row)
    return {
        "enabled": bool(cat.get("enabled")),
        "show_prices": bool(cat.get("show_prices")),
        "max_extra_items": int(cat.get("max_extra_items") or 8),
        "max_reference_images": int(cat.get("max_reference_images") or 3),
        "categorias": categorias_activas,
        "sizes": sizes_publicas,
        "sabores": sabores_activos,
        "extras": extras_activos,
        "toppers": toppers_activos,
    }


def _catalogo_torta_categoria_publica(catalogo_publico, categoria_id):
    cid = str(categoria_id or "").strip().lower()
    if not cid:
        return None
    for row in (catalogo_publico.get("categorias") or []):
        if str(row.get("id") or "").strip().lower() == cid:
            return row
    return None


def _validar_payload_catalogo_torta(payload, catalogo_publico):
    data = dict(payload or {})
    categoria_id = str(data.get("categoria_id") or "").strip().lower()
    size_id = str(data.get("size_id") or "").strip().lower()
    sabor_ids = data.get("sabor_ids") if isinstance(data.get("sabor_ids"), list) else []
    extra_items = data.get("extra_items") if isinstance(data.get("extra_items"), list) else []
    topper_id = str(data.get("topper_id") or "").strip().lower()
    referencia_urls = data.get("referencia_urls") if isinstance(data.get("referencia_urls"), list) else []
    nota = str(data.get("nota") or "").strip()[:500]

    sizes = {str(x.get("id")): x for x in (catalogo_publico.get("sizes") or [])}
    sabores = {str(x.get("id")): x for x in (catalogo_publico.get("sabores") or [])}
    extras = {str(x.get("id")): x for x in (catalogo_publico.get("extras") or [])}
    toppers = {str(x.get("id")): x for x in (catalogo_publico.get("toppers") or [])}

    size = sizes.get(size_id)
    if not size:
        raise ValueError("Debes seleccionar un tamano de torta valido")
    size_categoria_id = str(size.get("categoria_id") or "").strip().lower()
    if not categoria_id:
        categoria_id = size_categoria_id
    categoria = _catalogo_torta_categoria_publica(catalogo_publico, categoria_id) or _catalogo_torta_categoria_publica(catalogo_publico, size_categoria_id)
    if not categoria:
        categoria = {"id": size_categoria_id or categoria_id or "general", "nombre": "Categoria", "min_lead_hours": 48, "use_category_ingredients": False}
    categoria_id = str(categoria.get("id") or size_categoria_id or categoria_id or "general")
    use_category_ingredients = bool(categoria.get("use_category_ingredients", False))
    allowed_sabores = {str(x or "").strip().lower() for x in (categoria.get("sabores_ids") or []) if str(x or "").strip()} if use_category_ingredients else set()
    allowed_extras = {str(x or "").strip().lower() for x in (categoria.get("extras_ids") or []) if str(x or "").strip()} if use_category_ingredients else set()
    allowed_toppers = {str(x or "").strip().lower() for x in (categoria.get("toppers_ids") or []) if str(x or "").strip()} if use_category_ingredients else set()

    sabores_limpios = []
    seen_flavors = set()
    max_sabores = max(1, int(size.get("max_sabores") or 3))
    for raw_sid in sabor_ids[: max_sabores + 2]:
        sid = str(raw_sid or "").strip().lower()
        if not sid or sid in seen_flavors:
            continue
        sabor = sabores.get(sid)
        if not sabor:
            continue
        if use_category_ingredients and sid not in allowed_sabores:
            continue
        sabores_limpios.append(sabor)
        seen_flavors.add(sid)
        if len(sabores_limpios) >= max_sabores:
            break
    if not sabores_limpios:
        if use_category_ingredients:
            raise ValueError("Debes seleccionar al menos un sabor permitido para esta categoria")
        raise ValueError("Debes seleccionar al menos un sabor")

    extras_final = []
    max_extra_items = max(1, int(catalogo_publico.get("max_extra_items") or 8))
    for raw_item in extra_items[:40]:
        item = dict(raw_item or {})
        eid = str(item.get("id") or "").strip().lower()
        if not eid:
            continue
        extra = extras.get(eid)
        if not extra:
            continue
        if use_category_ingredients and eid not in allowed_extras:
            continue
        try:
            qty = int(item.get("qty") or 0)
        except (TypeError, ValueError):
            qty = 0
        if qty <= 0:
            continue
        qty = min(qty, int(extra.get("max_cantidad") or 1))
        extras_final.append({"id": extra.get("id"), "nombre": extra.get("nombre"), "qty": qty, "precio": float(extra.get("precio") or 0)})
        if len(extras_final) >= max_extra_items:
            break

    topper = toppers.get(topper_id) if topper_id else None
    if topper_id and not topper:
        raise ValueError("El topper seleccionado no es valido")
    if topper and use_category_ingredients and str(topper.get("id") or "").strip().lower() not in allowed_toppers:
        raise ValueError("El topper seleccionado no esta permitido para esta categoria")

    max_refs = max(0, int(catalogo_publico.get("max_reference_images") or 3))
    refs_limpias = []
    for raw_url in referencia_urls[: max_refs + 2]:
        url = _normalizar_url_personalizacion(raw_url)
        if not url:
            continue
        refs_limpias.append(url)
        if len(refs_limpias) >= max_refs:
            break

    subtotal = float(size.get("precio") or 0) + sum(float(x.get("precio") or 0) for x in sabores_limpios)
    subtotal += sum(float(x.get("precio") or 0) * int(x.get("qty") or 0) for x in extras_final)
    if topper:
        subtotal += float(topper.get("precio") or 0)

    return {
        "categoria": {
            "id": categoria_id,
            "nombre": str(categoria.get("nombre") or "Categoria"),
            "min_lead_hours": max(1, int(categoria.get("min_lead_hours") or 48)),
            "use_category_ingredients": use_category_ingredients,
        },
        "size": {"id": size.get("id"), "nombre": size.get("nombre"), "precio": float(size.get("precio") or 0), "max_sabores": max_sabores},
        "sabores": [{"id": s.get("id"), "nombre": s.get("nombre"), "precio": float(s.get("precio") or 0)} for s in sabores_limpios],
        "extras": extras_final,
        "topper": {"id": topper.get("id"), "nombre": topper.get("nombre"), "precio": float(topper.get("precio") or 0)} if topper else None,
        "referencia_urls": refs_limpias,
        "nota": nota,
        "subtotal": round(subtotal, 2),
    }


def _obtener_tienda_personalizacion(apply_programacion=True, editor_mode="live"):
    conn = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT config_json, draft_config_json
            FROM tienda_personalizacion
            WHERE id = 1
            LIMIT 1
            """
        )
        row = cursor.fetchone()
        if not row:
            return _default_tienda_personalizacion()
        mode = str(editor_mode or "live").strip().lower()
        if mode == "draft":
            raw_json = str(row["draft_config_json"] or "").strip()
            if not raw_json:
                raw_json = str(row["config_json"] or "").strip()
        else:
            raw_json = str(row["config_json"] or "").strip()
        if not raw_json:
            return _default_tienda_personalizacion()
        try:
            payload = json.loads(raw_json)
        except Exception:
            payload = {}
        base = _normalizar_tienda_personalizacion(payload)
        if (not apply_programacion) or mode == "draft":
            return base
        return _aplicar_programacion_personalizacion(conn, base)
    except sqlite3.OperationalError:
        # Evita tumbar la tienda publica si la BD esta temporalmente en modo
        # solo lectura, sin espacio o con error de I/O.
        return _default_tienda_personalizacion()
    except Exception:
        return _default_tienda_personalizacion()
    finally:
        if conn:
            conn.close()


def _guardar_tienda_personalizacion(payload, target="live", origen="manual"):
    target_mode = str(target or "live").strip().lower()
    if target_mode not in {"live", "draft"}:
        target_mode = "live"
    actual = _obtener_tienda_personalizacion(apply_programacion=False, editor_mode=target_mode)
    merged = dict(actual)
    merged.update(dict(payload or {}))
    config = _normalizar_tienda_personalizacion(merged)
    conn = None
    try:
        conn = get_db()
        _asegurar_presets_personalizacion(conn)
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO tienda_personalizacion_versiones (origen, config_json, creado_en)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            """,
            (str(origen or "manual")[:40], json.dumps(actual, ensure_ascii=False)),
        )
        cursor.execute(
            """
            SELECT config_json, draft_config_json
            FROM tienda_personalizacion
            WHERE id = 1
            LIMIT 1
            """
        )
        row = cursor.fetchone() or {}
        current_live = str((row.get("config_json") if isinstance(row, dict) else row["config_json"]) or "").strip() if row else ""
        current_draft = str((row.get("draft_config_json") if isinstance(row, dict) else row["draft_config_json"]) or "").strip() if row else ""
        if not current_live:
            current_live = json.dumps(_default_tienda_personalizacion(), ensure_ascii=False)
        if not current_draft:
            current_draft = current_live
        next_live = json.dumps(config, ensure_ascii=False) if target_mode == "live" else current_live
        next_draft = json.dumps(config, ensure_ascii=False) if target_mode == "draft" else current_draft
        cursor.execute(
            """
            INSERT INTO tienda_personalizacion (id, config_json, draft_config_json, actualizado_en)
            VALUES (1, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(id) DO UPDATE SET
                config_json = excluded.config_json,
                draft_config_json = excluded.draft_config_json,
                actualizado_en = CURRENT_TIMESTAMP
            """,
            (next_live, next_draft),
        )
        conn.commit()
        return config
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def _slug_simple(text):
    slug = re.sub(r"[^a-z0-9]+", "-", str(text or "").strip().lower())
    slug = slug.strip("-")
    return slug[:60] or f"preset-{int(time.time())}"


def _presets_built_in():
    base = _default_tienda_personalizacion()
    minimal = dict(base)
    minimal.update({
        "brand_text": "Sucree Store",
        "hero_enabled": False,
        "banner_enabled": False,
        "color_bg": "#f8fafc",
        "color_panel": "#ffffff",
        "color_accent": "#2563eb",
        "color_accent_dark": "#1d4ed8",
        "offer_badge_text": "Oferta",
        "offer_badge_icon": "🏷️",
        "offer_float_icon": "✨",
    })
    premium = dict(base)
    premium.update({
        "brand_text": "Sucree Boutique",
        "hero_enabled": True,
        "hero_badge": "Coleccion exclusiva",
        "hero_title": "Edicion Premium",
        "hero_subtitle": "Postres de autor con retiro en tienda.",
        "banner_enabled": True,
        "banner_text": "Envios limitados hoy. Reserva temprano.",
        "color_bg": "#0f172a",
        "color_panel": "#111827",
        "color_line": "#334155",
        "color_text": "#f8fafc",
        "color_muted": "#cbd5e1",
        "color_accent": "#f59e0b",
        "color_accent_dark": "#d97706",
        "offer_badge_text": "Premium",
        "offer_badge_icon": "💎",
        "offer_float_icon": "🌟",
        "offer_discount_chip_bg": "#b91c1c",
    })
    temporada = dict(base)
    temporada.update({
        "brand_text": "Sucree Temporada",
        "hero_enabled": True,
        "hero_badge": "Especial de temporada",
        "hero_title": "Sabores de edicion limitada",
        "hero_subtitle": "Aprovecha nuestras recetas por tiempo limitado.",
        "banner_enabled": True,
        "banner_text": "Campana activa: no te quedes sin tu favorito.",
        "color_bg": "#fff7ed",
        "color_panel": "#ffffff",
        "color_line": "#fed7aa",
        "color_accent": "#ea580c",
        "color_accent_dark": "#c2410c",
        "offer_badge_text": "Temporada",
        "offer_badge_icon": "🎉",
        "offer_float_icon": "🍓",
        "offer_card_glow_color": "#f97316",
    })
    return [
        {"slug": "minimal", "nombre": "Minimal", "config": minimal},
        {"slug": "premium", "nombre": "Premium", "config": premium},
        {"slug": "temporada", "nombre": "Temporada", "config": temporada},
    ]


def _asegurar_presets_personalizacion(conn):
    cursor = conn.cursor()
    for item in _presets_built_in():
        cursor.execute(
            """
            INSERT INTO tienda_personalizacion_presets (nombre, slug, config_json, built_in, creado_en, actualizado_en)
            VALUES (?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(slug) DO UPDATE SET
                nombre = excluded.nombre,
                config_json = excluded.config_json,
                built_in = 1,
                actualizado_en = CURRENT_TIMESTAMP
            """,
            (item["nombre"], item["slug"], json.dumps(item["config"], ensure_ascii=False)),
        )
    conn.commit()


def _serializar_preset_row(row):
    item = dict(row)
    try:
        payload = json.loads(str(item.get("config_json") or "{}"))
    except Exception:
        payload = {}
    return {
        "id": int(item.get("id") or 0),
        "nombre": str(item.get("nombre") or "").strip(),
        "slug": str(item.get("slug") or "").strip(),
        "built_in": bool(item.get("built_in")),
        "config": _normalizar_tienda_personalizacion(payload),
        "creado_en": item.get("creado_en"),
        "actualizado_en": item.get("actualizado_en"),
    }


def _aplicar_programacion_personalizacion(conn, base_cfg):
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, nombre, config_json, fecha_inicio, fecha_fin, dias_semana, hora_inicio, hora_fin, prioridad
            FROM tienda_personalizacion_programaciones
            WHERE activo = 1
            ORDER BY prioridad DESC, id DESC
            """
        )
        rows = cursor.fetchall()
    except Exception:
        return base_cfg
    if not rows:
        return base_cfg

    now_local = datetime.now(ZoneInfo("America/Santiago"))
    now_date = now_local.date()
    active_overrides = []
    for row in rows:
        item = dict(row)
        f_ini = _parse_fecha_yyyy_mm_dd(item.get("fecha_inicio"))
        f_fin = _parse_fecha_yyyy_mm_dd(item.get("fecha_fin"))
        if f_ini and now_date < f_ini:
            continue
        if f_fin and now_date > f_fin:
            continue
        dias = _parse_dias_semana(item.get("dias_semana"))
        if dias and now_local.isoweekday() not in dias:
            continue
        if not _franja_horaria_activa(item.get("hora_inicio"), item.get("hora_fin"), now_local):
            continue
        try:
            cfg = json.loads(str(item.get("config_json") or "{}"))
        except Exception:
            cfg = {}
        active_overrides.append(cfg)

    if not active_overrides:
        return base_cfg
    merged = dict(base_cfg or {})
    for override in active_overrides:
        merged.update(dict(override or {}))
    return _normalizar_tienda_personalizacion(merged)


def _evaluar_estado_tienda(config):
    cfg = dict(config or {})
    modo = str(cfg.get("modo_manual") or "auto").strip().lower()
    horario_habilitado = bool(cfg.get("horario_habilitado"))
    hora_apertura = str(cfg.get("hora_apertura") or "09:00").strip()
    hora_cierre = str(cfg.get("hora_cierre") or "19:00").strip()
    now_local = datetime.now(ZoneInfo("America/Santiago"))
    hora_actual = now_local.strftime("%H:%M")

    if modo == "abierta":
        return {
            "abierta": True,
            "modo": "manual_abierta",
            "mensaje": "La tienda esta abierta por control manual del administrador.",
            "hora_actual": hora_actual,
        }
    if modo == "cerrada":
        return {
            "abierta": False,
            "modo": "manual_cerrada",
            "mensaje": "La tienda esta cerrada por control manual del administrador.",
            "hora_actual": hora_actual,
        }
    if not horario_habilitado:
        return {
            "abierta": True,
            "modo": "auto_sin_horario",
            "mensaje": "La tienda esta abierta (sin horario restringido).",
            "hora_actual": hora_actual,
        }

    inicio = _parse_hora_hhmm(hora_apertura)
    fin = _parse_hora_hhmm(hora_cierre)
    if not inicio or not fin:
        return {
            "abierta": True,
            "modo": "auto_error_horario",
            "mensaje": "Horario invalido en configuracion. Se mantiene abierta.",
            "hora_actual": hora_actual,
        }

    min_inicio = inicio[0] * 60 + inicio[1]
    min_fin = fin[0] * 60 + fin[1]
    min_actual = now_local.hour * 60 + now_local.minute
    if min_inicio == min_fin:
        abierta = True
    elif min_inicio < min_fin:
        abierta = min_inicio <= min_actual < min_fin
    else:
        # tramo que cruza medianoche, ejemplo 22:00 -> 06:00
        abierta = min_actual >= min_inicio or min_actual < min_fin

    if abierta:
        msg = f"Tienda abierta por horario ({hora_apertura} - {hora_cierre})."
        modo_final = "auto_horario_abierta"
    else:
        msg = f"Tienda cerrada por horario ({hora_apertura} - {hora_cierre})."
        modo_final = "auto_horario_cerrada"
    return {
        "abierta": bool(abierta),
        "modo": modo_final,
        "mensaje": msg,
        "hora_actual": hora_actual,
    }


def _parse_dias_semana(raw):
    txt = str(raw or "").strip()
    if not txt:
        return set()
    dias = set()
    for part in re.split(r"[\s,;|]+", txt):
        if not part:
            continue
        try:
            d = int(part)
        except (TypeError, ValueError):
            continue
        if 1 <= d <= 7:
            dias.add(d)
    return dias


def _franja_horaria_activa(hora_inicio, hora_fin, now_local):
    ini = _parse_hora_hhmm(hora_inicio)
    fin = _parse_hora_hhmm(hora_fin)
    if not ini or not fin:
        return True
    min_ini = ini[0] * 60 + ini[1]
    min_fin = fin[0] * 60 + fin[1]
    min_now = now_local.hour * 60 + now_local.minute
    if min_ini == min_fin:
        return True
    if min_ini < min_fin:
        return min_ini <= min_now < min_fin
    return min_now >= min_ini or min_now < min_fin


def _cargar_categorias_tienda():
    conn = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, nombre, activo, orden, descuento_pct, horario_habilitado, dias_semana, hora_inicio, hora_fin
            FROM tienda_categorias
            ORDER BY orden ASC, nombre COLLATE NOCASE ASC
            """
        )
        return [dict(r) for r in cursor.fetchall()]
    finally:
        if conn:
            conn.close()


def _evaluar_categoria_activa(cat, now_local=None):
    item = dict(cat or {})
    if not now_local:
        now_local = datetime.now(ZoneInfo("America/Santiago"))
    if not bool(item.get("activo")):
        return {"activa": False, "motivo": "Categoria desactivada"}
    if not bool(item.get("horario_habilitado")):
        return {"activa": True, "motivo": "Sin restriccion horaria"}

    dias = _parse_dias_semana(item.get("dias_semana"))
    if dias and now_local.isoweekday() not in dias:
        return {"activa": False, "motivo": "Fuera de dias habilitados"}
    if not _franja_horaria_activa(item.get("hora_inicio"), item.get("hora_fin"), now_local):
        return {"activa": False, "motivo": "Fuera de horario"}
    return {"activa": True, "motivo": "Activa por horario"}


def _obtener_cupon_por_codigo(codigo):
    codigo_norm = _normalizar_cupon_codigo(codigo)
    if not codigo_norm:
        return None
    conn = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM tienda_cupones WHERE codigo = ? LIMIT 1", (codigo_norm,))
        row = cursor.fetchone()
        return dict(row) if row else None
    finally:
        if conn:
            conn.close()


def _listar_cupones_regalados_cliente_cursor(cursor, cliente_id, solo_disponibles=False):
    sql = """
        SELECT cc.id, cc.cliente_id, cc.cupon_id, cc.activo, cc.usado, cc.usado_venta_id, cc.nota,
               cc.asignado_por, cc.fecha_asignado, cc.fecha_vencimiento, cc.fecha_usado,
               c.codigo, c.nombre, c.tipo_descuento, c.valor_descuento, c.activo AS cupon_activo,
               c.fecha_inicio, c.fecha_fin, c.monto_minimo
        FROM tienda_cliente_cupones cc
        JOIN tienda_cupones c ON c.id = cc.cupon_id
        WHERE cc.cliente_id = ?
    """
    params = [int(cliente_id)]
    if solo_disponibles:
        hoy = datetime.now().date().isoformat()
        sql += """
            AND cc.activo = 1
            AND cc.usado = 0
            AND c.activo = 1
            AND (cc.fecha_vencimiento IS NULL OR TRIM(cc.fecha_vencimiento) = '' OR cc.fecha_vencimiento >= ?)
            AND (c.fecha_inicio IS NULL OR TRIM(c.fecha_inicio) = '' OR c.fecha_inicio <= ?)
            AND (c.fecha_fin IS NULL OR TRIM(c.fecha_fin) = '' OR c.fecha_fin >= ?)
        """
        params.extend([hoy, hoy, hoy])
    sql += " ORDER BY datetime(cc.fecha_asignado) DESC, cc.id DESC"
    cursor.execute(sql, tuple(params))
    return [dict(r) for r in cursor.fetchall()]


def _marcar_cupon_regalado_usado_cursor(cursor, cupon_id, cliente_ref, venta_id):
    if not cupon_id or not cliente_ref:
        return 0
    today = datetime.now().date().isoformat()
    cursor.execute(
        """
        SELECT id
        FROM tienda_cliente_cupones
        WHERE cupon_id = ?
          AND LOWER(TRIM(COALESCE(cliente_ref, ''))) = LOWER(TRIM(?))
          AND activo = 1
          AND usado = 0
          AND (fecha_vencimiento IS NULL OR TRIM(fecha_vencimiento) = '' OR fecha_vencimiento >= ?)
        ORDER BY datetime(fecha_asignado) ASC, id ASC
        LIMIT 1
        """,
        (int(cupon_id), str(cliente_ref or "").strip(), today),
    )
    row = cursor.fetchone()
    if not row:
        return 0
    rid = int(row["id"])
    cursor.execute(
        """
        UPDATE tienda_cliente_cupones
        SET usado = 1,
            usado_venta_id = ?,
            fecha_usado = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (int(venta_id), rid),
    )
    return int(cursor.rowcount or 0)


def _validar_cupon_y_calcular_descuento(cupon, subtotal, items_serializados, cliente_ref):
    if not cupon:
        return {"ok": False, "error": "Cupon no encontrado"}
    if not int(cupon.get("activo") or 0):
        return {"ok": False, "error": "Cupon inactivo"}

    now_dt = datetime.now()
    hoy = now_dt.date().isoformat()
    hora_actual = now_dt.strftime("%H:%M")

    fecha_inicio = str(cupon.get("fecha_inicio") or "").strip()
    fecha_fin = str(cupon.get("fecha_fin") or "").strip()
    if fecha_inicio and hoy < fecha_inicio:
        return {"ok": False, "error": "Cupon aun no disponible"}
    if fecha_fin and hoy > fecha_fin:
        return {"ok": False, "error": "Cupon vencido"}

    hora_inicio = _parse_hora_hhmm(cupon.get("hora_inicio"))
    hora_fin = _parse_hora_hhmm(cupon.get("hora_fin"))
    if hora_inicio and hora_actual < f"{hora_inicio[0]:02d}:{hora_inicio[1]:02d}":
        return {"ok": False, "error": "Cupon fuera de horario"}
    if hora_fin and hora_actual > f"{hora_fin[0]:02d}:{hora_fin[1]:02d}":
        return {"ok": False, "error": "Cupon fuera de horario"}

    monto_minimo = float(cupon.get("monto_minimo") or 0)
    if subtotal < monto_minimo:
        return {"ok": False, "error": f"Compra minima para este cupon: ${monto_minimo:,.0f}"}

    solo_sin_oferta = bool(cupon.get("solo_sin_oferta"))
    if solo_sin_oferta:
        if any(float(it.get("descuento_tienda_pct") or 0) > 0 for it in (items_serializados or [])):
            return {"ok": False, "error": "Este cupon solo aplica a productos sin oferta"}

    conn = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) AS total FROM tienda_cliente_cupones WHERE cupon_id = ? AND activo = 1",
            (int(cupon["id"]),),
        )
        tiene_regalos = int(cursor.fetchone()["total"] or 0) > 0
        if tiene_regalos:
            if not cliente_ref:
                return {"ok": False, "error": "Este cupon es personalizado. Inicia sesion en Mi cuenta para usarlo"}
            cursor.execute(
                """
                SELECT COUNT(*) AS total
                FROM tienda_cliente_cupones
                WHERE cupon_id = ?
                  AND LOWER(TRIM(COALESCE(cliente_ref, ''))) = LOWER(TRIM(?))
                  AND activo = 1
                  AND usado = 0
                  AND (fecha_vencimiento IS NULL OR TRIM(fecha_vencimiento) = '' OR fecha_vencimiento >= ?)
                """,
                (int(cupon["id"]), str(cliente_ref or "").strip(), hoy),
            )
            match_cliente = int(cursor.fetchone()["total"] or 0)
            if match_cliente <= 0:
                return {"ok": False, "error": "Este cupon no esta disponible para tu cuenta"}
        usos_total_max = cupon.get("usos_max_total")
        if usos_total_max is not None and str(usos_total_max).strip() != "":
            max_total = int(usos_total_max)
            cursor.execute("SELECT COUNT(*) AS total FROM tienda_cupon_usos WHERE cupon_id = ?", (int(cupon["id"]),))
            total_usos = int(cursor.fetchone()["total"] or 0)
            if total_usos >= max_total:
                return {"ok": False, "error": "Cupon sin usos disponibles"}

        usos_por_cliente_max = cupon.get("usos_max_por_cliente")
        if cliente_ref and usos_por_cliente_max is not None and str(usos_por_cliente_max).strip() != "":
            max_por_cliente = int(usos_por_cliente_max)
            cursor.execute(
                "SELECT COUNT(*) AS total FROM tienda_cupon_usos WHERE cupon_id = ? AND cliente_ref = ?",
                (int(cupon["id"]), cliente_ref),
            )
            total_cliente = int(cursor.fetchone()["total"] or 0)
            if total_cliente >= max_por_cliente:
                return {"ok": False, "error": "Ya alcanzaste el limite de uso de este cupon"}
    finally:
        if conn:
            conn.close()

    tipo = str(cupon.get("tipo_descuento") or "porcentaje").strip().lower()
    valor = float(cupon.get("valor_descuento") or 0)
    descuento = 0.0
    if tipo == "monto_fijo":
        descuento = min(max(0.0, valor), max(0.0, subtotal))
    else:
        pct = max(0.0, min(100.0, valor))
        descuento = subtotal * (pct / 100.0)
    if descuento < 0:
        descuento = 0
    if descuento > subtotal:
        descuento = subtotal
    return {"ok": True, "descuento_monto": round(descuento, 2)}


@app.route('/api/tienda/productos', methods=['GET'])
def api_tienda_productos():
    try:
        config = _obtener_tienda_config()
        estado = _evaluar_estado_tienda(config)
        now_local = datetime.now(ZoneInfo("America/Santiago"))
        categorias = _cargar_categorias_tienda()
        categorias_map = {str(c.get("nombre") or "").strip().lower(): c for c in categorias}
        categorias_activas_map = {
            str(c.get("nombre") or "").strip().lower(): c
            for c in categorias
            if _evaluar_categoria_activa(c, now_local=now_local).get("activa")
        }
        productos = _obtener_productos_para_venta(include_zero_stock=True)
        disponibles = [
            _serializar_producto_tienda(p, categorias_map=categorias_map, now_local=now_local)
            for p in productos
            if int(p.get("activo_tienda") if p.get("activo_tienda") is not None else 1) == 1
            and (str(p.get("categoria_tienda") or "General").strip().lower() in categorias_activas_map or not categorias_activas_map)
        ]
        conn_pack = None
        try:
            pack_ids = [int(d.get("id") or 0) for d in disponibles if int(d.get("id") or 0) > 0]
            if pack_ids:
                conn_pack = get_db()
                cur_pack = conn_pack.cursor()
                _ensure_producto_pack_subopciones_table(cur_pack)
                placeholders = ",".join(["?"] * len(pack_ids))
                cur_pack.execute(
                    f"""
                    SELECT s.producto_pack_id, s.subproducto_id, s.max_cantidad, s.orden,
                           COALESCE(p.nombre, 'Producto #' || s.subproducto_id) AS subproducto_nombre,
                           COALESCE(p.activo_tienda, 1) AS subproducto_activo_tienda
                    FROM producto_pack_subopciones s
                    LEFT JOIN productos p ON p.id = s.subproducto_id
                    WHERE s.producto_pack_id IN ({placeholders})
                    ORDER BY s.producto_pack_id ASC, s.orden ASC, s.id ASC
                    """,
                    tuple(pack_ids),
                )
                pack_map = {}
                for r in cur_pack.fetchall():
                    pid = int(r["producto_pack_id"] or 0)
                    pack_map.setdefault(pid, []).append(
                        {
                            "subproducto_id": int(r["subproducto_id"] or 0),
                            "subproducto_nombre": str(r["subproducto_nombre"] or "").strip() or f"Producto #{int(r['subproducto_id'] or 0)}",
                            "subproducto_activo_tienda": bool(r["subproducto_activo_tienda"]),
                            "max_cantidad": int(r["max_cantidad"] or 1),
                            "orden": int(r["orden"] or 0),
                        }
                    )
                cur_pack.execute(
                    f"""
                    SELECT producto_pack_id, max_total
                    FROM producto_pack_subopciones_config
                    WHERE producto_pack_id IN ({placeholders})
                    """,
                    tuple(pack_ids),
                )
                cfg_map = {int(r["producto_pack_id"] or 0): int(r["max_total"] or 0) for r in cur_pack.fetchall()}
                for d in disponibles:
                    pid = int(d.get("id") or 0)
                    d["pack_subopciones"] = pack_map.get(pid) or []
                    d["pack_max_total"] = int(cfg_map.get(pid, 0) or 0)
        finally:
            if conn_pack:
                conn_pack.close()
        categorias_payload = []
        for c in categorias:
            eval_cat = _evaluar_categoria_activa(c, now_local=now_local)
            categorias_payload.append(
                {
                    "id": int(c.get("id") or 0),
                    "nombre": str(c.get("nombre") or "").strip() or "General",
                    "activo": bool(c.get("activo")),
                    "orden": int(c.get("orden") or 0),
                    "descuento_pct": float(c.get("descuento_pct") or 0),
                    "horario_habilitado": bool(c.get("horario_habilitado")),
                    "dias_semana": str(c.get("dias_semana") or ""),
                    "hora_inicio": c.get("hora_inicio"),
                    "hora_fin": c.get("hora_fin"),
                    "activa_en_tienda": bool(eval_cat.get("activa")),
                    "motivo_estado": eval_cat.get("motivo"),
                }
            )
        return jsonify(
            {
                "success": True,
                "productos": disponibles,
                "categorias": categorias_payload,
                "tienda_abierta": bool(estado.get("abierta")),
                "estado_tienda": estado,
                "mensaje_post_pedido": str(config.get("mensaje_post_pedido") or "").strip(),
                "personalizacion": _obtener_tienda_personalizacion(),
                "flow_enabled": bool(_flow_cfg().get("enabled")),
                "admin_flow_sim_enabled": bool(session.get(_ADMIN_SESSION_KEY)),
                "admin_mode": bool(session.get(_ADMIN_SESSION_KEY)),
                "payment_pricing": {
                    "flow_enabled": bool(_flow_cfg().get("enabled")),
                    **_flow_fee_cfg(),
                },
            }
        )
    except Exception as e:
        return jsonify({"success": False, "productos": [], "error": str(e)}), 500


@app.route('/api/tienda/clientes/registrar', methods=['POST'])
def api_tienda_clientes_registrar():
    conn = None
    try:
        data = request.get_json(silent=True) or {}
        email = _normalizar_email(data.get("email"))
        email_confirm = _normalizar_email(data.get("email_confirm"))
        if not email:
            return jsonify({"success": False, "error": "Correo electronico invalido"}), 400
        if email_confirm and email_confirm != email:
            return jsonify({"success": False, "error": "El correo y la confirmacion no coinciden"}), 400
        telefono = _normalizar_telefono_cl(data.get("telefono"))
        if not telefono:
            return jsonify({"success": False, "error": "Telefono invalido. Debe tener 8 digitos"}), 400
        nombre = str(data.get("nombre") or "").strip()[:80]
        if len(nombre) < 2:
            return jsonify({"success": False, "error": "Nombre invalido"}), 400
        fecha_nacimiento = _normalizar_fecha_nacimiento(data.get("fecha_nacimiento"))
        if not fecha_nacimiento:
            return jsonify({"success": False, "error": "Fecha de nacimiento invalida"}), 400
        direccion_default = str(data.get("direccion_default") or "").strip()[:240]
        try:
            direccion_lat = float(data.get("direccion_lat")) if data.get("direccion_lat") not in (None, "") else None
            direccion_lng = float(data.get("direccion_lng")) if data.get("direccion_lng") not in (None, "") else None
        except (TypeError, ValueError):
            direccion_lat, direccion_lng = None, None

        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("BEGIN IMMEDIATE")
        cliente = _upsert_cliente_tienda_cursor(
            cursor,
            nombre=nombre,
            email=email,
            telefono=telefono,
            fecha_nacimiento=fecha_nacimiento,
            email_confirmado=1 if (not email_confirm or email_confirm == email) else 0,
            direccion_default=direccion_default,
            direccion_lat=direccion_lat,
            direccion_lng=direccion_lng,
        )
        conn.commit()
        if not cliente:
            cliente = _obtener_cliente_por_contacto_cursor(cursor, email, telefono) or {"nombre": nombre, "email": email, "telefono": telefono}
        return jsonify({"success": True, "cliente": cliente})
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/clientes/historial', methods=['POST'])
def api_tienda_clientes_historial():
    conn = None
    try:
        data = request.get_json(silent=True) or {}
        email = _normalizar_email(data.get("email"))
        telefono = _normalizar_telefono_cl(data.get("telefono"))
        if not email or not telefono:
            return jsonify({"success": False, "error": "Debes indicar correo y telefono validos"}), 400

        conn = get_db()
        cursor = conn.cursor()
        cli = _obtener_cliente_por_contacto_cursor(cursor, email, telefono)
        if not cli or not bool(cli.get("activo", 1)):
            return jsonify({"success": False, "error": "Cliente no registrado"}), 404

        cursor.execute(
            """
            SELECT id, fecha_hora, codigo_pedido, codigo_operacion, total_monto, descuento_codigo, descuento_monto,
                   COALESCE(NULLIF(TRIM(pedido_estado), ''), 'recibido') AS pedido_estado,
                   pedido_estado_actualizado,
                   pedido_timer_minutos, pedido_timer_inicio
            FROM ventas
            WHERE canal_venta = 'tienda_online'
              AND LOWER(TRIM(COALESCE(cliente_email, ''))) = LOWER(TRIM(?))
              AND TRIM(COALESCE(cliente_telefono, '')) = TRIM(?)
            ORDER BY datetime(fecha_hora) DESC, id DESC
            LIMIT 30
            """,
            (email, telefono),
        )
        ventas = []
        for vrow in cursor.fetchall():
            venta = dict(vrow)
            venta_id = int(venta.get("id") or 0)
            cursor.execute(
                """
                SELECT vd.producto_id, COALESCE(p.nombre, '') AS producto_nombre, vd.cantidad, vd.precio_unitario
                FROM venta_detalles vd
                LEFT JOIN productos p ON p.id = vd.producto_id
                WHERE vd.venta_id = ?
                ORDER BY vd.id ASC
                """,
                (venta_id,),
            )
            items = [dict(r) for r in cursor.fetchall()]
            if not items:
                cursor.execute(
                    """
                    SELECT vi.producto_id, COALESCE(vi.producto_nombre, '') AS producto_nombre, vi.cantidad, 0 AS precio_unitario
                    FROM venta_items vi
                    WHERE vi.venta_id = ?
                    ORDER BY vi.id ASC
                    """,
                    (venta_id,),
                )
                items = [dict(r) for r in cursor.fetchall()]
            venta["items"] = [
                {
                    "producto_id": int(it.get("producto_id") or 0),
                    "producto_nombre": str(it.get("producto_nombre") or "").strip(),
                    "cantidad": max(0, int(it.get("cantidad") or 0)),
                    "precio_unitario": float(it.get("precio_unitario") or 0),
                }
                for it in items
                if int(it.get("producto_id") or 0) > 0 and int(it.get("cantidad") or 0) > 0
            ]
            venta.update(
                _pedido_timer_payload(
                    venta.get("pedido_estado"),
                    venta.get("pedido_timer_minutos"),
                    venta.get("pedido_timer_inicio"),
                )
            )
            ventas.append(venta)

        cursor.execute(
            """
            SELECT id, fecha, hora_inicio, hora_fin, estado, tipo, titulo, direccion, es_envio, cliente_email, cliente_telefono, creado
            FROM agenda_eventos
            WHERE LOWER(TRIM(COALESCE(cliente_email, ''))) = LOWER(TRIM(?))
              AND TRIM(COALESCE(cliente_telefono, '')) = TRIM(?)
            ORDER BY fecha DESC, hora_inicio DESC, id DESC
            LIMIT 40
            """,
            (email, telefono),
        )
        agenda_reservas = [dict(r) for r in cursor.fetchall()]
        cupones_disponibles = _listar_cupones_regalados_cliente_cursor(cursor, int(cli.get("id") or 0), solo_disponibles=True)
        niveles = _cargar_niveles_clientes(conn, solo_activos=True)
        nivel_actual = cli.get("nivel") if isinstance(cli.get("nivel"), dict) else None
        puntos_total = int(cli.get("puntos_total") or 0)
        puntos_faltantes = 0
        siguiente_nivel = None
        if niveles:
            for n in niveles:
                if int(n.get("puntos_minimos") or 0) > puntos_total:
                    siguiente_nivel = n
                    puntos_faltantes = int(n.get("puntos_minimos") or 0) - puntos_total
                    break
        return jsonify(
            {
                "success": True,
                "cliente": cli,
                "ventas": ventas,
                "agenda_reservas": agenda_reservas,
                "cupones_disponibles": cupones_disponibles,
                "programa": {
                    "nivel_actual": nivel_actual,
                    "siguiente_nivel": siguiente_nivel,
                    "puntos_total": puntos_total,
                    "puntos_actual": int(cli.get("puntos_actual") or 0),
                    "puntos_faltantes": max(0, puntos_faltantes),
                },
            }
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/clientes/perfil', methods=['POST'])
def api_tienda_clientes_perfil():
    conn = None
    try:
        data = request.get_json(silent=True) or {}
        email = _normalizar_email(data.get("email"))
        telefono = _normalizar_telefono_cl(data.get("telefono"))
        if not email or not telefono:
            return jsonify({"success": False, "error": "Debes indicar correo y telefono validos"}), 400
        conn = get_db()
        cursor = conn.cursor()
        cli = _obtener_cliente_por_contacto_cursor(cursor, email, telefono)
        if not cli:
            return jsonify({"success": False, "error": "Cliente no registrado"}), 404
        return jsonify({"success": True, "cliente": cli})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/clientes/actualizar', methods=['POST'])
def api_tienda_clientes_actualizar():
    conn = None
    try:
        data = request.get_json(silent=True) or {}
        email = _normalizar_email(data.get("email"))
        telefono = _normalizar_telefono_cl(data.get("telefono"))
        if not email or not telefono:
            return jsonify({"success": False, "error": "Debes indicar correo y telefono validos"}), 400
        nombre = str(data.get("nombre") or "").strip()[:80]
        fecha_nacimiento = _normalizar_fecha_nacimiento(data.get("fecha_nacimiento"))
        direccion_default = str(data.get("direccion_default") or "").strip()[:240]
        try:
            direccion_lat = float(data.get("direccion_lat")) if data.get("direccion_lat") not in (None, "") else None
            direccion_lng = float(data.get("direccion_lng")) if data.get("direccion_lng") not in (None, "") else None
        except (TypeError, ValueError):
            return jsonify({"success": False, "error": "Coordenadas de direccion invalidas"}), 400

        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("BEGIN IMMEDIATE")
        cli = _upsert_cliente_tienda_cursor(
            cursor,
            nombre=nombre or _nombre_desde_email(email),
            email=email,
            telefono=telefono,
            fecha_nacimiento=fecha_nacimiento,
            email_confirmado=1,
            direccion_default=direccion_default,
            direccion_lat=direccion_lat,
            direccion_lng=direccion_lng,
        )
        conn.commit()
        return jsonify({"success": True, "cliente": cli})
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/clientes/pedidos-estados', methods=['POST'])
def api_tienda_clientes_pedidos_estados():
    conn = None
    try:
        data = request.get_json(silent=True) or {}
        email = _normalizar_email(data.get("email"))
        telefono = _normalizar_telefono_cl(data.get("telefono"))
        if not email or not telefono:
            return jsonify({"success": False, "error": "Debes indicar correo y telefono validos"}), 400

        raw_ids = data.get("venta_ids") or []
        if not isinstance(raw_ids, list) or not raw_ids:
            return jsonify({"success": True, "pedidos": []})

        ids = []
        seen = set()
        for raw in raw_ids:
            try:
                vid = int(raw or 0)
            except (TypeError, ValueError):
                vid = 0
            if vid <= 0 or vid in seen:
                continue
            ids.append(vid)
            seen.add(vid)
            if len(ids) >= 40:
                break
        if not ids:
            return jsonify({"success": True, "pedidos": []})

        conn = get_db()
        cursor = conn.cursor()
        placeholders = ",".join("?" for _ in ids)
        params = [email, telefono] + ids
        cursor.execute(
            f"""
            SELECT id,
                   COALESCE(NULLIF(TRIM(pedido_estado), ''), 'recibido') AS pedido_estado,
                   pedido_estado_actualizado,
                   pedido_timer_minutos, pedido_timer_inicio
            FROM ventas
            WHERE canal_venta = 'tienda_online'
              AND LOWER(TRIM(COALESCE(cliente_email, ''))) = LOWER(TRIM(?))
              AND TRIM(COALESCE(cliente_telefono, '')) = TRIM(?)
              AND id IN ({placeholders})
            """,
            tuple(params),
        )
        pedidos = []
        for row in cursor.fetchall():
            item = dict(row)
            estado = _normalizar_pedido_estado(item.get("pedido_estado"))
            pedidos.append(
                {
                    "id": int(item.get("id") or 0),
                    "estado": estado,
                    "estado_label": _pedido_estado_label(estado),
                    "estado_actualizado": item.get("pedido_estado_actualizado"),
                    **_pedido_timer_payload(
                        estado,
                        item.get("pedido_timer_minutos"),
                        item.get("pedido_timer_inicio"),
                    ),
                }
            )
        return jsonify({"success": True, "pedidos": pedidos})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/ventas/admin-catalogo')
def ventas_admin_catalogo():
    return render_template('tienda_admin.html')


@app.route('/ventas/admin-personalizacion')
def ventas_admin_personalizacion():
    return render_template('tienda_personalizacion_admin.html')


@app.route('/ventas/admin-visualizador')
def ventas_admin_visualizador():
    return render_template('tienda_visualizador_admin.html')


@app.route('/ventas/admin-catalogo-torta')
def ventas_admin_catalogo_torta():
    return render_template('tienda_catalogo_torta_admin.html')


@app.route('/ventas/admin-clientes')
def ventas_admin_clientes():
    return render_template('tienda_clientes_admin.html')


@app.route('/ventas/cupones')
def ventas_admin_cupones():
    return render_template('cupones_admin.html')


@app.route('/api/tienda/admin/clientes/programa', methods=['GET', 'POST'])
def api_tienda_admin_clientes_programa():
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        conn = get_db()
        if request.method == "GET":
            return jsonify({"success": True, "programa": _obtener_config_programa_clientes(conn)})
        data = request.get_json(silent=True) or {}
        conn.execute("BEGIN IMMEDIATE")
        programa = _guardar_config_programa_clientes(conn, data.get("programa") if isinstance(data.get("programa"), dict) else data)
        conn.commit()
        crear_backup()
        return jsonify({"success": True, "programa": programa})
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/clientes/niveles', methods=['GET', 'POST'])
def api_tienda_admin_clientes_niveles():
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        conn = get_db()
        if request.method == "GET":
            return jsonify({"success": True, "niveles": _cargar_niveles_clientes(conn, solo_activos=False)})
        data = request.get_json(silent=True) or {}
        niveles = data.get("niveles") if isinstance(data.get("niveles"), list) else []
        _guardar_niveles_clientes(conn, niveles)
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM tienda_clientes")
        ids = [int(r["id"]) for r in cursor.fetchall()]
        for cid in ids:
            _actualizar_nivel_cliente_cursor(cursor, cid)
        conn.commit()
        crear_backup()
        return jsonify({"success": True, "niveles": _cargar_niveles_clientes(conn, solo_activos=False)})
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/clientes', methods=['GET'])
def api_tienda_admin_clientes():
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        q = str(request.args.get("q") or "").strip().lower()
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT c.id, c.nombre, c.email, c.telefono, c.fecha_nacimiento, c.activo, c.creado_en, c.ultimo_login,
                   c.puntos_actual, c.puntos_total, c.nivel_id,
                   n.nombre AS nivel_nombre, n.descuento_pct AS nivel_descuento_pct
            FROM tienda_clientes c
            LEFT JOIN tienda_clientes_niveles n ON n.id = c.nivel_id
            ORDER BY datetime(c.actualizado_en) DESC, c.id DESC
            """
        )
        clientes = []
        for row in cursor.fetchall():
            item = dict(row)
            if q:
                bucket = " ".join([
                    str(item.get("nombre") or ""),
                    str(item.get("email") or ""),
                    str(item.get("telefono") or ""),
                    str(item.get("nivel_nombre") or ""),
                ]).lower()
                if q not in bucket:
                    continue
            cid = int(item.get("id") or 0)
            cursor.execute("SELECT COUNT(*) AS total FROM ventas WHERE canal_venta='tienda_online' AND LOWER(TRIM(COALESCE(cliente_email,'')))=LOWER(TRIM(?)) AND TRIM(COALESCE(cliente_telefono,''))=TRIM(?)", (item.get("email"), item.get("telefono")))
            row_compras = cursor.fetchone()
            compras = int((dict(row_compras).get("total") if row_compras else 0) or 0)
            cursor.execute("SELECT COUNT(*) AS total FROM agenda_eventos WHERE LOWER(TRIM(COALESCE(cliente_email,'')))=LOWER(TRIM(?)) AND TRIM(COALESCE(cliente_telefono,''))=TRIM(?)", (item.get("email"), item.get("telefono")))
            row_reservas = cursor.fetchone()
            reservas = int((dict(row_reservas).get("total") if row_reservas else 0) or 0)
            cursor.execute(
                """
                SELECT COUNT(*) AS total
                FROM tienda_cliente_cupones
                WHERE cliente_id = ?
                  AND activo = 1
                  AND usado = 0
                """,
                (cid,),
            )
            row_cupones = cursor.fetchone()
            cupones_pendientes = int((dict(row_cupones).get("total") if row_cupones else 0) or 0)
            item["compras_total"] = compras
            item["reservas_total"] = reservas
            item["cupones_pendientes"] = cupones_pendientes
            item["id"] = cid
            item["puntos_actual"] = int(item.get("puntos_actual") or 0)
            item["puntos_total"] = int(item.get("puntos_total") or 0)
            item["nivel_descuento_pct"] = float(item.get("nivel_descuento_pct") or 0)
            clientes.append(item)
        return jsonify({"success": True, "clientes": clientes})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/clientes/<int:cliente_id>', methods=['GET'])
def api_tienda_admin_cliente_detalle(cliente_id):
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT c.*, n.nombre AS nivel_nombre, n.descuento_pct AS nivel_descuento_pct
            FROM tienda_clientes c
            LEFT JOIN tienda_clientes_niveles n ON n.id = c.nivel_id
            WHERE c.id = ?
            LIMIT 1
            """,
            (int(cliente_id),),
        )
        row = cursor.fetchone()
        if not row:
            return jsonify({"success": False, "error": "Cliente no encontrado"}), 404
        cli = dict(row)
        email = _normalizar_email(cli.get("email"))
        telefono = _normalizar_telefono_cl(cli.get("telefono"))
        cursor.execute(
            """
            SELECT id, fecha_hora, codigo_pedido, codigo_operacion, total_monto, descuento_codigo, descuento_monto,
                   COALESCE(NULLIF(TRIM(pedido_estado), ''), 'recibido') AS pedido_estado, pedido_estado_actualizado,
                   pedido_timer_minutos, pedido_timer_inicio
            FROM ventas
            WHERE canal_venta='tienda_online'
              AND LOWER(TRIM(COALESCE(cliente_email,'')))=LOWER(TRIM(?))
              AND TRIM(COALESCE(cliente_telefono,''))=TRIM(?)
            ORDER BY datetime(fecha_hora) DESC, id DESC
            LIMIT 120
            """,
            (email, telefono),
        )
        ventas = []
        for vrow in cursor.fetchall():
            venta = dict(vrow)
            venta_id = int(venta.get("id") or 0)
            cursor.execute(
                """
                SELECT vd.producto_id, COALESCE(p.nombre, '') AS producto_nombre, vd.cantidad, vd.precio_unitario
                FROM venta_detalles vd
                LEFT JOIN productos p ON p.id = vd.producto_id
                WHERE vd.venta_id = ?
                ORDER BY vd.id ASC
                """,
                (venta_id,),
            )
            items = [dict(r) for r in cursor.fetchall()]
            if not items:
                cursor.execute(
                    """
                    SELECT vi.producto_id, COALESCE(vi.producto_nombre, '') AS producto_nombre, vi.cantidad, 0 AS precio_unitario
                    FROM venta_items vi
                    WHERE vi.venta_id = ?
                    ORDER BY vi.id ASC
                    """,
                    (venta_id,),
                )
                items = [dict(r) for r in cursor.fetchall()]
            venta["items"] = [
                {
                    "producto_id": int(it.get("producto_id") or 0),
                    "producto_nombre": str(it.get("producto_nombre") or "").strip(),
                    "cantidad": max(0, int(it.get("cantidad") or 0)),
                    "precio_unitario": float(it.get("precio_unitario") or 0),
                }
                for it in items
                if int(it.get("cantidad") or 0) > 0 and (str(it.get("producto_nombre") or "").strip() or int(it.get("producto_id") or 0) > 0)
            ]
            venta.update(
                _pedido_timer_payload(
                    venta.get("pedido_estado"),
                    venta.get("pedido_timer_minutos"),
                    venta.get("pedido_timer_inicio"),
                )
            )
            ventas.append(venta)
        cursor.execute(
            """
            SELECT id, fecha, hora_inicio, hora_fin, estado, tipo, titulo, direccion, es_envio, creado,
                   ingredientes, cliente, telefono, codigo_operacion, cliente_email, cliente_telefono
            FROM agenda_eventos
            WHERE LOWER(TRIM(COALESCE(cliente_email,'')))=LOWER(TRIM(?))
              AND TRIM(COALESCE(cliente_telefono,''))=TRIM(?)
            ORDER BY fecha DESC, hora_inicio DESC, id DESC
            LIMIT 120
            """,
            (email, telefono),
        )
        reservas = []
        for r in cursor.fetchall():
            reserva = dict(r)
            rid = int(reserva.get("id") or 0)
            reserva["pdf_url"] = f"/api/tienda/admin/agenda/reserva/{rid}/pdf"
            reservas.append(reserva)
        cursor.execute(
            """
            SELECT id, tipo, origen_tipo, origen_id, puntos, detalle, creado_en
            FROM tienda_clientes_puntos_mov
            WHERE cliente_id = ?
            ORDER BY datetime(creado_en) DESC, id DESC
            LIMIT 200
            """,
            (int(cliente_id),),
        )
        movimientos = [dict(r) for r in cursor.fetchall()]
        cupones_regalados = _listar_cupones_regalados_cliente_cursor(cursor, int(cliente_id), solo_disponibles=False)
        return jsonify({
            "success": True,
            "cliente": cli,
            "ventas": ventas,
            "reservas": reservas,
            "movimientos": movimientos,
            "cupones_regalados": cupones_regalados,
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/agenda/reserva/<int:reserva_id>/pdf', methods=['GET'])
def api_tienda_admin_agenda_reserva_pdf(reserva_id):
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        if int(reserva_id or 0) <= 0:
            return jsonify({"success": False, "error": "ID de reserva invalido"}), 400
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, tipo, fecha, hora_inicio, hora_fin, cliente, telefono, direccion, ingredientes, codigo_operacion
            FROM agenda_eventos
            WHERE id = ?
            LIMIT 1
            """,
            (int(reserva_id),),
        )
        row = cur.fetchone()
        if not row:
            return jsonify({"success": False, "error": "Reserva no encontrada"}), 404
        reserva = dict(row)
        filename = _crear_pdf_reserva_agenda_tienda(reserva)
        abs_path = os.path.join(static_dir, "tienda_pedidos_pdf", filename)
        if not os.path.exists(abs_path):
            return jsonify({"success": False, "error": "No se pudo generar el PDF"}), 500
        download = str(request.args.get("download") or "").strip() in ("1", "true", "si")
        return send_file(abs_path, as_attachment=download, download_name=filename, mimetype="application/pdf")
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/clientes/<int:cliente_id>/eliminar', methods=['POST'])
def api_tienda_admin_cliente_eliminar(cliente_id):
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("BEGIN IMMEDIATE")
        cursor.execute("SELECT id, nombre, email, telefono FROM tienda_clientes WHERE id = ? LIMIT 1", (int(cliente_id),))
        row = cursor.fetchone()
        if not row:
            return jsonify({"success": False, "error": "Cliente no encontrado"}), 404
        cli = dict(row)
        cursor.execute("DELETE FROM tienda_clientes WHERE id = ?", (int(cliente_id),))
        conn.commit()
        crear_backup()
        return jsonify({"success": True, "cliente_eliminado": cli})
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/clientes/<int:cliente_id>/regalar-cupon', methods=['POST'])
def api_tienda_admin_cliente_regalar_cupon(cliente_id):
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        data = request.get_json(silent=True) or {}
        cupon_id = int(data.get("cupon_id") or 0)
        if cupon_id <= 0:
            return jsonify({"success": False, "error": "Debes seleccionar un cupon"}), 400
        nota = str(data.get("nota") or "").strip()[:240]
        fecha_vencimiento = str(data.get("fecha_vencimiento") or "").strip()[:10]
        if fecha_vencimiento:
            try:
                datetime.strptime(fecha_vencimiento, "%Y-%m-%d")
            except ValueError:
                return jsonify({"success": False, "error": "Fecha de vencimiento invalida"}), 400

        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("BEGIN IMMEDIATE")
        cursor.execute("SELECT id, nombre, email, telefono, activo FROM tienda_clientes WHERE id = ? LIMIT 1", (int(cliente_id),))
        cli_row = cursor.fetchone()
        if not cli_row:
            return jsonify({"success": False, "error": "Cliente no encontrado"}), 404
        cli = dict(cli_row)
        if not bool(cli.get("activo", 1)):
            return jsonify({"success": False, "error": "El cliente esta inactivo"}), 400

        cursor.execute("SELECT * FROM tienda_cupones WHERE id = ? LIMIT 1", (cupon_id,))
        cup_row = cursor.fetchone()
        if not cup_row:
            return jsonify({"success": False, "error": "Cupon no encontrado"}), 404
        cupon = dict(cup_row)
        if not bool(cupon.get("activo", 0)):
            return jsonify({"success": False, "error": "El cupon esta inactivo"}), 400

        cliente_ref = _normalizar_cliente_ref(cli.get("email"), cli.get("telefono"))
        cursor.execute(
            """
            SELECT id
            FROM tienda_cliente_cupones
            WHERE cliente_id = ?
              AND cupon_id = ?
              AND activo = 1
              AND usado = 0
            ORDER BY datetime(fecha_asignado) DESC, id DESC
            LIMIT 1
            """,
            (int(cliente_id), int(cupon_id)),
        )
        dup = cursor.fetchone()
        if dup:
            return jsonify({"success": False, "error": "Este cliente ya tiene este cupon pendiente"}), 400

        cursor.execute(
            """
            INSERT INTO tienda_cliente_cupones (
                cliente_id, cliente_ref, cupon_id, activo, usado, nota, asignado_por, fecha_vencimiento, fecha_asignado
            )
            VALUES (?, ?, ?, 1, 0, ?, 'admin', ?, CURRENT_TIMESTAMP)
            """,
            (int(cliente_id), cliente_ref, int(cupon_id), nota, (fecha_vencimiento or None)),
        )
        regalo_id = int(cursor.lastrowid or 0)
        conn.commit()
        crear_backup()
        return jsonify({
            "success": True,
            "regalo_id": regalo_id,
            "cliente_id": int(cliente_id),
            "cupon_id": int(cupon_id),
            "codigo": str(cupon.get("codigo") or ""),
        })
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/personalizacion', methods=['GET', 'POST'])
def api_tienda_admin_personalizacion():
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    try:
        if request.method == "GET":
            return jsonify({"success": True, "config": _obtener_tienda_personalizacion()})
        data = request.get_json(silent=True) or {}
        config = _guardar_tienda_personalizacion(data)
        crear_backup()
        return jsonify({"success": True, "config": config})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/tienda/admin/visualizador/state', methods=['GET'])
def api_tienda_admin_visualizador_state():
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        live_cfg = _obtener_tienda_personalizacion(apply_programacion=True, editor_mode="live")
        draft_cfg = _obtener_tienda_personalizacion(apply_programacion=False, editor_mode="draft")
        conn = get_db()
        _asegurar_presets_personalizacion(conn)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, nombre, slug, config_json, built_in, creado_en, actualizado_en
            FROM tienda_personalizacion_presets
            ORDER BY built_in DESC, nombre COLLATE NOCASE ASC
            """
        )
        presets = [_serializar_preset_row(r) for r in cur.fetchall()]
        cur.execute(
            """
            SELECT id, origen, creado_en
            FROM tienda_personalizacion_versiones
            ORDER BY id DESC
            LIMIT 80
            """
        )
        versiones = [dict(r) for r in cur.fetchall()]
        return jsonify({
            "success": True,
            "live": live_cfg,
            "draft": draft_cfg,
            "presets": presets,
            "versiones": versiones,
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/visualizador/draft', methods=['POST'])
def api_tienda_admin_visualizador_draft():
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    try:
        data = request.get_json(silent=True) or {}
        payload = data.get("config") if isinstance(data.get("config"), dict) else data
        cfg = _guardar_tienda_personalizacion(payload, target="draft", origen="visualizer_draft")
        return jsonify({"success": True, "draft": cfg})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/tienda/admin/visualizador/live', methods=['POST'])
def api_tienda_admin_visualizador_live():
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    try:
        data = request.get_json(silent=True) or {}
        payload = data.get("config") if isinstance(data.get("config"), dict) else data
        cfg = _guardar_tienda_personalizacion(payload, target="live", origen="visualizer_live")
        crear_backup()
        return jsonify({"success": True, "live": cfg})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/tienda/admin/visualizador/publish', methods=['POST'])
def api_tienda_admin_visualizador_publish():
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    try:
        draft = _obtener_tienda_personalizacion(apply_programacion=False, editor_mode="draft")
        live = _guardar_tienda_personalizacion(draft, target="live", origen="visualizer_publish")
        crear_backup()
        return jsonify({"success": True, "live": live})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/tienda/admin/visualizador/preset-aplicar', methods=['POST'])
def api_tienda_admin_visualizador_preset_aplicar():
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        data = request.get_json(silent=True) or {}
        preset_id = int(data.get("preset_id") or 0)
        target = str(data.get("target") or "draft").strip().lower()
        if target not in {"draft", "live"}:
            target = "draft"
        if preset_id <= 0:
            return jsonify({"success": False, "error": "Preset invalido"}), 400
        conn = get_db()
        _asegurar_presets_personalizacion(conn)
        cur = conn.cursor()
        cur.execute("SELECT config_json FROM tienda_personalizacion_presets WHERE id = ? LIMIT 1", (preset_id,))
        row = cur.fetchone()
        if not row:
            return jsonify({"success": False, "error": "Preset no encontrado"}), 404
        try:
            cfg = json.loads(str(row["config_json"] or "{}"))
        except Exception:
            cfg = {}
        saved = _guardar_tienda_personalizacion(cfg, target=target, origen=f"visualizer_preset_{target}")
        if target == "live":
            crear_backup()
            return jsonify({"success": True, "live": saved})
        return jsonify({"success": True, "draft": saved})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/tienda/admin/catalogo-torta', methods=['GET', 'POST'])
def api_tienda_admin_catalogo_torta():
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    try:
        if request.method == "GET":
            cfg = _obtener_tienda_personalizacion(apply_programacion=False)
            cat = _normalizar_catalogo_torta_cfg(cfg.get("catalogo_torta"))
            return jsonify({"success": True, "catalogo": cat})
        data = request.get_json(silent=True) or {}
        payload = data.get("catalogo") if isinstance(data.get("catalogo"), dict) else data
        payload = dict(payload or {})
        clear_raw = payload.pop("clear_image_category_keys", []) if isinstance(payload.get("clear_image_category_keys"), list) else []
        if not clear_raw and isinstance(payload.get("clear_image_category_ids"), list):
            clear_raw = payload.pop("clear_image_category_ids", [])
        clear_ids = {
            re.sub(r"[^a-z0-9_\-]+", "-", str(x or "").strip().lower()).strip("-_")[:40]
            for x in clear_raw
            if str(x or "").strip()
        }
        prev_cfg = _obtener_tienda_personalizacion(apply_programacion=False)
        prev_cat = _normalizar_catalogo_torta_cfg((prev_cfg or {}).get("catalogo_torta") or {})
        prev_by_key = {}
        prev_by_id = {}
        prev_by_name = {}
        for c in (prev_cat.get("categorias") or []):
            ckey = re.sub(r"[^a-z0-9_\-]+", "-", str(c.get("key") or "").strip().lower()).strip("-_")[:40]
            cid = re.sub(r"[^a-z0-9\-]+", "-", str(c.get("id") or "").strip().lower()).strip("-")[:60]
            nm = re.sub(r"[^a-z0-9\-]+", "-", str(c.get("nombre") or "").strip().lower()).strip("-")[:60]
            img = _normalizar_url_personalizacion(c.get("imagen_url"))
            if ckey and img:
                prev_by_key[ckey] = img
            if cid and img:
                prev_by_id[cid] = img
            if nm and img:
                prev_by_name[nm] = img
        payload_norm = _normalizar_catalogo_torta_cfg(payload)
        for c in (payload_norm.get("categorias") or []):
            ckey = re.sub(r"[^a-z0-9_\-]+", "-", str(c.get("key") or "").strip().lower()).strip("-_")[:40]
            cid = re.sub(r"[^a-z0-9\-]+", "-", str(c.get("id") or "").strip().lower()).strip("-")[:60]
            nm = re.sub(r"[^a-z0-9\-]+", "-", str(c.get("nombre") or "").strip().lower()).strip("-")[:60]
            img = _normalizar_url_personalizacion(c.get("imagen_url"))
            if ckey in clear_ids:
                c["imagen_url"] = ""
                continue
            if img:
                continue
            c["imagen_url"] = prev_by_key.get(ckey) or prev_by_id.get(cid) or prev_by_name.get(nm) or ""
        cfg = _guardar_tienda_personalizacion({"catalogo_torta": payload_norm})
        crear_backup()
        return jsonify({"success": True, "catalogo": _normalizar_catalogo_torta_cfg(cfg.get("catalogo_torta"))})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/tienda/catalogo-torta', methods=['GET'])
def api_tienda_catalogo_torta_publico():
    try:
        cfg = _obtener_tienda_personalizacion()
        cat = _catalogo_torta_publico(cfg.get("catalogo_torta") or {})
        return jsonify({"success": True, "catalogo": cat})
    except Exception as e:
        return jsonify({"success": False, "error": str(e), "catalogo": _catalogo_torta_publico({})}), 500


@app.route('/api/tienda/agenda/referencia-foto', methods=['POST'])
def api_tienda_agenda_referencia_foto():
    try:
        archivo = request.files.get("foto")
        if not archivo or not getattr(archivo, "filename", ""):
            return jsonify({"success": False, "error": "Archivo no recibido"}), 400
        nombre_seguro = secure_filename(archivo.filename)
        ext = os.path.splitext(nombre_seguro)[1].lower()
        permitidas = {".jpg", ".jpeg", ".png", ".webp"}
        if ext not in permitidas:
            return jsonify({"success": False, "error": "Formato no permitido. Usa JPG, PNG o WEBP"}), 400

        base_dir = os.path.join(static_dir, "agenda_referencias")
        os.makedirs(base_dir, exist_ok=True)
        unique_name = f"ref_{int(time.time())}_{uuid.uuid4().hex[:8]}{ext}"
        abs_path = os.path.join(base_dir, unique_name)
        archivo.save(abs_path)
        try:
            size_bytes = os.path.getsize(abs_path)
        except Exception:
            size_bytes = 0
        if size_bytes > 4 * 1024 * 1024:
            try:
                os.remove(abs_path)
            except Exception:
                pass
            return jsonify({"success": False, "error": "Imagen supera 4MB"}), 400
        return jsonify({"success": True, "url": f"/static/agenda_referencias/{unique_name}"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/tienda/admin/catalogo-torta/categoria-foto', methods=['POST'])
def api_tienda_admin_catalogo_torta_categoria_foto():
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    try:
        archivo = request.files.get("foto")
        if not archivo or not getattr(archivo, "filename", ""):
            return jsonify({"success": False, "error": "Archivo no recibido"}), 400
        nombre_seguro = secure_filename(archivo.filename)
        ext = os.path.splitext(nombre_seguro)[1].lower()
        permitidas = {".jpg", ".jpeg", ".png", ".webp"}
        if ext not in permitidas:
            return jsonify({"success": False, "error": "Formato no permitido. Usa JPG, PNG o WEBP"}), 400

        base_dir = os.path.join(static_dir, "agenda_categorias_torta")
        os.makedirs(base_dir, exist_ok=True)
        unique_name = f"cat_{int(time.time())}_{uuid.uuid4().hex[:8]}{ext}"
        abs_path = os.path.join(base_dir, unique_name)
        archivo.save(abs_path)
        try:
            size_bytes = os.path.getsize(abs_path)
        except Exception:
            size_bytes = 0
        if size_bytes > 5 * 1024 * 1024:
            try:
                os.remove(abs_path)
            except Exception:
                pass
            return jsonify({"success": False, "error": "Imagen supera 5MB"}), 400
        url_img = f"/static/agenda_categorias_torta/{unique_name}"
        categoria_key = re.sub(
            r"[^a-z0-9_\-]+",
            "-",
            str(request.form.get("categoria_key") or "").strip().lower(),
        ).strip("-_")[:40]
        categoria_id = re.sub(
            r"[^a-z0-9\-]+",
            "-",
            str(request.form.get("categoria_id") or "").strip().lower(),
        ).strip("-")[:60]
        if categoria_key or categoria_id:
            try:
                cfg = _obtener_tienda_personalizacion()
                cat_cfg = _normalizar_catalogo_torta_cfg((cfg or {}).get("catalogo_torta") or {})
                actualizado = False
                for row in (cat_cfg.get("categorias") or []):
                    row_key = re.sub(r"[^a-z0-9_\-]+", "-", str(row.get("key") or "").strip().lower()).strip("-_")[:40]
                    row_id = re.sub(r"[^a-z0-9\-]+", "-", str(row.get("id") or "").strip().lower()).strip("-")[:60]
                    if (categoria_key and row_key == categoria_key) or (categoria_id and row_id == categoria_id):
                        row["imagen_url"] = url_img
                        actualizado = True
                        break
                if actualizado:
                    _guardar_tienda_personalizacion({"catalogo_torta": cat_cfg})
            except Exception:
                pass
        return jsonify({"success": True, "url": url_img})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/tienda/admin/personalizacion/presets', methods=['GET', 'POST'])
def api_tienda_admin_personalizacion_presets():
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        conn = get_db()
        _asegurar_presets_personalizacion(conn)
        cursor = conn.cursor()
        if request.method == "GET":
            cursor.execute(
                """
                SELECT id, nombre, slug, config_json, built_in, creado_en, actualizado_en
                FROM tienda_personalizacion_presets
                ORDER BY built_in DESC, nombre COLLATE NOCASE ASC
                """
            )
            return jsonify({"success": True, "presets": [_serializar_preset_row(r) for r in cursor.fetchall()]})

        data = request.get_json(silent=True) or {}
        nombre = str(data.get("nombre") or "").strip()[:80]
        if not nombre:
            return jsonify({"success": False, "error": "Nombre de preset requerido"}), 400
        source_id = int(data.get("source_id") or 0)
        source_cfg = _obtener_tienda_personalizacion()
        if source_id > 0:
            cursor.execute("SELECT config_json FROM tienda_personalizacion_presets WHERE id = ? LIMIT 1", (source_id,))
            row = cursor.fetchone()
            if row:
                try:
                    source_cfg = json.loads(str(row["config_json"] or "{}"))
                except Exception:
                    source_cfg = _obtener_tienda_personalizacion()
        cfg = _normalizar_tienda_personalizacion(source_cfg)
        slug = _slug_simple(nombre)
        cursor.execute("SELECT COUNT(*) AS total FROM tienda_personalizacion_presets WHERE slug = ?", (slug,))
        if int(cursor.fetchone()["total"] or 0) > 0:
            slug = f"{slug}-{int(time.time())}"
        cursor.execute(
            """
            INSERT INTO tienda_personalizacion_presets (nombre, slug, config_json, built_in, creado_en, actualizado_en)
            VALUES (?, ?, ?, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (nombre, slug, json.dumps(cfg, ensure_ascii=False)),
        )
        conn.commit()
        return jsonify({"success": True, "preset_id": int(cursor.lastrowid)})
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/personalizacion/presets/<int:preset_id>/aplicar', methods=['POST'])
def api_tienda_admin_personalizacion_preset_aplicar(preset_id):
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        conn = get_db()
        _asegurar_presets_personalizacion(conn)
        cursor = conn.cursor()
        cursor.execute("SELECT config_json FROM tienda_personalizacion_presets WHERE id = ? LIMIT 1", (int(preset_id),))
        row = cursor.fetchone()
        if not row:
            return jsonify({"success": False, "error": "Preset no encontrado"}), 404
        try:
            cfg = json.loads(str(row["config_json"] or "{}"))
        except Exception:
            cfg = {}
        applied = _guardar_tienda_personalizacion(cfg)
        crear_backup()
        return jsonify({"success": True, "config": applied})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/personalizacion/presets/<int:preset_id>/eliminar', methods=['POST'])
def api_tienda_admin_personalizacion_preset_eliminar(preset_id):
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT built_in FROM tienda_personalizacion_presets WHERE id = ? LIMIT 1", (int(preset_id),))
        row = cursor.fetchone()
        if not row:
            return jsonify({"success": False, "error": "Preset no encontrado"}), 404
        if bool(row["built_in"]):
            return jsonify({"success": False, "error": "No puedes eliminar presets base"}), 400
        cursor.execute("DELETE FROM tienda_personalizacion_presets WHERE id = ?", (int(preset_id),))
        conn.commit()
        return jsonify({"success": True})
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/personalizacion/programaciones', methods=['GET', 'POST'])
def api_tienda_admin_personalizacion_programaciones():
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        conn = get_db()
        _asegurar_presets_personalizacion(conn)
        cursor = conn.cursor()
        if request.method == "GET":
            cursor.execute(
                """
                SELECT p.id, p.nombre, p.preset_id, p.fecha_inicio, p.fecha_fin, p.dias_semana, p.hora_inicio, p.hora_fin,
                       p.prioridad, p.activo, p.creado_en, p.actualizado_en, pr.nombre AS preset_nombre
                FROM tienda_personalizacion_programaciones p
                LEFT JOIN tienda_personalizacion_presets pr ON pr.id = p.preset_id
                ORDER BY p.activo DESC, p.prioridad DESC, p.id DESC
                """
            )
            return jsonify({"success": True, "programaciones": [dict(r) for r in cursor.fetchall()]})

        data = request.get_json(silent=True) or {}
        nombre = str(data.get("nombre") or "").strip()[:90]
        if not nombre:
            return jsonify({"success": False, "error": "Nombre requerido"}), 400
        preset_id = int(data.get("preset_id") or 0)
        if preset_id <= 0:
            return jsonify({"success": False, "error": "Selecciona un preset"}), 400
        cursor.execute("SELECT config_json FROM tienda_personalizacion_presets WHERE id = ? LIMIT 1", (preset_id,))
        row = cursor.fetchone()
        if not row:
            return jsonify({"success": False, "error": "Preset no encontrado"}), 404
        try:
            cfg = _normalizar_tienda_personalizacion(json.loads(str(row["config_json"] or "{}")))
        except Exception:
            cfg = _default_tienda_personalizacion()
        fecha_inicio = str(data.get("fecha_inicio") or "").strip()[:10] or None
        fecha_fin = str(data.get("fecha_fin") or "").strip()[:10] or None
        dias_semana = str(data.get("dias_semana") or "").strip()[:50]
        hora_inicio = str(data.get("hora_inicio") or "").strip()[:5] or None
        hora_fin = str(data.get("hora_fin") or "").strip()[:5] or None
        try:
            prioridad = int(data.get("prioridad") or 0)
        except (TypeError, ValueError):
            prioridad = 0
        activo = 1 if bool(data.get("activo", True)) else 0
        cursor.execute(
            """
            INSERT INTO tienda_personalizacion_programaciones (
                nombre, preset_id, config_json, fecha_inicio, fecha_fin, dias_semana, hora_inicio, hora_fin, prioridad, activo, creado_en, actualizado_en
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (
                nombre,
                preset_id,
                json.dumps(cfg, ensure_ascii=False),
                fecha_inicio,
                fecha_fin,
                dias_semana,
                hora_inicio,
                hora_fin,
                prioridad,
                activo,
            ),
        )
        conn.commit()
        return jsonify({"success": True, "programacion_id": int(cursor.lastrowid)})
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/personalizacion/programaciones/<int:programacion_id>/eliminar', methods=['POST'])
def api_tienda_admin_personalizacion_programacion_eliminar(programacion_id):
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM tienda_personalizacion_programaciones WHERE id = ?", (int(programacion_id),))
        conn.commit()
        return jsonify({"success": True})
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/personalizacion/versiones', methods=['GET'])
def api_tienda_admin_personalizacion_versiones():
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, origen, creado_en
            FROM tienda_personalizacion_versiones
            ORDER BY id DESC
            LIMIT 80
            """
        )
        return jsonify({"success": True, "versiones": [dict(r) for r in cursor.fetchall()]})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/personalizacion/versiones/<int:version_id>/rollback', methods=['POST'])
def api_tienda_admin_personalizacion_version_rollback(version_id):
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT config_json FROM tienda_personalizacion_versiones WHERE id = ? LIMIT 1", (int(version_id),))
        row = cursor.fetchone()
        if not row:
            return jsonify({"success": False, "error": "Version no encontrada"}), 404
        try:
            cfg = json.loads(str(row["config_json"] or "{}"))
        except Exception:
            cfg = {}
        config = _guardar_tienda_personalizacion(cfg)
        crear_backup()
        return jsonify({"success": True, "config": config})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/estado', methods=['GET'])
def api_tienda_estado():
    try:
        config = _obtener_tienda_config()
        estado = _evaluar_estado_tienda(config)
        return jsonify({"success": True, "config": config, "estado": estado})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/tienda/admin/config', methods=['GET', 'POST'])
def api_tienda_admin_config():
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401

    conn = None
    try:
        if request.method == 'GET':
            config = _obtener_tienda_config()
            estado = _evaluar_estado_tienda(config)
            return jsonify({"success": True, "config": config, "estado": estado})

        data = request.get_json(silent=True) or {}
        current_cfg = _obtener_tienda_config()
        modo_manual = str(data.get("modo_manual") or "auto").strip().lower()
        if modo_manual not in {"auto", "abierta", "cerrada"}:
            modo_manual = str(current_cfg.get("modo_manual") or "auto").strip().lower()
        horario_habilitado = 1 if bool(data.get("horario_habilitado", current_cfg.get("horario_habilitado"))) else 0
        hora_apertura = str(data.get("hora_apertura") or current_cfg.get("hora_apertura") or "09:00").strip()
        hora_cierre = str(data.get("hora_cierre") or current_cfg.get("hora_cierre") or "19:00").strip()
        mensaje_post_pedido = str(
            data.get("mensaje_post_pedido")
            if "mensaje_post_pedido" in data
            else current_cfg.get("mensaje_post_pedido")
        ).strip()[:600]
        if not mensaje_post_pedido:
            mensaje_post_pedido = "Tu pedido fue ingresado correctamente y sera contactado a la brevedad."
        if not _parse_hora_hhmm(hora_apertura):
            return jsonify({"success": False, "error": "Hora apertura invalida (HH:MM)"}), 400
        if not _parse_hora_hhmm(hora_cierre):
            return jsonify({"success": False, "error": "Hora cierre invalida (HH:MM)"}), 400

        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO tienda_config (id, modo_manual, horario_habilitado, hora_apertura, hora_cierre, mensaje_post_pedido, actualizado_en)
            VALUES (1, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(id) DO UPDATE SET
                modo_manual = excluded.modo_manual,
                horario_habilitado = excluded.horario_habilitado,
                hora_apertura = excluded.hora_apertura,
                hora_cierre = excluded.hora_cierre,
                mensaje_post_pedido = excluded.mensaje_post_pedido,
                actualizado_en = CURRENT_TIMESTAMP
            """,
            (modo_manual, horario_habilitado, hora_apertura, hora_cierre, mensaje_post_pedido),
        )
        conn.commit()
        crear_backup()
        config = _obtener_tienda_config()
        estado = _evaluar_estado_tienda(config)
        return jsonify({"success": True, "config": config, "estado": estado})
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/categorias', methods=['GET', 'POST'])
def api_tienda_admin_categorias():
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        if request.method == 'GET':
            now_local = datetime.now(ZoneInfo("America/Santiago"))
            categorias = _cargar_categorias_tienda()
            payload = []
            for c in categorias:
                ev = _evaluar_categoria_activa(c, now_local=now_local)
                payload.append(
                    {
                        "id": int(c.get("id") or 0),
                        "nombre": str(c.get("nombre") or "").strip() or "General",
                        "activo": bool(c.get("activo")),
                        "orden": int(c.get("orden") or 0),
                        "descuento_pct": float(c.get("descuento_pct") or 0),
                        "horario_habilitado": bool(c.get("horario_habilitado")),
                        "dias_semana": str(c.get("dias_semana") or ""),
                        "hora_inicio": c.get("hora_inicio"),
                        "hora_fin": c.get("hora_fin"),
                        "activa_en_tienda": bool(ev.get("activa")),
                        "motivo_estado": ev.get("motivo"),
                    }
                )
            return jsonify({"success": True, "categorias": payload})

        data = request.get_json(silent=True) or {}
        categoria_id = int(data.get("id") or 0)
        nombre = str(data.get("nombre") or "").strip()[:60]
        if not nombre:
            return jsonify({"success": False, "error": "Nombre de categoria obligatorio"}), 400
        activo = 1 if bool(data.get("activo", True)) else 0
        orden = int(data.get("orden") or 0)
        if orden < 0:
            orden = 0
        descuento_pct = float(data.get("descuento_pct") or 0)
        if descuento_pct < 0:
            descuento_pct = 0
        if descuento_pct > 100:
            descuento_pct = 100
        horario_habilitado = 1 if bool(data.get("horario_habilitado")) else 0
        dias_semana = str(data.get("dias_semana") or "").strip()
        dias_normalizados = sorted(_parse_dias_semana(dias_semana))
        dias_semana_db = ",".join(str(d) for d in dias_normalizados)
        hora_inicio = str(data.get("hora_inicio") or "").strip() or None
        hora_fin = str(data.get("hora_fin") or "").strip() or None
        if hora_inicio and not _parse_hora_hhmm(hora_inicio):
            return jsonify({"success": False, "error": "Hora inicio invalida (HH:MM)"}), 400
        if hora_fin and not _parse_hora_hhmm(hora_fin):
            return jsonify({"success": False, "error": "Hora fin invalida (HH:MM)"}), 400

        conn = get_db()
        cursor = conn.cursor()
        if categoria_id > 0:
            cursor.execute("SELECT id FROM tienda_categorias WHERE id = ?", (categoria_id,))
            if not cursor.fetchone():
                return jsonify({"success": False, "error": "Categoria no encontrada"}), 404
            cursor.execute(
                """
                UPDATE tienda_categorias
                SET nombre = ?, activo = ?, orden = ?, descuento_pct = ?, horario_habilitado = ?,
                    dias_semana = ?, hora_inicio = ?, hora_fin = ?, actualizado_en = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (nombre, activo, orden, descuento_pct, horario_habilitado, dias_semana_db, hora_inicio, hora_fin, categoria_id),
            )
        else:
            cursor.execute(
                """
                INSERT INTO tienda_categorias (nombre, activo, orden, descuento_pct, horario_habilitado, dias_semana, hora_inicio, hora_fin)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (nombre, activo, orden, descuento_pct, horario_habilitado, dias_semana_db, hora_inicio, hora_fin),
            )
            categoria_id = int(cursor.lastrowid)
        conn.commit()
        crear_backup()
        return jsonify({"success": True, "id": categoria_id})
    except sqlite3.IntegrityError:
        return jsonify({"success": False, "error": "Ya existe una categoria con ese nombre"}), 400
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/categorias/<int:categoria_id>/eliminar', methods=['POST'])
def api_tienda_admin_categorias_eliminar(categoria_id):
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT nombre FROM tienda_categorias WHERE id = ?", (categoria_id,))
        row = cursor.fetchone()
        if not row:
            return jsonify({"success": False, "error": "Categoria no encontrada"}), 404
        nombre = str(row["nombre"] or "").strip()
        if nombre.lower() == "general":
            return jsonify({"success": False, "error": "No puedes eliminar la categoria General"}), 400
        cursor.execute("UPDATE productos SET categoria_tienda = 'General' WHERE LOWER(TRIM(categoria_tienda)) = LOWER(TRIM(?))", (nombre,))
        cursor.execute("DELETE FROM tienda_categorias WHERE id = ?", (categoria_id,))
        conn.commit()
        crear_backup()
        return jsonify({"success": True})
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/track', methods=['POST'])
def api_tienda_track():
    conn = None
    try:
        data = request.get_json(silent=True) or {}
        session_id = str(data.get("session_id") or "").strip()
        if not re.match(r"^[A-Za-z0-9._:-]{8,80}$", session_id):
            return jsonify({"success": False, "error": "session_id invalido"}), 400
        pagina = str(data.get("pagina") or "/tienda").strip()[:120] or "/tienda"
        carrito_items = int(data.get("carrito_items") or 0)
        if carrito_items < 0:
            carrito_items = 0
        carrito_total = float(data.get("carrito_total") or 0)
        if carrito_total < 0:
            carrito_total = 0
        evento = str(data.get("evento") or "heartbeat").strip().lower()
        checkout_delta = 1 if evento == "checkout" else 0
        user_agent = str(request.headers.get("User-Agent") or "")[:260]
        ip_address = _obtener_ip_cliente()
        cliente_email = _normalizar_email(data.get("cliente_email"))
        cliente_telefono = str(data.get("cliente_telefono") or "").strip()[:24]
        cliente_registrado = 1 if bool(data.get("cliente_registrado")) and bool(cliente_email) else 0

        conn = get_db()
        cursor = conn.cursor()
        if evento in {"view", "enter", "checkout"}:
            try:
                cursor.execute(
                    """
                    INSERT INTO tienda_visitas_eventos (
                        session_id, evento, pagina, ip_address, cliente_email, cliente_telefono, cliente_registrado, creado_en
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (session_id, evento, pagina, ip_address, cliente_email, cliente_telefono, cliente_registrado),
                )
            except sqlite3.OperationalError:
                pass
        if evento in {"leave", "close", "salida"}:
            cursor.execute(
                """
                UPDATE tienda_visitas
                SET ultima_actividad = datetime('now', '-1 day'),
                    carrito_items = 0,
                    carrito_total = 0,
                    pagina = ?
                WHERE session_id = ?
                """,
                (pagina, session_id),
            )
            conn.commit()
            return jsonify({"success": True, "left": True})
        try:
            cursor.execute(
                """
                INSERT INTO tienda_visitas (
                    session_id, primera_visita, ultima_actividad, pagina,
                    carrito_items, carrito_total, checkouts, ultimo_checkout, user_agent, ip_address
                )
                VALUES (?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?, ?, ?, CASE WHEN ? = 1 THEN CURRENT_TIMESTAMP ELSE NULL END, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    ultima_actividad = CURRENT_TIMESTAMP,
                    pagina = excluded.pagina,
                    carrito_items = excluded.carrito_items,
                    carrito_total = excluded.carrito_total,
                    checkouts = tienda_visitas.checkouts + excluded.checkouts,
                    ultimo_checkout = CASE WHEN excluded.checkouts > 0 THEN CURRENT_TIMESTAMP ELSE tienda_visitas.ultimo_checkout END,
                    user_agent = excluded.user_agent,
                    ip_address = excluded.ip_address
                """,
                (session_id, pagina, carrito_items, carrito_total, checkout_delta, checkout_delta, user_agent, ip_address),
            )
        except sqlite3.OperationalError as e:
            if "ip_address" not in str(e).lower():
                raise
            # Compatibilidad temporal si la columna ip_address aun no existe.
            cursor.execute(
                """
                INSERT INTO tienda_visitas (
                    session_id, primera_visita, ultima_actividad, pagina,
                    carrito_items, carrito_total, checkouts, ultimo_checkout, user_agent
                )
                VALUES (?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?, ?, ?, CASE WHEN ? = 1 THEN CURRENT_TIMESTAMP ELSE NULL END, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    ultima_actividad = CURRENT_TIMESTAMP,
                    pagina = excluded.pagina,
                    carrito_items = excluded.carrito_items,
                    carrito_total = excluded.carrito_total,
                    checkouts = tienda_visitas.checkouts + excluded.checkouts,
                    ultimo_checkout = CASE WHEN excluded.checkouts > 0 THEN CURRENT_TIMESTAMP ELSE tienda_visitas.ultimo_checkout END,
                    user_agent = excluded.user_agent
                """,
                (session_id, pagina, carrito_items, carrito_total, checkout_delta, checkout_delta, user_agent),
            )
        conn.commit()
        return jsonify({"success": True})
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/dashboard/visitas-detalle-hoy', methods=['GET'])
def api_dashboard_visitas_detalle_hoy():
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(tienda_visitas_eventos)")
        cols = {str(r["name"]).strip().lower() for r in (cursor.fetchall() or []) if r and r["name"]}
        if "id" not in cols:
            return jsonify(
                {
                    "success": True,
                    "resumen": {
                        "eventos_hoy": 0,
                        "registrados_eventos_hoy": 0,
                        "anon_eventos_hoy": 0,
                        "registrados_unicos_hoy": 0,
                        "anon_unicos_hoy": 0,
                    },
                    "por_hora": [],
                    "top_registrados": [],
                }
            )

        cursor.execute(
            """
            SELECT COUNT(*) AS total
            FROM tienda_visitas_eventos
            WHERE evento IN ('view','enter')
              AND date(creado_en, 'localtime') = date('now','localtime')
            """
        )
        eventos_hoy = int(cursor.fetchone()["total"] or 0)

        cursor.execute(
            """
            SELECT COUNT(*) AS total
            FROM tienda_visitas_eventos
            WHERE evento IN ('view','enter')
              AND COALESCE(cliente_registrado,0) = 1
              AND date(creado_en, 'localtime') = date('now','localtime')
            """
        )
        registrados_eventos_hoy = int(cursor.fetchone()["total"] or 0)
        anon_eventos_hoy = max(0, eventos_hoy - registrados_eventos_hoy)

        cursor.execute(
            """
            SELECT COUNT(DISTINCT COALESCE(NULLIF(TRIM(cliente_email), ''), session_id)) AS total
            FROM tienda_visitas_eventos
            WHERE evento IN ('view','enter')
              AND COALESCE(cliente_registrado,0) = 1
              AND date(creado_en, 'localtime') = date('now','localtime')
            """
        )
        registrados_unicos_hoy = int(cursor.fetchone()["total"] or 0)

        cursor.execute(
            """
            SELECT COUNT(DISTINCT session_id) AS total
            FROM tienda_visitas_eventos
            WHERE evento IN ('view','enter')
              AND COALESCE(cliente_registrado,0) = 0
              AND date(creado_en, 'localtime') = date('now','localtime')
            """
        )
        anon_unicos_hoy = int(cursor.fetchone()["total"] or 0)

        cursor.execute(
            """
            SELECT strftime('%H', datetime(creado_en, 'localtime')) AS hora,
                   COUNT(*) AS total,
                   SUM(CASE WHEN COALESCE(cliente_registrado,0)=1 THEN 1 ELSE 0 END) AS registrados,
                   SUM(CASE WHEN COALESCE(cliente_registrado,0)=0 THEN 1 ELSE 0 END) AS anonimos
            FROM tienda_visitas_eventos
            WHERE evento IN ('view','enter')
              AND date(creado_en, 'localtime') = date('now','localtime')
            GROUP BY strftime('%H', datetime(creado_en, 'localtime'))
            ORDER BY hora ASC
            """
        )
        por_hora = [
            {
                "hora": f"{int(r['hora'] or 0):02d}:00",
                "total": int(r["total"] or 0),
                "registrados": int(r["registrados"] or 0),
                "anonimos": int(r["anonimos"] or 0),
            }
            for r in cursor.fetchall()
        ]

        cursor.execute(
            """
            SELECT COALESCE(NULLIF(TRIM(cliente_email), ''), '(sin correo)') AS cliente,
                   COUNT(*) AS total
            FROM tienda_visitas_eventos
            WHERE evento IN ('view','enter')
              AND COALESCE(cliente_registrado,0) = 1
              AND date(creado_en, 'localtime') = date('now','localtime')
            GROUP BY COALESCE(NULLIF(TRIM(cliente_email), ''), '(sin correo)')
            ORDER BY total DESC, cliente ASC
            LIMIT 20
            """
        )
        top_registrados = [{"cliente": str(r["cliente"]), "total": int(r["total"] or 0)} for r in cursor.fetchall()]

        return jsonify(
            {
                "success": True,
                "resumen": {
                    "eventos_hoy": eventos_hoy,
                    "registrados_eventos_hoy": registrados_eventos_hoy,
                    "anon_eventos_hoy": anon_eventos_hoy,
                    "registrados_unicos_hoy": registrados_unicos_hoy,
                    "anon_unicos_hoy": anon_unicos_hoy,
                },
                "por_hora": por_hora,
                "top_registrados": top_registrados,
            }
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/actividad', methods=['GET'])
def api_tienda_admin_actividad():
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) AS total FROM tienda_visitas WHERE datetime(ultima_actividad) >= datetime('now', '-15 seconds')")
        conectados = int(cursor.fetchone()["total"] or 0)
        cursor.execute(
            """
            SELECT COUNT(*) AS total
            FROM tienda_visitas
            WHERE carrito_items > 0
              AND datetime(ultima_actividad) >= datetime('now', '-30 minutes')
            """
        )
        carritos_activos = int(cursor.fetchone()["total"] or 0)
        cursor.execute(
            """
            SELECT session_id, ultima_actividad, carrito_items, carrito_total, pagina
            FROM tienda_visitas
            WHERE carrito_items > 0
              AND datetime(ultima_actividad) < datetime('now', '-30 minutes')
            ORDER BY datetime(ultima_actividad) DESC
            LIMIT 50
            """
        )
        abandonados = [dict(r) for r in cursor.fetchall()]
        cursor.execute(
            """
            SELECT COUNT(*) AS total
            FROM ventas
            WHERE canal_venta = 'tienda_online'
              AND COALESCE(NULLIF(TRIM(pedido_estado), ''), 'recibido') = 'preparando'
            """
        )
        pedidos_preparando = int(cursor.fetchone()["total"] or 0)
        cursor.execute(
            """
            SELECT COUNT(*) AS total
            FROM ventas
            WHERE canal_venta = 'tienda_online'
              AND COALESCE(NULLIF(TRIM(pedido_estado), ''), 'recibido') = 'recibido'
            """
        )
        pedidos_recibidos = int(cursor.fetchone()["total"] or 0)
        try:
            cursor.execute(
                """
                SELECT
                    COALESCE(NULLIF(TRIM(ip_address), ''), 'desconocida') AS ip_address,
                    COUNT(*) AS sesiones,
                    MAX(ultima_actividad) AS ultima_actividad
                FROM tienda_visitas
                WHERE datetime(ultima_actividad) >= datetime('now', '-15 seconds')
                GROUP BY COALESCE(NULLIF(TRIM(ip_address), ''), 'desconocida')
                ORDER BY sesiones DESC, datetime(ultima_actividad) DESC
                LIMIT 5
                """
            )
            top_ips = [dict(r) for r in cursor.fetchall()]
        except sqlite3.OperationalError as e:
            if "ip_address" not in str(e).lower():
                raise
            top_ips = []
        return jsonify(
            {
                "success": True,
                "resumen": {
                    "conectados": conectados,
                    "carritos_activos": carritos_activos,
                    "carritos_abandonados": len(abandonados),
                    "pedidos_preparando": pedidos_preparando,
                    "pedidos_recibidos": pedidos_recibidos,
                },
                "abandonados": abandonados,
                "top_ips": top_ips,
            }
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/productos', methods=['GET'])
def api_tienda_admin_productos():
    conn = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT *
            FROM productos
            WHERE COALESCE(eliminado, 0) = 0
            ORDER BY
                COALESCE(NULLIF(TRIM(categoria_tienda), ''), 'General') COLLATE NOCASE ASC,
                COALESCE(orden_tienda, 0) ASC,
                nombre COLLATE NOCASE ASC
            """
        )
        categorias = _cargar_categorias_tienda()
        categorias_map = {str(c.get("nombre") or "").strip().lower(): c for c in categorias}
        now_local = datetime.now(ZoneInfo("America/Santiago"))
        productos = []
        for row in cursor.fetchall():
            item = dict(row)
            serial = _serializar_producto_tienda(item, categorias_map=categorias_map, now_local=now_local)
            serial["precio"] = float(item.get("precio") or 0)
            serial["stock"] = float(item.get("stock") or 0)
            productos.append(serial)
        categorias_payload = []
        for c in categorias:
            eval_cat = _evaluar_categoria_activa(c, now_local=now_local)
            categorias_payload.append(
                {
                    "id": int(c.get("id") or 0),
                    "nombre": str(c.get("nombre") or "").strip() or "General",
                    "activo": bool(c.get("activo")),
                    "orden": int(c.get("orden") or 0),
                    "descuento_pct": float(c.get("descuento_pct") or 0),
                    "horario_habilitado": bool(c.get("horario_habilitado")),
                    "dias_semana": str(c.get("dias_semana") or ""),
                    "hora_inicio": c.get("hora_inicio"),
                    "hora_fin": c.get("hora_fin"),
                    "activa_en_tienda": bool(eval_cat.get("activa")),
                    "motivo_estado": eval_cat.get("motivo"),
                }
            )
        return jsonify({"success": True, "productos": productos, "categorias": categorias_payload})
    except Exception as e:
        return jsonify({"success": False, "productos": [], "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/producto/<int:producto_id>/quick-update', methods=['POST'])
def api_tienda_admin_producto_quick_update(producto_id):
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        data = request.get_json(silent=True) or {}
        precio = float(data.get("precio") or 0)
        stock = float(data.get("stock") or 0)
        oculto = 1 if bool(data.get("oculto", False)) else 0
        if precio < 0:
            return jsonify({"success": False, "error": "El precio no puede ser negativo"}), 400
        if stock < 0:
            return jsonify({"success": False, "error": "El stock no puede ser negativo"}), 400
        activo_tienda = 0 if oculto else 1
        if stock <= 0:
            activo_tienda = 0

        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "SELECT id FROM productos WHERE id = ? AND COALESCE(eliminado, 0) = 0 LIMIT 1",
            (int(producto_id),),
        )
        if not cur.fetchone():
            return jsonify({"success": False, "error": "Producto no encontrado"}), 404

        cur.execute("PRAGMA table_info(productos)")
        cols = {str(r["name"]).strip().lower() for r in (cur.fetchall() or []) if r and r["name"]}
        if "actualizado_en" in cols:
            cur.execute(
                """
                UPDATE productos
                SET precio = ?, stock = ?, activo_tienda = ?, actualizado_en = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (float(precio), float(stock), int(activo_tienda), int(producto_id)),
            )
        else:
            cur.execute(
                """
                UPDATE productos
                SET precio = ?, stock = ?, activo_tienda = ?
                WHERE id = ?
                """,
                (float(precio), float(stock), int(activo_tienda), int(producto_id)),
            )
        conn.commit()
        crear_backup()
        return jsonify({"success": True, "message": "Producto actualizado"})
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/cupones', methods=['GET'])
def api_tienda_admin_cupones_list():
    conn = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT *
            FROM tienda_cupones
            ORDER BY COALESCE(actualizado_en, creado_en) DESC, id DESC
            """
        )
        cupones = [dict(r) for r in cursor.fetchall()]
        return jsonify({"success": True, "cupones": cupones})
    except Exception as e:
        return jsonify({"success": False, "cupones": [], "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/cupones', methods=['POST'])
def api_tienda_admin_cupones_save():
    conn = None
    try:
        data = request.get_json(silent=True) or {}
        cupon_id = int(data.get("id") or 0)
        codigo = _normalizar_cupon_codigo(data.get("codigo"))
        if not codigo:
            return jsonify({"success": False, "error": "Codigo invalido"}), 400
        nombre = str(data.get("nombre") or "").strip()[:120]
        tipo = str(data.get("tipo_descuento") or "porcentaje").strip().lower()
        if tipo not in {"porcentaje", "monto_fijo"}:
            tipo = "porcentaje"
        valor = float(data.get("valor_descuento") or 0)
        if valor < 0:
            return jsonify({"success": False, "error": "Valor de descuento invalido"}), 400
        if tipo == "porcentaje" and valor > 100:
            return jsonify({"success": False, "error": "El porcentaje no puede superar 100"}), 400
        activo = 1 if bool(data.get("activo", True)) else 0
        fecha_inicio = str(data.get("fecha_inicio") or "").strip() or None
        fecha_fin = str(data.get("fecha_fin") or "").strip() or None
        hora_inicio = str(data.get("hora_inicio") or "").strip() or None
        hora_fin = str(data.get("hora_fin") or "").strip() or None
        usos_max_total = data.get("usos_max_total")
        usos_max_por_cliente = data.get("usos_max_por_cliente")
        monto_minimo = float(data.get("monto_minimo") or 0)
        solo_sin_oferta = 1 if bool(data.get("solo_sin_oferta")) else 0

        def _to_int_or_none(v):
            if v in (None, "", 0, "0"):
                return None
            iv = int(v)
            if iv < 0:
                return None
            return iv

        usos_max_total = _to_int_or_none(usos_max_total)
        usos_max_por_cliente = _to_int_or_none(usos_max_por_cliente)
        if monto_minimo < 0:
            monto_minimo = 0
        if hora_inicio and not _parse_hora_hhmm(hora_inicio):
            return jsonify({"success": False, "error": "Hora inicio invalida (HH:MM)"}), 400
        if hora_fin and not _parse_hora_hhmm(hora_fin):
            return jsonify({"success": False, "error": "Hora fin invalida (HH:MM)"}), 400

        conn = get_db()
        cursor = conn.cursor()
        if cupon_id > 0:
            cursor.execute("SELECT id FROM tienda_cupones WHERE id = ?", (cupon_id,))
            if not cursor.fetchone():
                return jsonify({"success": False, "error": "Cupon no encontrado"}), 404
            cursor.execute(
                """
                UPDATE tienda_cupones
                SET codigo = ?, nombre = ?, tipo_descuento = ?, valor_descuento = ?, activo = ?,
                    fecha_inicio = ?, fecha_fin = ?, hora_inicio = ?, hora_fin = ?,
                    usos_max_total = ?, usos_max_por_cliente = ?, monto_minimo = ?, solo_sin_oferta = ?,
                    actualizado_en = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    codigo, nombre, tipo, valor, activo,
                    fecha_inicio, fecha_fin, hora_inicio, hora_fin,
                    usos_max_total, usos_max_por_cliente, monto_minimo, solo_sin_oferta,
                    cupon_id,
                ),
            )
        else:
            cursor.execute(
                """
                INSERT INTO tienda_cupones (
                    codigo, nombre, tipo_descuento, valor_descuento, activo,
                    fecha_inicio, fecha_fin, hora_inicio, hora_fin,
                    usos_max_total, usos_max_por_cliente, monto_minimo, solo_sin_oferta
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    codigo, nombre, tipo, valor, activo,
                    fecha_inicio, fecha_fin, hora_inicio, hora_fin,
                    usos_max_total, usos_max_por_cliente, monto_minimo, solo_sin_oferta,
                ),
            )
            cupon_id = int(cursor.lastrowid)
        conn.commit()
        crear_backup()
        return jsonify({"success": True, "id": cupon_id})
    except sqlite3.IntegrityError:
        return jsonify({"success": False, "error": "Codigo de cupon ya existe"}), 400
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/cupones/<int:cupon_id>/eliminar', methods=['POST'])
def api_tienda_admin_cupones_delete(cupon_id):
    conn = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM tienda_cupones WHERE id = ?", (cupon_id,))
        if cursor.rowcount <= 0:
            return jsonify({"success": False, "error": "Cupon no encontrado"}), 404
        conn.commit()
        crear_backup()
        return jsonify({"success": True})
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


def _ensure_adm_pagina_tables(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS tienda_descuento_campanas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT NOT NULL DEFAULT '',
            tipo TEXT NOT NULL DEFAULT 'productos',
            descuento_tipo TEXT NOT NULL DEFAULT 'porcentaje',
            valor REAL NOT NULL DEFAULT 0,
            fecha_inicio TEXT,
            fecha_fin TEXT,
            target_json TEXT,
            cupon_id INTEGER,
            activo INTEGER NOT NULL DEFAULT 1,
            creado_en TEXT DEFAULT CURRENT_TIMESTAMP,
            actualizado_en TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(cupon_id) REFERENCES tienda_cupones(id)
        )
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_tienda_descuento_campanas_estado
        ON tienda_descuento_campanas(activo, fecha_inicio, fecha_fin)
        """
    )


@app.route('/adm-pagina')
def adm_pagina():
    return render_template('adm_pagina.html')


@app.route('/api/adm-pagina/realtime', methods=['GET'])
def api_adm_pagina_realtime():
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) AS total FROM tienda_visitas WHERE datetime(ultima_actividad) >= datetime('now', '-30 seconds')")
        visitantes = int(cursor.fetchone()["total"] or 0)
        cursor.execute(
            """
            SELECT COUNT(*) AS total
            FROM tienda_visitas_eventos
            WHERE evento IN ('view', 'enter')
              AND datetime(creado_en) >= datetime('now', '-30 minutes')
            """
        )
        sesiones = int(cursor.fetchone()["total"] or 0)
        cursor.execute(
            """
            SELECT COALESCE(SUM(COALESCE(total_monto, 0)), 0) AS total
            FROM ventas
            WHERE canal_venta IN ('tienda_online', 'tienda_online_flow_pendiente')
              AND date(fecha_hora, 'localtime') = date('now', 'localtime')
              AND LOWER(COALESCE(pedido_estado, '')) NOT IN ('anulado', 'cancelado')
            """
        )
        ventas_total = float(cursor.fetchone()["total"] or 0)
        cursor.execute(
            """
            SELECT COUNT(*) AS total
            FROM ventas
            WHERE canal_venta IN ('tienda_online', 'tienda_online_flow_pendiente')
              AND date(fecha_hora, 'localtime') = date('now', 'localtime')
              AND LOWER(COALESCE(pedido_estado, '')) NOT IN ('anulado', 'cancelado')
            """
        )
        pedidos = int(cursor.fetchone()["total"] or 0)
        cursor.execute(
            """
            SELECT COUNT(*) AS total
            FROM tienda_visitas
            WHERE carrito_items > 0
              AND datetime(ultima_actividad) >= datetime('now', '-10 minutes')
            """
        )
        carritos = int(cursor.fetchone()["total"] or 0)
        cursor.execute(
            """
            SELECT COUNT(*) AS total
            FROM tienda_visitas
            WHERE checkouts > 0
              AND datetime(COALESCE(ultimo_checkout, ultima_actividad)) >= datetime('now', '-15 minutes')
            """
        )
        en_pago = int(cursor.fetchone()["total"] or 0)
        cursor.execute(
            """
            SELECT COUNT(*) AS total
            FROM ventas
            WHERE canal_venta IN ('tienda_online', 'tienda_online_flow_pendiente')
              AND datetime(fecha_hora) >= datetime('now', '-30 minutes')
              AND LOWER(COALESCE(pedido_estado, '')) NOT IN ('anulado', 'cancelado')
            """
        )
        compras_recientes = int(cursor.fetchone()["total"] or 0)
        cursor.execute(
            """
            SELECT COALESCE(NULLIF(TRIM(ip_address), ''), 'Chile / ubicacion no detectada') AS ubicacion,
                   COUNT(*) AS sesiones,
                   MAX(ultima_actividad) AS ultima
            FROM tienda_visitas
            WHERE datetime(ultima_actividad) >= datetime('now', '-30 minutes')
            GROUP BY COALESCE(NULLIF(TRIM(ip_address), ''), 'Chile / ubicacion no detectada')
            ORDER BY sesiones DESC, datetime(ultima) DESC
            LIMIT 8
            """
        )
        ubicaciones = [dict(r) for r in cursor.fetchall()]
        cursor.execute(
            """
            SELECT evento, pagina, creado_en
            FROM tienda_visitas_eventos
            ORDER BY datetime(creado_en) DESC
            LIMIT 12
            """
        )
        eventos = [dict(r) for r in cursor.fetchall()]
        cursor.execute("PRAGMA table_info(ventas)")
        ventas_cols = {str(r["name"]).strip().lower() for r in cursor.fetchall()}
        metodo_expr = "metodo_pago" if "metodo_pago" in ventas_cols else "'' AS metodo_pago"
        cursor.execute(
            f"""
            SELECT id, fecha_hora, cliente_nombre, total_monto, pedido_estado, {metodo_expr}
            FROM ventas
            WHERE canal_venta IN ('tienda_online', 'tienda_online_flow_pendiente')
            ORDER BY datetime(fecha_hora) DESC, id DESC
            LIMIT 8
            """
        )
        pedidos_recientes = [dict(r) for r in cursor.fetchall()]
        return jsonify({
            "success": True,
            "metrics": {
                "visitantes": visitantes,
                "ventas_total": ventas_total,
                "sesiones": sesiones,
                "pedidos": pedidos,
                "carritos": carritos,
                "en_pago": en_pago,
                "compras_recientes": compras_recientes,
            },
            "ubicaciones": ubicaciones,
            "eventos": eventos,
            "pedidos_recientes": pedidos_recientes,
            "server_time": datetime.now(ZoneInfo("America/Santiago")).isoformat(),
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


def _adm_parse_fecha(raw):
    txt = str(raw or "").strip()
    if not txt:
        return None
    datetime.strptime(txt, "%Y-%m-%d")
    return txt


@app.route('/api/adm-pagina/descuentos', methods=['GET'])
def api_adm_pagina_descuentos_get():
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        _ensure_adm_pagina_tables(cursor)
        cursor.execute(
            """
            SELECT id, nombre, precio, stock, categoria_tienda, COALESCE(activo_tienda, 1) AS activo_tienda,
                   COALESCE(descuento_tienda_pct, 0) AS descuento_tienda_pct,
                   oferta_inicio_tienda, oferta_fin_tienda
            FROM productos
            WHERE COALESCE(eliminado, 0) = 0
            ORDER BY nombre COLLATE NOCASE ASC
            """
        )
        productos = [dict(r) for r in cursor.fetchall()]
        cursor.execute(
            """
            SELECT id, nombre, activo, orden, COALESCE(descuento_pct, 0) AS descuento_pct
            FROM tienda_categorias
            ORDER BY orden ASC, nombre COLLATE NOCASE ASC
            """
        )
        categorias = [dict(r) for r in cursor.fetchall()]
        cursor.execute(
            """
            SELECT c.*, tc.codigo AS cupon_codigo
            FROM tienda_descuento_campanas c
            LEFT JOIN tienda_cupones tc ON tc.id = c.cupon_id
            ORDER BY COALESCE(c.activo, 1) DESC, datetime(COALESCE(c.fecha_fin, c.actualizado_en, c.creado_en)) DESC, c.id DESC
            LIMIT 100
            """
        )
        campanas = [dict(r) for r in cursor.fetchall()]
        cursor.execute(
            """
            SELECT COUNT(*) AS total
            FROM tienda_descuento_campanas
            WHERE activo = 1
              AND (fecha_inicio IS NULL OR date(fecha_inicio) <= date('now', 'localtime'))
              AND (fecha_fin IS NULL OR date(fecha_fin) >= date('now', 'localtime'))
            """
        )
        activas = int(cursor.fetchone()["total"] or 0)
        return jsonify({"success": True, "productos": productos, "categorias": categorias, "campanas": campanas, "activas": activas})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/adm-pagina/descuentos', methods=['POST'])
def api_adm_pagina_descuentos_post():
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        data = request.get_json(silent=True) or {}
        tipo = str(data.get("tipo") or "productos").strip().lower()
        if tipo not in {"productos", "pedido"}:
            return jsonify({"success": False, "error": "Por ahora solo estan activos: productos y descuento en pedido."}), 400
        nombre = str(data.get("nombre") or "").strip()[:140] or ("Campana productos" if tipo == "productos" else "Cupon pedido")
        descuento_tipo = str(data.get("descuento_tipo") or "porcentaje").strip().lower()
        if descuento_tipo not in {"porcentaje", "monto_fijo"}:
            descuento_tipo = "porcentaje"
        valor = float(data.get("valor") or 0)
        if valor <= 0:
            return jsonify({"success": False, "error": "El descuento debe ser mayor a 0."}), 400
        if descuento_tipo == "porcentaje" and valor > 100:
            return jsonify({"success": False, "error": "El porcentaje no puede superar 100."}), 400
        fecha_inicio = _adm_parse_fecha(data.get("fecha_inicio"))
        fecha_fin = _adm_parse_fecha(data.get("fecha_fin"))
        if fecha_inicio and fecha_fin and fecha_fin < fecha_inicio:
            return jsonify({"success": False, "error": "La fecha de termino no puede ser anterior al inicio."}), 400
        conn = get_db()
        cursor = conn.cursor()
        _ensure_adm_pagina_tables(cursor)
        target = {}
        cupon_id = None
        if tipo == "productos":
            if descuento_tipo != "porcentaje":
                return jsonify({"success": False, "error": "En productos se usa porcentaje para mantener precios consistentes."}), 400
            producto_ids = []
            for raw in data.get("producto_ids") or []:
                try:
                    pid = int(raw)
                except (TypeError, ValueError):
                    continue
                if pid > 0:
                    producto_ids.append(pid)
            producto_ids = sorted(set(producto_ids))
            if not producto_ids:
                return jsonify({"success": False, "error": "Selecciona al menos un producto."}), 400
            placeholders = ",".join(["?"] * len(producto_ids))
            cursor.execute(f"SELECT id FROM productos WHERE id IN ({placeholders}) AND COALESCE(eliminado, 0) = 0", tuple(producto_ids))
            validos = [int(r["id"]) for r in cursor.fetchall()]
            if not validos:
                return jsonify({"success": False, "error": "No se encontraron productos validos."}), 400
            placeholders = ",".join(["?"] * len(validos))
            cursor.execute(
                f"""
                UPDATE productos
                SET descuento_tienda_pct = ?, oferta_inicio_tienda = ?, oferta_fin_tienda = ?
                WHERE id IN ({placeholders})
                """,
                tuple([valor, fecha_inicio, fecha_fin] + validos),
            )
            target = {"producto_ids": validos}
        else:
            codigo = _normalizar_cupon_codigo(data.get("codigo") or nombre)
            if not codigo:
                return jsonify({"success": False, "error": "Codigo de cupon invalido."}), 400
            cursor.execute(
                """
                INSERT INTO tienda_cupones (
                    codigo, nombre, tipo_descuento, valor_descuento, activo,
                    fecha_inicio, fecha_fin, usos_max_total, usos_max_por_cliente, monto_minimo, solo_sin_oferta
                ) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?, 0)
                """,
                (
                    codigo,
                    nombre,
                    descuento_tipo,
                    valor,
                    fecha_inicio,
                    fecha_fin,
                    int(data.get("usos_max_total") or 0) or None,
                    int(data.get("usos_max_por_cliente") or 0) or None,
                    float(data.get("monto_minimo") or 0),
                ),
            )
            cupon_id = int(cursor.lastrowid)
            target = {"codigo": codigo}
        cursor.execute(
            """
            INSERT INTO tienda_descuento_campanas (
                nombre, tipo, descuento_tipo, valor, fecha_inicio, fecha_fin, target_json, cupon_id, activo
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (nombre, tipo, descuento_tipo, valor, fecha_inicio, fecha_fin, json.dumps(target, ensure_ascii=False), cupon_id),
        )
        campana_id = int(cursor.lastrowid)
        conn.commit()
        crear_backup()
        return jsonify({"success": True, "id": campana_id, "cupon_id": cupon_id})
    except sqlite3.IntegrityError:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": "El codigo de cupon ya existe."}), 400
    except ValueError as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/adm-pagina/descuentos/<int:campana_id>/desactivar', methods=['POST'])
def api_adm_pagina_descuentos_desactivar(campana_id):
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        _ensure_adm_pagina_tables(cursor)
        cursor.execute("SELECT * FROM tienda_descuento_campanas WHERE id = ? LIMIT 1", (campana_id,))
        row = cursor.fetchone()
        if not row:
            return jsonify({"success": False, "error": "Campana no encontrada."}), 404
        campana = dict(row)
        target = {}
        try:
            target = json.loads(campana.get("target_json") or "{}")
        except Exception:
            target = {}
        if str(campana.get("tipo") or "") == "productos":
            ids = []
            for raw in target.get("producto_ids") or []:
                try:
                    val = int(raw)
                except (TypeError, ValueError):
                    continue
                if val > 0:
                    ids.append(val)
            if ids:
                placeholders = ",".join(["?"] * len(ids))
                cursor.execute(
                    f"""
                    UPDATE productos
                    SET descuento_tienda_pct = 0, oferta_inicio_tienda = NULL, oferta_fin_tienda = NULL
                    WHERE id IN ({placeholders})
                    """,
                    tuple(ids),
                )
        cupon_id = int(campana.get("cupon_id") or 0)
        if cupon_id > 0:
            cursor.execute("UPDATE tienda_cupones SET activo = 0, actualizado_en = CURRENT_TIMESTAMP WHERE id = ?", (cupon_id,))
        cursor.execute(
            "UPDATE tienda_descuento_campanas SET activo = 0, actualizado_en = CURRENT_TIMESTAMP WHERE id = ?",
            (campana_id,),
        )
        conn.commit()
        crear_backup()
        return jsonify({"success": True})
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/pedidos-nuevos', methods=['GET'])
def api_tienda_admin_pedidos_nuevos():
    conn = None
    try:
        _flow_reconciliar_pendientes(limit=20, horas=72)
        _ensure_ventas_metodo_pago_column()
        _ensure_ventas_flow_admin_alert_column()
        _ensure_ventas_flow_return_column()
        since_id = int(request.args.get("since_id") or 0)
        conn = get_db()
        cursor = conn.cursor()
        _normalizar_metodo_pago_flow_por_canal(cursor)
        cursor.execute(
            """
            SELECT COALESCE(MAX(id), 0) AS max_online_id
            FROM ventas
            WHERE canal_venta IN ('tienda_online', 'tienda_online_flow_pendiente')
            """
        )
        max_online_id = int(cursor.fetchone()["max_online_id"] or 0)
        cursor.execute(
            """
            SELECT v.id, v.fecha_hora, v.total_monto, v.cliente_nombre, v.cliente_email, v.cliente_telefono, v.codigo_pedido,
                   COALESCE(NULLIF(TRIM(v.pedido_estado), ''), 'recibido') AS pedido_estado,
                   v.pedido_estado_actualizado,
                   CASE
                       WHEN v.canal_venta = 'tienda_online_flow_pendiente' THEN 'flow_pendiente'
                       WHEN LOWER(TRIM(COALESCE(v.metodo_pago, ''))) IN ('flow', 'flow_pending', 'flow_pendiente', 'flow_pagado')
                           THEN LOWER(TRIM(COALESCE(v.metodo_pago, '')))
                       ELSE COALESCE(NULLIF(TRIM(v.metodo_pago), ''), 'efectivo')
                   END AS metodo_pago,
                   COALESCE(v.flow_cliente_regreso, 1) AS flow_cliente_regreso,
                   COALESCE(v.flow_admin_alertado, 1) AS flow_admin_alertado,
                   v.pedido_timer_minutos, v.pedido_timer_inicio,
                   COALESCE(NULLIF(TRIM(v.entrega_tipo), ''), 'retiro') AS entrega_tipo,
                   v.hora_retiro,
                   v.direccion_entrega,
                   v.despacho_monto,
                   COALESCE(vp.productos, '') AS productos
            FROM ventas v
            LEFT JOIN (
                SELECT venta_id,
                       GROUP_CONCAT(producto_nombre || ' (x' || cantidad || ')', ', ') AS productos
                FROM venta_items
                GROUP BY venta_id
            ) vp ON vp.venta_id = v.id
            WHERE v.canal_venta IN ('tienda_online', 'tienda_online_flow_pendiente')
              AND (
                    (
                        v.id > ?
                        AND (
                            CASE
                                WHEN v.canal_venta = 'tienda_online_flow_pendiente' THEN 'flow_pendiente'
                                WHEN LOWER(TRIM(COALESCE(v.metodo_pago, ''))) IN ('flow', 'flow_pending', 'flow_pendiente', 'flow_pagado')
                                    THEN LOWER(TRIM(COALESCE(v.metodo_pago, '')))
                                ELSE COALESCE(NULLIF(TRIM(v.metodo_pago), ''), 'efectivo')
                            END <> 'flow_pendiente'
                            OR COALESCE(v.flow_cliente_regreso, 1) = 1
                        )
                    )
                    OR (
                        CASE
                            WHEN v.canal_venta = 'tienda_online_flow_pendiente' THEN 'flow_pendiente'
                            WHEN LOWER(TRIM(COALESCE(v.metodo_pago, ''))) IN ('flow', 'flow_pending', 'flow_pendiente', 'flow_pagado')
                                THEN LOWER(TRIM(COALESCE(v.metodo_pago, '')))
                            ELSE COALESCE(NULLIF(TRIM(v.metodo_pago), ''), 'efectivo')
                        END = 'flow_pagado'
                        AND COALESCE(v.flow_admin_alertado, 1) = 0
                    )
                  )
            ORDER BY v.id ASC
            LIMIT 50
            """,
            (since_id,),
        )
        rows = []
        flow_ids_alertar = []
        for r in cursor.fetchall():
            item = dict(r)
            flow_pending = _pedido_pago_flow_pendiente(item.get("metodo_pago"))
            item["pago_flow_pendiente"] = flow_pending
            item["pago_bloquea_preparacion"] = flow_pending
            item["pago_alerta"] = (
                "Pago por Flow pendiente de confirmacion. Espera aprobacion para aceptar y preparar."
                if flow_pending
                else ""
            )
            item.update(
                _pedido_timer_payload(
                    item.get("pedido_estado"),
                    item.get("pedido_timer_minutos"),
                    item.get("pedido_timer_inicio"),
                )
            )
            rows.append(item)
            if str(item.get("metodo_pago") or "").strip().lower() == "flow_pagado" and int(item.get("flow_admin_alertado") or 0) == 0:
                flow_ids_alertar.append(int(item.get("id") or 0))
        if flow_ids_alertar:
            placeholders = ",".join(["?"] * len(flow_ids_alertar))
            cursor.execute(
                f"UPDATE ventas SET flow_admin_alertado = 1 WHERE id IN ({placeholders})",
                tuple(flow_ids_alertar),
            )
            conn.commit()
        max_id = max(since_id, max_online_id)
        return jsonify({"success": True, "pedidos": rows, "max_id": max_id})
    except Exception as e:
        return jsonify({"success": False, "pedidos": [], "max_id": 0, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/flow/validar-pago', methods=['POST'])
def api_tienda_admin_flow_validar_pago():
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        payload = request.get_json(silent=True) or {}
        venta_id = int(payload.get("venta_id") or 0)
        decision = str(payload.get("decision") or "").strip().lower()
        if venta_id <= 0:
            return jsonify({"success": False, "error": "venta_id invalido"}), 400
        if decision not in {"confirmado", "rechazado"}:
            return jsonify({"success": False, "error": "Decision invalida"}), 400

        _ensure_ventas_metodo_pago_column()
        _ensure_ventas_flow_admin_alert_column()

        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, COALESCE(NULLIF(TRIM(metodo_pago), ''), '') AS metodo_pago
            FROM ventas
            WHERE id = ? AND canal_venta IN ('tienda_online', 'tienda_online_flow_pendiente')
            LIMIT 1
            """,
            (venta_id,),
        )
        row = cur.fetchone()
        if not row:
            return jsonify({"success": False, "error": "Pedido no encontrado"}), 404
        metodo = str((dict(row).get("metodo_pago")) or "").strip().lower()
        if metodo not in {"flow", "flow_pendiente", "flow_pending", "flow_pagado"}:
            return jsonify({"success": False, "error": "Pedido no corresponde a Flow"}), 400
        conn.close()
        conn = None

        if decision == "confirmado":
            _finalizar_venta_flow_pagada(
                venta_id,
                status_payload={
                    "manual_validacion_admin": True,
                    "decision": "confirmado",
                    "at": datetime.now(ZoneInfo("America/Santiago")).isoformat(),
                },
            )
            # Al confirmar manualmente, mover pedido a preparacion con tiempo base
            # para que cliente y panel vean estado operativo inmediato.
            conn = get_db()
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE ventas
                SET pedido_estado = 'preparando',
                    pedido_estado_actualizado = CURRENT_TIMESTAMP,
                    pedido_timer_minutos = COALESCE(NULLIF(pedido_timer_minutos, 0), 25),
                    pedido_timer_inicio = COALESCE(NULLIF(TRIM(pedido_timer_inicio), ''), CURRENT_TIMESTAMP)
                WHERE id = ? AND canal_venta IN ('tienda_online', 'tienda_online_flow_pendiente')
                """,
                (venta_id,),
            )
            conn.commit()
            conn.close()
            conn = None
            _actualizar_flow_pago(
                venta_id=venta_id,
                estado="pagado",
                payment_data={
                    "manual_validacion_admin": True,
                    "decision": "confirmado",
                    "at": datetime.now(ZoneInfo("America/Santiago")).isoformat(),
                },
            )
            return jsonify({"success": True, "venta_id": venta_id, "estado_pago": "pagado"})

        # Rechazado manual: mantenemos pendiente para reintento de pago.
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE ventas
            SET metodo_pago = 'flow_pendiente',
                canal_venta = 'tienda_online_flow_pendiente',
                flow_admin_alertado = 1
            WHERE id = ?
            """,
            (venta_id,),
        )
        conn.commit()
        conn.close()
        conn = None
        _actualizar_flow_pago(
            venta_id=venta_id,
            estado="pendiente",
            payment_data={
                "manual_validacion_admin": True,
                "decision": "rechazado",
                "at": datetime.now(ZoneInfo("America/Santiago")).isoformat(),
            },
        )
        return jsonify({"success": True, "venta_id": venta_id, "estado_pago": "pendiente"})
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/flow/reconciliar', methods=['POST'])
def api_tienda_admin_flow_reconciliar():
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        data = request.get_json(silent=True) or {}
        limit = int(data.get("limit") or 40)
        horas = int(data.get("horas") or 120)
        limit = max(1, min(200, limit))
        horas = max(1, min(720, horas))

        conn = get_db()
        cur = conn.cursor()
        _ensure_flow_pago_table(cur)
        cur.execute("SELECT COUNT(*) AS total FROM tienda_flow_pagos WHERE estado = 'pendiente'")
        row_pa = cur.fetchone()
        pendientes_antes = int((row_pa["total"] if row_pa else 0) or 0)
        conn.close()
        conn = None

        resultado = _flow_reconciliar_pendientes(limit=limit, horas=horas)

        conn = get_db()
        cur = conn.cursor()
        _ensure_flow_pago_table(cur)
        cur.execute("SELECT COUNT(*) AS total FROM tienda_flow_pagos WHERE estado = 'pendiente'")
        row_pd = cur.fetchone()
        pendientes_despues = int((row_pd["total"] if row_pd else 0) or 0)
        cur.execute(
            """
            SELECT
                fp.venta_id,
                COALESCE(NULLIF(TRIM(fp.flow_order), ''), '-') AS flow_order,
                COALESCE(NULLIF(TRIM(fp.flow_token), ''), '-') AS flow_token,
                COALESCE(fp.confirm_attempts, 0) AS confirm_attempts,
                COALESCE(NULLIF(TRIM(fp.last_error), ''), '-') AS last_error,
                COALESCE(fp.created_at, '') AS created_at,
                COALESCE(fp.updated_at, '') AS updated_at,
                ROUND((julianday('now') - julianday(COALESCE(fp.created_at, fp.updated_at, datetime('now')))) * 24.0 * 60.0, 1) AS age_min,
                COALESCE(NULLIF(TRIM(v.cliente_nombre), ''), 'Cliente') AS cliente_nombre,
                COALESCE(v.total_monto, 0) AS total_monto,
                COALESCE(NULLIF(TRIM(v.metodo_pago), ''), 'flow_pendiente') AS metodo_pago
            FROM tienda_flow_pagos fp
            LEFT JOIN ventas v ON v.id = fp.venta_id
            WHERE fp.estado = 'pendiente'
            ORDER BY COALESCE(fp.updated_at, fp.created_at) DESC
            LIMIT 20
            """
        )
        pendientes_rows = cur.fetchall() or []
        pendientes_detalle = []
        for rr in pendientes_rows:
            it = dict(rr)
            token_raw = str(it.get("flow_token") or "").strip()
            token_mask = "-"
            if token_raw and token_raw != "-":
                token_mask = f"{token_raw[:6]}...{token_raw[-4:]}" if len(token_raw) > 14 else token_raw
            pendientes_detalle.append(
                {
                    "venta_id": int(it.get("venta_id") or 0),
                    "flow_order": str(it.get("flow_order") or "-"),
                    "flow_token_mask": token_mask,
                    "confirm_attempts": int(it.get("confirm_attempts") or 0),
                    "last_error": str(it.get("last_error") or "-"),
                    "created_at": str(it.get("created_at") or ""),
                    "updated_at": str(it.get("updated_at") or ""),
                    "age_min": float(it.get("age_min") or 0.0),
                    "cliente_nombre": str(it.get("cliente_nombre") or "Cliente"),
                    "total_monto": int(float(it.get("total_monto") or 0)),
                    "metodo_pago": str(it.get("metodo_pago") or "flow_pendiente"),
                }
            )

        cur.execute(
            """
            SELECT COUNT(*) AS total
            FROM ventas
            WHERE LOWER(TRIM(COALESCE(metodo_pago, ''))) = 'flow_pagado'
              AND COALESCE(flow_admin_alertado, 1) = 0
            """
        )
        row_an = cur.fetchone()
        alertas_nuevas = int((row_an["total"] if row_an else 0) or 0)
        conn.close()
        conn = None

        return jsonify(
            {
                "success": True,
                "reconciliados": int(resultado.get("reconciliados") or 0),
                "pendientes_antes": pendientes_antes,
                "pendientes_despues": pendientes_despues,
                "alertas_nuevas": alertas_nuevas,
                "limit": limit,
                "horas": horas,
                "pendientes_detalle": pendientes_detalle,
            }
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/pedidos-chat-activos', methods=['GET'])
def api_tienda_admin_pedidos_chat_activos():
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        _ensure_ventas_metodo_pago_column()
        _ensure_ventas_flow_return_column()
        conn = get_db()
        cursor = conn.cursor()
        _normalizar_metodo_pago_flow_por_canal(cursor)
        cursor.execute(
            """
            SELECT v.id, v.fecha_hora, v.total_monto, v.cliente_nombre, v.cliente_email, v.cliente_telefono,
                   COALESCE(NULLIF(TRIM(v.pedido_estado), ''), 'recibido') AS pedido_estado,
                   v.pedido_estado_actualizado,
                   CASE
                       WHEN v.canal_venta = 'tienda_online_flow_pendiente' THEN 'flow_pendiente'
                       WHEN LOWER(TRIM(COALESCE(v.metodo_pago, ''))) IN ('flow', 'flow_pending', 'flow_pendiente', 'flow_pagado')
                           THEN LOWER(TRIM(COALESCE(v.metodo_pago, '')))
                       ELSE COALESCE(NULLIF(TRIM(v.metodo_pago), ''), 'efectivo')
                   END AS metodo_pago,
                   COALESCE(v.flow_cliente_regreso, 1) AS flow_cliente_regreso,
                   v.pedido_timer_minutos, v.pedido_timer_inicio,
                   COALESCE(NULLIF(TRIM(v.entrega_tipo), ''), 'retiro') AS entrega_tipo,
                   v.hora_retiro,
                   COALESCE(vp.productos, '') AS productos
            FROM ventas v
            LEFT JOIN (
                SELECT venta_id,
                       GROUP_CONCAT(producto_nombre || ' (x' || cantidad || ')', ', ') AS productos
                FROM venta_items
                GROUP BY venta_id
            ) vp ON vp.venta_id = v.id
            WHERE v.canal_venta IN ('tienda_online', 'tienda_online_flow_pendiente')
              AND COALESCE(NULLIF(TRIM(v.pedido_estado), ''), 'recibido') IN ('recibido', 'confirmado', 'preparando', 'listo')
              AND (
                   CASE
                       WHEN v.canal_venta = 'tienda_online_flow_pendiente' THEN 'flow_pendiente'
                       WHEN LOWER(TRIM(COALESCE(v.metodo_pago, ''))) IN ('flow', 'flow_pending', 'flow_pendiente', 'flow_pagado')
                           THEN LOWER(TRIM(COALESCE(v.metodo_pago, '')))
                       ELSE COALESCE(NULLIF(TRIM(v.metodo_pago), ''), 'efectivo')
                   END <> 'flow_pendiente'
                   OR COALESCE(v.flow_cliente_regreso, 1) = 1
              )
            ORDER BY v.id DESC
            LIMIT 40
            """
        )
        rows = []
        for r in cursor.fetchall():
            item = dict(r)
            flow_pending = _pedido_pago_flow_pendiente(item.get("metodo_pago"))
            item["pago_flow_pendiente"] = flow_pending
            item["pago_bloquea_preparacion"] = flow_pending
            item["pago_alerta"] = (
                "Pago por Flow pendiente de confirmacion. Espera aprobacion para aceptar y preparar."
                if flow_pending
                else ""
            )
            item.update(
                _pedido_timer_payload(
                    item.get("pedido_estado"),
                    item.get("pedido_timer_minutos"),
                    item.get("pedido_timer_inicio"),
                )
            )
            rows.append(item)
        rows = [
            r for r in rows
            if _chat_estado_activo("venta", r.get("pedido_estado"), r.get("pedido_estado_actualizado"))
        ]
        return jsonify({"success": True, "pedidos": rows})
    except Exception as e:
        return jsonify({"success": False, "error": str(e), "pedidos": []}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/pedido/<int:venta_id>/estado', methods=['POST'])
def api_tienda_admin_pedido_estado(venta_id):
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        _ensure_ventas_metodo_pago_column()
        data = request.get_json(silent=True) or {}
        nuevo_estado = _normalizar_pedido_estado(data.get("estado"))
        raw_timer = data.get("timer_minutos")
        timer_in_payload = raw_timer is not None and str(raw_timer).strip() != ""
        timer_minutos = _normalizar_pedido_timer_minutos(raw_timer) if timer_in_payload else None
        if timer_in_payload and timer_minutos is None:
            return jsonify({"success": False, "error": "Temporizador invalido. Usa: 10, 15, 25, 30 o +30"}), 400
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, pedido_timer_minutos, pedido_timer_inicio,
                   COALESCE(NULLIF(TRIM(metodo_pago), ''), 'efectivo') AS metodo_pago
            FROM ventas
            WHERE id = ? AND canal_venta IN ('tienda_online', 'tienda_online_flow_pendiente')
            LIMIT 1
            """,
            (int(venta_id),),
        )
        actual = cursor.fetchone()
        if not actual:
            return jsonify({"success": False, "error": "Pedido no encontrado"}), 404
        actual = dict(actual)
        if _pedido_pago_flow_pendiente(actual.get("metodo_pago")) and nuevo_estado in {"confirmado", "preparando", "listo", "entregado"}:
            return jsonify({
                "success": False,
                "error": "Pago Flow pendiente. Espera la confirmacion del pago para aceptar y preparar el pedido."
            }), 409
        actual_timer = _normalizar_pedido_timer_minutos(actual.get("pedido_timer_minutos"))
        actual_inicio = actual.get("pedido_timer_inicio")

        set_inicio_now = False
        guardar_timer = actual_timer
        guardar_inicio = actual_inicio

        if nuevo_estado == "confirmado":
            guardar_timer = timer_minutos if timer_minutos is not None else (actual_timer if actual_timer is not None else 25)
            guardar_inicio = None
            set_inicio_now = True
        elif nuevo_estado == "preparando":
            if timer_minutos is not None:
                guardar_timer = timer_minutos
                guardar_inicio = None
                set_inicio_now = True
            elif actual_timer is None:
                guardar_timer = 25
                guardar_inicio = None
                set_inicio_now = True
        elif nuevo_estado in {"listo", "entregado", "cancelado"}:
            guardar_timer = None
            guardar_inicio = None
            set_inicio_now = False
        elif nuevo_estado == "recibido":
            guardar_timer = None
            guardar_inicio = None
            set_inicio_now = False

        if set_inicio_now:
            cursor.execute(
                """
                UPDATE ventas
                SET pedido_estado = ?, pedido_estado_actualizado = CURRENT_TIMESTAMP,
                    pedido_timer_minutos = ?, pedido_timer_inicio = CURRENT_TIMESTAMP
                WHERE id = ? AND canal_venta IN ('tienda_online', 'tienda_online_flow_pendiente')
                """,
                (nuevo_estado, guardar_timer, int(venta_id)),
            )
        else:
            cursor.execute(
                """
                UPDATE ventas
                SET pedido_estado = ?, pedido_estado_actualizado = CURRENT_TIMESTAMP,
                    pedido_timer_minutos = ?, pedido_timer_inicio = ?
                WHERE id = ? AND canal_venta IN ('tienda_online', 'tienda_online_flow_pendiente')
                """,
                (nuevo_estado, guardar_timer, guardar_inicio, int(venta_id)),
            )
        if nuevo_estado == "entregado":
            _purge_transfer_proofs_for_venta_cursor(cursor, int(venta_id))
        conn.commit()
        cursor.execute(
            """
            SELECT COALESCE(NULLIF(TRIM(pedido_estado), ''), 'recibido') AS pedido_estado,
                   pedido_timer_minutos, pedido_timer_inicio
            FROM ventas
            WHERE id = ? AND canal_venta IN ('tienda_online', 'tienda_online_flow_pendiente')
            LIMIT 1
            """,
            (int(venta_id),),
        )
        row = cursor.fetchone()
        rowd = dict(row) if row else {}
        timer_payload = _pedido_timer_payload(
            rowd.get("pedido_estado") or nuevo_estado,
            rowd.get("pedido_timer_minutos"),
            rowd.get("pedido_timer_inicio"),
        )
        return jsonify(
            {
                "success": True,
                "estado": nuevo_estado,
                "estado_label": _pedido_estado_label(nuevo_estado),
                **timer_payload,
            }
        )
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/pedido/<int:venta_id>/eliminar', methods=['POST'])
def api_tienda_admin_pedido_eliminar(venta_id):
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    try:
        conn = None
        try:
            conn = get_db()
            cur = conn.cursor()
            _purge_transfer_proofs_for_venta_cursor(cur, int(venta_id))
            conn.commit()
        finally:
            if conn:
                conn.close()
        eliminar_venta(int(venta_id))
        crear_backup()
        return jsonify({"success": True, "venta_id": int(venta_id), "mensaje": "Pedido eliminado y stock restablecido"})
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 404
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/tienda/pedido/<int:venta_id>/estado', methods=['GET'])
def api_tienda_pedido_estado(venta_id):
    conn = None
    try:
        _ensure_ventas_metodo_pago_column()
        conn = get_db()
        cursor = conn.cursor()
        _normalizar_metodo_pago_flow_por_canal(cursor)
        cursor.execute(
            """
            SELECT id, fecha_hora, total_monto, canal_venta,
                   COALESCE(NULLIF(TRIM(pedido_estado), ''), 'recibido') AS pedido_estado,
                   COALESCE(NULLIF(TRIM(metodo_pago), ''), 'efectivo') AS metodo_pago,
                   pedido_estado_actualizado,
                   pedido_timer_minutos, pedido_timer_inicio
            FROM ventas
            WHERE id = ? AND canal_venta IN ('tienda_online', 'tienda_online_flow_pendiente')
            LIMIT 1
            """,
            (int(venta_id),),
        )
        row = cursor.fetchone()
        if not row:
            return jsonify({"success": False, "error": "Pedido no encontrado"}), 404
        item = dict(row)
        flow_retry_url = ""
        metodo_pago = str(item.get("metodo_pago") or "").strip().lower()
        canal_venta = str(item.get("canal_venta") or "").strip().lower()
        if canal_venta == "tienda_online_flow_pendiente" and metodo_pago not in {"flow_pendiente", "flow_pending", "flow_pagado"}:
            metodo_pago = "flow_pendiente"
        if metodo_pago in {"flow", "flow_pendiente", "flow_pagado"}:
            try:
                _ensure_flow_pago_table(cursor)
                cursor.execute(
                    "SELECT flow_redirect_url FROM tienda_flow_pagos WHERE venta_id = ? LIMIT 1",
                    (int(venta_id),),
                )
                flow_row = cursor.fetchone()
                if flow_row:
                    flow_retry_url = str((dict(flow_row).get("flow_redirect_url")) or "").strip()
            except Exception:
                flow_retry_url = ""
        estado = _normalizar_pedido_estado(item.get("pedido_estado"))
        timer_payload = _pedido_timer_payload(
            estado,
            item.get("pedido_timer_minutos"),
            item.get("pedido_timer_inicio"),
        )
        return jsonify(
            {
                "success": True,
                "pedido": {
                    "id": int(item.get("id") or 0),
                    "estado": estado,
                    "estado_label": _pedido_estado_label(estado),
                    "estado_actualizado": item.get("pedido_estado_actualizado"),
                    "metodo_pago": metodo_pago,
                    "flow_retry_url": flow_retry_url,
                    "fecha_hora": item.get("fecha_hora"),
                    "total_monto": float(item.get("total_monto") or 0),
                    **timer_payload,
                },
            }
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/chat/<string:origen_tipo>/<int:origen_id>', methods=['GET'])
def api_tienda_chat_listar(origen_tipo, origen_id):
    conn = None
    try:
        origen_tipo = _chat_origen_tipo(origen_tipo)
        role = str(request.args.get("role") or "cliente").strip().lower()
        conn = get_db()
        cursor = conn.cursor()
        info = _chat_info_origen_cursor(cursor, origen_tipo, int(origen_id))
        if not info:
            return jsonify({"success": False, "error": "Pedido/Reserva no encontrado"}), 404

        if role == "admin":
            if not session.get(_ADMIN_SESSION_KEY):
                return jsonify({"success": False, "error": "No autorizado"}), 401
        else:
            email = str(request.args.get("email") or "").strip().lower()
            telefono = str(request.args.get("telefono") or "").strip()
            if not _chat_cliente_autorizado(info, email, telefono):
                return jsonify({"success": False, "error": "Cliente no autorizado para este chat"}), 403

        cursor.execute(
            """
            SELECT id, remitente_tipo, mensaje, creado_en
            FROM tienda_pedido_chat
            WHERE origen_tipo = ? AND origen_id = ?
            ORDER BY id ASC
            LIMIT 250
            """,
            (origen_tipo, int(origen_id)),
        )
        mensajes = [dict(r) for r in cursor.fetchall()]
        return jsonify(
            {
                "success": True,
                "chat_activo": bool(info.get("chat_activo")),
                "chat_cierre_restante_segundos": int(info.get("chat_cierre_restante_segundos") or 0),
                "origen_tipo": origen_tipo,
                "origen_id": int(origen_id),
                "estado": str(info.get("estado") or ""),
                "mensajes": mensajes,
            }
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/chat/<string:origen_tipo>/<int:origen_id>/enviar', methods=['POST'])
def api_tienda_chat_enviar(origen_tipo, origen_id):
    conn = None
    try:
        origen_tipo = _chat_origen_tipo(origen_tipo)
        role = str((request.get_json(silent=True) or {}).get("role") or "cliente").strip().lower()
        data = request.get_json(silent=True) or {}
        mensaje = str(data.get("mensaje") or "").strip()
        if len(mensaje) < 1:
            return jsonify({"success": False, "error": "Mensaje vacio"}), 400
        if len(mensaje) > 1000:
            return jsonify({"success": False, "error": "Mensaje demasiado largo (max 1000)"}), 400

        conn = get_db()
        cursor = conn.cursor()
        info = _chat_info_origen_cursor(cursor, origen_tipo, int(origen_id))
        if not info:
            return jsonify({"success": False, "error": "Pedido/Reserva no encontrado"}), 404
        if not bool(info.get("chat_activo")):
            return jsonify({"success": False, "error": "Chat cerrado para este pedido/reserva"}), 409

        if role == "admin":
            if not session.get(_ADMIN_SESSION_KEY):
                return jsonify({"success": False, "error": "No autorizado"}), 401
            remitente = "pasteleria"
            email = str(info.get("cliente_email") or "").strip().lower()
            telefono = str(info.get("cliente_telefono") or "").strip()
        else:
            email = str(data.get("email") or "").strip().lower()
            telefono = str(data.get("telefono") or "").strip()
            if not _chat_cliente_autorizado(info, email, telefono):
                return jsonify({"success": False, "error": "Cliente no autorizado para este chat"}), 403
            remitente = "cliente"
            telefono = _normalizar_telefono_cl(telefono)

        cursor.execute(
            """
            INSERT INTO tienda_pedido_chat (
                origen_tipo, origen_id, cliente_email, cliente_telefono, remitente_tipo, mensaje
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (origen_tipo, int(origen_id), email, telefono, remitente, mensaje),
        )
        msg_id = int(cursor.lastrowid or 0)
        conn.commit()
        return jsonify(
            {
                "success": True,
                "mensaje": {
                    "id": msg_id,
                    "remitente_tipo": remitente,
                    "mensaje": mensaje,
                },
            }
        )
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/chat/venta/<int:origen_id>/subir-comprobante', methods=['POST'])
def api_tienda_chat_subir_comprobante_transferencia(origen_id):
    conn = None
    try:
        archivo = request.files.get("comprobante")
        if not archivo or not getattr(archivo, "filename", ""):
            return jsonify({"success": False, "error": "Debes seleccionar un comprobante"}), 400

        nombre_original = os.path.basename(str(archivo.filename or "").strip())
        nombre_seguro = secure_filename(nombre_original)
        ext = os.path.splitext(nombre_seguro)[1].lower()
        permitidas = {".jpg", ".jpeg", ".png", ".webp", ".pdf"}
        if ext not in permitidas:
            return jsonify({"success": False, "error": "Formato no permitido. Usa JPG, PNG, WEBP o PDF"}), 400

        email = str(request.form.get("email") or "").strip().lower()
        telefono = str(request.form.get("telefono") or "").strip()
        telefono_norm = _normalizar_telefono_cl(telefono)
        if not email or not telefono_norm:
            return jsonify({"success": False, "error": "Falta validar email/telefono del pedido"}), 400

        conn = get_db()
        cursor = conn.cursor()
        info = _chat_info_origen_cursor(cursor, "venta", int(origen_id))
        if not info:
            return jsonify({"success": False, "error": "Pedido no encontrado"}), 404
        if not _chat_cliente_autorizado(info, email, telefono_norm):
            return jsonify({"success": False, "error": "Cliente no autorizado para este pedido"}), 403
        if not bool(info.get("chat_activo")):
            return jsonify({"success": False, "error": "Este pedido ya no acepta nuevos comprobantes"}), 409

        base_dir = os.path.join(static_dir, "tienda_comprobantes_transferencia")
        os.makedirs(base_dir, exist_ok=True)
        unique_name = f"comp_{int(origen_id)}_{int(time.time())}_{uuid.uuid4().hex[:8]}{ext}"
        abs_path = os.path.join(base_dir, unique_name)
        archivo.save(abs_path)
        try:
            size_bytes = os.path.getsize(abs_path)
        except Exception:
            size_bytes = 0
        if size_bytes <= 0:
            try:
                os.remove(abs_path)
            except Exception:
                pass
            return jsonify({"success": False, "error": "No se pudo guardar el comprobante"}), 400
        if size_bytes > (8 * 1024 * 1024):
            try:
                os.remove(abs_path)
            except Exception:
                pass
            return jsonify({"success": False, "error": "El comprobante supera 8MB"}), 400

        url_archivo = f"/static/tienda_comprobantes_transferencia/{unique_name}"
        etiqueta = "PDF" if ext == ".pdf" else "Imagen"
        mensaje = (
            f"Comprobante de transferencia adjunto ({etiqueta})\n"
            f"Archivo: {nombre_original or unique_name}\n"
            f"Ver/descargar: {url_archivo}"
        )
        cursor.execute(
            """
            INSERT INTO tienda_pedido_chat (
                origen_tipo, origen_id, cliente_email, cliente_telefono, remitente_tipo, mensaje
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("venta", int(origen_id), email, telefono_norm, "cliente", mensaje),
        )
        msg_id = int(cursor.lastrowid or 0)
        conn.commit()
        return jsonify(
            {
                "success": True,
                "archivo_url": url_archivo,
                "archivo_nombre": (nombre_original or unique_name),
                "mensaje": {
                    "id": msg_id,
                    "remitente_tipo": "cliente",
                    "mensaje": mensaje,
                },
            }
        )
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/comprobante-transferencia/<path:filename>', methods=['GET'])
def api_tienda_admin_comprobante_transferencia(filename):
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        safe_name = secure_filename(os.path.basename(str(filename or "").strip()))
        if not safe_name:
            return jsonify({"success": False, "error": "Archivo invalido"}), 400
        abs_path = _transfer_proof_abs_path(safe_name)
        if not abs_path or not os.path.isfile(abs_path):
            return jsonify({"success": False, "error": "Comprobante no encontrado"}), 404

        with open(abs_path, "rb") as fh:
            payload = fh.read()
        if not payload:
            return jsonify({"success": False, "error": "Comprobante vacio"}), 404

        conn = get_db()
        cur = conn.cursor()
        needle = f"{_TRANSFER_PROOF_PREFIX}{safe_name}"
        cur.execute(
            """
            UPDATE tienda_pedido_chat
            SET mensaje = REPLACE(mensaje, ?, '[Comprobante eliminado por revision admin]')
            WHERE mensaje LIKE ?
            """,
            (needle, f"%{needle}%"),
        )
        conn.commit()
        try:
            os.remove(abs_path)
        except Exception:
            pass

        ext = os.path.splitext(safe_name)[1].lower()
        mime = {
            ".pdf": "application/pdf",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".png": "image/png",
            ".webp": "image/webp",
        }.get(ext, "application/octet-stream")
        force_download = str(request.args.get("download") or "").strip().lower() in {"1", "true", "yes", "on"}
        disp = "attachment" if force_download else "inline"
        resp = make_response(payload)
        resp.headers["Content-Type"] = mime
        resp.headers["Content-Length"] = str(len(payload))
        resp.headers["Content-Disposition"] = f'{disp}; filename="{safe_name}"'
        return resp
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/agenda/disponibilidad', methods=['GET'])
def api_tienda_agenda_disponibilidad():
    conn = None
    try:
        cfg = _obtener_cfg_agenda_tienda()
        if not cfg.get("enabled"):
            return jsonify({"success": True, "enabled": False, "dias": []})

        fecha_desde = str(request.args.get("fecha_desde") or "").strip()
        fecha_hasta = str(request.args.get("fecha_hasta") or "").strip()
        admin_scope = str(request.args.get("admin_scope") or "").strip().lower()
        admin_manual_scope = admin_scope == "agenda_manual" and bool(session.get(_ADMIN_SESSION_KEY))
        tipo_reserva = _normalizar_tipo_reserva_tienda(request.args.get("tipo"))
        topper_id = str(request.args.get("topper_id") or "").strip().lower()
        topper_96h = tipo_reserva == "torta" and _topper_requiere_96h(topper_id=topper_id)
        categoria_id = str(request.args.get("categoria_id") or "").strip().lower()
        min_horas_categoria = None
        if tipo_reserva == "torta":
            cfg_tienda = _obtener_tienda_personalizacion()
            catalogo_publico = _catalogo_torta_publico((cfg_tienda or {}).get("catalogo_torta") or {})
            categoria = _catalogo_torta_categoria_publica(catalogo_publico, categoria_id)
            if categoria:
                try:
                    min_horas_categoria = int(categoria.get("min_lead_hours") or 0)
                except (TypeError, ValueError):
                    min_horas_categoria = None
        pastel_modo = str(request.args.get("pastel_modo") or "catalogo").strip().lower()
        pastel_fuera_lista = tipo_reserva == "pastel" and pastel_modo in {"especial", "fuera_lista", "fuera-lista"}
        if tipo_reserva not in {"torta", "pastel"}:
            return jsonify(
                {
                    "success": True,
                    "enabled": True,
                    "tipo_reserva": "",
                    "cfg": {
                        "days_ahead": int(cfg["days_ahead"]),
                        "slot_minutes": int(cfg["slot_minutes"]),
                        "slot_capacity": int(cfg["slot_capacity"]),
                        "hour_start": str(cfg["hour_start"]),
                        "hour_end": str(cfg["hour_end"]),
                    },
                    "dias": [],
                }
            )
        hoy_dt = datetime.now(ZoneInfo("America/Santiago")).date()
        hoy_iso = hoy_dt.strftime("%Y-%m-%d")
        # Solo para agenda manual de SucreeStock (admin): ampliar ventana hasta fin del mes siguiente.
        if admin_manual_scope:
            first_this_month = hoy_dt.replace(day=1)
            first_next_month = (first_this_month + timedelta(days=32)).replace(day=1)
            first_after_next = (first_next_month + timedelta(days=32)).replace(day=1)
            fecha_hasta_max_dt = first_after_next - timedelta(days=1)
        else:
            fecha_hasta_max_dt = hoy_dt + timedelta(days=max(1, int(cfg["days_ahead"])) - 1)
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", fecha_desde):
            fecha_desde = hoy_iso
        fecha_desde_dt = datetime.strptime(fecha_desde, "%Y-%m-%d").date()
        if fecha_desde_dt > fecha_hasta_max_dt:
            fecha_desde_dt = fecha_hasta_max_dt
            fecha_desde = fecha_desde_dt.strftime("%Y-%m-%d")
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", fecha_hasta):
            fecha_hasta = fecha_hasta_max_dt.strftime("%Y-%m-%d")
        else:
            fecha_hasta_dt = datetime.strptime(fecha_hasta, "%Y-%m-%d").date()
            if fecha_hasta_dt > fecha_hasta_max_dt:
                fecha_hasta = fecha_hasta_max_dt.strftime("%Y-%m-%d")
            if fecha_hasta_dt < fecha_desde_dt:
                fecha_hasta = fecha_desde_dt.strftime("%Y-%m-%d")
        fecha_hasta_dt = datetime.strptime(fecha_hasta, "%Y-%m-%d").date()

        conn = get_db()
        cursor = conn.cursor()
        cfg_dispo = dict(cfg or {})
        if admin_manual_scope:
            cfg_dispo["days_ahead"] = max(
                int(cfg_dispo.get("days_ahead") or 1),
                max(1, (fecha_hasta_dt - fecha_desde_dt).days + 1),
            )
        data = _calcular_disponibilidad_agenda_tienda(cursor, cfg_dispo, fecha_desde, fecha_hasta)
        now_local = datetime.now(ZoneInfo("America/Santiago"))
        dias_filtrados = []
        for dia in (data.get("dias") or []):
            horas = []
            for h in (dia.get("horas") or []):
                cumple_regla = _cumple_anticipacion_reserva(
                    dia.get("fecha"),
                    h.get("hora_inicio"),
                    tipo_reserva,
                    cfg_agenda=cfg,
                    now_local=now_local,
                    topper_requiere_96h=topper_96h,
                    pastel_fuera_lista=pastel_fuera_lista,
                    min_horas_categoria=min_horas_categoria,
                )
                disponible = bool(h.get("disponible")) and bool(cumple_regla)
                hh = dict(h)
                hh["disponible"] = disponible
                hh["sin_cupos"] = not disponible
                if not disponible and not cumple_regla:
                    hh["cupos_disponibles"] = 0
                horas.append(hh)
            d = dict(dia)
            d["horas"] = horas
            d["sin_cupos"] = all(bool(x.get("sin_cupos")) for x in horas) if horas else True
            dias_filtrados.append(d)
        return jsonify(
            {
                "success": True,
                "enabled": True,
                "tipo_reserva": tipo_reserva,
                "topper_requiere_96h": bool(topper_96h),
                "pastel_fuera_lista": bool(pastel_fuera_lista),
                "min_horas_categoria": int(min_horas_categoria or 0),
                "cfg": {
                    "days_ahead": max(1, (fecha_hasta_dt - fecha_desde_dt).days + 1),
                    "slot_minutes": int(cfg["slot_minutes"]),
                    "slot_capacity": int(cfg["slot_capacity"]),
                    "hour_start": str(cfg["hour_start"]),
                    "hour_end": str(cfg["hour_end"]),
                },
                "dias": dias_filtrados,
            }
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e), "dias": []}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/agenda/despacho-cotizar', methods=['POST'])
def api_tienda_agenda_despacho_cotizar():
    try:
        data = request.get_json(silent=True) or {}
        try:
            lat = float(data.get("lat"))
            lng = float(data.get("lng"))
        except (TypeError, ValueError):
            return jsonify({"success": False, "error": "Coordenadas invalidas para cotizar despacho"}), 400
        if not (-90 <= lat <= 90 and -180 <= lng <= 180):
            return jsonify({"success": False, "error": "Coordenadas fuera de rango"}), 400
        hora_inicio = str(data.get("hora_inicio") or "").strip()
        if not _parse_hora_hhmm(hora_inicio):
            return jsonify({"success": False, "error": "Selecciona primero un horario valido para calcular despacho"}), 400
        cfg_tienda = _obtener_tienda_personalizacion()
        quote = _cotizar_envio_agenda(lat, lng, cfg_tienda=cfg_tienda, hora_inicio=hora_inicio)
        return jsonify({"success": True, "quote": quote})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/tienda/checkout/despacho-cotizar', methods=['POST'])
def api_tienda_checkout_despacho_cotizar():
    try:
        data = request.get_json(silent=True) or {}
        try:
            lat = float(data.get("lat"))
            lng = float(data.get("lng"))
        except (TypeError, ValueError):
            return jsonify({"success": False, "error": "Coordenadas invalidas para cotizar despacho"}), 400
        if not (-90 <= lat <= 90 and -180 <= lng <= 180):
            return jsonify({"success": False, "error": "Coordenadas fuera de rango"}), 400
        hora_inicio = str(data.get("hora_retiro") or data.get("hora_inicio") or "").strip()
        if not _parse_hora_hhmm(hora_inicio):
            return jsonify({"success": False, "error": "Selecciona una hora valida para calcular despacho"}), 400
        cfg_tienda = _obtener_tienda_personalizacion()
        quote = _cotizar_envio_checkout_tienda(lat, lng, cfg_tienda=cfg_tienda, hora_inicio=hora_inicio)
        return jsonify({"success": True, "quote": quote})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/tienda/agenda/reservar', methods=['POST'])
def api_tienda_agenda_reservar():
    conn = None
    try:
        cfg = _obtener_cfg_agenda_tienda()
        if not cfg.get("enabled"):
            return jsonify({"success": False, "error": "La agenda publica no esta habilitada"}), 403

        data = request.get_json(silent=True) or {}
        fecha = str(data.get("fecha") or "").strip()
        hora_inicio = str(data.get("hora_inicio") or "").strip()
        nombre = str(data.get("nombre") or "").strip()[:80]
        email = _normalizar_email(data.get("email"))
        telefono = _normalizar_telefono_cl(data.get("telefono"))
        tipo_pedido = _normalizar_tipo_reserva_tienda(data.get("tipo"))
        detalle = str(data.get("detalle") or "").strip()[:400]
        catalogo_torta_payload = data.get("catalogo_torta") if isinstance(data.get("catalogo_torta"), dict) else {}
        pastel_catalogo_payload = data.get("pastel_catalogo") if isinstance(data.get("pastel_catalogo"), list) else []
        pastel_especial_payload = data.get("pastel_especial") if isinstance(data.get("pastel_especial"), dict) else {}
        pastel_modo = str(data.get("pastel_modo") or "catalogo").strip().lower()
        if pastel_modo not in {"catalogo", "especial"}:
            pastel_modo = "especial" if str(pastel_especial_payload.get("detalle") or "").strip() else "catalogo"
        entrega_tipo = str(data.get("entrega_tipo") or "retiro").strip().lower()
        if entrega_tipo not in {"retiro", "despacho"}:
            entrega_tipo = "retiro"
        direccion = str(data.get("direccion") or "").strip()[:240]
        direccion_confirmada = bool(data.get("direccion_confirmada"))
        try:
            latitud = float(data.get("lat") or 0)
        except (TypeError, ValueError):
            latitud = 0.0
        try:
            longitud = float(data.get("lng") or 0)
        except (TypeError, ValueError):
            longitud = 0.0

        if not re.match(r"^\d{4}-\d{2}-\d{2}$", fecha):
            return jsonify({"success": False, "error": "Fecha invalida"}), 400
        if not _parse_hora_hhmm(hora_inicio):
            return jsonify({"success": False, "error": "Hora invalida (HH:MM)"}), 400
        if len(nombre) < 2:
            return jsonify({"success": False, "error": "Nombre invalido"}), 400
        if not email:
            return jsonify({"success": False, "error": "Correo invalido"}), 400
        if not telefono:
            return jsonify({"success": False, "error": "Telefono invalido. Debe tener 8 digitos"}), 400
        if tipo_pedido not in {"torta", "pastel"}:
            return jsonify({"success": False, "error": "Selecciona tipo de pedido: Torta o Pastel"}), 400

        cfg_tienda = _obtener_tienda_personalizacion()
        shipping_quote = None
        catalogo_torta_resumen = None
        pastel_catalogo_resumen = []
        pastel_especial_detalle = None
        subtotal_estimado = 0.0
        if tipo_pedido == "torta":
            catalogo_publico = _catalogo_torta_publico((cfg_tienda or {}).get("catalogo_torta") or {})
            if not bool(catalogo_publico.get("enabled")):
                return jsonify({"success": False, "error": "El armado de tortas no esta habilitado"}), 400
            try:
                catalogo_torta_resumen = _validar_payload_catalogo_torta(catalogo_torta_payload, catalogo_publico)
            except ValueError as ve:
                return jsonify({"success": False, "error": str(ve)}), 400
            subtotal_estimado = float(catalogo_torta_resumen.get("subtotal") or 0)
        elif tipo_pedido == "pastel":
            categorias_raw = str((cfg_tienda or {}).get("agenda_pastel_category_filter") or "Pasteles")
            categorias_allow = {
                c.strip().lower()
                for c in categorias_raw.replace(";", ",").split(",")
                if c.strip()
            }
            disponibles = []
            for p in _obtener_productos_para_venta(include_zero_stock=False):
                try:
                    pid = int(p.get("id") or 0)
                except (TypeError, ValueError):
                    pid = 0
                if pid <= 0:
                    continue
                stock_max = int(p.get("max_compra") or 0)
                if stock_max <= 0:
                    continue
                cat_nombre = str(p.get("categoria_tienda") or "General").strip().lower()
                if categorias_allow and cat_nombre not in categorias_allow:
                    continue
                disponibles.append(
                    {
                        "id": pid,
                        "nombre": str(p.get("nombre") or "Producto").strip(),
                        "categoria": str(p.get("categoria_tienda") or "General").strip(),
                        "precio": float(p.get("precio_final") or 0),
                        "max_compra": stock_max,
                    }
                )
            if not disponibles and categorias_allow:
                for p in _obtener_productos_para_venta(include_zero_stock=False):
                    try:
                        pid = int(p.get("id") or 0)
                    except (TypeError, ValueError):
                        pid = 0
                    stock_max = int(p.get("max_compra") or 0)
                    if pid <= 0 or stock_max <= 0:
                        continue
                    disponibles.append(
                        {
                            "id": pid,
                            "nombre": str(p.get("nombre") or "Producto").strip(),
                            "categoria": str(p.get("categoria_tienda") or "General").strip(),
                            "precio": float(p.get("precio_final") or 0),
                            "max_compra": stock_max,
                        }
                    )
            mapa_disponibles = {int(p["id"]): p for p in disponibles}
            if pastel_modo == "catalogo":
                if not pastel_catalogo_payload:
                    return jsonify({"success": False, "error": "Selecciona al menos un pastel del catalogo"}), 400
                acumulado = {}
                for row in pastel_catalogo_payload:
                    try:
                        pid = int(row.get("id") or 0)
                    except (TypeError, ValueError):
                        pid = 0
                    try:
                        qty = int(row.get("cantidad") or 0)
                    except (TypeError, ValueError):
                        qty = 0
                    if pid <= 0 or qty <= 0:
                        continue
                    acumulado[pid] = int(acumulado.get(pid, 0)) + qty
                if not acumulado:
                    return jsonify({"success": False, "error": "Selecciona al menos un pastel del catalogo"}), 400
                for pid, qty in acumulado.items():
                    prod = mapa_disponibles.get(int(pid))
                    if not prod:
                        return jsonify({"success": False, "error": "Uno de los pasteles seleccionados ya no esta disponible"}), 409
                    max_compra = int(prod.get("max_compra") or 0)
                    if qty > max_compra:
                        return jsonify({"success": False, "error": f"{prod.get('nombre')}: maximo {max_compra} unidad(es)"}), 400
                    pastel_catalogo_resumen.append(
                        {
                            "id": int(pid),
                            "nombre": str(prod.get("nombre") or "Producto"),
                            "cantidad": int(qty),
                            "precio_unitario": float(prod.get("precio") or 0),
                            "subtotal": float(prod.get("precio") or 0) * int(qty),
                        }
                    )
                subtotal_estimado = sum(float(it.get("subtotal") or 0) for it in pastel_catalogo_resumen)
            else:
                especial_nombre = str(pastel_especial_payload.get("nombre") or "").strip()[:120]
                especial_detalle = str(pastel_especial_payload.get("detalle") or "").strip()[:400]
                if len(especial_detalle) < 8:
                    return jsonify({"success": False, "error": "Describe el producto fuera de lista para procesar tu solicitud"}), 400
                pastel_especial_detalle = {
                    "nombre": especial_nombre,
                    "detalle": especial_detalle,
                }

        if entrega_tipo == "despacho":
            if len(direccion) < 8:
                return jsonify({"success": False, "error": "Ingresa una direccion valida para despacho"}), 400
            if not direccion_confirmada:
                return jsonify({"success": False, "error": "Debes confirmar la direccion con el pin del mapa"}), 400
            if not (-90 <= latitud <= 90 and -180 <= longitud <= 180):
                return jsonify({"success": False, "error": "Coordenadas de despacho invalidas"}), 400
            shipping_quote = _cotizar_envio_agenda(latitud, longitud, cfg_tienda=cfg_tienda, hora_inicio=hora_inicio)

        fecha_hoy = datetime.now(ZoneInfo("America/Santiago")).date()
        fecha_req = datetime.strptime(fecha, "%Y-%m-%d").date()
        if fecha_req < fecha_hoy:
            return jsonify({"success": False, "error": "No puedes reservar fechas pasadas"}), 400
        topper_96h = False
        pastel_fuera_lista = False
        min_horas_categoria = None
        if tipo_pedido == "torta" and catalogo_torta_resumen:
            topper = catalogo_torta_resumen.get("topper") or {}
            topper_96h = _topper_requiere_96h(topper_id=topper.get("id"), topper_nombre=topper.get("nombre"))
            try:
                min_horas_categoria = int((catalogo_torta_resumen.get("categoria") or {}).get("min_lead_hours") or 0)
            except (TypeError, ValueError):
                min_horas_categoria = None
        if tipo_pedido == "pastel":
            pastel_fuera_lista = pastel_modo == "especial"

        if not _cumple_anticipacion_reserva(
            fecha,
            hora_inicio,
            tipo_pedido,
            cfg_agenda=cfg,
            topper_requiere_96h=topper_96h,
            pastel_fuera_lista=pastel_fuera_lista,
            min_horas_categoria=min_horas_categoria,
        ):
            minutos = _minutos_anticipacion_reserva(
                tipo_pedido,
                topper_requiere_96h=topper_96h,
                pastel_fuera_lista=pastel_fuera_lista,
                min_horas_categoria=min_horas_categoria,
            )
            if tipo_pedido == "torta":
                horas_min = int(max(1, minutos // 60))
                if min_horas_categoria and int(min_horas_categoria) > 48:
                    msg = f"La categoria seleccionada requiere minimo {horas_min} horas de anticipacion"
                else:
                    msg = "Las tortas con topper requieren minimo 96 horas de anticipacion" if topper_96h else "Las tortas requieren minimo 48 horas de anticipacion"
            elif tipo_pedido == "pastel":
                msg = "Las solicitudes fuera de lista requieren minimo 36 horas de anticipacion" if pastel_fuera_lista else "Los pasteles requieren minimo 24 horas de anticipacion"
            else:
                horas_min = int(max(1, minutos // 60))
                msg = f"Este tipo de reserva requiere minimo {horas_min} horas de anticipacion"
            return jsonify({"success": False, "error": msg}), 400

        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("BEGIN IMMEDIATE")
        disp = _calcular_disponibilidad_agenda_tienda(cursor, cfg, fecha, fecha)
        mapa_horas = ((disp.get("mapa") or {}).get(fecha) or {})
        slot = mapa_horas.get(hora_inicio)
        if not slot:
            conn.rollback()
            return jsonify({"success": False, "error": "Horario no disponible en configuracion actual"}), 400
        if not bool(slot.get("disponible")):
            conn.rollback()
            return jsonify({"success": False, "error": "Ese horario ya no tiene cupo disponible"}), 409

        min_ini = _hhmm_a_minutos(hora_inicio)
        min_fin = (min_ini or 0) + int(cfg["slot_minutes"])
        hora_fin = _minutos_a_hhmm(min_fin)
        titulo = f"Reserva tienda - {tipo_pedido.capitalize()}"
        def _fmt_clp(value):
            try:
                n = int(round(float(value or 0)))
            except (TypeError, ValueError):
                n = 0
            return f"${n:,}".replace(",", ".")
        ingredientes = (
            f"Reserva desde tienda online\n"
            f"Email: {email}\n"
            f"Entrega: {'Despacho' if entrega_tipo == 'despacho' else 'Retiro'}"
        )
        if catalogo_torta_resumen:
            categoria_row = catalogo_torta_resumen.get("categoria") or {}
            size_row = catalogo_torta_resumen.get("size") or {}
            sabores_rows = list(catalogo_torta_resumen.get("sabores") or [])
            extras_rows = list(catalogo_torta_resumen.get("extras") or [])
            topper_row = catalogo_torta_resumen.get("topper") or None
            subtotal_cat = float(catalogo_torta_resumen.get("subtotal") or 0)
            ingredientes = f"{ingredientes}\n--- Resumen de cotizacion (cliente) ---"
            ingredientes = f"{ingredientes}\nCategoria: {categoria_row.get('nombre') or '-'}"
            ingredientes = f"{ingredientes}\nTamano: {size_row.get('nombre')} ({_fmt_clp(size_row.get('precio') or 0)})"
            if sabores_rows:
                ingredientes = f"{ingredientes}\nSabores:"
                for sb in sabores_rows:
                    ingredientes = f"{ingredientes}\n- {sb.get('nombre')} ({_fmt_clp(sb.get('precio') or 0)})"
            else:
                ingredientes = f"{ingredientes}\nSabores:\n- -"
            if extras_rows:
                ingredientes = f"{ingredientes}\nExtras:"
                for ex in extras_rows:
                    qty = int(ex.get("qty") or 0)
                    precio_u = float(ex.get("precio") or 0)
                    ingredientes = f"{ingredientes}\n- {ex.get('nombre')} x{qty} ({_fmt_clp(precio_u * qty)})"
            else:
                ingredientes = f"{ingredientes}\nExtras:\n- -"
            if topper_row:
                ingredientes = f"{ingredientes}\nTopper:\n- {topper_row.get('nombre')} ({_fmt_clp(topper_row.get('precio') or 0)})"
            else:
                ingredientes = f"{ingredientes}\nTopper:\n- Sin topper"
            ingredientes = f"{ingredientes}\nSubtotal estimado productos: {_fmt_clp(subtotal_cat)}"
            if catalogo_torta_resumen.get("nota"):
                ingredientes = f"{ingredientes}\nNota catalogo: {catalogo_torta_resumen['nota']}"
            refs = [str(r or "").strip() for r in (catalogo_torta_resumen.get("referencia_urls") or []) if str(r or "").strip()]
            if refs:
                ingredientes = f"{ingredientes}\nReferencias:"
                for ref in refs:
                    ingredientes = f"{ingredientes}\n- {ref}"
            else:
                ingredientes = f"{ingredientes}\nReferencias: -"
            try:
                if isinstance(catalogo_torta_payload, dict) and catalogo_torta_payload:
                    builder_raw = {
                        "categoria_id": str(catalogo_torta_payload.get("categoria_id") or ""),
                        "size_id": str(catalogo_torta_payload.get("size_id") or ""),
                        "sabor_ids": list(catalogo_torta_payload.get("sabor_ids") or []),
                        "extra_items": list(catalogo_torta_payload.get("extra_items") or []),
                        "topper_id": str(catalogo_torta_payload.get("topper_id") or ""),
                        "referencia_urls": list(catalogo_torta_payload.get("referencia_urls") or []),
                        "nota": str(catalogo_torta_payload.get("nota") or ""),
                    }
                else:
                    builder_raw = {
                        "categoria_id": str(categoria_row.get("id") or ""),
                        "size_id": str(size_row.get("id") or ""),
                        "sabor_ids": [str((sb or {}).get("id") or "") for sb in sabores_rows if str((sb or {}).get("id") or "")],
                        "extra_items": [
                            {"id": str((ex or {}).get("id") or ""), "qty": int((ex or {}).get("qty") or 0)}
                            for ex in extras_rows
                            if str((ex or {}).get("id") or "")
                        ],
                        "topper_id": str((topper_row or {}).get("id") or "") if topper_row else "",
                        "referencia_urls": refs,
                        "nota": str(catalogo_torta_resumen.get("nota") or ""),
                    }
                ingredientes = f"{ingredientes}\n--- Builder JSON ---\n{json.dumps(builder_raw, ensure_ascii=False, separators=(',', ':'))}"
            except Exception:
                pass
        if tipo_pedido == "pastel":
            if pastel_modo == "catalogo" and pastel_catalogo_resumen:
                ingredientes = f"{ingredientes}\n--- Resumen de cotizacion (cliente) ---"
                ingredientes = f"{ingredientes}\nPasteles catalogo:"
                for item in pastel_catalogo_resumen:
                    ingredientes = (
                        f"{ingredientes}\n- {item.get('nombre')} x{int(item.get('cantidad') or 0)} ({_fmt_clp(item.get('subtotal') or 0)})"
                    )
                ingredientes = f"{ingredientes}\nSubtotal estimado productos: {_fmt_clp(subtotal_estimado)}"
            if pastel_modo == "especial" and pastel_especial_detalle:
                ingredientes = f"{ingredientes}\nSolicitud fuera de lista (36h):"
                if pastel_especial_detalle.get("nombre"):
                    ingredientes = f"{ingredientes}\nProducto solicitado: {pastel_especial_detalle.get('nombre')}"
                ingredientes = f"{ingredientes}\nDetalle solicitud: {pastel_especial_detalle.get('detalle')}"
        if detalle:
            ingredientes = f"{ingredientes}\nDetalle: {detalle}"
        if entrega_tipo == "despacho":
            ingredientes = f"{ingredientes}\nMapa pin: {latitud:.6f},{longitud:.6f}"
            if shipping_quote:
                if bool(shipping_quote.get("inside_maipu")) and shipping_quote.get("shipping_fee") is not None:
                    ingredientes = f"{ingredientes}\nEnvio: {_fmt_clp(shipping_quote.get('shipping_fee') or 0)}"
                    ingredientes = f"{ingredientes}\nDistancia estimada: {float(shipping_quote.get('distance_km') or 0):.2f} km ({shipping_quote.get('range_label')})"
                else:
                    ingredientes = f"{ingredientes}\n{str(shipping_quote.get('warning') or '')}"
        despacho_estimado_txt = float(shipping_quote.get("shipping_fee") or 0) if (shipping_quote and bool(shipping_quote.get("inside_maipu"))) else 0.0
        total_estimado_txt = float(subtotal_estimado) + float(despacho_estimado_txt)
        ingredientes = f"{ingredientes}\nTotal estimado pedido: {_fmt_clp(total_estimado_txt)}"
        codigo_op = f"OPA-TI-{int(time.time())}-{uuid.uuid4().hex[:6].upper()}"[:80]
        codigo_pedido = ""

        cursor.execute(
            """
            INSERT INTO agenda_eventos (
                tipo, titulo, fecha, hora_inicio, hora_fin, hora_entrega,
                cliente, telefono, cliente_email, cliente_telefono, es_envio, direccion, ingredientes,
                total, abono, motivo, alerta_minutos, estado, codigo_operacion
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, 1440, 'pendiente', ?)
            """,
            (
                tipo_pedido,
                titulo,
                fecha,
                hora_inicio,
                hora_fin,
                hora_inicio,
                nombre,
                telefono,
                email,
                telefono,
                1 if entrega_tipo == "despacho" else 0,
                direccion if entrega_tipo == "despacho" else None,
                ingredientes,
                "Reserva cliente tienda online",
                codigo_op,
            ),
        )
        reserva_id = int(cursor.lastrowid or 0)
        if reserva_id > 0:
            fecha_codigo = re.sub(r"[^0-9]", "", str(fecha or "").strip())[:8] or datetime.now(ZoneInfo("America/Santiago")).strftime("%Y%m%d")
            codigo_pedido = f"AGD-{fecha_codigo}-{reserva_id:06d}"
            cursor.execute(
                "UPDATE agenda_eventos SET codigo_pedido = ? WHERE id = ?",
                (codigo_pedido, reserva_id),
            )
        cliente = _upsert_cliente_tienda_cursor(
            cursor,
            nombre=nombre,
            email=email,
            telefono=telefono,
            email_confirmado=1,
            direccion_default=(direccion if entrega_tipo == "despacho" else ""),
            direccion_lat=(latitud if entrega_tipo == "despacho" else None),
            direccion_lng=(longitud if entrega_tipo == "despacho" else None),
        )
        if cliente:
            cfg_prog = _obtener_config_programa_clientes(conn)
            puntos = _puntos_agenda(cfg_prog)
            _registrar_puntos_cliente_cursor(
                cursor,
                cliente_id=int(cliente.get("id") or 0),
                puntos=puntos,
                tipo="agenda",
                origen_tipo="agenda_reserva",
                origen_id=reserva_id,
                detalle=f"Reserva agenda #{reserva_id}",
            )
        conn.commit()
        crear_backup()
        despacho_estimado = float(shipping_quote.get("shipping_fee") or 0) if (shipping_quote and bool(shipping_quote.get("inside_maipu"))) else 0.0
        total_estimado = float(subtotal_estimado) + float(despacho_estimado)
        return jsonify(
            {
                "success": True,
                "reserva": {
                    "id": reserva_id,
                    "codigo_pedido": codigo_pedido or None,
                    "fecha": fecha,
                    "hora_inicio": hora_inicio,
                    "hora_fin": hora_fin,
                    "cliente": nombre,
                    "telefono": telefono,
                    "entrega_tipo": entrega_tipo,
                    "direccion": direccion if entrega_tipo == "despacho" else "",
                    "lat": latitud if entrega_tipo == "despacho" else None,
                    "lng": longitud if entrega_tipo == "despacho" else None,
                    "catalogo_torta": catalogo_torta_resumen,
                    "pastel_modo": pastel_modo if tipo_pedido == "pastel" else "",
                    "pastel_catalogo": pastel_catalogo_resumen if tipo_pedido == "pastel" else [],
                    "pastel_especial": pastel_especial_detalle if tipo_pedido == "pastel" else None,
                    "shipping_quote": shipping_quote if entrega_tipo == "despacho" else None,
                    "subtotal_estimado": round(float(subtotal_estimado), 2),
                    "despacho_estimado": round(float(despacho_estimado), 2),
                    "total_estimado": round(float(total_estimado), 2),
                },
            }
        )
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/agenda/reserva/<int:reserva_id>/whatsapp-pasteleria', methods=['POST'])
def api_tienda_agenda_reserva_whatsapp_pasteleria(reserva_id):
    conn = None
    try:
        if int(reserva_id or 0) <= 0:
            return jsonify({"success": False, "error": "ID de reserva invalido"}), 400
        if not _bool_env("GESTIONSTOCK_WHATSAPP_ENABLED", default=False):
            return jsonify({"success": False, "error": "WhatsApp automatico deshabilitado en servidor"}), 400
        if not _twilio_whatsapp_configurado():
            return jsonify({"success": False, "error": "Twilio WhatsApp no esta configurado en servidor"}), 400

        data = request.get_json(silent=True) or {}
        nombre_origen = str(data.get("nombre") or "").strip()
        email_origen = str(data.get("email") or "").strip()

        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, tipo, fecha, hora_inicio, hora_fin, cliente, telefono, direccion, ingredientes, codigo_operacion
            FROM agenda_eventos
            WHERE id = ?
            LIMIT 1
            """,
            (int(reserva_id),),
        )
        row = cur.fetchone()
        if not row:
            return jsonify({"success": False, "error": "Reserva no encontrada"}), 404
        reserva = dict(row)
        filename = _crear_pdf_reserva_agenda_tienda(reserva)
        media_url = f"{_public_base_url(request.url_root)}/static/tienda_pedidos_pdf/{quote(filename)}"

        contacto = nombre_origen or email_origen or str(reserva.get("cliente") or "").strip() or f"reserva #{int(reserva_id)}"
        body = (
            f"Hola mi nombre {contacto} envio mi cotizacion para una pronta revision.\n"
            f"Reserva #{int(reserva_id)}\n"
            f"Tipo: {str(reserva.get('tipo') or '').capitalize()}\n"
            f"Fecha: {reserva.get('fecha') or '-'} {reserva.get('hora_inicio') or '-'}\n"
            "Adjunto PDF de la reserva."
        )
        ok, err = _enviar_whatsapp_twilio(body, media_url=media_url)
        if not ok:
            return jsonify({"success": False, "error": err or "No se pudo enviar WhatsApp"}), 502
        return jsonify({"success": True, "media_url": media_url})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/cupon/validar', methods=['POST'])
def api_tienda_validar_cupon():
    try:
        estado_tienda = _evaluar_estado_tienda(_obtener_tienda_config())
        if not bool(estado_tienda.get("abierta")):
            return jsonify({"success": False, "error": "La tienda esta cerrada por el momento"}), 403
        data = request.get_json(silent=True) or {}
        cupon_codigo = _normalizar_cupon_codigo(data.get("codigo_descuento"))
        if not cupon_codigo:
            return jsonify({"success": False, "error": "Ingresa un codigo de descuento"}), 400
        items_req = data.get("items") or []
        if not isinstance(items_req, list) or not items_req:
            return jsonify({"success": False, "error": "Carrito vacio"}), 400

        now_local = datetime.now(ZoneInfo("America/Santiago"))
        categorias = _cargar_categorias_tienda()
        categorias_map = {str(c.get("nombre") or "").strip().lower(): c for c in categorias}
        mapa = {
            int(p.get("id") or 0): _serializar_producto_tienda(p, categorias_map=categorias_map, now_local=now_local)
            for p in _obtener_productos_para_venta()
        }
        items_serializados = []
        subtotal = 0.0
        for idx, raw in enumerate(items_req, start=1):
            if not isinstance(raw, dict):
                return jsonify({'success': False, 'error': f'Item #{idx} invalido'}), 400
            try:
                pid = int(raw.get("id") or 0)
                cantidad = int(raw.get("cantidad") or 0)
            except (TypeError, ValueError):
                return jsonify({'success': False, 'error': f'Item #{idx}: formato invalido'}), 400
            if pid <= 0 or cantidad <= 0:
                return jsonify({'success': False, 'error': f'Item #{idx}: datos invalidos'}), 400
            prod = mapa.get(pid)
            if not prod:
                return jsonify({'success': False, 'error': f'Producto #{pid} no disponible'}), 400
            pack_rule = pack_rules_by_product.get(pid) or {"max_total": 0, "items": {}}
            pack_items_input = raw.get("pack_items")
            if pack_rule.get("items"):
                if not isinstance(pack_items_input, list) or not pack_items_input:
                    return jsonify({'success': False, 'error': f'{prod.get("nombre")}: debes elegir subitems del pack'}), 400
                resumen_pack = {}
                for pidx, pick in enumerate(pack_items_input, start=1):
                    if not isinstance(pick, dict):
                        return jsonify({'success': False, 'error': f'{prod.get("nombre")}: subitem #{pidx} invalido'}), 400
                    sid = int(pick.get("subproducto_id") or 0)
                    sqty = int(pick.get("cantidad") or 0)
                    if sid <= 0 or sqty <= 0:
                        return jsonify({'success': False, 'error': f'{prod.get("nombre")}: subitem #{pidx} invalido'}), 400
                    if sid not in pack_rule["items"]:
                        return jsonify({'success': False, 'error': f'{prod.get("nombre")}: subitem no permitido en este pack'}), 400
                    if not bool(pack_rule["items"][sid].get("activo_tienda")):
                        return jsonify({'success': False, 'error': f'{prod.get("nombre")}: {pack_rule["items"][sid].get("nombre")} esta apagado en tienda'}), 400
                    resumen_pack[sid] = int(resumen_pack.get(sid, 0)) + sqty
                total_sel = sum(int(v or 0) for v in resumen_pack.values())
                if total_sel <= 0:
                    return jsonify({'success': False, 'error': f'{prod.get("nombre")}: selecciona al menos 1 subitem'}), 400
                max_total_pack = int(pack_rule.get("max_total") or 0)
                if max_total_pack > 0 and total_sel > max_total_pack:
                    return jsonify({'success': False, 'error': f'{prod.get("nombre")}: maximo total {max_total_pack} subitems'}), 400
                pack_items_final = []
                partes = []
                for sid, sqty in resumen_pack.items():
                    max_item = int(pack_rule["items"][sid].get("max_cantidad") or 1)
                    if sqty > max_item:
                        return jsonify({'success': False, 'error': f'{prod.get("nombre")}: {pack_rule["items"][sid].get("nombre")} maximo {max_item}'}), 400
                    nombre_sub = str(pack_rule["items"][sid].get("nombre") or f"Producto #{sid}")
                    pack_items_final.append({"subproducto_id": int(sid), "cantidad": int(sqty), "nombre": nombre_sub})
                    partes.append(f"{nombre_sub} x{int(sqty)}")
                pack_detalle_por_producto[pid] = "Pack: " + ", ".join(partes)
            else:
                pack_items_final = []
            if not bool(prod.get("categoria_activa", True)):
                return jsonify({'success': False, 'error': f'{prod.get("nombre")}: categoria no disponible en este horario'}), 400
            max_compra = int(prod.get("max_compra") or 0)
            if cantidad > max_compra:
                return jsonify({'success': False, 'error': f'{prod.get("nombre")}: maximo {max_compra} unidad(es)'}), 400
            precio_final = float(prod.get("precio_final") or 0)
            subtotal += (precio_final * cantidad)
            items_serializados.append(
                {
                    "id": pid,
                    "cantidad": cantidad,
                    "precio_unitario": precio_final,
                    "descuento_tienda_pct": float(prod.get("descuento_tienda_pct") or 0),
                }
            )

        cliente_ref = _normalizar_cliente_ref(data.get("cliente_email"), data.get("cliente_telefono"))
        cupon = _obtener_cupon_por_codigo(cupon_codigo)
        valid = _validar_cupon_y_calcular_descuento(cupon, subtotal, items_serializados, cliente_ref)
        if not valid.get("ok"):
            return jsonify({"success": False, "error": valid.get("error", "Cupon invalido")}), 400

        descuento_monto = float(valid.get("descuento_monto") or 0)
        total = subtotal - descuento_monto
        if total < 0:
            total = 0
        return jsonify(
            {
                "success": True,
                "codigo_descuento": cupon_codigo,
                "subtotal": round(subtotal, 2),
                "descuento_monto": round(descuento_monto, 2),
                "total_monto": round(total, 2),
            }
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/historial-cambios')
def historial_cambios():
    try:
        movimientos = listar_historial_cambios(limit=1500)
        return render_template('historial_cambios.html', movimientos=movimientos)
    except Exception as e:
        return f"Error cargando historial de cambios: {str(e)}", 500


@app.route('/api/historial-cambios/<int:movimiento_id>/eliminar', methods=['POST'])
def api_eliminar_historial_cambios(movimiento_id):
    try:
        resultado = eliminar_historial_cambio(movimiento_id)
        if not resultado.get('success'):
            msg = str(resultado.get('error') or '').lower()
            status = 404 if 'no encontrado' in msg else 400
            return jsonify(resultado), status
        crear_backup()
        return jsonify(resultado)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/estadisticas')
def estadisticas():
    try:
        conn = get_db()
        cursor = conn.cursor()

        cursor.execute("PRAGMA table_info(productos)")
        columnas_productos = {str(r["name"]).strip().lower() for r in (cursor.fetchall() or []) if r and r["name"]}
        tiene_activo_tienda = "activo_tienda" in columnas_productos

        cursor.execute("SELECT COUNT(*) FROM productos WHERE COALESCE(eliminado, 0) = 0")
        total_productos = int(cursor.fetchone()[0] or 0)
        if tiene_activo_tienda:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM productos
                WHERE COALESCE(eliminado, 0) = 0
                  AND COALESCE(activo_tienda, 1) = 1
                """
            )
            activos_tienda = int(cursor.fetchone()[0] or 0)
        else:
            activos_tienda = total_productos

        cursor.execute("SELECT COUNT(*) FROM insumos")
        total_insumos = int(cursor.fetchone()[0] or 0)

        alertas_productos = _obtener_alertas_productos(cursor)
        alertas_prod = len(alertas_productos["ids_union"])
        
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM insumos
            WHERE CAST(stock_minimo AS REAL) > 0
              AND CAST(stock AS REAL) <= CAST(stock_minimo AS REAL)
            """
        )
        alertas_ins = int(cursor.fetchone()[0] or 0)
        haccp_vencidos = int(contar_haccp_vencidos(conn=conn) or 0)

        cursor.execute("SELECT COUNT(*) FROM ventas WHERE date(fecha_hora) = date('now','localtime')")
        ventas_hoy = int(cursor.fetchone()[0] or 0)
        cursor.execute("SELECT COUNT(*) FROM ventas WHERE date(fecha_hora) = date('now','-7 day','localtime')")
        ventas_hoy_semana_pasada = int(cursor.fetchone()[0] or 0)

        cursor.execute("SELECT COALESCE(SUM(COALESCE(total_monto,0)),0) FROM ventas WHERE date(fecha_hora) = date('now','localtime')")
        monto_ventas_hoy = float(cursor.fetchone()[0] or 0)
        cursor.execute("SELECT COALESCE(SUM(COALESCE(total_monto,0)),0) FROM ventas WHERE date(fecha_hora) = date('now','-7 day','localtime')")
        monto_ventas_semana_pasada = float(cursor.fetchone()[0] or 0)

        try:
            cursor.execute(
                """
                SELECT COUNT(*) AS total
                FROM tienda_visitas_eventos
                WHERE evento IN ('view', 'enter')
                  AND date(creado_en, 'localtime') = date('now', 'localtime')
                """
            )
            visitas_hoy = int(cursor.fetchone()["total"] or 0)
            cursor.execute(
                """
                SELECT COUNT(*) AS total
                FROM tienda_visitas_eventos
                WHERE evento IN ('view', 'enter')
                  AND date(creado_en, 'localtime') = date('now', '-7 day', 'localtime')
                """
            )
            visitas_hoy_semana_pasada = int(cursor.fetchone()["total"] or 0)
        except sqlite3.OperationalError:
            cursor.execute(
                """
                SELECT COUNT(*) AS total
                FROM tienda_visitas
                WHERE date(ultima_actividad, 'localtime') = date('now', 'localtime')
                """
            )
            visitas_hoy = int(cursor.fetchone()["total"] or 0)
            cursor.execute(
                """
                SELECT COUNT(*) AS total
                FROM tienda_visitas
                WHERE date(ultima_actividad, 'localtime') = date('now', '-7 day', 'localtime')
                """
            )
            visitas_hoy_semana_pasada = int(cursor.fetchone()["total"] or 0)

        def _pct(actual, previo):
            a = float(actual or 0)
            p = float(previo or 0)
            if p <= 0:
                return 100.0 if a > 0 else 0.0
            return round(((a - p) / p) * 100.0, 1)

        cursor.execute("""
            SELECT COUNT(*) FROM productos 
            WHERE COALESCE(eliminado, 0) = 0
              AND fecha_vencimiento IS NOT NULL 
              AND fecha_vencimiento <= date('now', '+2 days')
        """)
        por_vencer = int(cursor.fetchone()[0] or 0)
        
        conn.close()
        
        return jsonify({
            'productos': total_productos,
            'productos_activos_tienda': activos_tienda,
            'productos_total_tienda': total_productos,
            'insumos': total_insumos,
            'alertas': alertas_prod + alertas_ins + haccp_vencidos,
            'haccp_vencidos': haccp_vencidos,
            'ventas_hoy': ventas_hoy,
            'ventas_hoy_semana_pasada': ventas_hoy_semana_pasada,
            'monto_ventas_hoy': round(monto_ventas_hoy, 2),
            'monto_ventas_semana_pasada': round(monto_ventas_semana_pasada, 2),
            'monto_ventas_hoy_pct': _pct(monto_ventas_hoy, monto_ventas_semana_pasada),
            'visitas_hoy': visitas_hoy,
            'visitas_hoy_semana_pasada': visitas_hoy_semana_pasada,
            'visitas_hoy_pct': _pct(visitas_hoy, visitas_hoy_semana_pasada),
            'por_vencer': por_vencer
        })
    except Exception as e:
        print(f"Error estadisticas: {e}")
        return jsonify({
            'productos': 0,
            'productos_activos_tienda': 0,
            'productos_total_tienda': 0,
            'insumos': 0,
            'alertas': 0,
            'haccp_vencidos': 0,
            'ventas_hoy': 0,
            'ventas_hoy_semana_pasada': 0,
            'monto_ventas_hoy': 0,
            'monto_ventas_semana_pasada': 0,
            'monto_ventas_hoy_pct': 0,
            'visitas_hoy': 0,
            'visitas_hoy_semana_pasada': 0,
            'visitas_hoy_pct': 0,
            'por_vencer': 0,
        })

@app.route('/productos')
def productos():
    try:
        orden = request.args.get('orden', 'nombre')
        direccion = request.args.get('dir', 'asc')
        solo_cero = request.args.get('cero', '0') == '1'
        
        productos = obtener_productos_con_dias(orden, direccion, solo_cero)
        insumos_dependencia = []
        conn = None
        try:
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, nombre, COALESCE(NULLIF(TRIM(unidad), ''), 'unidad') AS unidad
                FROM insumos
                ORDER BY nombre COLLATE NOCASE ASC
                """
            )
            insumos_dependencia = [
                {
                    "id": int(r["id"]),
                    "nombre": r["nombre"] or "Insumo",
                    "unidad": _normalizar_unidad_producto(r["unidad"] or "unidad"),
                }
                for r in cursor.fetchall()
            ]
        finally:
            if conn:
                conn.close()
        
        return render_template('productos.html', 
                             productos=productos, 
                             orden=orden, 
                             direccion=direccion,
                              solo_cero=solo_cero,
                             iconos_catalogo=_catalogo_iconos_producto(),
                             insumos_dependencia=insumos_dependencia)
    except Exception as e:
        print(f"Error en productos: {e}")
        import traceback
        traceback.print_exc()
        return f"Error: {str(e)}", 500

@app.route('/api/productos/todos')
def api_productos_todos():
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, nombre, stock,
                   COALESCE(NULLIF(TRIM(unidad), ''), 'unidad') AS unidad
            FROM productos
            WHERE COALESCE(eliminado, 0) = 0
            ORDER BY nombre COLLATE NOCASE ASC
            """
        )
        productos = cursor.fetchall()
        conn.close()
        return jsonify(
            [
                {
                    'id': int(p['id']),
                    'nombre': p['nombre'] or 'Producto',
                    'stock': float(p['stock'] or 0),
                    'unidad': _normalizar_unidad_producto(p['unidad'] or 'unidad'),
                }
                for p in productos
            ]
        )
    except Exception as e:
        return jsonify([])


@app.route('/api/insumos/todos')
def api_insumos_todos():
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, nombre, COALESCE(NULLIF(TRIM(unidad), ''), 'unidad') AS unidad, stock
            FROM insumos
            ORDER BY nombre COLLATE NOCASE ASC
            """
        )
        filas = cursor.fetchall()
        return jsonify(
            {
                "success": True,
                "insumos": [
                    {
                        "id": int(f["id"]),
                        "nombre": f["nombre"] or "Insumo",
                        "unidad": _normalizar_unidad_producto(f["unidad"] or "unidad"),
                        "stock": float(f["stock"] or 0),
                    }
                    for f in filas
                ],
            }
        )
    except Exception as e:
        return jsonify({"success": False, "insumos": [], "error": str(e)}), 500
    finally:
        try:
            conn.close()
        except Exception:
            pass


@app.route('/api/productos/stock-disponible')
def api_productos_stock_disponible():
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(productos)")
        columnas = {str(row["name"]).strip().lower() for row in cursor.fetchall()}

        sel_categoria = "categoria" if "categoria" in columnas else "'General' AS categoria"
        sel_unidad = "COALESCE(NULLIF(TRIM(unidad), ''), 'unidad') AS unidad" if "unidad" in columnas else "'unidad' AS unidad"

        if "fecha_vencimiento" in columnas:
            sel_fecha_venc = "fecha_vencimiento"
            sel_dias = (
                "CASE "
                "WHEN fecha_vencimiento IS NOT NULL "
                "THEN CAST(julianday(fecha_vencimiento) - julianday(date('now')) AS INTEGER) "
                "ELSE NULL END AS dias_para_vencer"
            )
        else:
            sel_fecha_venc = "NULL AS fecha_vencimiento"
            sel_dias = "NULL AS dias_para_vencer"

        cursor.execute(
            f"""
            SELECT
                id,
                nombre,
                {sel_categoria},
                stock,
                {sel_unidad},
                {sel_fecha_venc},
                {sel_dias}
            FROM productos
            WHERE COALESCE(eliminado, 0) = 0
              AND COALESCE(stock, 0) > 0
            ORDER BY nombre COLLATE NOCASE ASC
            """
        )
        rows = cursor.fetchall()
        conn.close()

        productos = []
        for row in rows:
            productos.append(
                {
                    "id": row["id"],
                    "nombre": row["nombre"],
                    "categoria": row["categoria"] or "General",
                    "stock": float(row["stock"] or 0),
                    "unidad": row["unidad"] or "unidad",
                    "fecha_vencimiento": row["fecha_vencimiento"],
                    "dias_para_vencer": row["dias_para_vencer"],
                }
            )
        return jsonify({"success": True, "productos": productos})
    except Exception as e:
        return jsonify({"success": False, "error": str(e), "productos": []}), 500


@app.route('/api/producto/<int:id>/detalle')
def api_detalle_producto(id):
    try:
        producto = obtener_producto_detalle(id)
        if not producto:
            return jsonify({'success': False, 'error': 'Producto no encontrado'}), 404
        return jsonify({'success': True, 'producto': producto})
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


def _ensure_producto_pack_subopciones_table(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS producto_pack_subopciones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            producto_pack_id INTEGER NOT NULL,
            subproducto_id INTEGER NOT NULL,
            max_cantidad INTEGER NOT NULL DEFAULT 1,
            orden INTEGER NOT NULL DEFAULT 0,
            creado_en TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(producto_pack_id, subproducto_id)
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_pack_subopciones_pack ON producto_pack_subopciones(producto_pack_id, orden, id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_pack_subopciones_sub ON producto_pack_subopciones(subproducto_id)"
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS producto_pack_subopciones_config (
            producto_pack_id INTEGER PRIMARY KEY,
            max_total INTEGER NOT NULL DEFAULT 0,
            actualizado_en TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


@app.route('/api/producto/<int:id>/pack-subopciones', methods=['GET'])
def api_producto_pack_subopciones_get(id):
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()
        _ensure_producto_pack_subopciones_table(cur)
        cur.execute("SELECT id, nombre, COALESCE(activo_tienda, 1) AS activo_tienda FROM productos WHERE id = ? LIMIT 1", (int(id),))
        pack = cur.fetchone()
        if not pack:
            return jsonify({"success": False, "error": "Producto no encontrado"}), 404
        cur.execute(
            """
            SELECT s.id, s.producto_pack_id, s.subproducto_id, s.max_cantidad, s.orden,
                   COALESCE(p.nombre, 'Producto #' || s.subproducto_id) AS subproducto_nombre,
                   COALESCE(p.activo_tienda, 1) AS subproducto_activo_tienda
            FROM producto_pack_subopciones s
            LEFT JOIN productos p ON p.id = s.subproducto_id
            WHERE s.producto_pack_id = ?
            ORDER BY s.orden ASC, s.id ASC
            """,
            (int(id),),
        )
        opciones = [dict(r) for r in cur.fetchall()]
        cur.execute(
            "SELECT max_total FROM producto_pack_subopciones_config WHERE producto_pack_id = ? LIMIT 1",
            (int(id),),
        )
        row_cfg = cur.fetchone()
        max_total = int((row_cfg["max_total"] if row_cfg else 0) or 0)
        return jsonify({"success": True, "opciones": opciones, "max_total": max_total})
    except Exception as e:
        return jsonify({"success": False, "error": str(e), "opciones": []}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/producto/<int:id>/pack-subopciones', methods=['POST'])
def api_producto_pack_subopciones_post(id):
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        data = request.get_json(silent=True) or {}
        raw_items = data.get("items") or []
        if not isinstance(raw_items, list):
            return jsonify({"success": False, "error": "items debe ser una lista"}), 400
        max_total = int(data.get("max_total") or 0)
        if max_total < 1:
            max_total = 1
        if max_total > 1000:
            max_total = 1000
        conn = get_db()
        cur = conn.cursor()
        _ensure_producto_pack_subopciones_table(cur)
        cur.execute("SELECT id FROM productos WHERE id = ? LIMIT 1", (int(id),))
        if not cur.fetchone():
            return jsonify({"success": False, "error": "Producto no encontrado"}), 404

        items_limpios = []
        usados = set()
        for idx, item in enumerate(raw_items, start=1):
            if not isinstance(item, dict):
                return jsonify({"success": False, "error": f"Subopcion #{idx} invalida"}), 400
            sub_id = int(item.get("subproducto_id") or 0)
            if sub_id <= 0:
                return jsonify({"success": False, "error": f"Subopcion #{idx}: producto invalido"}), 400
            if sub_id == int(id):
                return jsonify({"success": False, "error": "No puedes agregar el mismo producto dentro de su pack"}), 400
            if sub_id in usados:
                continue
            usados.add(sub_id)
            max_cantidad = int(item.get("max_cantidad") or 1)
            if max_cantidad < 1:
                max_cantidad = 1
            if max_cantidad > 100:
                max_cantidad = 100
            items_limpios.append({
                "subproducto_id": sub_id,
                "max_cantidad": max_cantidad,
                "orden": len(items_limpios) + 1,
            })

        if items_limpios:
            placeholders = ",".join(["?"] * len(items_limpios))
            cur.execute(
                f"""
                SELECT id, nombre, COALESCE(activo_tienda, 1) AS activo_tienda
                FROM productos
                WHERE id IN ({placeholders})
                """,
                tuple(x["subproducto_id"] for x in items_limpios),
            )
            mapa = {int(r["id"]): dict(r) for r in cur.fetchall()}
            for it in items_limpios:
                p = mapa.get(int(it["subproducto_id"]))
                if not p:
                    return jsonify({"success": False, "error": f"Producto asociado #{it['subproducto_id']} no existe"}), 400
                if int(p.get("activo_tienda") or 0) != 1:
                    return jsonify({"success": False, "error": f"Producto asociado '{p.get('nombre')}' esta apagado en tienda"}), 400

        cur.execute("DELETE FROM producto_pack_subopciones WHERE producto_pack_id = ?", (int(id),))
        cur.execute(
            """
            INSERT INTO producto_pack_subopciones_config (producto_pack_id, max_total, actualizado_en)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(producto_pack_id) DO UPDATE SET
                max_total = excluded.max_total,
                actualizado_en = CURRENT_TIMESTAMP
            """,
            (int(id), int(max_total)),
        )
        for it in items_limpios:
            cur.execute(
                """
                INSERT INTO producto_pack_subopciones (producto_pack_id, subproducto_id, max_cantidad, orden)
                VALUES (?, ?, ?, ?)
                """,
                (int(id), int(it["subproducto_id"]), int(it["max_cantidad"]), int(it["orden"])),
            )
        conn.commit()
        crear_backup()
        return jsonify({"success": True, "guardadas": len(items_limpios)})
    except ValueError as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/producto/<int:id>/actualizar', methods=['POST'])
def api_actualizar_producto(id):
    try:
        data = request.get_json(silent=True) or {}
        if "unidad" in data:
            data["unidad"] = _normalizar_unidad_producto(data.get("unidad"))
        if "porcion_unidad" in data:
            data["porcion_unidad"] = _normalizar_unidad_producto(data.get("porcion_unidad"))
        if "porcion_cantidad" in data:
            data["porcion_cantidad"] = float(data.get("porcion_cantidad") or 1)
        if "icono" in data:
            data["icono"] = _normalizar_icono_producto(data.get("icono"))
        if "stock_dependencia_tipo" in data:
            tipo_dep = str(data.get("stock_dependencia_tipo") or "").strip().lower()
            data["stock_dependencia_tipo"] = tipo_dep
        if "stock_dependencia_id" in data:
            raw_id = data.get("stock_dependencia_id")
            if raw_id in (None, "", "null"):
                data["stock_dependencia_id"] = None
            else:
                data["stock_dependencia_id"] = int(raw_id)
        if "stock_dependencia_cantidad" in data:
            data["stock_dependencia_cantidad"] = float(data.get("stock_dependencia_cantidad") or 1)
        if "categoria_tienda" in data:
            data["categoria_tienda"] = str(data.get("categoria_tienda") or "").strip()[:60] or "General"
        if "descripcion_tienda" in data:
            data["descripcion_tienda"] = str(data.get("descripcion_tienda") or "").strip()[:800]
        if "descuento_tienda_pct" in data:
            data["descuento_tienda_pct"] = float(data.get("descuento_tienda_pct") or 0)
        if "stock" in data:
            data["stock"] = float(data.get("stock") or 0)
            if float(data["stock"]) < 0:
                raise ValueError("El stock no puede ser negativo")
            # Regla operativa: si el stock queda en 0, el producto debe quedar apagado en tienda.
            if float(data["stock"]) <= 0:
                data["activo_tienda"] = False
            # Si se repone stock desde ventas y no se envio el toggle manual,
            # reactivar tienda automaticamente para evitar que quede oculto.
            elif "activo_tienda" not in data:
                data["activo_tienda"] = True
        if "oferta_inicio_tienda" in data:
            data["oferta_inicio_tienda"] = str(data.get("oferta_inicio_tienda") or "").strip() or None
        if "oferta_fin_tienda" in data:
            data["oferta_fin_tienda"] = str(data.get("oferta_fin_tienda") or "").strip() or None
        if "fecha_reposicion_tienda" in data:
            data["fecha_reposicion_tienda"] = str(data.get("fecha_reposicion_tienda") or "").strip() or None
        if "foto_fit_tienda" in data:
            fit = str(data.get("foto_fit_tienda") or "cover").strip().lower()
            data["foto_fit_tienda"] = fit if fit in {"cover", "contain"} else "cover"
        if "foto_pos_tienda" in data:
            pos = str(data.get("foto_pos_tienda") or "center").strip().lower()
            data["foto_pos_tienda"] = pos if pos in {"center", "top", "bottom"} else "center"
        if "foto_pos_x_tienda" in data:
            data["foto_pos_x_tienda"] = float(data.get("foto_pos_x_tienda") or 50)
        if "foto_pos_y_tienda" in data:
            data["foto_pos_y_tienda"] = float(data.get("foto_pos_y_tienda") or 50)
        if "foto_zoom_tienda" in data:
            data["foto_zoom_tienda"] = float(data.get("foto_zoom_tienda") or 100)
        if "destacado_tienda" in data:
            raw_dest = data.get("destacado_tienda")
            if isinstance(raw_dest, str):
                data["destacado_tienda"] = raw_dest.strip().lower() in {"1", "true", "si", "yes", "on"}
            else:
                data["destacado_tienda"] = bool(raw_dest)
        if "orden_tienda" in data:
            data["orden_tienda"] = int(data.get("orden_tienda") or 0)
        if "activo_tienda" in data:
            raw_activo = data.get("activo_tienda")
            if isinstance(raw_activo, str):
                data["activo_tienda"] = raw_activo.strip().lower() in {"1", "true", "si", "yes", "on"}
            else:
                data["activo_tienda"] = bool(raw_activo)
        if "insumos_venta" in data:
            if not isinstance(data.get("insumos_venta"), list):
                raise ValueError("Los insumos asociados deben enviarse en una lista")
            insumos_limpios = []
            for idx, fila in enumerate(data.get("insumos_venta") or [], start=1):
                if not isinstance(fila, dict):
                    raise ValueError(f"Insumo asociado #{idx} inválido")
                try:
                    insumo_id = int(fila.get("insumo_id") or 0)
                except (TypeError, ValueError):
                    raise ValueError(f"Insumo asociado #{idx}: ID inválido")
                try:
                    cantidad = float(fila.get("cantidad") or 0)
                except (TypeError, ValueError):
                    raise ValueError(f"Insumo asociado #{idx}: cantidad inválida")

                insumos_limpios.append(
                    {
                        "insumo_id": insumo_id,
                        "cantidad": cantidad,
                        "unidad": _normalizar_unidad_producto(fila.get("unidad") or "unidad"),
                    }
                )
            data["insumos_venta"] = insumos_limpios
        if "productos_venta" in data:
            if not isinstance(data.get("productos_venta"), list):
                raise ValueError("Los productos asociados deben enviarse en una lista")
            productos_limpios = []
            for idx, fila in enumerate(data.get("productos_venta") or [], start=1):
                if not isinstance(fila, dict):
                    raise ValueError(f"Producto asociado #{idx} inválido")
                try:
                    producto_asociado_id = int(fila.get("producto_id") or fila.get("producto_asociado_id") or 0)
                except (TypeError, ValueError):
                    raise ValueError(f"Producto asociado #{idx}: ID inválido")
                try:
                    cantidad = float(fila.get("cantidad") or 0)
                except (TypeError, ValueError):
                    raise ValueError(f"Producto asociado #{idx}: cantidad inválida")
                productos_limpios.append(
                    {
                        "producto_id": producto_asociado_id,
                        "cantidad": cantidad,
                    }
                )
            data["productos_venta"] = productos_limpios
        actualizar_producto(id, data)
        producto_actualizado = calcular_disponibilidad_producto(id)
        crear_backup()
        return jsonify({'success': True, 'producto': producto_actualizado})
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/producto/<int:id>/stock', methods=['POST'])
def actualizar_producto_stock(id):
    try:
        data = request.get_json(silent=True) or {}
        cantidad = float(data.get('cantidad', 0) or 0)
        if cantidad == 0:
            return jsonify({'success': False, 'error': 'La cantidad no puede ser 0'}), 400

        resultado = actualizar_stock_producto(
            id,
            cantidad,
            referencia_tipo='ajuste_manual',
            detalle='Ajuste manual desde pantalla de productos',
            fecha_vencimiento=data.get('fecha_vencimiento'),
        )
        crear_backup()

        return jsonify({'success': True, 'nuevo_stock': resultado.get('nuevo_stock')})
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/producto/merma', methods=['POST'])
def api_registrar_merma_producto():
    try:
        data = request.get_json(silent=True) or {}
        producto_id = data.get('producto_id')
        cantidad = data.get('cantidad')
        motivo = data.get('motivo')
        detalle = data.get('detalle')

        resultado = registrar_merma_producto(producto_id, cantidad, motivo, detalle)
        if resultado.get('success'):
            crear_backup()
            return jsonify(resultado)
        return jsonify(resultado), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/producto/merma/<int:merma_id>/revertir', methods=['POST'])
def api_revertir_merma_producto(merma_id):
    try:
        resultado = revertir_merma_producto(merma_id)
        if resultado.get('success'):
            crear_backup()
            return jsonify(resultado)
        return jsonify(resultado), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/producto/<int:id>/eliminar', methods=['POST'])
def eliminar_producto(id):
    conn = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, nombre
            FROM productos
            WHERE id = ?
              AND COALESCE(eliminado, 0) = 0
            """,
            (id,),
        )
        producto = cursor.fetchone()
        if not producto:
            return jsonify({'success': False, 'error': 'Producto no encontrado'}), 404

        # Se desacopla del sistema activo, pero se conserva para historial.
        cursor.execute(
            """
            UPDATE productos
            SET stock_dependencia_tipo = NULL,
                stock_dependencia_id = NULL,
                stock_dependencia_cantidad = 1
            WHERE lower(coalesce(stock_dependencia_tipo, '')) = 'producto'
              AND stock_dependencia_id = ?
            """,
            (id,),
        )
        cursor.execute("UPDATE recetas SET producto_id = NULL WHERE producto_id = ?", (id,))
        cursor.execute(
            """
            DELETE FROM receta_items
            WHERE producto_id = ?
              AND lower(coalesce(tipo, '')) = 'producto'
            """,
            (id,),
        )
        cursor.execute("DELETE FROM producto_insumos_venta WHERE producto_id = ?", (id,))
        cursor.execute(
            "DELETE FROM producto_productos_venta WHERE producto_id = ? OR producto_asociado_id = ?",
            (id, id),
        )
        cursor.execute("DELETE FROM producto_desactivaciones_manuales WHERE producto_id = ?", (id,))
        cursor.execute("DELETE FROM producto_lotes WHERE producto_id = ?", (id,))
        cursor.execute(
            """
            UPDATE productos
            SET eliminado = 1,
                stock = 0,
                fecha_vencimiento = NULL,
                stock_dependencia_tipo = NULL,
                stock_dependencia_id = NULL,
                stock_dependencia_cantidad = 1
            WHERE id = ?
            """,
            (id,),
        )
        registrar_historial_cambio(
            recurso_tipo='producto',
            recurso_id=id,
            recurso_nombre=producto['nombre'] or f'Producto #{id}',
            accion='eliminado',
            detalle='Eliminado de listas activas (conserva historial)',
            origen_modulo='productos',
            conn=conn,
        )
        conn.commit()
        crear_backup()
        return jsonify({'success': True})
    except sqlite3.IntegrityError:
        if conn:
            conn.rollback()
        return jsonify({
            'success': False,
            'error': 'No se puede eliminar el producto porque está relacionado con otros registros.'
        }), 400
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/producto/<int:id>/duplicar', methods=['POST'])
def duplicar_producto(id):
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        cursor.execute(
            """
            SELECT *
            FROM productos
            WHERE id = ?
              AND COALESCE(eliminado, 0) = 0
            """,
            (id,),
        )
        original = cursor.fetchone()
        
        if not original:
            return jsonify({'success': False, 'error': 'Producto no encontrado'}), 404
        
        cursor.execute(
            """
            INSERT INTO productos (
                nombre, icono, foto, stock, stock_minimo, unidad, porcion_cantidad, porcion_unidad,
                stock_dependencia_tipo, stock_dependencia_id, stock_dependencia_cantidad,
                fecha_vencimiento, alerta_dias, precio, vida_util_dias
            )
            VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"{original['nombre']} (Copia)",
                _normalizar_icono_producto(original["icono"] if "icono" in original.keys() else "cupcake"),
                original["foto"] if "foto" in original.keys() else None,
                original['stock_minimo'],
                original['unidad'] or 'unidad',
                float(original['porcion_cantidad'] or 1) if 'porcion_cantidad' in original.keys() else 1,
                (original['porcion_unidad'] if 'porcion_unidad' in original.keys() else None) or original['unidad'] or 'unidad',
                (str(original['stock_dependencia_tipo']).strip().lower() if 'stock_dependencia_tipo' in original.keys() and original['stock_dependencia_tipo'] is not None else None),
                (int(original['stock_dependencia_id'] or 0) if 'stock_dependencia_id' in original.keys() and original['stock_dependencia_id'] is not None else None),
                (float(original['stock_dependencia_cantidad'] or 1) if 'stock_dependencia_cantidad' in original.keys() else 1),
                original['fecha_vencimiento'],
                original['alerta_dias'] or 2,
                original['precio'] or 0,
                original['vida_util_dias'] or 0,
            ),
        )
        nuevo_id = cursor.lastrowid
        nombre_nuevo = f"{original['nombre']} (Copia)"
        if float(original['stock'] or 0) > 0:
            cursor.execute(
                "INSERT INTO producto_lotes (producto_id, cantidad, fecha_vencimiento) VALUES (?, ?, ?)",
                (nuevo_id, float(original['stock']), original['fecha_vencimiento']),
            )
            cursor.execute("UPDATE productos SET stock = ? WHERE id = ?", (float(original['stock']), nuevo_id))

        cursor.execute(
            """
            INSERT OR IGNORE INTO producto_insumos_venta (
                producto_id, insumo_id, cantidad, unidad, creado, actualizado
            )
            SELECT ?, insumo_id, cantidad, unidad, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            FROM producto_insumos_venta
            WHERE producto_id = ?
            """,
            (nuevo_id, id),
        )
        cursor.execute(
            """
            INSERT OR IGNORE INTO producto_productos_venta (
                producto_id, producto_asociado_id, cantidad, creado, actualizado
            )
            SELECT ?, producto_asociado_id, cantidad, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            FROM producto_productos_venta
            WHERE producto_id = ?
            """,
            (nuevo_id, id),
        )

        registrar_historial_cambio(
            recurso_tipo='producto',
            recurso_id=nuevo_id,
            recurso_nombre=nombre_nuevo,
            accion='agregado',
            detalle=f'Duplicado desde producto #{id}',
            origen_modulo='productos',
            conn=conn,
        )

        conn.commit()
        conn.close()
        crear_backup()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/agenda')
def agenda():
    return render_template('agenda.html')

@app.route('/insumos')
def insumos():
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        orden = request.args.get('orden', 'nombre')
        direccion = request.args.get('dir', 'asc')
        solo_cero = request.args.get('cero', '0') == '1'
        
        orden_valido = 'nombre' if orden == 'nombre' else 'stock'
        dir_valido = 'DESC' if direccion == 'desc' else 'ASC'
        
        query = f"SELECT * FROM insumos"
        if solo_cero:
            query += " WHERE stock = 0"
        query += f" ORDER BY {orden_valido} {dir_valido}"
        
        cursor.execute(query)
        insumos = cursor.fetchall()
        conn.close()

        recetas = obtener_recetas()
        beta_costeo_alertas = str(os.environ.get('GESTIONSTOCK_BETA_COSTEO_ALERTAS', '1')).strip().lower() not in {'0', 'false', 'off', 'no'}
        
        return render_template('insumos.html',
                             insumos=insumos,
                             recetas=recetas,
                             beta_costeo_alertas=beta_costeo_alertas,
                             orden=orden,
                             direccion=direccion,
                             solo_cero=solo_cero)
    except Exception as e:
        print(f"Error en insumos: {e}")
        return f"Error: {str(e)}", 500


@app.route('/api/insumos/calculo-recetas', methods=['POST'])
def api_insumos_calculo_recetas():
    """
    Calcula los insumos requeridos para una selección de recetas y lotes.
    payload esperado:
    {
      "seleccion": [
        {"receta_id": 1, "lotes": 2.5},
        ...
      ]
    }
    """
    def _factor_unidad_local(unidad):
        u = str(unidad or '').strip().lower().split('(')[0].strip()
        if not u:
            return 1.0, 'unidad'
        if u in {'mg', 'miligramo', 'miligramos'}:
            return 0.001, 'g'
        if u in {'g', 'gr', 'gramo', 'gramos'}:
            return 1.0, 'g'
        if u in {'kg', 'kilo', 'kilos', 'kilogramo', 'kilogramos'}:
            return 1000.0, 'g'
        if u in {'ml', 'mililitro', 'mililitros', 'cc', 'cm3'}:
            return 1.0, 'ml'
        if u in {'l', 'lt', 'litro', 'litros'}:
            return 1000.0, 'ml'
        if u in {'docena', 'docenas'}:
            return 12.0, 'unidad'
        return 1.0, 'unidad'

    def _convertir_a_unidad_costeo(cantidad_receta, unidad_receta, unidad_costeo):
        """
        Convierte cantidad de receta a la unidad de costeo/compra del insumo.
        Ej: 3960 gr -> 3.96 kg, 1200 cc -> 1.2 l
        """
        try:
            qty = float(cantidad_receta or 0)
        except Exception:
            return 0.0
        if qty <= 0:
            return 0.0

        f_receta, base_receta = _factor_unidad_local(unidad_receta)
        f_costeo, base_costeo = _factor_unidad_local(unidad_costeo)

        # Si no son compatibles, mantenemos cantidad original.
        if base_receta != base_costeo:
            return qty

        cantidad_base = qty * f_receta
        if f_costeo <= 0:
            return cantidad_base
        return cantidad_base / f_costeo

    try:
        data = request.get_json(silent=True) or {}
        seleccion = data.get('seleccion')
        if not isinstance(seleccion, list) or not seleccion:
            return jsonify({'success': False, 'error': 'Debes enviar al menos una receta seleccionada'}), 400

        recetas = obtener_recetas() or []
        mapa_recetas = {int(r['id']): r for r in recetas if r.get('id') is not None}

        acumulado = {}
        resumen_recetas = []

        for idx, fila in enumerate(seleccion, start=1):
            if not isinstance(fila, dict):
                return jsonify({'success': False, 'error': f'Fila {idx} de selección inválida'}), 400

            try:
                receta_id = int(fila.get('receta_id'))
            except Exception:
                return jsonify({'success': False, 'error': f'Receta inválida en fila {idx}'}), 400

            try:
                lotes = float(fila.get('lotes', 1) or 1)
            except Exception:
                return jsonify({'success': False, 'error': f'Lotes inválidos en fila {idx}'}), 400

            if lotes <= 0:
                return jsonify({'success': False, 'error': f'Los lotes deben ser mayores a 0 (fila {idx})'}), 400

            receta = mapa_recetas.get(receta_id)
            if not receta:
                return jsonify({'success': False, 'error': f'No se encontró la receta #{receta_id}'}), 404

            resumen_recetas.append({
                'receta_id': receta_id,
                'receta_nombre': receta.get('nombre') or f'Receta {receta_id}',
                'lotes': lotes,
            })

            for item in (receta.get('insumos') or []):
                insumo_id = item.get('insumo_id')
                if not insumo_id:
                    continue
                try:
                    insumo_id = int(insumo_id)
                    cantidad_base = float(item.get('cantidad') or 0)
                except Exception:
                    continue
                if cantidad_base <= 0:
                    continue

                row = acumulado.setdefault(
                    insumo_id,
                    {
                        'insumo_id': insumo_id,
                        'nombre': item.get('insumo_nombre') or f'Insumo {insumo_id}',
                        'unidad': item.get('unidad') or 'unidad',
                        'cantidad': 0.0,
                        'precio_unitario': 0.0,
                        'precio_incluye_iva': True,
                        'componentes': [],
                    },
                )
                row['componentes'].append(
                    {
                        'cantidad': cantidad_base * lotes,
                        'unidad': item.get('unidad') or 'unidad',
                    }
                )

        if not acumulado:
            return jsonify({'success': True, 'items': [], 'recetas': resumen_recetas})

        conn = get_db()
        try:
            cursor = conn.cursor()
            ids = list(acumulado.keys())
            placeholders = ','.join(['?'] * len(ids))
            cursor.execute(
                f"""
                SELECT id, nombre, unidad, unidad_compra, COALESCE(precio_unitario, 0) AS precio_unitario,
                       COALESCE(cantidad_comprada, 1) AS cantidad_comprada,
                       CASE WHEN precio_incluye_iva IS NULL THEN 1 ELSE precio_incluye_iva END AS precio_incluye_iva
                FROM insumos
                WHERE id IN ({placeholders})
                """,
                tuple(ids),
            )
            for dbrow in cursor.fetchall():
                iid = int(dbrow['id'])
                if iid not in acumulado:
                    continue
                unidad_costeo = dbrow['unidad_compra'] or dbrow['unidad'] or acumulado[iid]['unidad']
                cantidad_convertida = 0.0
                for comp in (acumulado[iid].get('componentes') or []):
                    cantidad_convertida += _convertir_a_unidad_costeo(
                        comp.get('cantidad') or 0,
                        comp.get('unidad') or 'unidad',
                        unidad_costeo,
                    )

                acumulado[iid]['nombre'] = dbrow['nombre'] or acumulado[iid]['nombre']
                acumulado[iid]['unidad'] = unidad_costeo
                acumulado[iid]['cantidad'] = cantidad_convertida
                cantidad_compra = float(dbrow['cantidad_comprada'] or 1)
                if cantidad_compra <= 0:
                    cantidad_compra = 1.0
                acumulado[iid]['precio_unitario'] = float(dbrow['precio_unitario'] or 0) / cantidad_compra
                acumulado[iid]['precio_incluye_iva'] = bool(dbrow['precio_incluye_iva'])
                acumulado[iid].pop('componentes', None)
        finally:
            conn.close()

        for iid, row in acumulado.items():
            if float(row.get('cantidad') or 0) > 0:
                continue
            total_raw = 0.0
            for comp in (row.get('componentes') or []):
                try:
                    total_raw += float(comp.get('cantidad') or 0)
                except Exception:
                    continue
            row['cantidad'] = total_raw
            row.pop('componentes', None)

        items = sorted(
            [
                {
                    **v,
                    'cantidad': round(float(v.get('cantidad') or 0), 4),
                }
                for v in acumulado.values()
            ],
            key=lambda x: (x.get('nombre') or '').lower(),
        )

        return jsonify({'success': True, 'items': items, 'recetas': resumen_recetas})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/compras-pendientes', methods=['GET'])
def api_compras_pendientes_listar():
    try:
        incluir_comprados = request.args.get('incluir_comprados', '1') != '0'
        data = obtener_compras_pendientes(incluir_comprados=incluir_comprados)
        return jsonify({'success': True, 'items': data['items'], 'resumen': data['resumen']})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e), 'items': [], 'resumen': {}}), 500


@app.route('/api/compras-pendientes', methods=['POST'])
def api_compras_pendientes_agregar():
    try:
        data = request.get_json() or {}
        if isinstance(data.get('items'), list):
            resultado = agregar_lote_compras_pendientes(data.get('items') or [], combinar=True)
        else:
            resultado = agregar_compra_pendiente(data)

        if resultado.get('success'):
            crear_backup()
            resumen = obtener_compras_pendientes()
            resultado['items'] = resumen['items']
            resultado['resumen'] = resumen['resumen']
            return jsonify(resultado)
        return jsonify(resultado), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/compras-pendientes/<int:item_id>', methods=['PUT'])
def api_compras_pendientes_actualizar(item_id):
    try:
        data = request.get_json() or {}
        resultado = actualizar_compra_pendiente(item_id, data)
        if resultado.get('success'):
            crear_backup()
            return jsonify(resultado)
        return jsonify(resultado), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/compras-pendientes/<int:item_id>', methods=['DELETE'])
def api_compras_pendientes_eliminar(item_id):
    try:
        resultado = eliminar_compra_pendiente(item_id)
        if resultado.get('success'):
            crear_backup()
            return jsonify(resultado)
        msg = str(resultado.get('error') or '').lower()
        status = 404 if 'no encontrado' in msg else 400
        return jsonify(resultado), status
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/compras-pendientes/finalizar', methods=['POST'])
def api_compras_pendientes_finalizar():
    try:
        data = request.get_json(silent=True) or {}
        aplicar_stock = bool(data.get('aplicar_stock', True))
        factura_info = data.get('factura') if isinstance(data.get('factura'), dict) else None

        if aplicar_stock:
            resultado = finalizar_compras_pendientes_con_stock(aplicar_stock=True, factura_info=factura_info)
        else:
            resultado = marcar_compras_pendientes_completadas()

        if resultado.get('success'):
            crear_backup()
            resumen = obtener_compras_pendientes()
            resultado['items'] = resumen['items']
            resultado['resumen'] = resumen['resumen']
            return jsonify(resultado)
        return jsonify(resultado), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/compras-pendientes/finalizar/preview', methods=['GET'])
def api_compras_pendientes_finalizar_preview():
    try:
        resultado = previsualizar_finalizacion_compras_pendientes()
        if resultado.get('success'):
            return jsonify(resultado)
        return jsonify(resultado), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e), 'items': [], 'omitidos': [], 'resumen': {}}), 500


@app.route('/api/compras-pendientes/limpiar', methods=['POST'])
def api_compras_pendientes_limpiar():
    try:
        data = request.get_json() or {}
        solo_comprados = bool(data.get('solo_comprados', False))
        resultado = limpiar_compras_pendientes(solo_comprados=solo_comprados)
        if resultado.get('success'):
            crear_backup()
            resumen = obtener_compras_pendientes()
            resultado['items'] = resumen['items']
            resultado['resumen'] = resumen['resumen']
            return jsonify(resultado)
        return jsonify(resultado), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/calculadora-insumos/estado', methods=['GET'])
def api_calculadora_insumos_estado_get():
    try:
        resultado = obtener_calculadora_compras_draft()
        if resultado.get('success'):
            resp = jsonify(resultado)
            resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
            resp.headers['Pragma'] = 'no-cache'
            return resp
        return jsonify(resultado), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e), 'items': []}), 500


@app.route('/api/calculadora-insumos/estado', methods=['POST'])
def api_calculadora_insumos_estado_save():
    try:
        data = request.get_json(silent=True) or {}
        items = data.get('items') if isinstance(data.get('items'), list) else []
        resultado = guardar_calculadora_compras_draft(items)
        if resultado.get('success'):
            return jsonify(resultado)
        return jsonify(resultado), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/calculadora-insumos/estado', methods=['DELETE'])
def api_calculadora_insumos_estado_clear():
    try:
        resultado = limpiar_calculadora_compras_draft()
        if resultado.get('success'):
            return jsonify(resultado)
        return jsonify(resultado), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/insumo/escanear-avanzado', methods=['POST'])
def escanear_insumo_avanzado():
    try:
        data = request.get_json()
        codigo = data.get('codigo', '').strip()
        cantidad_raw = data.get('cantidad')
        cantidad = float(cantidad_raw) if cantidad_raw is not None else None
        unidad = data.get('unidad')
        actualizar_precio = data.get('actualizar_precio', False)
        lote_codigo = str(data.get('lote_codigo') or '').strip() or None
        fecha_elaboracion = _as_optional_date(data.get('fecha_elaboracion'), "fecha de elaboración")
        fecha_vencimiento = _as_optional_date(data.get('fecha_vencimiento'), "fecha de vencimiento")
        
        if not codigo:
            return jsonify({'success': False, 'error': 'Código vacío'}), 400
        
        conn = get_db()
        cursor = conn.cursor()

        # Buscar insumo por código principal o alias
        insumo, _ = _buscar_insumo_por_codigo_cursor(cursor, codigo)

        if not insumo:
            conn.close()
            return jsonify({'success': False, 'error': 'Insumo no encontrado'}), 404

        if cantidad is None:
            cantidad = float(insumo['cantidad_por_scan'] or 1)
        if cantidad <= 0:
            conn.close()
            return jsonify({'success': False, 'error': 'Cantidad debe ser mayor a 0'}), 400

        if not unidad:
            unidad = insumo['unidad_por_scan'] or insumo['unidad'] or 'unidad'
        
        # Actualizar precio si se solicitó
        if actualizar_precio:
            precio_unitario = data.get('precio_unitario')
            cantidad_comprada = data.get('cantidad_comprada')
            unidad_compra = data.get('unidad_compra')
            # Manejar tanto boolean como integer
            precio_incluye_iva_raw = data.get('precio_incluye_iva', True)
            precio_incluye_iva = 1 if precio_incluye_iva_raw in [True, 1, 'true', 'True'] else 0
            
            if precio_unitario and cantidad_comprada:
                cursor.execute('''
                    UPDATE insumos 
                    SET precio_unitario = ?, 
                        cantidad_comprada = ?, 
                        unidad_compra = ?, 
                        precio_incluye_iva = ?
                    WHERE id = ?
                ''', (precio_unitario, cantidad_comprada, unidad_compra, precio_incluye_iva, insumo['id']))
                conn.commit()
        conn.close()

        # Actualizar stock con conversión de unidades
        resultado = actualizar_stock_insumo_con_unidad(
            codigo,
            cantidad,
            unidad,
            lote_codigo=lote_codigo,
            fecha_elaboracion=fecha_elaboracion,
            fecha_vencimiento=fecha_vencimiento,
        )

        # Obtener datos actualizados
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, stock, unidad, precio_unitario FROM insumos WHERE id = ?",
            (int(resultado.get('insumo_id') or insumo['id']),),
        )
        insumo_actualizado = cursor.fetchone()
        conn.close()
        if not insumo_actualizado:
            return jsonify({'success': False, 'error': 'No se pudo leer el insumo actualizado'}), 500
        
        crear_backup()
        
        return jsonify({
            'success': True,
            'insumo_id': int(insumo_actualizado['id']),
            'nombre': resultado['nombre'],
            'stock': insumo_actualizado['stock'],
            'unidad': insumo_actualizado['unidad'],
            'es_nuevo': resultado['es_nuevo'],
            'cantidad_agregada': cantidad,
            'precio_actualizado': actualizar_precio
        })
        
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/insumo/<int:id>/stock', methods=['POST'])
def actualizar_insumo_manual(id):
    conn = None
    try:
        data = request.get_json(silent=True) or {}
        cantidad = float(data.get('cantidad', 0))
        actualizar_metadata_lote = any(
            key in data for key in ('lote_codigo', 'fecha_elaboracion', 'fecha_vencimiento')
        )
        lote_codigo = (
            str(data.get('lote_codigo') or '').strip() or None
            if actualizar_metadata_lote else None
        )
        fecha_elaboracion = (
            _as_optional_date(data.get('fecha_elaboracion'), "fecha de elaboración")
            if actualizar_metadata_lote else None
        )
        fecha_vencimiento = (
            _as_optional_date(data.get('fecha_vencimiento'), "fecha de vencimiento")
            if actualizar_metadata_lote else None
        )
        if fecha_elaboracion and fecha_vencimiento and fecha_vencimiento < fecha_elaboracion:
            return jsonify({'success': False, 'error': 'La fecha de vencimiento no puede ser anterior a la fecha de elaboración'}), 400
        
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT stock FROM insumos WHERE id = ?", (id,))
        row = cursor.fetchone()
        if not row:
            conn.close()
            return jsonify({'success': False, 'error': 'Insumo no encontrado'}), 404

        stock_anterior = float(row[0])
        nuevo_stock = stock_anterior + cantidad
        if nuevo_stock < 0:
            conn.close()
            return jsonify({'success': False, 'error': 'El stock no puede quedar negativo'}), 400

        cursor.execute("UPDATE insumos SET stock = ? WHERE id = ?", (nuevo_stock, id))
        registrar_movimiento_stock(
            'insumo',
            id,
            'entrada_manual' if cantidad > 0 else 'salida_manual',
            abs(cantidad),
            stock_anterior=stock_anterior,
            stock_nuevo=nuevo_stock,
            referencia_tipo='ajuste_manual',
            conn=conn
        )
        sincronizar_lotes_insumo_stock(
            id,
            nuevo_stock,
            lote_codigo=lote_codigo,
            fecha_elaboracion=fecha_elaboracion,
            fecha_vencimiento=fecha_vencimiento,
            actualizar_metadata=actualizar_metadata_lote,
            conn=conn,
        )
        conn.commit()
        
        crear_backup()
        return jsonify({'success': True, 'nuevo_stock': nuevo_stock})
    except ValueError as e:
        if conn:
            conn.rollback()
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/insumo/descarte-masivo', methods=['POST'])
def api_descartar_insumos_masivo():
    try:
        data = request.get_json(silent=True) or {}
        titulo = str(data.get('titulo') or '').strip() or 'Descarte de insumos'
        motivo_raw = str(data.get('motivo') or '').strip()
        motivo_custom = str(data.get('motivo_custom') or '').strip()
        if motivo_raw.lower() in {'otro', 'personalizado', 'custom'}:
            motivo = motivo_custom or 'Otro'
        else:
            motivo = motivo_raw or 'Descarte'
        comentario = str(data.get('comentario') or '').strip()
        items = data.get('items') or []

        resultado = descartar_insumos_masivo(
            titulo=titulo,
            motivo=motivo,
            comentario=comentario,
            items=items,
        )
        if not resultado.get('success'):
            return jsonify(resultado), 400
        crear_backup()
        return jsonify(resultado)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/producto/<int:id>/foto', methods=['POST'])
def api_actualizar_foto_producto(id):
    conn = None
    try:
        if 'foto' not in request.files:
            return jsonify({'success': False, 'error': 'No se recibió imagen'}), 400
        archivo = request.files['foto']
        if not archivo or not archivo.filename:
            return jsonify({'success': False, 'error': 'Archivo inválido'}), 400

        nombre_seguro = secure_filename(archivo.filename)
        ext = os.path.splitext(nombre_seguro)[1].lower()
        permitidas = {'.jpg', '.jpeg', '.png', '.webp'}
        if ext not in permitidas:
            return jsonify({'success': False, 'error': 'Formato no permitido (usa JPG, PNG o WebP)'}), 400

        data = archivo.read()
        if not data:
            return jsonify({'success': False, 'error': 'Archivo vacío'}), 400
        if len(data) > 4 * 1024 * 1024:
            return jsonify({'success': False, 'error': 'Imagen demasiado grande (máx 4MB)'}), 400

        tipo = _detectar_tipo_imagen(data)
        if tipo not in {'jpeg', 'png', 'webp'}:
            return jsonify({'success': False, 'error': 'No se pudo validar la imagen'}), 400
        ext_normalizada = '.jpg' if tipo == 'jpeg' else f".{tipo}"

        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, foto
            FROM productos
            WHERE id = ?
              AND COALESCE(eliminado, 0) = 0
            """,
            (id,),
        )
        row = cursor.fetchone()
        if not row:
            return jsonify({'success': False, 'error': 'Producto no encontrado'}), 404

        carpeta_fotos = os.path.join(static_dir, 'productos')
        os.makedirs(carpeta_fotos, exist_ok=True)
        nombre_archivo = f"producto_{id}_{int(time.time())}{ext_normalizada}"
        ruta_archivo = os.path.join(carpeta_fotos, nombre_archivo)
        with open(ruta_archivo, 'wb') as f:
            f.write(data)

        foto_relativa = f"productos/{nombre_archivo}"
        cursor.execute(
            "UPDATE productos SET foto = ? WHERE id = ?",
            (foto_relativa, id),
        )
        conn.commit()

        foto_anterior = (row["foto"] if row and "foto" in row.keys() else None) or ""
        if foto_anterior:
            ruta_anterior = os.path.normpath(os.path.join(static_dir, foto_anterior.replace('/', os.sep)))
            if ruta_anterior.startswith(os.path.normpath(carpeta_fotos)) and os.path.isfile(ruta_anterior):
                try:
                    os.remove(ruta_anterior)
                except Exception:
                    pass

        return jsonify(
            {
                'success': True,
                'foto': foto_relativa,
                'foto_url': url_for('static', filename=foto_relativa),
            }
        )
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/insumo/<int:id>/detalle')
def api_detalle_insumo(id):
    try:
        insumo = obtener_insumo_detalle(id)
        if not insumo:
            return jsonify({'success': False, 'error': 'Insumo no encontrado'}), 404
        return jsonify({'success': True, 'insumo': insumo})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/insumo/<int:id>/precio-historial')
def api_historial_precio_insumo(id):
    try:
        from database import obtener_historial_precio_insumo
        meses = request.args.get('meses', default=3, type=int)
        limite = request.args.get('limit', default=120, type=int)
        data = obtener_historial_precio_insumo(id, meses=meses, limite=limite)
        if not data.get('success'):
            codigo = 404 if 'no encontrado' in str(data.get('error', '')).lower() else 400
            return jsonify(data), codigo
        return jsonify(data)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e), 'items': []}), 500


@app.route('/api/insumo/<int:id>/actualizar', methods=['POST'])
def api_actualizar_insumo(id):
    try:
        data = request.get_json(silent=True) or {}
        actualizar_insumo(id, data)
        crear_backup()
        return jsonify({'success': True})
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/insumo/<int:id>/eliminar', methods=['POST'])
def eliminar_insumo(id):
    conn = None
    try:
        conn = get_db()
        cursor = conn.cursor()

        cursor.execute("SELECT id, nombre FROM insumos WHERE id = ?", (id,))
        insumo = cursor.fetchone()
        if not insumo:
            return jsonify({'success': False, 'error': 'Insumo no encontrado'}), 404

        cursor.execute("SELECT COUNT(*) AS total FROM receta_items WHERE insumo_id = ?", (id,))
        en_recetas = int(cursor.fetchone()["total"] or 0)

        cursor.execute("SELECT COUNT(*) AS total FROM producto_insumos_venta WHERE insumo_id = ?", (id,))
        en_productos_asociados = int(cursor.fetchone()["total"] or 0)

        cursor.execute("SELECT COUNT(*) AS total FROM venta_insumos WHERE insumo_id = ?", (id,))
        en_historial_ventas = int(cursor.fetchone()["total"] or 0)

        cursor.execute("SELECT COUNT(*) AS total FROM insumo_lotes WHERE insumo_id = ?", (id,))
        en_lotes = int(cursor.fetchone()["total"] or 0)

        cursor.execute("SELECT COUNT(*) AS total FROM venta_insumo_lotes WHERE insumo_id = ?", (id,))
        en_historial_lotes = int(cursor.fetchone()["total"] or 0)

        cursor.execute("SELECT COUNT(*) AS total FROM compras_pendientes WHERE insumo_id = ?", (id,))
        en_compras_pendientes = int(cursor.fetchone()["total"] or 0)

        cursor.execute("SELECT COUNT(*) AS total FROM haccp_trazabilidad_insumos WHERE insumo_id = ?", (id,))
        en_haccp_trazabilidad = int(cursor.fetchone()["total"] or 0)

        dependencias = []
        if en_recetas > 0:
            dependencias.append(f"usado en {en_recetas} receta(s)")
        if en_productos_asociados > 0:
            dependencias.append(f"asociado a {en_productos_asociados} producto(s) de venta")
        if en_historial_ventas > 0:
            dependencias.append(f"presente en {en_historial_ventas} registro(s) históricos de venta")
        if en_historial_lotes > 0:
            dependencias.append(f"con {en_historial_lotes} registro(s) de trazabilidad de lote en ventas")
        if en_compras_pendientes > 0:
            dependencias.append(f"incluido en {en_compras_pendientes} compra(s) pendiente(s)")
        if en_haccp_trazabilidad > 0:
            dependencias.append(f"con {en_haccp_trazabilidad} registro(s) de trazabilidad HACCP")

        if dependencias:
            return jsonify({
                'success': False,
                'error': f"No se puede eliminar '{insumo['nombre']}': " + "; ".join(dependencias)
            }), 400

        if en_lotes > 0:
            cursor.execute("DELETE FROM insumo_lotes WHERE insumo_id = ?", (id,))

        cursor.execute("DELETE FROM insumos WHERE id = ?", (id,))
        registrar_historial_cambio(
            recurso_tipo='insumo',
            recurso_id=id,
            recurso_nombre=insumo['nombre'] or f'Insumo #{id}',
            accion='eliminado',
            detalle='Eliminacion manual de insumo',
            origen_modulo='insumos',
            conn=conn,
        )
        conn.commit()
        crear_backup()
        return jsonify({'success': True})
    except sqlite3.IntegrityError:
        if conn:
            conn.rollback()
        return jsonify({
            'success': False,
            'error': 'No se puede eliminar el insumo porque está relacionado con otras tablas.'
        }), 400
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        if conn:
            conn.close()

@app.route('/produccion')
def produccion():
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        recetas = obtener_recetas()
        
        cursor.execute(
            """
            SELECT id, nombre, stock
            FROM productos
            WHERE COALESCE(eliminado, 0) = 0
            ORDER BY nombre
            """
        )
        productos = cursor.fetchall()
        
        cursor.execute("SELECT id, nombre, stock, unidad FROM insumos ORDER BY nombre")
        insumos = cursor.fetchall()
        
        conn.close()
        agenda_evento_id = request.args.get('agenda_evento', type=int)
        agenda_evento = obtener_evento_agenda_por_id(agenda_evento_id) if agenda_evento_id else None
        try:
            agenda_produccion_inicial = obtener_agenda_produccion_semanal(dias=7)
        except Exception:
            agenda_produccion_inicial = {"days": [], "resumen": {}, "fecha_desde": "", "fecha_hasta": ""}
        try:
            plan_semanal_inicial = obtener_plan_produccion_semanal(dias_historial=28, dias_proyeccion=7)
        except Exception:
            plan_semanal_inicial = {"days": [], "resumen": {}}
        
        return render_template('produccion.html',
                             recetas=recetas,
                             productos=productos,
                             insumos=insumos,
                             agenda_evento=agenda_evento,
                             agenda_produccion_inicial=agenda_produccion_inicial,
                             plan_semanal_inicial=plan_semanal_inicial)
    except Exception as e:
        print(f"Error en produccion: {e}")
        return f"Error: {str(e)}", 500

@app.route('/api/receta/crear', methods=['POST'])
def api_crear_receta():
    try:
        nombre = (request.form.get('nombre') or '').strip()
        if not nombre:
            return _error_or_text("El nombre de la receta es obligatorio", 400)

        producto_id_raw = request.form.get('producto_id') or None
        producto_id = int(producto_id_raw) if producto_id_raw else None
        rendimiento = _as_float(request.form.get('rendimiento', 1) or 1, "rendimiento", min_value=0.01)
        
        items = []
        index = 0
        while True:
            id_key = f'insumos[{index}][id]'
            cantidad_key = f'insumos[{index}][cantidad]'
            if id_key not in request.form and cantidad_key not in request.form:
                break

            recurso_id = request.form.get(id_key)
            if not recurso_id:
                index += 1
                continue

            cantidad = _as_float(request.form.get(cantidad_key, 0) or 0, "cantidad", min_value=0.0001)
            items.append({
                'tipo': request.form.get(f'insumos[{index}][tipo]', 'insumo'),
                'id': int(recurso_id),
                'cantidad': cantidad,
                'unidad': request.form.get(f'insumos[{index}][unidad]', 'unidad'),
            })
            index += 1

        if not items:
            return _error_or_text("La receta debe tener al menos un componente", 400)
        
        receta_id = guardar_receta(nombre, producto_id, items, rendimiento=rendimiento)
        crear_backup()
        
        return _ok_or_redirect(
            {
                'success': True,
                'receta_id': receta_id,
                'message': 'Receta creada correctamente'
            },
            'produccion'
        )
    except ValueError as e:
        return _error_or_text(e, 400)
    except Exception as e:
        return _error_or_text(f"Error: {str(e)}", 500)


@app.route('/api/receta/<int:receta_id>/detalle')
def api_detalle_receta(receta_id):
    try:
        receta = obtener_receta_detalle(receta_id)
        if not receta:
            return jsonify({'success': False, 'error': 'Receta no encontrada'}), 404
        return jsonify({'success': True, 'receta': receta})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/receta/<int:receta_id>/actualizar', methods=['POST'])
def api_actualizar_receta(receta_id):
    try:
        data = request.get_json(silent=True) or {}
        nombre = data.get('nombre')
        producto_id = data.get('producto_id') or None
        items = data.get('items') or []
        rendimiento = float(data.get('rendimiento', 1) or 1)

        actualizar_receta(receta_id, nombre, producto_id, items, rendimiento=rendimiento)
        crear_backup()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/receta/<int:receta_id>/producir', methods=['POST'])
def api_producir_receta(receta_id):
    try:
        data = request.get_json(silent=True) or {}
        cantidad = _as_int(data.get('cantidad', 1), 'cantidad', min_value=1)
        cantidad_resultado = data.get('cantidad_resultado')
        if cantidad_resultado is not None and str(cantidad_resultado).strip() != '':
            cantidad_resultado = float(cantidad_resultado)
        else:
            cantidad_resultado = None
        fecha_vencimiento = data.get('fecha_vencimiento') or None
        
        resultado = producir_receta(
            receta_id,
            cantidad,
            cantidad_resultado=cantidad_resultado,
            fecha_vencimiento=fecha_vencimiento,
        )
        limpiar_producciones_antiguas(meses=6)
        if resultado.get('success'):
            crear_backup()
        
        status = 200 if resultado.get('success') else 400
        return jsonify(resultado), status
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/receta/<int:receta_id>/eliminar', methods=['POST'])
def api_eliminar_receta(receta_id):
    try:
        resultado = eliminar_receta(receta_id)
        if resultado.get('success'):
            crear_backup()
            return jsonify(resultado)
        msg = str(resultado.get('error') or '').lower()
        status = 404 if 'no encontrada' in msg or 'no encontrado' in msg else 400
        return jsonify(resultado), status
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/produccion/historial')
def obtener_historial_produccion():
    try:
        try:
            page = max(int(request.args.get('page', 1)), 1)
        except (TypeError, ValueError):
            page = 1

        try:
            limit = max(int(request.args.get('limit', 6)), 1)
        except (TypeError, ValueError):
            limit = 6

        data = obtener_historial_produccion_semanal(page=page, weeks_per_page=limit, meses=6)

        # Compatibilidad con render antiguo (lista rápida de esta semana).
        data['historial'] = data.get('this_week', [])
        data['success'] = True
        return jsonify(data)
    except Exception as e:
        print(f"Error historial: {e}")
        return jsonify({'success': False, 'error': str(e), 'historial': []}), 500


@app.route('/api/produccion/plan-semanal')
def api_plan_produccion_semanal():
    try:
        dias_hist = _as_int(request.args.get('dias_hist', 28) or 28, "dias historicos", min_value=7)
        dias_plan = _as_int(request.args.get('dias_plan', 7) or 7, "dias plan", min_value=3)
        data = obtener_plan_produccion_semanal(dias_historial=dias_hist, dias_proyeccion=dias_plan)
        data["success"] = True
        return jsonify(data)
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e), 'days': [], 'resumen': {}}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e), 'days': [], 'resumen': {}}), 500


@app.route('/api/produccion/agenda-semanal')
def api_agenda_produccion_semanal():
    try:
        dias = _as_int(request.args.get('dias', 7) or 7, "dias", min_value=1)
        data = obtener_agenda_produccion_semanal(dias=dias)
        data["success"] = True
        return jsonify(data)
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e), 'days': [], 'resumen': {}}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e), 'days': [], 'resumen': {}}), 500


@app.route('/api/produccion/agenda', methods=['POST'])
def api_agendar_produccion_manual():
    try:
        data = request.get_json(silent=True) or request.form
        receta_id = _as_int(data.get('receta_id'), 'receta', min_value=1)
        fecha = str(data.get('fecha') or '').strip()
        cantidad = _as_float(data.get('cantidad', 1) or 1, 'cantidad', min_value=0.01)
        nota = str(data.get('nota') or '').strip()

        resultado = agendar_produccion_manual(
            receta_id=receta_id,
            fecha=fecha,
            cantidad=cantidad,
            nota=nota,
        )
        if resultado.get('success'):
            try:
                plan = obtener_plan_produccion_semanal(dias_historial=28, dias_proyeccion=7)
            except Exception:
                plan = None
            try:
                agenda = obtener_agenda_produccion_semanal(dias=7)
            except Exception:
                agenda = None
            crear_backup()
            if plan is not None:
                resultado["plan"] = plan
            if agenda is not None:
                resultado["agenda"] = agenda
            return jsonify(resultado)

        msg = str(resultado.get('error') or '').lower()
        status = 404 if 'no encontrada' in msg or 'no encontrado' in msg else 400
        return jsonify(resultado), status
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/produccion/agenda/<int:agendado_id>/eliminar', methods=['POST'])
def api_eliminar_agendado_produccion(agendado_id):
    try:
        resultado = eliminar_produccion_agendada(agendado_id)
        if resultado.get('success'):
            try:
                resultado["agenda"] = obtener_agenda_produccion_semanal(dias=7)
            except Exception:
                pass
            crear_backup()
            return jsonify(resultado)

        msg = str(resultado.get('error') or '').lower()
        status = 404 if 'no encontrada' in msg or 'no encontrado' in msg else 400
        return jsonify(resultado), status
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/produccion/agenda/<int:agendado_id>/requerimientos', methods=['GET'])
def api_requerimientos_agenda_produccion(agendado_id):
    try:
        resultado = obtener_requerimientos_agenda_produccion(agendado_id)
        if resultado.get('success'):
            return jsonify(resultado)

        msg = str(resultado.get('error') or '').lower()
        status = 404 if 'no encontrado' in msg or 'no encontrada' in msg else 400
        return jsonify(resultado), status
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/produccion/agenda/<int:agendado_id>/confirmar', methods=['POST'])
def api_confirmar_agendado_produccion(agendado_id):
    try:
        req = obtener_requerimientos_agenda_produccion(agendado_id)
        if not req.get('success'):
            msg = str(req.get('error') or '').lower()
            status = 404 if 'no encontrado' in msg or 'no encontrada' in msg else 400
            return jsonify(req), status

        agenda = req.get('agenda') or {}
        resumen = req.get('resumen') or {}
        if bool(resumen.get('hay_faltantes')) or int(resumen.get('componentes_incompatibles') or 0) > 0:
            return jsonify({
                'success': False,
                'error': 'No se puede confirmar la produccion: existen faltantes o unidades incompatibles.'
            }), 400

        receta_id = int(agenda.get('receta_id') or 0)
        if receta_id <= 0:
            return jsonify({'success': False, 'error': 'La agenda seleccionada no tiene receta asociada valida.'}), 400

        cantidad_lotes_raw = float(agenda.get('cantidad_lotes') or 0)
        if cantidad_lotes_raw <= 0:
            return jsonify({'success': False, 'error': 'La cantidad de lotes agendada es invalida.'}), 400
        cantidad_lotes = int(round(cantidad_lotes_raw))
        if abs(cantidad_lotes_raw - cantidad_lotes) > 1e-9:
            return jsonify({
                'success': False,
                'error': 'La agenda tiene lotes decimales. Ajusta a un numero entero para confirmar produccion.'
            }), 400

        resultado = producir_receta(receta_id, cantidad_lotes)
        if not resultado.get('success'):
            return jsonify(resultado), 400

        completar_res = completar_produccion_agendada(agendado_id)
        if not completar_res.get('success'):
            # La produccion ya fue aplicada; devolvemos warning sin ocultar exito.
            resultado['warning'] = f"Produccion aplicada, pero no se pudo marcar agenda como completada: {completar_res.get('error')}"

        try:
            limpiar_producciones_antiguas(meses=6)
        except Exception:
            pass
        try:
            resultado["agenda"] = obtener_agenda_produccion_semanal(dias=7)
        except Exception:
            pass
        try:
            resultado["plan"] = obtener_plan_produccion_semanal(dias_historial=28, dias_proyeccion=7)
        except Exception:
            pass
        crear_backup()
        resultado['success'] = True
        resultado['agendado_id'] = int(agendado_id or 0)
        resultado['cantidad_lotes'] = cantidad_lotes
        return jsonify(resultado)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/produccion/<int:produccion_id>/eliminar', methods=['POST'])
def eliminar_produccion_registro(produccion_id):
    try:
        resultado = revertir_produccion(produccion_id)
        if resultado['success']:
            crear_backup()
            return jsonify(resultado)
        msg = str(resultado.get('error') or '').lower()
        status = 404 if 'no encontrada' in msg or 'no encontrado' in msg else 400
        return jsonify(resultado), status
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/alertas')
def alertas():
    try:
        conn = get_db()
        cursor = conn.cursor()

        alertas_productos = _obtener_alertas_productos(cursor)
        productos_bajos = alertas_productos["productos_stock_bajo"]
        productos_baja_porcion = alertas_productos["productos_baja_porcion"]
        
        cursor.execute(
            """
            SELECT *,
                   CAST(stock AS REAL) AS stock,
                   CAST(stock_minimo AS REAL) AS stock_minimo
            FROM insumos
            WHERE CAST(stock_minimo AS REAL) > 0
              AND CAST(stock AS REAL) <= CAST(stock_minimo AS REAL)
            ORDER BY CAST(stock AS REAL) ASC
            """
        )
        insumos_bajos = [dict(r) for r in cursor.fetchall()]
        haccp_vencidos = obtener_haccp_puntos_vencidos(limit=30, conn=conn)

        for p in productos_bajos:
            p["nivel"] = _nivel_alerta_producto(p)
        for p in productos_baja_porcion:
            p["nivel"] = _nivel_alerta_producto(p)
        for i in insumos_bajos:
            i["nivel"] = _nivel_alerta_insumo(i)
            i["unidad"] = (i.get("unidad") or "unidad")
            faltante = max(0.0, float(i.get("stock_minimo") or 0) - float(i.get("stock") or 0))
            i["faltante_alerta"] = round(faltante, 4)

        criticidad = _resumen_criticidad_alertas(productos_bajos, productos_baja_porcion, insumos_bajos)
        
        conn.close()
        
        return render_template('alertas.html',
                             productos=productos_bajos,
                             productos_baja_porcion=productos_baja_porcion,
                              insumos=insumos_bajos,
                              productos_stock_bajo_count=len(alertas_productos["ids_stock_bajo"]),
                              productos_baja_porcion_count=len(alertas_productos["ids_baja_porcion"]),
                              productos_total_alerta_count=len(alertas_productos["ids_union"]),
                             alertas_criticas_count=criticidad.get("critica", 0),
                              alertas_altas_count=criticidad.get("alta", 0),
                              alertas_medias_count=criticidad.get("media", 0),
                              alertas_bajas_count=criticidad.get("baja", 0),
                              haccp_vencidos=haccp_vencidos,
                              haccp_vencidos_count=len(haccp_vencidos))
    except Exception as e:
        print(f"Error en alertas: {e}")
        return f"Error: {str(e)}", 500

@app.route('/api/alertas/contador')
def contador_alertas():
    try:
        conn = get_db()
        cursor = conn.cursor()

        alertas_productos = _obtener_alertas_productos(cursor)
        prod_count = len(alertas_productos["ids_union"])
        prod_stock_count = len(alertas_productos["ids_stock_bajo"])
        prod_porcion_count = len(alertas_productos["ids_baja_porcion"])
        
        cursor.execute(
            """
            SELECT id,
                   nombre,
                   CAST(stock AS REAL) AS stock,
                   CAST(stock_minimo AS REAL) AS stock_minimo,
                   unidad
            FROM insumos
            WHERE CAST(stock_minimo AS REAL) > 0
              AND CAST(stock AS REAL) <= CAST(stock_minimo AS REAL)
            """
        )
        insumos_bajos = [dict(r) for r in cursor.fetchall()]
        ins_count = len(insumos_bajos)
        haccp_count = contar_haccp_vencidos(conn=conn)
        criticidad = _resumen_criticidad_alertas(
            alertas_productos["productos_stock_bajo"],
            alertas_productos["productos_baja_porcion"],
            insumos_bajos,
        )
        
        conn.close()
        
        return jsonify({
            'total': prod_count + ins_count + haccp_count,
            'productos': prod_count,
            'productos_stock_bajo': prod_stock_count,
            'productos_baja_porcion': prod_porcion_count,
            'insumos': ins_count,
            'haccp_vencidos': haccp_count,
            'criticas': criticidad.get("critica", 0),
            'altas': criticidad.get("alta", 0),
            'medias': criticidad.get("media", 0),
            'bajas': criticidad.get("baja", 0),
        })
    except Exception as e:
        return jsonify({
            'total': 0,
            'productos': 0,
            'productos_stock_bajo': 0,
            'productos_baja_porcion': 0,
            'insumos': 0,
            'haccp_vencidos': 0,
            'criticas': 0,
            'altas': 0,
            'medias': 0,
            'bajas': 0,
        })


_SIDEBAR_WEATHER_CACHE_LOCK = threading.Lock()
_SIDEBAR_WEATHER_CACHE = {
    "key": "",
    "fetched_at": 0.0,
    "payload": None,
}
_SIDEBAR_WEATHER_TTL_SECONDS = 15 * 60


def _weather_http_get_json(url):
    req = UrlRequest(
        url,
        headers={
            "User-Agent": f"SucreeStock/{APP_VERSION} (weather-widget)",
            "Accept": "application/json",
        },
    )
    with urlopen(req, timeout=12) as response:
        raw = response.read().decode("utf-8", errors="replace")
    return json.loads(raw)


def _weather_float(value):
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(num):
        return None
    return num


def _weather_code_meta(code, is_day=True):
    try:
        c = int(code)
    except Exception:
        c = -1
    day = bool(int(is_day or 0))
    if c == 0:
        return ("Despejado", "SUN" if day else "MOON")
    if c in (1, 2, 3):
        return ("Parcial nublado", "CLOUD-SUN" if day else "CLOUD")
    if c in (45, 48):
        return ("Neblina", "FOG")
    if c in (51, 53, 55, 56, 57):
        return ("Llovizna", "DRIZZLE")
    if c in (61, 63, 65, 66, 67, 80, 81, 82):
        return ("Lluvia", "RAIN")
    if c in (71, 73, 75, 77, 85, 86):
        return ("Nieve", "SNOW")
    if c in (95, 96, 99):
        return ("Tormenta", "STORM")
    return ("Clima variable", "TEMP")


def _weather_normalize_text(value):
    txt = str(value or "").strip().lower()
    if not txt:
        return ""
    txt = "".join(ch for ch in unicodedata.normalize("NFD", txt) if unicodedata.category(ch) != "Mn")
    txt = re.sub(r"[^a-z0-9]+", " ", txt).strip()
    return txt


def _weather_query_candidates(raw_query):
    q = str(raw_query or "").strip()
    if not q:
        return []
    base = re.sub(r"\s+", " ", q).strip()
    no_acc = "".join(ch for ch in unicodedata.normalize("NFD", base) if unicodedata.category(ch) != "Mn")
    candidates = []
    for item in (base, no_acc):
        item = str(item or "").strip()
        if item and item not in candidates:
            candidates.append(item)
    norm = _weather_normalize_text(base)
    if "chile" not in norm:
        for item in list(candidates):
            ext = f"{item}, Chile"
            if ext not in candidates:
                candidates.append(ext)
    return candidates[:5]


def _weather_pick_best_result(results, query):
    if not isinstance(results, list) or not results:
        return None
    q_norm = _weather_normalize_text(query)
    q_tokens = [t for t in q_norm.split() if len(t) >= 2]
    wants_chile = "chile" in q_tokens
    best = None
    best_score = -10**9
    for row in results:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "").strip()
        admin1 = str(row.get("admin1") or "").strip()
        country = str(row.get("country") or "").strip()
        country_code = str(row.get("country_code") or "").strip().upper()
        haystack = _weather_normalize_text(f"{name} {admin1} {country}")
        score = 0
        if haystack:
            for token in q_tokens:
                if token == _weather_normalize_text(name):
                    score += 18
                elif token in haystack:
                    score += 6
        if wants_chile:
            if country_code == "CL":
                score += 30
            elif "chile" in _weather_normalize_text(country):
                score += 20
            else:
                score -= 8
        elif country_code == "CL":
            # En LatAm suele ser el caso más esperado para entradas como "Maipu".
            score += 6
        pop = _weather_float(row.get("population"))
        if pop is not None and pop > 0:
            score += min(8, int(math.log10(pop)))
        if score > best_score:
            best_score = score
            best = row
    return best if isinstance(best, dict) else None


def _weather_geocode(ubicacion):
    q = str(ubicacion or "").strip()
    if not q:
        raise ValueError("Debes indicar una ubicación.")
    results_all = []
    for query_item in _weather_query_candidates(q):
        params = urlencode(
            {
                "name": query_item,
                "count": 10,
                "language": "es",
                "format": "json",
            }
        )
        data = _weather_http_get_json(f"https://geocoding-api.open-meteo.com/v1/search?{params}")
        results = data.get("results") if isinstance(data, dict) else None
        if isinstance(results, list) and results:
            results_all.extend(results)
            chosen = _weather_pick_best_result(results, q)
            if isinstance(chosen, dict):
                row = chosen
                break
    else:
        row = None
    if not isinstance(row, dict):
        row = _weather_pick_best_result(results_all, q)
    if not isinstance(row, dict):
        raise ValueError("No se encontró la ubicación. Prueba con formato 'Comuna, Ciudad, País'.")
    lat = _weather_float(row.get("latitude"))
    lon = _weather_float(row.get("longitude"))
    if lat is None or lon is None:
        raise ValueError("La ubicación no devolvió coordenadas válidas.")
    parts = [
        str(row.get("name") or "").strip(),
        str(row.get("admin1") or "").strip(),
        str(row.get("country") or "").strip(),
    ]
    pretty = ", ".join([p for p in parts if p])
    return {
        "latitud": lat,
        "longitud": lon,
        "timezone": str(row.get("timezone") or "").strip(),
        "nombre_mostrado": pretty or q,
        "ubicacion": q,
    }


def _weather_fetch_current(latitud, longitud):
    params = urlencode(
        {
            "latitude": f"{float(latitud):.6f}",
            "longitude": f"{float(longitud):.6f}",
            "current": "temperature_2m,relative_humidity_2m,apparent_temperature,weather_code,wind_speed_10m,is_day",
            "timezone": "auto",
        }
    )
    data = _weather_http_get_json(f"https://api.open-meteo.com/v1/forecast?{params}")
    current = data.get("current") if isinstance(data, dict) else None
    if not isinstance(current, dict):
        raise RuntimeError("No se pudo leer el clima actual desde Open-Meteo.")
    temp = _weather_float(current.get("temperature_2m"))
    hum = _weather_float(current.get("relative_humidity_2m"))
    wind = _weather_float(current.get("wind_speed_10m"))
    apparent = _weather_float(current.get("apparent_temperature"))
    weather_code = int(current.get("weather_code") or 0)
    is_day = int(current.get("is_day") or 0)
    desc, icon = _weather_code_meta(weather_code, is_day=is_day)
    return {
        "temperatura_c": temp,
        "humedad_pct": hum,
        "viento_kmh": wind,
        "sensacion_c": apparent,
        "weather_code": weather_code,
        "descripcion": desc,
        "icono": icon,
        "is_day": is_day,
        "observado_en": str(current.get("time") or ""),
    }


@app.route('/api/alertas/vencimiento')
def alertas_vencimiento():
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, nombre, fecha_vencimiento, 
                   julianday(fecha_vencimiento) - julianday('now') as dias_restantes
            FROM productos 
            WHERE COALESCE(eliminado, 0) = 0
              AND fecha_vencimiento IS NOT NULL 
              AND fecha_vencimiento <= date('now', '+3 days')
            ORDER BY fecha_vencimiento ASC
        """)
        productos = cursor.fetchall()
        
        conn.close()
        
        return jsonify({
            'productos': [dict(p) for p in productos],
            'urgente': any(p['dias_restantes'] <= 1 for p in productos)
        })
    except Exception as e:
        return jsonify({'productos': [], 'urgente': False})


@app.route('/api/alertas/config', methods=['GET'])
def api_obtener_config_alertas():
    try:
        return jsonify({'success': True, 'config': obtener_config_alertas()})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/alertas/config', methods=['POST'])
def api_guardar_config_alertas():
    try:
        data = request.get_json(silent=True) or {}
        guardar_config_alertas(data)
        try:
            from background_agent import ensure_background_startup
            ensure_background_startup()
        except Exception:
            pass
        crear_backup()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/weather/config', methods=['GET'])
def api_obtener_config_clima_sidebar():
    try:
        config = obtener_config_clima_sidebar()
        return jsonify({"success": True, "config": config})
    except Exception as e:
        return jsonify({"success": False, "error": str(e), "config": {}}), 500


@app.route('/api/weather/config', methods=['POST'])
def api_guardar_config_clima_sidebar():
    try:
        data = request.get_json(silent=True) or {}
        habilitado = bool(data.get("habilitado", True))
        ubicacion = str(data.get("ubicacion") or "").strip()

        payload = {
            "habilitado": habilitado,
            "ubicacion": ubicacion or "Santiago, Chile",
        }
        if payload["ubicacion"]:
            geo = _weather_geocode(payload["ubicacion"])
            payload.update(geo)

        config = guardar_config_clima_sidebar(payload)
        with _SIDEBAR_WEATHER_CACHE_LOCK:
            _SIDEBAR_WEATHER_CACHE["key"] = ""
            _SIDEBAR_WEATHER_CACHE["payload"] = None
            _SIDEBAR_WEATHER_CACHE["fetched_at"] = 0.0
        crear_backup()
        return jsonify({"success": True, "config": config})
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/weather/current', methods=['GET'])
def api_clima_sidebar_actual():
    try:
        config = obtener_config_clima_sidebar()
        if not bool(config.get("habilitado", 1)):
            return jsonify({"success": True, "disabled": True, "config": config})

        lat = _weather_float(config.get("latitud"))
        lon = _weather_float(config.get("longitud"))
        if lat is None or lon is None:
            geo = _weather_geocode(config.get("ubicacion"))
            config = guardar_config_clima_sidebar(
                {
                    "habilitado": bool(config.get("habilitado", 1)),
                    "ubicacion": geo.get("ubicacion") or config.get("ubicacion"),
                    "latitud": geo.get("latitud"),
                    "longitud": geo.get("longitud"),
                    "nombre_mostrado": geo.get("nombre_mostrado"),
                    "timezone": geo.get("timezone"),
                }
            )
            lat = _weather_float(config.get("latitud"))
            lon = _weather_float(config.get("longitud"))
        if lat is None or lon is None:
            raise RuntimeError("No hay coordenadas válidas para el widget de clima.")

        cache_key = f"{lat:.4f}|{lon:.4f}"
        now_ts = time.time()
        with _SIDEBAR_WEATHER_CACHE_LOCK:
            if (
                _SIDEBAR_WEATHER_CACHE.get("key") == cache_key
                and _SIDEBAR_WEATHER_CACHE.get("payload")
                and (now_ts - float(_SIDEBAR_WEATHER_CACHE.get("fetched_at") or 0.0)) < _SIDEBAR_WEATHER_TTL_SECONDS
            ):
                payload = dict(_SIDEBAR_WEATHER_CACHE["payload"])
                payload["cached"] = True
                return jsonify(payload)

        weather = _weather_fetch_current(lat, lon)
        payload = {
            "success": True,
            "disabled": False,
            "cached": False,
            "location": config.get("nombre_mostrado") or config.get("ubicacion"),
            "config": {
                "habilitado": int(config.get("habilitado") or 0),
                "ubicacion": config.get("ubicacion") or "",
            },
            "weather": weather,
            "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        with _SIDEBAR_WEATHER_CACHE_LOCK:
            _SIDEBAR_WEATHER_CACHE["key"] = cache_key
            _SIDEBAR_WEATHER_CACHE["payload"] = payload
            _SIDEBAR_WEATHER_CACHE["fetched_at"] = now_ts
        return jsonify(payload)
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/alertas/recordatorios', methods=['GET'])
def api_recordatorios_pendientes():
    try:
        limpiar_recordatorios_descartados(meses=6)
        eventos = obtener_recordatorios_agenda_pendientes()
        return jsonify({'success': True, 'eventos': eventos})
    except Exception as e:
        return jsonify({'success': False, 'eventos': [], 'error': str(e)}), 500


@app.route('/api/alertas/recordatorio/descartar', methods=['POST'])
def api_descartar_recordatorio():
    try:
        data = request.get_json(silent=True) or {}
        evento_id = int(data.get('evento_id'))
        ventana_clave = data.get('ventana_clave')
        if not ventana_clave:
            ventana_clave = f"{data.get('fecha')}T{data.get('hora_inicio') or '00:00'}"
        descartar_recordatorio_agenda(evento_id, ventana_clave)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/producto/agregar', methods=['POST'])
def agregar_producto():
    try:
        nombre = (request.form.get('nombre_producto') or '').strip()
        if not nombre:
            return _error_or_text("El nombre del producto es obligatorio", 400)

        stock = _as_float(request.form.get('stock_producto', 0) or 0, "stock inicial", min_value=0)
        stock_minimo = _as_float(request.form.get('stock_minimo', 2) or 2, "stock mínimo", min_value=0)
        unidad = _normalizar_unidad_producto(request.form.get('unidad', 'unidad'))
        icono = _normalizar_icono_producto(request.form.get('icono'))
        porcion_cantidad = _as_float(request.form.get('porcion_cantidad', 1) or 1, "porción de venta", min_value=0.0001)
        porcion_unidad = _normalizar_unidad_producto(request.form.get('porcion_unidad', unidad))
        stock_dependencia_tipo = str(request.form.get('stock_dependencia_tipo') or '').strip().lower()
        if stock_dependencia_tipo in {'', 'none', 'ninguna', 'null', 'sin'}:
            stock_dependencia_tipo = None
        stock_dependencia_cantidad = _as_float(
            request.form.get('stock_dependencia_cantidad', 1) or 1,
            'cantidad de dependencia de stock',
            min_value=0.0001,
        )
        stock_dependencia_id = 0
        if stock_dependencia_tipo == 'producto':
            stock_dependencia_id = _as_int(
                request.form.get('stock_dependencia_producto_id', 0) or 0,
                'producto de dependencia de stock',
                min_value=1,
            )
        elif stock_dependencia_tipo == 'insumo':
            stock_dependencia_id = _as_int(
                request.form.get('stock_dependencia_insumo_id', 0) or 0,
                'insumo de dependencia de stock',
                min_value=1,
            )
        elif stock_dependencia_tipo:
            return _error_or_text("Tipo de dependencia de stock inválido", 400)
        else:
            stock_dependencia_tipo = None
            stock_dependencia_id = 0
            stock_dependencia_cantidad = 1
        if not _son_unidades_compatibles_porcion(unidad, porcion_unidad):
            return _error_or_text(
                f"La unidad de porción ({porcion_unidad}) no es compatible con la unidad del stock ({unidad})",
                400,
            )
        
        vencimiento_cantidad = request.form.get('vencimiento_cantidad')
        vencimiento_tipo = request.form.get('vencimiento_tipo')
        alerta_previa = _as_int(request.form.get('alerta_previa', 2) or 2, "alerta previa", min_value=0)
        
        fecha_vencimiento = None
        vida_util_dias = 0
        
        # Calcular fecha de vencimiento estimada
        if vencimiento_cantidad and vencimiento_tipo:
            cantidad = _as_int(vencimiento_cantidad, "vencimiento", min_value=1)

            if vencimiento_tipo == 'dias':
                vida_util_dias = cantidad
            elif vencimiento_tipo == 'semanas':
                vida_util_dias = cantidad * 7
            elif vencimiento_tipo == 'meses':
                vida_util_dias = cantidad * 30
            else:
                return _error_or_text("Tipo de vencimiento inválido", 400)
            if vida_util_dias > 0:
                fecha_vencimiento = (datetime.now() + timedelta(days=vida_util_dias)).strftime('%Y-%m-%d')
        
        # Insertar producto
        conn = get_db()
        cursor = conn.cursor()
        if stock_dependencia_tipo == 'producto':
            cursor.execute(
                """
                SELECT id
                FROM productos
                WHERE id = ?
                  AND COALESCE(eliminado, 0) = 0
                """,
                (stock_dependencia_id,),
            )
            if not cursor.fetchone():
                conn.close()
                return _error_or_text("El producto seleccionado para dependencia de stock no existe", 400)
        elif stock_dependencia_tipo == 'insumo':
            cursor.execute("SELECT id FROM insumos WHERE id = ?", (stock_dependencia_id,))
            if not cursor.fetchone():
                conn.close()
                return _error_or_text("El insumo seleccionado para dependencia de stock no existe", 400)
        
        cursor.execute("""
            INSERT INTO productos (
                nombre, icono, stock, stock_minimo, unidad, porcion_cantidad, porcion_unidad,
                stock_dependencia_tipo, stock_dependencia_id, stock_dependencia_cantidad,
                fecha_vencimiento, alerta_dias, vida_util_dias
            ) 
            VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            nombre,
            icono,
            stock_minimo,
            unidad,
            porcion_cantidad,
            porcion_unidad,
            stock_dependencia_tipo,
            stock_dependencia_id if stock_dependencia_id > 0 else None,
            stock_dependencia_cantidad,
            fecha_vencimiento,
            alerta_previa,
            vida_util_dias,
        ))
        producto_id = cursor.lastrowid

        registrar_historial_cambio(
            recurso_tipo='producto',
            recurso_id=producto_id,
            recurso_nombre=nombre,
            accion='agregado',
            detalle='Alta manual de producto',
            origen_modulo='productos',
            metadata={
                'stock_inicial': stock,
                'unidad': unidad,
                'stock_minimo': stock_minimo,
            },
            conn=conn,
        )
        
        conn.commit()
        conn.close()

        if stock > 0:
            fecha_lote = None
            if vida_util_dias > 0:
                fecha_lote = (datetime.now() + timedelta(days=vida_util_dias)).strftime('%Y-%m-%d')
            agregar_lote_producto(producto_id, stock, fecha_lote)

        crear_backup()
        return _ok_or_redirect(
            {
                'success': True,
                'producto_id': producto_id,
                'message': 'Producto agregado correctamente'
            },
            'productos'
        )
    except ValueError as e:
        return _error_or_text(e, 400)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return _error_or_text(e, 500)

@app.route('/ventas')
def ventas():
    try:
        productos = _obtener_productos_para_venta(include_zero_stock=True)
        ventas_totales_map = {}
        conn_rank = None
        try:
            conn_rank = get_db()
            cur_rank = conn_rank.cursor()
            cur_rank.execute(
                """
                SELECT producto_id, COALESCE(SUM(cantidad), 0) AS total_vendido
                FROM venta_items
                GROUP BY producto_id
                """
            )
            for row in cur_rank.fetchall():
                try:
                    pid = int(row["producto_id"] or 0)
                except (TypeError, ValueError):
                    pid = 0
                if pid > 0:
                    ventas_totales_map[pid] = int(row["total_vendido"] or 0)
        except Exception:
            ventas_totales_map = {}
        finally:
            if conn_rank:
                conn_rank.close()

        for p in productos:
            try:
                pid = int(p.get("id") or 0)
            except (TypeError, ValueError):
                pid = 0
            p["ventas_totales"] = int(ventas_totales_map.get(pid, 0))
        categorias_tienda = _cargar_categorias_tienda()
        agenda_evento_id = request.args.get('agenda_evento', type=int)
        agenda_evento = obtener_evento_agenda_por_id(agenda_evento_id) if agenda_evento_id else None
        return render_template(
            'ventas.html',
            productos=productos,
            agenda_evento=agenda_evento,
            categorias_tienda=categorias_tienda,
        )
    except Exception as e:
        print(f"Error en ventas: {e}")
        return f"Error: {str(e)}", 500


def _ensure_ventas_mayoristas_tables(cursor):
    conn = getattr(cursor, "connection", None)
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS ventas_mayoristas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha TEXT NOT NULL,
            vendedor_id INTEGER,
            vendedor_nombre TEXT NOT NULL,
            vendedor_contacto TEXT,
            cliente_nombre TEXT,
            notas TEXT,
            total_bruto REAL DEFAULT 0,
            total_comision REAL DEFAULT 0,
            total_neto REAL DEFAULT 0,
            descontar_stock INTEGER DEFAULT 1,
            codigo_operacion TEXT,
            creado TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS ventas_mayoristas_vendedores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT NOT NULL,
            contacto TEXT,
            notas TEXT,
            comision_pct REAL DEFAULT 0,
            activo INTEGER DEFAULT 1,
            creado TEXT DEFAULT CURRENT_TIMESTAMP,
            actualizado TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute("PRAGMA table_info(ventas_mayoristas)")
    columnas_ventas_mayoristas = {str(r["name"] if hasattr(r, "keys") and "name" in r.keys() else r[1]) for r in cursor.fetchall()}
    if "vendedor_id" not in columnas_ventas_mayoristas:
        cursor.execute("ALTER TABLE ventas_mayoristas ADD COLUMN vendedor_id INTEGER")
    cursor.execute("PRAGMA table_info(ventas_mayoristas_vendedores)")
    columnas_vendedores_mayoristas = {str(r["name"] if hasattr(r, "keys") and "name" in r.keys() else r[1]) for r in cursor.fetchall()}
    if "comision_pct" not in columnas_vendedores_mayoristas:
        cursor.execute("ALTER TABLE ventas_mayoristas_vendedores ADD COLUMN comision_pct REAL DEFAULT 0")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS ventas_mayoristas_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            venta_mayorista_id INTEGER NOT NULL,
            producto_id INTEGER NOT NULL,
            producto_nombre TEXT NOT NULL,
            cantidad REAL NOT NULL DEFAULT 0,
            precio_unitario REAL NOT NULL DEFAULT 0,
            subtotal REAL NOT NULL DEFAULT 0,
            comision_pct REAL NOT NULL DEFAULT 0,
            comision_monto REAL NOT NULL DEFAULT 0,
            neto_negocio REAL NOT NULL DEFAULT 0,
            FOREIGN KEY (venta_mayorista_id) REFERENCES ventas_mayoristas(id) ON DELETE CASCADE,
            FOREIGN KEY (producto_id) REFERENCES productos(id)
        )
        """
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ventas_mayoristas_vendedor_id ON ventas_mayoristas(vendedor_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ventas_mayoristas_fecha ON ventas_mayoristas(fecha)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ventas_mayoristas_vendedor ON ventas_mayoristas(vendedor_nombre)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ventas_mayoristas_items_venta ON ventas_mayoristas_items(venta_mayorista_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ventas_mayoristas_vendedores_activo ON ventas_mayoristas_vendedores(activo, nombre)")


def _listar_vendedores_mayoristas(cursor, solo_activos=False):
    _ensure_ventas_mayoristas_tables(cursor)
    where = "WHERE COALESCE(activo, 1) = 1" if solo_activos else ""
    cursor.execute(
        f"""
        SELECT id, nombre, contacto, notas, COALESCE(comision_pct, 0) AS comision_pct,
               COALESCE(activo, 1) AS activo, creado, actualizado
        FROM ventas_mayoristas_vendedores
        {where}
        ORDER BY COALESCE(activo, 1) DESC, nombre COLLATE NOCASE ASC
        """
    )
    return [dict(r) for r in cursor.fetchall()]


def _ventas_mayor_semana_por_fecha(fecha_raw):
    raw = str(fecha_raw or "").strip()[:10]
    if not raw:
        base = datetime.now(ZoneInfo("America/Santiago")).date()
    else:
        try:
            base = datetime.strptime(raw, "%Y-%m-%d").date()
        except ValueError:
            raise ValueError("Fecha de semana invalida")
    inicio = base - timedelta(days=base.weekday())
    fin = inicio + timedelta(days=6)
    return inicio.strftime("%Y-%m-%d"), fin.strftime("%Y-%m-%d")


def _ventas_mayor_mes_actual():
    hoy = datetime.now(ZoneInfo("America/Santiago")).date()
    inicio = hoy.replace(day=1)
    if hoy.month == 12:
        siguiente = hoy.replace(year=hoy.year + 1, month=1, day=1)
    else:
        siguiente = hoy.replace(month=hoy.month + 1, day=1)
    fin = siguiente - timedelta(days=1)
    return inicio.strftime("%Y-%m-%d"), fin.strftime("%Y-%m-%d")


def _fmt_clp_mayor(value):
    try:
        num = float(value or 0)
    except (TypeError, ValueError):
        num = 0
    return f"${num:,.0f}".replace(",", ".")


def _parse_pct_mayor(value):
    try:
        pct = float(str(value or "0").replace(",", "."))
    except (TypeError, ValueError):
        pct = 0.0
    return max(0.0, min(100.0, pct))


def _parse_int_mayor(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _crear_pdf_ventas_mayoristas_semanal(vendedor, ventas, fecha_desde, fecha_hasta):
    if canvas is None:
        raise RuntimeError("ReportLab no esta instalado en el entorno.")

    buff = BytesIO()
    c = canvas.Canvas(buff, pagesize=A4)
    width, height = A4
    y = height - 44

    def new_page():
        nonlocal y
        c.showPage()
        y = height - 44

    def line(text, x=42, size=9, bold=False, dy=14):
        nonlocal y
        if y < 60:
            new_page()
        c.setFont("Helvetica-Bold" if bold else "Helvetica", size)
        c.drawString(x, y, str(text or "")[:115])
        y -= dy

    def wrap_text(text, max_len=92):
        raw = str(text or "").strip()
        if not raw:
            return ["-"]
        out = []
        while len(raw) > max_len:
            cut = raw.rfind(" ", 0, max_len + 1)
            if cut <= 0:
                cut = max_len
            out.append(raw[:cut].strip())
            raw = raw[cut:].strip()
        if raw:
            out.append(raw)
        return out or ["-"]

    total_bruto = sum(float(v.get("total_bruto") or 0) for v in ventas)
    total_comision = sum(float(v.get("total_comision") or 0) for v in ventas)
    total_neto = sum(float(v.get("total_neto") or 0) for v in ventas)
    comision_base = float(vendedor.get("comision_pct") or 0)

    c.setFillColorRGB(0.96, 0.52, 0.05)
    c.rect(36, height - 96, width - 72, 58, stroke=0, fill=1)
    c.setFillColorRGB(1, 1, 1)
    c.setFont("Helvetica-Bold", 16)
    c.drawString(50, height - 62, "Sucree Pasteleria")
    c.setFont("Helvetica", 9)
    c.drawString(50, height - 78, "Reporte de ventas por mayor")
    c.drawRightString(width - 50, height - 62, f"{fecha_desde} al {fecha_hasta}")
    c.setFillColorRGB(0, 0, 0)
    y = height - 122

    line("Vendedor", bold=True, size=11, dy=16)
    line(f"Nombre: {vendedor.get('nombre') or '-'}")
    line(f"Contacto: {vendedor.get('contacto') or '-'}")
    line(f"Comision base perfil: {comision_base:.2f}%")
    y -= 8

    c.setFillColorRGB(0.92, 0.99, 0.95)
    c.rect(42, y - 62, width - 84, 64, stroke=1, fill=1)
    c.setFillColorRGB(0, 0, 0)
    c.setFont("Helvetica-Bold", 10)
    c.drawString(56, y - 18, "Total vendido")
    c.drawString(220, y - 18, "Comision venta")
    c.drawString(410, y - 18, "A transferir")
    c.setFont("Helvetica-Bold", 13)
    c.drawString(56, y - 42, _fmt_clp_mayor(total_bruto))
    c.drawString(220, y - 42, _fmt_clp_mayor(total_comision))
    c.setFillColorRGB(0.0, 0.45, 0.26)
    c.drawString(410, y - 42, _fmt_clp_mayor(total_neto))
    c.setFillColorRGB(0, 0, 0)
    y -= 88

    line("Detalle del periodo", bold=True, size=11, dy=18)
    if not ventas:
        line("No hay ventas registradas para este vendedor en el rango seleccionado.")
    for venta in ventas:
        line(f"{venta.get('fecha')} - {venta.get('codigo_operacion') or '#' + str(venta.get('id'))}", bold=True, size=10)
        line(f"Cliente final: {venta.get('cliente_nombre') or '-'} | Total: {_fmt_clp_mayor(venta.get('total_bruto'))} | Comision venta: {_fmt_clp_mayor(venta.get('total_comision'))} | A transferir: {_fmt_clp_mayor(venta.get('total_neto'))}", size=8)
        for it in venta.get("items") or []:
            item_line = (
                f"- {it.get('producto_nombre') or '-'} x{float(it.get('cantidad') or 0):g} | "
                f"Precio: {_fmt_clp_mayor(it.get('precio_unitario'))} | "
                f"Subtotal: {_fmt_clp_mayor(it.get('subtotal'))} | "
                f"Comision {float(it.get('comision_pct') or 0):g}%: {_fmt_clp_mayor(it.get('comision_monto'))}"
            )
            for part in wrap_text(item_line, 104):
                line(part, x=54, size=8, dy=11)
        if venta.get("notas"):
            for part in wrap_text(f"Notas: {venta.get('notas')}", 104):
                line(part, x=54, size=8, dy=11)
        y -= 8

    line("Resumen para vendedor", bold=True, size=11, dy=18)
    line(f"Ventas registradas: {len(ventas)}")
    line(f"Total vendido: {_fmt_clp_mayor(total_bruto)}")
    line(f"Comision venta: {_fmt_clp_mayor(total_comision)}")
    line(f"A transferir: {_fmt_clp_mayor(total_neto)}")
    y -= 8
    line("Documento informativo generado desde SucreeStock.", size=8)

    c.save()
    buff.seek(0)
    return buff


@app.route('/ventas/mayor')
def ventas_mayor():
    conn = None
    try:
        productos = _obtener_productos_para_venta(include_zero_stock=True)
        conn = get_db()
        cur = conn.cursor()
        vendedores = _listar_vendedores_mayoristas(cur, solo_activos=True)
        return render_template('ventas_mayor.html', productos=productos, vendedores=vendedores)
    except Exception as e:
        print(f"Error en ventas por mayor: {e}")
        return f"Error: {str(e)}", 500
    finally:
        if conn:
            conn.close()


@app.route('/api/ventas/mayoristas/vendedores', methods=['GET', 'POST'])
def api_ventas_mayoristas_vendedores():
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()
        _ensure_ventas_mayoristas_tables(cur)
        if request.method == 'GET':
            solo_activos = str(request.args.get("activos") or "").strip().lower() in {"1", "true", "si", "yes"}
            return jsonify({"success": True, "vendedores": _listar_vendedores_mayoristas(cur, solo_activos=solo_activos)})

        data = request.get_json(silent=True) or {}
        nombre = str(data.get("nombre") or "").strip()[:120]
        contacto = str(data.get("contacto") or "").strip()[:120]
        notas = str(data.get("notas") or "").strip()[:500]
        comision_pct = _parse_pct_mayor(data.get("comision_pct"))
        activo = 1 if bool(data.get("activo", True)) else 0
        if len(nombre) < 2:
            return jsonify({"success": False, "error": "Ingresa el nombre del vendedor"}), 400
        cur.execute(
            """
            INSERT INTO ventas_mayoristas_vendedores (nombre, contacto, notas, comision_pct, activo, actualizado)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (nombre, contacto, notas, comision_pct, activo),
        )
        conn.commit()
        crear_backup()
        return jsonify({"success": True, "vendedor_id": int(cur.lastrowid), "vendedores": _listar_vendedores_mayoristas(cur, solo_activos=True)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/ventas/mayoristas/vendedores/<int:vendedor_id>', methods=['POST'])
def api_ventas_mayoristas_vendedor_actualizar(vendedor_id):
    conn = None
    try:
        data = request.get_json(silent=True) or {}
        nombre = str(data.get("nombre") or "").strip()[:120]
        contacto = str(data.get("contacto") or "").strip()[:120]
        notas = str(data.get("notas") or "").strip()[:500]
        comision_pct = _parse_pct_mayor(data.get("comision_pct"))
        activo = 1 if bool(data.get("activo", True)) else 0
        if len(nombre) < 2:
            return jsonify({"success": False, "error": "Ingresa el nombre del vendedor"}), 400
        conn = get_db()
        cur = conn.cursor()
        _ensure_ventas_mayoristas_tables(cur)
        cur.execute(
            """
            UPDATE ventas_mayoristas_vendedores
            SET nombre = ?, contacto = ?, notas = ?, comision_pct = ?, activo = ?, actualizado = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (nombre, contacto, notas, comision_pct, activo, int(vendedor_id)),
        )
        if cur.rowcount <= 0:
            return jsonify({"success": False, "error": "Vendedor no encontrado"}), 404
        conn.commit()
        crear_backup()
        return jsonify({"success": True, "vendedores": _listar_vendedores_mayoristas(cur, solo_activos=False)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/ventas/mayoristas', methods=['GET'])
def api_ventas_mayoristas_listar():
    conn = None
    try:
        fecha_desde = str(request.args.get("desde") or "").strip()[:10]
        fecha_hasta = str(request.args.get("hasta") or "").strip()[:10]
        vendedor = str(request.args.get("vendedor") or "").strip()
        vendedor_id = _parse_int_mayor(request.args.get("vendedor_id"), 0)
        if not fecha_desde and not fecha_hasta:
            fecha_desde, fecha_hasta = _ventas_mayor_mes_actual()
        if fecha_desde and fecha_hasta and fecha_desde > fecha_hasta:
            fecha_desde, fecha_hasta = fecha_hasta, fecha_desde
        conn = get_db()
        cur = conn.cursor()
        _ensure_ventas_mayoristas_tables(cur)
        where = []
        params = []
        if fecha_desde:
            where.append("fecha >= ?")
            params.append(fecha_desde)
        if fecha_hasta:
            where.append("fecha <= ?")
            params.append(fecha_hasta)
        if vendedor_id > 0:
            where.append("vendedor_id = ?")
            params.append(vendedor_id)
        elif vendedor:
            where.append("LOWER(vendedor_nombre) LIKE ?")
            params.append(f"%{vendedor.lower()}%")
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        cur.execute(
            f"""
            SELECT *
            FROM ventas_mayoristas
            {where_sql}
            ORDER BY fecha DESC, id DESC
            LIMIT 250
            """,
            tuple(params),
        )
        ventas = [dict(r) for r in cur.fetchall()]
        ids = [int(v["id"]) for v in ventas]
        items_map = {}
        if ids:
            placeholders = ",".join(["?"] * len(ids))
            cur.execute(
                f"""
                SELECT *
                FROM ventas_mayoristas_items
                WHERE venta_mayorista_id IN ({placeholders})
                ORDER BY id ASC
                """,
                tuple(ids),
            )
            for row in cur.fetchall():
                item = dict(row)
                items_map.setdefault(int(item["venta_mayorista_id"]), []).append(item)
        for venta in ventas:
            venta["items"] = items_map.get(int(venta["id"]), [])
        resumen = {
            "ventas": len(ventas),
            "total_bruto": round(sum(float(v.get("total_bruto") or 0) for v in ventas), 2),
            "total_comision": round(sum(float(v.get("total_comision") or 0) for v in ventas), 2),
            "total_neto": round(sum(float(v.get("total_neto") or 0) for v in ventas), 2),
            "fecha_desde": fecha_desde,
            "fecha_hasta": fecha_hasta,
        }
        cur.execute(
            f"""
            SELECT vendedor_nombre,
                   COUNT(*) AS ventas,
                   COALESCE(SUM(total_bruto), 0) AS total_bruto,
                   COALESCE(SUM(total_comision), 0) AS total_comision,
                   COALESCE(SUM(total_neto), 0) AS total_neto
            FROM ventas_mayoristas
            {where_sql}
            GROUP BY vendedor_nombre
            ORDER BY total_bruto DESC, vendedor_nombre COLLATE NOCASE ASC
            LIMIT 80
            """,
            tuple(params),
        )
        vendedores = [dict(r) for r in cur.fetchall()]
        return jsonify({"success": True, "ventas": ventas, "resumen": resumen, "vendedores": vendedores})
    except Exception as e:
        return jsonify({"success": False, "error": str(e), "ventas": []}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/ventas/mayoristas/reporte-semanal.pdf', methods=['GET'])
def api_ventas_mayoristas_reporte_semanal_pdf():
    conn = None
    try:
        vendedor_id = _parse_int_mayor(request.args.get("vendedor_id"), 0)
        if vendedor_id <= 0:
            return jsonify({"success": False, "error": "Selecciona un vendedor"}), 400
        fecha_desde = str(request.args.get("desde") or "").strip()[:10]
        fecha_hasta = str(request.args.get("hasta") or "").strip()[:10]
        if not fecha_desde and not fecha_hasta:
            fecha_desde, fecha_hasta = _ventas_mayor_semana_por_fecha(request.args.get("semana") or request.args.get("fecha"))
        elif not fecha_desde:
            fecha_desde = fecha_hasta
        elif not fecha_hasta:
            fecha_hasta = fecha_desde
        if fecha_desde and fecha_hasta and fecha_desde > fecha_hasta:
            fecha_desde, fecha_hasta = fecha_hasta, fecha_desde
        conn = get_db()
        cur = conn.cursor()
        _ensure_ventas_mayoristas_tables(cur)
        cur.execute(
            """
            SELECT id, nombre, contacto, notas, COALESCE(comision_pct, 0) AS comision_pct, COALESCE(activo, 1) AS activo
            FROM ventas_mayoristas_vendedores
            WHERE id = ?
            """,
            (vendedor_id,),
        )
        vendedor = cur.fetchone()
        if not vendedor:
            return jsonify({"success": False, "error": "Vendedor no encontrado"}), 404
        vendedor = dict(vendedor)
        cur.execute(
            """
            SELECT *
            FROM ventas_mayoristas
            WHERE vendedor_id = ? AND fecha >= ? AND fecha <= ?
            ORDER BY fecha ASC, id ASC
            """,
            (vendedor_id, fecha_desde, fecha_hasta),
        )
        ventas = [dict(r) for r in cur.fetchall()]
        ids = [int(v["id"]) for v in ventas]
        items_map = {}
        if ids:
            placeholders = ",".join(["?"] * len(ids))
            cur.execute(
                f"""
                SELECT *
                FROM ventas_mayoristas_items
                WHERE venta_mayorista_id IN ({placeholders})
                ORDER BY id ASC
                """,
                tuple(ids),
            )
            for row in cur.fetchall():
                item = dict(row)
                items_map.setdefault(int(item["venta_mayorista_id"]), []).append(item)
        for venta in ventas:
            venta["items"] = items_map.get(int(venta["id"]), [])

        pdf_buff = _crear_pdf_ventas_mayoristas_semanal(vendedor, ventas, fecha_desde, fecha_hasta)
        filename = f"reporte_mayorista_{vendedor_id}_{fecha_desde}_al_{fecha_hasta}.pdf"
        return send_file(
            pdf_buff,
            as_attachment=True,
            download_name=filename,
            mimetype="application/pdf",
        )
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/ventas/mayoristas', methods=['POST'])
def api_ventas_mayoristas_guardar():
    conn = None
    stock_updates = []
    try:
        data = request.get_json(silent=True) or {}
        vendedor_id = int(data.get("vendedor_id") or 0)
        vendedor_nombre = str(data.get("vendedor_nombre") or "").strip()[:120]
        vendedor_contacto = str(data.get("vendedor_contacto") or "").strip()[:120]
        cliente_nombre = str(data.get("cliente_nombre") or "").strip()[:120]
        notas = str(data.get("notas") or "").strip()[:1000]
        fecha = str(data.get("fecha") or "").strip()[:10] or datetime.now(ZoneInfo("America/Santiago")).strftime("%Y-%m-%d")
        try:
            datetime.strptime(fecha, "%Y-%m-%d")
        except ValueError:
            return jsonify({"success": False, "error": "Fecha invalida"}), 400
        descontar_stock = 1 if bool(data.get("descontar_stock", True)) else 0
        raw_items = data.get("items") or []
        if not isinstance(raw_items, list) or not raw_items:
            return jsonify({"success": False, "error": "Agrega productos a la venta"}), 400

        conn = get_db()
        cur = conn.cursor()
        _ensure_ventas_mayoristas_tables(cur)
        if vendedor_id <= 0:
            return jsonify({"success": False, "error": "Selecciona un vendedor activo"}), 400
        cur.execute(
            """
            SELECT id, nombre, contacto, COALESCE(activo, 1) AS activo
            FROM ventas_mayoristas_vendedores
            WHERE id = ?
            """,
            (vendedor_id,),
        )
        vendedor_row = cur.fetchone()
        if not vendedor_row or int(vendedor_row["activo"] or 0) != 1:
            return jsonify({"success": False, "error": "El vendedor seleccionado no esta activo"}), 400
        vendedor_nombre = str(vendedor_row["nombre"] or vendedor_nombre).strip()[:120]
        vendedor_contacto = str(vendedor_row["contacto"] or vendedor_contacto).strip()[:120]
        producto_ids = []
        items_in = []
        for idx, item in enumerate(raw_items, start=1):
            if not isinstance(item, dict):
                return jsonify({"success": False, "error": f"Item #{idx} invalido"}), 400
            pid = int(item.get("producto_id") or 0)
            cantidad = float(item.get("cantidad") or 0)
            precio_unitario = float(item.get("precio_unitario") or 0)
            comision_pct = float(item.get("comision_pct") or 0)
            if pid <= 0 or cantidad <= 0 or precio_unitario < 0:
                return jsonify({"success": False, "error": f"Item #{idx}: datos invalidos"}), 400
            comision_pct = max(0.0, min(100.0, comision_pct))
            producto_ids.append(pid)
            items_in.append({"producto_id": pid, "cantidad": cantidad, "precio_unitario": precio_unitario, "comision_pct": comision_pct})

        placeholders = ",".join(["?"] * len(set(producto_ids)))
        cur.execute(
            f"SELECT id, nombre, COALESCE(stock, 0) AS stock FROM productos WHERE id IN ({placeholders}) AND COALESCE(eliminado, 0) = 0",
            tuple(sorted(set(producto_ids))),
        )
        productos_map = {int(r["id"]): dict(r) for r in cur.fetchall()}
        for item in items_in:
            prod = productos_map.get(int(item["producto_id"]))
            if not prod:
                return jsonify({"success": False, "error": f"Producto #{item['producto_id']} no existe"}), 400
            if descontar_stock and float(prod.get("stock") or 0) + 1e-9 < float(item["cantidad"] or 0):
                return jsonify({"success": False, "error": f"Stock insuficiente para {prod.get('nombre')}"}), 400

        total_bruto = 0.0
        total_comision = 0.0
        rows = []
        for item in items_in:
            prod = productos_map[int(item["producto_id"])]
            subtotal = round(float(item["cantidad"]) * float(item["precio_unitario"]), 2)
            comision = round(subtotal * (float(item["comision_pct"]) / 100.0), 2)
            neto = round(subtotal - comision, 2)
            total_bruto += subtotal
            total_comision += comision
            rows.append({**item, "producto_nombre": str(prod.get("nombre") or ""), "subtotal": subtotal, "comision_monto": comision, "neto_negocio": neto})
        total_bruto = round(total_bruto, 2)
        total_comision = round(total_comision, 2)
        total_neto = round(total_bruto - total_comision, 2)
        codigo = f"VPM-{datetime.now(ZoneInfo('America/Santiago')).strftime('%Y%m%d')}-{uuid.uuid4().hex[:6].upper()}"

        cur.execute(
            """
            INSERT INTO ventas_mayoristas (
                fecha, vendedor_id, vendedor_nombre, vendedor_contacto, cliente_nombre, notas,
                total_bruto, total_comision, total_neto, descontar_stock, codigo_operacion
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (fecha, vendedor_id, vendedor_nombre, vendedor_contacto, cliente_nombre, notas, total_bruto, total_comision, total_neto, int(descontar_stock), codigo),
        )
        venta_id = int(cur.lastrowid)
        for row in rows:
            cur.execute(
                """
                INSERT INTO ventas_mayoristas_items (
                    venta_mayorista_id, producto_id, producto_nombre, cantidad, precio_unitario,
                    subtotal, comision_pct, comision_monto, neto_negocio
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    venta_id, int(row["producto_id"]), row["producto_nombre"], float(row["cantidad"]),
                    float(row["precio_unitario"]), float(row["subtotal"]), float(row["comision_pct"]),
                    float(row["comision_monto"]), float(row["neto_negocio"]),
                ),
            )
            if descontar_stock:
                stock_updates.append(row)
        conn.commit()

        for row in stock_updates:
            actualizar_stock_producto(
                int(row["producto_id"]),
                -float(row["cantidad"]),
                referencia_tipo="venta_mayorista",
                referencia_id=venta_id,
                detalle=f"Venta por mayor {codigo} - {vendedor_nombre}",
                codigo_operacion=codigo,
                origen_modulo="ventas_mayor",
            )

        crear_backup()
        return jsonify({"success": True, "venta_id": venta_id, "codigo_operacion": codigo})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


def _armar_producto_base_para_venta(data):
    item = dict(data or {})
    porcion_info = _calcular_info_porciones_producto(item)
    item["unidad"] = porcion_info["unidad_stock"]
    item["porcion_cantidad"] = porcion_info["porcion_cantidad"]
    item["porcion_unidad"] = porcion_info["porcion_unidad"]
    item["porcion_stock_equivalente"] = porcion_info["porcion_stock_equivalente"]
    item["porciones_disponibles"] = int(porcion_info["porciones_disponibles"] or 0)
    item["sin_porcion_disponible"] = bool(porcion_info["sin_porcion_disponible"])
    item["baja_porcion"] = bool(porcion_info["baja_porcion"])
    item["bajo_minimo"] = bool(porcion_info["bajo_minimo"])
    item["cerca_minimo"] = bool(porcion_info["cerca_minimo"])
    item["porcion_error"] = porcion_info["error"]
    item["porcion_cantidad_label"] = _formatear_numero_simple(porcion_info["porcion_cantidad"])
    item["stock_label"] = _formatear_numero_simple(item.get("stock"))
    try:
        stock_visual = float(item.get("stock") or 0)
    except (TypeError, ValueError):
        stock_visual = 0.0
    item["stock_visual"] = stock_visual
    item["stock_visual_unidad"] = item["unidad"]
    item["stock_visual_origen"] = "propio"
    item["stock_visual_dependencia_nombre"] = None
    item["stock_visual_label"] = _formatear_numero_simple(stock_visual)
    item["icono"] = _normalizar_icono_producto(item.get("icono"))
    foto_rel = str(item.get("foto") or "").strip()
    if foto_rel:
        try:
            item["foto_url"] = url_for('static', filename=foto_rel)
        except Exception:
            item["foto_url"] = f"/static/{foto_rel}"
    else:
        item["foto_url"] = ""
    tipo_dep = str(item.get("stock_dependencia_tipo") or "").strip().lower()
    if tipo_dep not in {"producto", "insumo"}:
        tipo_dep = None
    try:
        dep_id = int(item.get("stock_dependencia_id") or 0)
    except (TypeError, ValueError):
        dep_id = 0
    try:
        dep_cantidad = float(item.get("stock_dependencia_cantidad") or 1)
    except (TypeError, ValueError):
        dep_cantidad = 1
    if dep_cantidad <= 0:
        dep_cantidad = 1
    if not tipo_dep or dep_id <= 0:
        tipo_dep = None
        dep_id = 0
        dep_cantidad = 1
    item["stock_dependencia_tipo"] = tipo_dep
    item["stock_dependencia_id"] = dep_id if dep_id > 0 else None
    item["stock_dependencia_cantidad"] = dep_cantidad
    return item


def _enriquecer_productos_con_dependencias_venta(cursor, productos):
    if not isinstance(productos, list) or not productos:
        return productos

    cursor.execute(
        """
        SELECT id, nombre, stock, stock_minimo, unidad, porcion_cantidad, porcion_unidad,
               stock_dependencia_tipo, stock_dependencia_id, stock_dependencia_cantidad
        FROM productos
        """
    )
    productos_rows = cursor.fetchall()
    productos_todos = {int(row["id"]): dict(row) for row in productos_rows}
    info_propia = {
        pid: _calcular_info_porciones_producto(prod)
        for pid, prod in productos_todos.items()
    }
    mapa_stock_dependencia = {}
    for pid, prod in productos_todos.items():
        tipo_dep = str(prod.get("stock_dependencia_tipo") or "").strip().lower()
        if tipo_dep not in {"producto", "insumo"}:
            continue
        try:
            dep_id = int(prod.get("stock_dependencia_id") or 0)
        except (TypeError, ValueError):
            dep_id = 0
        try:
            dep_cantidad = float(prod.get("stock_dependencia_cantidad") or 1)
        except (TypeError, ValueError):
            dep_cantidad = 1
        if dep_id <= 0 or dep_cantidad <= 0:
            continue
        mapa_stock_dependencia[pid] = {
            "tipo": tipo_dep,
            "id": dep_id,
            "cantidad": dep_cantidad,
        }

    cursor.execute(
        """
        SELECT id, nombre, stock, stock_minimo, unidad
        FROM insumos
        """
    )
    insumos_mapa = {int(row["id"]): dict(row) for row in cursor.fetchall()}

    cursor.execute(
        """
        SELECT producto_id, insumo_id, cantidad, unidad
        FROM producto_insumos_venta
        WHERE cantidad > 0
        ORDER BY id ASC
        """
    )
    mapa_insumos = {}
    for row in cursor.fetchall():
        origen = int(row["producto_id"] or 0)
        insumo_id = int(row["insumo_id"] or 0)
        cantidad = float(row["cantidad"] or 0)
        if origen <= 0 or insumo_id <= 0 or cantidad <= 0:
            continue
        mapa_insumos.setdefault(origen, []).append(
            {
                "insumo_id": insumo_id,
                "cantidad": cantidad,
                "unidad": _normalizar_unidad_producto(row["unidad"] or "unidad"),
            }
        )

    cursor.execute(
        """
        SELECT producto_id, producto_asociado_id, cantidad
        FROM producto_productos_venta
        WHERE cantidad > 0
        ORDER BY id ASC
        """
    )
    mapa_productos = {}
    for row in cursor.fetchall():
        origen = int(row["producto_id"] or 0)
        destino = int(row["producto_asociado_id"] or 0)
        factor = float(row["cantidad"] or 0)
        if origen <= 0 or destino <= 0 or factor <= 0:
            continue
        mapa_productos.setdefault(origen, []).append(
            {
                "producto_id": destino,
                "factor": factor,
            }
        )

    memo_estado = {}

    def _estado_producto(pid, pila=None):
        if pid in memo_estado:
            return memo_estado[pid]

        info = info_propia.get(pid) or _calcular_info_porciones_producto(
            {
                "unidad": "unidad",
                "stock": 0,
                "stock_minimo": 0,
                "porcion_cantidad": 1,
                "porcion_unidad": "unidad",
            }
        )
        porciones_propias = max(0, int(info.get("porciones_disponibles") or 0))
        estado = {
            "porciones_totales": porciones_propias,
            "sin_porcion_total": bool(info.get("sin_porcion_disponible")),
            "baja_porcion_total": bool(info.get("baja_porcion")),
            "bajo_minimo_total": bool(info.get("bajo_minimo")),
            "cerca_minimo_total": bool(info.get("cerca_minimo")),
            "dependencias_criticas": False,
            "dependencias_baja_porcion": False,
            "dependencias_cerca_minimo": False,
            "dependencias_limita_porciones": False,
            "dependencias_alerta": [],
        }

        if pila is None:
            pila = set()
        if pid in pila:
            estado["dependencias_criticas"] = True
            estado["dependencias_alerta"] = ["Asociacion ciclica detectada"]
            estado["sin_porcion_total"] = True
            estado["baja_porcion_total"] = True
            memo_estado[pid] = estado
            return estado

        pila.add(pid)
        limite_porciones = porciones_propias
        dep_bajo_minimo = False
        dep_cerca_minimo = False
        dep_baja_porcion = False
        dep_sin_porcion = False
        alertas_dep = []

        dep_stock_cfg = mapa_stock_dependencia.get(pid)
        if dep_stock_cfg:
            dep_tipo = str(dep_stock_cfg.get("tipo") or "").strip().lower()
            dep_id = int(dep_stock_cfg.get("id") or 0)
            dep_cantidad = float(dep_stock_cfg.get("cantidad") or 1)
            if dep_id <= 0 or dep_cantidad <= 0:
                dep_sin_porcion = True
                dep_bajo_minimo = True
                limite_porciones = 0
                alertas_dep.append("Dependencia de stock inválida")
            elif dep_tipo == "producto":
                if dep_id == pid:
                    dep_sin_porcion = True
                    dep_bajo_minimo = True
                    limite_porciones = 0
                    alertas_dep.append("Dependencia de stock ciclica")
                else:
                    dep_producto = productos_todos.get(dep_id)
                    if not dep_producto:
                        dep_sin_porcion = True
                        dep_bajo_minimo = True
                        limite_porciones = 0
                        alertas_dep.append("Producto de dependencia no disponible")
                    else:
                        sub_dep = _estado_producto(dep_id, pila)
                        porciones_dep = max(0, int(sub_dep.get("porciones_totales") or 0))
                        porciones_por_dep = max(0, int(math.floor((porciones_dep + 1e-9) / dep_cantidad)))
                        if porciones_por_dep < limite_porciones:
                            estado["dependencias_limita_porciones"] = True
                        limite_porciones = min(limite_porciones, porciones_por_dep)

                        if sub_dep.get("bajo_minimo_total"):
                            dep_bajo_minimo = True
                        elif sub_dep.get("cerca_minimo_total"):
                            dep_cerca_minimo = True

                        if porciones_por_dep < 1 or sub_dep.get("sin_porcion_total"):
                            dep_sin_porcion = True
                        elif porciones_por_dep <= 1 or sub_dep.get("baja_porcion_total"):
                            dep_baja_porcion = True

                        nombre_dep = dep_producto.get("nombre") or f"Producto {dep_id}"
                        if porciones_por_dep <= 1 or sub_dep.get("dependencias_criticas") or sub_dep.get("bajo_minimo_total"):
                            alertas_dep.append(f"{nombre_dep}: dependencia de stock en nivel critico")
                        elif sub_dep.get("dependencias_cerca_minimo") or sub_dep.get("cerca_minimo_total"):
                            alertas_dep.append(f"{nombre_dep}: dependencia de stock cercana al minimo")
            elif dep_tipo == "insumo":
                dep_insumo = insumos_mapa.get(dep_id)
                if not dep_insumo:
                    dep_sin_porcion = True
                    dep_bajo_minimo = True
                    limite_porciones = 0
                    alertas_dep.append("Insumo de dependencia no disponible")
                else:
                    nombre_insumo = dep_insumo.get("nombre") or f"Insumo {dep_id}"
                    stock_insumo = float(dep_insumo.get("stock") or 0)
                    minimo_insumo = float(dep_insumo.get("stock_minimo") or 0)
                    porciones_por_dep = max(0, int(math.floor((stock_insumo + 1e-9) / dep_cantidad)))
                    if porciones_por_dep < limite_porciones:
                        estado["dependencias_limita_porciones"] = True
                    limite_porciones = min(limite_porciones, porciones_por_dep)

                    bajo_min = stock_insumo <= minimo_insumo
                    cerca_min = _esta_cerca_minimo(stock_insumo, minimo_insumo)
                    if bajo_min:
                        dep_bajo_minimo = True
                    elif cerca_min:
                        dep_cerca_minimo = True

                    if porciones_por_dep < 1:
                        dep_sin_porcion = True
                    elif porciones_por_dep <= 1:
                        dep_baja_porcion = True

                    if porciones_por_dep <= 1:
                        alertas_dep.append(f"{nombre_insumo}: dependencia de stock solo alcanza para {porciones_por_dep} porcion(es)")
                    elif bajo_min:
                        alertas_dep.append(f"{nombre_insumo}: dependencia de stock bajo minimo")
                    elif cerca_min:
                        alertas_dep.append(f"{nombre_insumo}: dependencia de stock cercana al minimo")
            else:
                dep_sin_porcion = True
                dep_bajo_minimo = True
                limite_porciones = 0
                alertas_dep.append("Dependencia de stock inválida")

        insumos_asociados_agregados = {}
        for assoc in mapa_insumos.get(pid, []):
            insumo_id = int(assoc.get("insumo_id") or 0)
            insumo = insumos_mapa.get(insumo_id)
            if not insumo:
                dep_sin_porcion = True
                dep_bajo_minimo = True
                limite_porciones = 0
                alertas_dep.append("Insumo asociado no disponible")
                continue

            nombre_insumo = str(insumo.get("nombre") or f"Insumo {insumo_id}")
            unidad_stock_insumo = _normalizar_unidad_producto(insumo.get("unidad") or "unidad")
            unidad_assoc = _normalizar_unidad_producto(assoc.get("unidad") or unidad_stock_insumo)
            cantidad_assoc = float(assoc.get("cantidad") or 0)

            conv = _convertir_cantidad_unidad(cantidad_assoc, unidad_assoc, unidad_stock_insumo)
            if not conv["success"]:
                dep_sin_porcion = True
                dep_bajo_minimo = True
                limite_porciones = 0
                alertas_dep.append(f"{nombre_insumo}: unidad asociada incompatible")
                continue

            consumo_stock = float(conv.get("cantidad") or 0)
            if consumo_stock <= 0:
                continue

            item_agregado = insumos_asociados_agregados.setdefault(
                insumo_id,
                {
                    "insumo_id": insumo_id,
                    "nombre": nombre_insumo,
                    "stock": float(insumo.get("stock") or 0),
                    "stock_minimo": float(insumo.get("stock_minimo") or 0),
                    "unidad_stock": unidad_stock_insumo,
                    "consumo_por_porcion_stock": 0.0,
                },
            )
            item_agregado["consumo_por_porcion_stock"] += consumo_stock

        for info_insumo in insumos_asociados_agregados.values():
            nombre_insumo = info_insumo["nombre"]
            stock_insumo = float(info_insumo["stock"] or 0)
            minimo_insumo = float(info_insumo["stock_minimo"] or 0)
            consumo_stock = float(info_insumo["consumo_por_porcion_stock"] or 0)
            if consumo_stock <= 0:
                continue

            porciones_por_insumo = max(0, int(math.floor((stock_insumo + 1e-9) / consumo_stock)))
            if porciones_por_insumo < limite_porciones:
                estado["dependencias_limita_porciones"] = True
            limite_porciones = min(limite_porciones, porciones_por_insumo)

            bajo_min = stock_insumo <= minimo_insumo
            cerca_min = _esta_cerca_minimo(stock_insumo, minimo_insumo)
            if bajo_min:
                dep_bajo_minimo = True
            elif cerca_min:
                dep_cerca_minimo = True

            if porciones_por_insumo < 1:
                dep_sin_porcion = True
            elif porciones_por_insumo <= 1:
                dep_baja_porcion = True

            if bajo_min or cerca_min or porciones_por_insumo <= 1:
                if porciones_por_insumo <= 1:
                    alertas_dep.append(f"{nombre_insumo}: solo alcanza para {porciones_por_insumo} porcion(es)")
                elif bajo_min:
                    alertas_dep.append(f"{nombre_insumo}: bajo minimo")
                else:
                    alertas_dep.append(f"{nombre_insumo}: cercano al minimo")

        for assoc in mapa_productos.get(pid, []):
            producto_asociado_id = int(assoc.get("producto_id") or 0)
            factor = float(assoc.get("factor") or 0)
            if producto_asociado_id <= 0 or factor <= 0:
                continue

            sub = _estado_producto(producto_asociado_id, pila)
            porciones_sub = max(0, int(sub.get("porciones_totales") or 0))
            porciones_por_producto = max(0, int(math.floor((porciones_sub + 1e-9) / factor)))
            if porciones_por_producto < limite_porciones:
                estado["dependencias_limita_porciones"] = True
            limite_porciones = min(limite_porciones, porciones_por_producto)

            if sub.get("bajo_minimo_total"):
                dep_bajo_minimo = True
            elif sub.get("cerca_minimo_total"):
                dep_cerca_minimo = True

            if porciones_por_producto < 1 or sub.get("sin_porcion_total"):
                dep_sin_porcion = True
            elif porciones_por_producto <= 1 or sub.get("baja_porcion_total"):
                dep_baja_porcion = True

            if porciones_por_producto <= 1 or sub.get("dependencias_criticas") or sub.get("bajo_minimo_total"):
                nombre_sub = (
                    productos_todos.get(producto_asociado_id, {}).get("nombre")
                    or f"Producto {producto_asociado_id}"
                )
                alertas_dep.append(f"{nombre_sub}: asociado en nivel critico")
            elif sub.get("dependencias_cerca_minimo") or sub.get("cerca_minimo_total"):
                nombre_sub = (
                    productos_todos.get(producto_asociado_id, {}).get("nombre")
                    or f"Producto {producto_asociado_id}"
                )
                alertas_dep.append(f"{nombre_sub}: asociado cercano al minimo")

        pila.discard(pid)

        limite_porciones = max(0, int(limite_porciones))
        if limite_porciones < porciones_propias:
            estado["dependencias_limita_porciones"] = True
        estado["porciones_totales"] = limite_porciones
        # Crítico solo cuando no alcanza porción o hay dependencia inválida/ausente.
        # "Bajo mínimo" en dependencias se considera advertencia operacional, no bloqueo.
        estado["dependencias_criticas"] = bool(dep_sin_porcion)
        estado["dependencias_baja_porcion"] = bool(dep_baja_porcion)
        estado["dependencias_cerca_minimo"] = bool(dep_cerca_minimo or dep_baja_porcion or dep_bajo_minimo)
        estado["dependencias_alerta"] = alertas_dep[:6]
        estado["sin_porcion_total"] = bool(estado["sin_porcion_total"] or dep_sin_porcion or limite_porciones < 1)
        estado["baja_porcion_total"] = bool(estado["baja_porcion_total"] or dep_baja_porcion or limite_porciones <= 1)
        # "Bajo mínimo" debe representar solo el stock propio del producto,
        # no el de sus dependencias/insumos asociados.
        estado["bajo_minimo_total"] = bool(estado["bajo_minimo_total"])
        estado["cerca_minimo_total"] = bool(estado["cerca_minimo_total"] or dep_cerca_minimo or dep_baja_porcion)

        memo_estado[pid] = estado
        return estado

    for item in productos:
        try:
            pid = int(item.get("id") or 0)
        except (TypeError, ValueError):
            pid = 0
        if pid <= 0:
            continue

        estado = _estado_producto(pid)
        porciones_base = max(0, int(item.get("porciones_disponibles") or 0))
        porciones_totales = max(0, int(estado.get("porciones_totales") or 0))

        item["porciones_disponibles_base"] = porciones_base
        item["porciones_disponibles"] = porciones_totales
        item["sin_porcion_disponible"] = bool(estado.get("sin_porcion_total"))
        item["baja_porcion"] = bool(estado.get("baja_porcion_total"))
        item["bajo_minimo"] = bool(estado.get("bajo_minimo_total"))
        item["cerca_minimo"] = bool(estado.get("cerca_minimo_total"))
        item["dependencias_criticas"] = bool(estado.get("dependencias_criticas"))
        item["dependencias_baja_porcion"] = bool(estado.get("dependencias_baja_porcion"))
        item["dependencias_cerca_minimo"] = bool(estado.get("dependencias_cerca_minimo"))
        item["dependencias_limita_porciones"] = bool(estado.get("dependencias_limita_porciones"))
        item["dependencias_alerta"] = list(estado.get("dependencias_alerta") or [])
        item["dependencias_alerta_texto"] = " | ".join(item["dependencias_alerta"][:2]) if item["dependencias_alerta"] else ""
        item["dependencias_referencia"] = bool(porciones_totales < porciones_base)
        cfg = mapa_stock_dependencia.get(pid) or {}
        item["stock_dependencia_tipo"] = cfg.get("tipo")
        item["stock_dependencia_id"] = cfg.get("id")
        item["stock_dependencia_cantidad"] = float(cfg.get("cantidad") or 1) if cfg else 1.0

        try:
            stock_visual = float(item.get("stock") or 0)
        except (TypeError, ValueError):
            stock_visual = 0.0
        stock_visual_unidad = _normalizar_unidad_producto(item.get("unidad") or "unidad")
        stock_visual_origen = "propio"
        stock_visual_dependencia_nombre = None

        tipo_dep = str(item.get("stock_dependencia_tipo") or "").strip().lower()
        try:
            dep_id = int(item.get("stock_dependencia_id") or 0)
        except (TypeError, ValueError):
            dep_id = 0

        if tipo_dep == "producto" and dep_id > 0:
            dep_producto = productos_todos.get(dep_id)
            if dep_producto:
                dep_info = info_propia.get(dep_id) or _calcular_info_porciones_producto(dep_producto)
                try:
                    stock_visual = float(dep_producto.get("stock") or 0)
                except (TypeError, ValueError):
                    stock_visual = 0.0
                stock_visual_unidad = _normalizar_unidad_producto(
                    dep_info.get("unidad_stock") or dep_producto.get("unidad") or "unidad"
                )
                stock_visual_origen = "producto"
                stock_visual_dependencia_nombre = dep_producto.get("nombre")
        elif tipo_dep == "insumo" and dep_id > 0:
            dep_insumo = insumos_mapa.get(dep_id)
            if dep_insumo:
                try:
                    stock_visual = float(dep_insumo.get("stock") or 0)
                except (TypeError, ValueError):
                    stock_visual = 0.0
                stock_visual_unidad = _normalizar_unidad_producto(dep_insumo.get("unidad") or "unidad")
                stock_visual_origen = "insumo"
                stock_visual_dependencia_nombre = dep_insumo.get("nombre")

        # Si el producto esta vinculado a una dependencia de stock (producto/insumo),
        # la disponibilidad en porciones debe seguir la misma base usada en "stock_visual".
        if tipo_dep in {"producto", "insumo"} and dep_id > 0:
            try:
                consumo_base = float(item.get("stock_dependencia_cantidad") or 1)
            except (TypeError, ValueError):
                consumo_base = 1.0
            if consumo_base <= 0:
                consumo_base = 1.0
            porciones_por_dependencia = max(0, int(math.floor((float(stock_visual) + 1e-9) / consumo_base)))
            item["porciones_disponibles"] = porciones_por_dependencia
            item["sin_porcion_disponible"] = porciones_por_dependencia < 1
            item["baja_porcion"] = porciones_por_dependencia <= 1

        item["stock_visual"] = stock_visual
        item["stock_visual_unidad"] = stock_visual_unidad
        item["stock_visual_origen"] = stock_visual_origen
        item["stock_visual_dependencia_nombre"] = stock_visual_dependencia_nombre
        item["stock_visual_label"] = _formatear_numero_simple(stock_visual)

    return productos


def _obtener_productos_para_venta(include_zero_stock=False):
    conn = get_db()
    cursor = conn.cursor()
    try:
        filtro_stock = "" if include_zero_stock else "AND stock > 0"
        cursor.execute(
            f"""
            SELECT *
            FROM productos
            WHERE COALESCE(eliminado, 0) = 0
              {filtro_stock}
            ORDER BY nombre
            """
        )
        filas = cursor.fetchall()
        productos = [_armar_producto_base_para_venta(dict(fila)) for fila in filas]
        _enriquecer_productos_con_dependencias_venta(cursor, productos)
        _anotar_estado_desactivacion_manual(cursor, productos, limpiar_resueltas=True)
        for item in productos:
            estado = _resolver_estado_disponibilidad_producto(item)
            item["estado_disponibilidad"] = estado["estado_final"]
            item["disponible"] = estado["disponible"]
            item["advertencia"] = estado["advertencia"]
            item["bloqueado"] = estado["bloqueado"]
        productos.sort(
            key=lambda item: (
                0 if item.get("desactivacion_manual_requiere_confirmacion") else 1,
                str(item.get("nombre") or "").strip().lower(),
            )
        )
        conn.commit()
        return productos
    finally:
        conn.close()


@app.route('/api/ventas/productos-disponibles')
def api_productos_venta_disponibles():
    try:
        productos = _obtener_productos_para_venta(include_zero_stock=True)
        return jsonify({"success": True, "productos": productos})
    except Exception as e:
        return jsonify({"success": False, "productos": [], "error": str(e)}), 500


@app.route('/api/ventas/semanales', methods=['GET', 'POST'])
def api_ventas_semanales():
    if request.method == 'GET':
        try:
            fecha_desde = (request.args.get('desde') or '').strip() or None
            fecha_hasta = (request.args.get('hasta') or '').strip() or None
            limit_raw = (request.args.get('limit') or '').strip().lower()
            if limit_raw == '':
                limit = 20
            elif limit_raw in {'all', '0'}:
                limit = None
            else:
                limit = _as_int(limit_raw, "límite", min_value=1)
            registros = listar_ventas_semanales(fecha_desde=fecha_desde, fecha_hasta=fecha_hasta, limite=limit)
            return jsonify({'success': True, 'data': registros})
        except ValueError as e:
            return jsonify({'success': False, 'data': [], 'error': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'data': [], 'error': str(e)}), 500

    try:
        payload = request.get_json(silent=True) or {}
        resultado = guardar_venta_semanal(payload)
        if not resultado.get('success'):
            return jsonify({'success': False, 'error': resultado.get('error', 'No se pudo guardar')}), 400
        crear_backup()
        return jsonify({'success': True, 'registro': resultado.get('registro')})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/ventas/semanales/<int:registro_id>/eliminar', methods=['POST'])
def api_eliminar_ventas_semanales(registro_id):
    try:
        resultado = eliminar_venta_semanal(registro_id)
        if not resultado.get('success'):
            msg = str(resultado.get('error') or '').lower()
            status = 404 if 'no encontrado' in msg else 400
            return jsonify({'success': False, 'error': resultado.get('error', 'No se pudo eliminar')}), status
        crear_backup()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


def _parse_fecha_iso_ymd(valor, campo):
    raw = str(valor or "").strip()
    if not raw:
        raise ValueError(f"{campo} es obligatorio")
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(f"{campo} invalida. Formato esperado: YYYY-MM-DD")


def _semana_lunes(fecha):
    return fecha - timedelta(days=fecha.weekday())


def _construir_reporte_version_prueba_canales(fecha_desde_raw=None, fecha_hasta_raw=None):
    fecha_hasta = _parse_fecha_iso_ymd(
        fecha_hasta_raw or datetime.now().strftime("%Y-%m-%d"),
        "fecha hasta",
    )
    fecha_desde = _parse_fecha_iso_ymd(
        fecha_desde_raw or (fecha_hasta - timedelta(days=29)).strftime("%Y-%m-%d"),
        "fecha desde",
    )
    if fecha_desde > fecha_hasta:
        raise ValueError("fecha desde no puede ser mayor que fecha hasta")

    semanas = {}
    cursor_semana = _semana_lunes(fecha_desde)
    while cursor_semana <= fecha_hasta:
        semana_inicio = cursor_semana.strftime("%Y-%m-%d")
        semanas[semana_inicio] = {
            "semana_inicio": semana_inicio,
            "semana_fin": (cursor_semana + timedelta(days=6)).strftime("%Y-%m-%d"),
            "auto_presencial": 0.0,
            "auto_tienda_online": 0.0,
            "auto_total": 0.0,
            "auto_operaciones": 0,
            "manual_uber": 0.0,
            "manual_pedidosya": 0.0,
            "manual_apps_total": 0.0,
            "total_combinado": 0.0,
        }
        cursor_semana += timedelta(days=7)

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT fecha_hora, COALESCE(canal_venta, 'presencial') AS canal_venta, COALESCE(total_monto, 0) AS total_monto
            FROM ventas
            WHERE date(fecha_hora) >= date(?)
              AND date(fecha_hora) <= date(?)
              AND LOWER(COALESCE(estado, '')) NOT IN ('anulada', 'anulado', 'cancelada', 'cancelado')
            """,
            (fecha_desde.strftime("%Y-%m-%d"), fecha_hasta.strftime("%Y-%m-%d")),
        )
        ventas = cursor.fetchall()

        for venta in ventas:
            fecha_hora = str(venta["fecha_hora"] or "").strip()
            if len(fecha_hora) < 10:
                continue
            try:
                fecha_venta = datetime.strptime(fecha_hora[:10], "%Y-%m-%d").date()
            except ValueError:
                continue
            semana_inicio = _semana_lunes(fecha_venta).strftime("%Y-%m-%d")
            item = semanas.get(semana_inicio)
            if item is None:
                continue

            canal = str(venta["canal_venta"] or "presencial").strip().lower()
            monto = max(0.0, float(venta["total_monto"] or 0))
            if canal in {"uber_eats", "pedidosya"}:
                continue

            if canal == "tienda_online":
                item["auto_tienda_online"] += monto
            else:
                item["auto_presencial"] += monto
            item["auto_total"] += monto
            item["auto_operaciones"] += 1

        cursor.execute(
            """
            SELECT semana_inicio, COALESCE(ventas_uber, 0) AS ventas_uber, COALESCE(ventas_pedidosya, 0) AS ventas_pedidosya
            FROM ventas_semanales
            WHERE date(semana_inicio) >= date(?)
              AND date(semana_inicio) <= date(?)
            """,
            (fecha_desde.strftime("%Y-%m-%d"), fecha_hasta.strftime("%Y-%m-%d")),
        )
        manuales = cursor.fetchall()

        for manual in manuales:
            semana_inicio = str(manual["semana_inicio"] or "").strip()
            item = semanas.get(semana_inicio)
            if item is None:
                continue
            item["manual_uber"] = max(0.0, float(manual["ventas_uber"] or 0))
            item["manual_pedidosya"] = max(0.0, float(manual["ventas_pedidosya"] or 0))
            item["manual_apps_total"] = item["manual_uber"] + item["manual_pedidosya"]

        cursor.execute(
            """
            SELECT
                date(fecha_factura) AS fecha_base,
                COALESCE(SUM(monto_total), 0) AS gasto_general
            FROM facturas_archivo
            WHERE COALESCE(eliminado, 0) = 0
              AND date(fecha_factura) >= date(?)
              AND date(fecha_factura) <= date(?)
            GROUP BY date(fecha_factura)
            """,
            (fecha_desde.strftime("%Y-%m-%d"), fecha_hasta.strftime("%Y-%m-%d")),
        )
        gastos_generales_rows = cursor.fetchall()
        for row in gastos_generales_rows:
            fecha_base = str(row["fecha_base"] or "").strip()
            if len(fecha_base) != 10:
                continue
            try:
                fecha_obj = datetime.strptime(fecha_base, "%Y-%m-%d").date()
            except ValueError:
                continue
            semana_inicio = _semana_lunes(fecha_obj).strftime("%Y-%m-%d")
            item = semanas.get(semana_inicio)
            if item is None:
                continue
            item["gasto_general"] = float(item.get("gasto_general") or 0) + max(0.0, float(row["gasto_general"] or 0))

        cursor.execute(
            """
            SELECT
                date(fecha) AS fecha_base,
                LOWER(TRIM(COALESCE(tipo, ''))) AS tipo,
                COALESCE(SUM(monto), 0) AS monto_total
            FROM finanzas_movimientos_manuales
            WHERE date(fecha) >= date(?)
              AND date(fecha) <= date(?)
            GROUP BY date(fecha), LOWER(TRIM(COALESCE(tipo, '')))
            """,
            (fecha_desde.strftime("%Y-%m-%d"), fecha_hasta.strftime("%Y-%m-%d")),
        )
        mov_manual_rows = cursor.fetchall()
        for row in mov_manual_rows:
            fecha_base = str(row["fecha_base"] or "").strip()
            if len(fecha_base) != 10:
                continue
            try:
                fecha_obj = datetime.strptime(fecha_base, "%Y-%m-%d").date()
            except ValueError:
                continue
            semana_inicio = _semana_lunes(fecha_obj).strftime("%Y-%m-%d")
            item = semanas.get(semana_inicio)
            if item is None:
                continue
            tipo = str(row["tipo"] or "").strip().lower()
            monto = max(0.0, float(row["monto_total"] or 0))
            if tipo == "ingreso":
                item["ingreso_manual"] = float(item.get("ingreso_manual") or 0) + monto
            elif tipo == "egreso":
                item["egreso_manual"] = float(item.get("egreso_manual") or 0) + monto

        filas = []
        for key in sorted(semanas.keys()):
            item = semanas[key]
            item["auto_presencial"] = round(item["auto_presencial"], 2)
            item["auto_tienda_online"] = round(item["auto_tienda_online"], 2)
            item["auto_total"] = round(item["auto_total"], 2)
            item["manual_uber"] = round(item["manual_uber"], 2)
            item["manual_pedidosya"] = round(item["manual_pedidosya"], 2)
            item["manual_apps_total"] = round(item["manual_apps_total"], 2)
            item["total_combinado"] = round(item["auto_total"] + item["manual_apps_total"], 2)
            item["gasto_general"] = round(float(item.get("gasto_general") or 0), 2)
            item["ingreso_manual"] = round(float(item.get("ingreso_manual") or 0), 2)
            item["egreso_manual"] = round(float(item.get("egreso_manual") or 0), 2)
            item["gastado_total"] = round(item["gasto_general"] + item["egreso_manual"], 2)
            item["saldo_favor"] = round(item["total_combinado"] + item["ingreso_manual"] - item["gastado_total"], 2)
            item["ajuste_manual_neto"] = round(item["ingreso_manual"] - item["egreso_manual"], 2)
            item["total_finanzas"] = item["saldo_favor"]
            filas.append(item)

        resumen = {
            "periodo_desde": fecha_desde.strftime("%Y-%m-%d"),
            "periodo_hasta": fecha_hasta.strftime("%Y-%m-%d"),
            "semanas": len(filas),
            "auto_presencial": round(sum(r["auto_presencial"] for r in filas), 2),
            "auto_tienda_online": round(sum(r["auto_tienda_online"] for r in filas), 2),
            "auto_total": round(sum(r["auto_total"] for r in filas), 2),
            "manual_uber": round(sum(r["manual_uber"] for r in filas), 2),
            "manual_pedidosya": round(sum(r["manual_pedidosya"] for r in filas), 2),
            "manual_apps_total": round(sum(r["manual_apps_total"] for r in filas), 2),
            "total_combinado": round(sum(r["total_combinado"] for r in filas), 2),
            "gasto_insumos": 0.0,
            "gasto_general": round(sum(r["gasto_general"] for r in filas), 2),
            "gastado_total": round(sum(r["gastado_total"] for r in filas), 2),
            "saldo_favor": round(sum(r["saldo_favor"] for r in filas), 2),
            "ingreso_manual": round(sum(r["ingreso_manual"] for r in filas), 2),
            "egreso_manual": round(sum(r["egreso_manual"] for r in filas), 2),
            "ajuste_manual_neto": round(sum(r["ajuste_manual_neto"] for r in filas), 2),
            "total_finanzas": round(sum(r["total_finanzas"] for r in filas), 2),
            "operaciones_auto": int(sum(r["auto_operaciones"] for r in filas)),
        }
        return {"resumen": resumen, "semanas": filas}
    finally:
        conn.close()


@app.route('/api/reportes/version-prueba/canales-resumen')
def api_reportes_version_prueba_canales_resumen():
    try:
        data = _construir_reporte_version_prueba_canales(
            fecha_desde_raw=request.args.get("desde"),
            fecha_hasta_raw=request.args.get("hasta"),
        )
        return jsonify({"success": True, "data": data})
    except ValueError as e:
        return jsonify({"success": False, "data": {}, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"success": False, "data": {}, "error": str(e)}), 500


def _guardar_resumen_manual_apps_semanal(payload):
    data = payload or {}
    fecha_ref = _parse_fecha_iso_ymd(
        data.get("semana_inicio") or data.get("fecha") or datetime.now().strftime("%Y-%m-%d"),
        "semana",
    )
    semana_inicio = _semana_lunes(fecha_ref).strftime("%Y-%m-%d")
    ventas_uber = max(0.0, float(data.get("ventas_uber") or 0))
    ventas_pedidosya = max(0.0, float(data.get("ventas_pedidosya") or 0))
    notas = str(data.get("notas") or "").strip()[:500] or None

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT * FROM ventas_semanales WHERE semana_inicio = ? LIMIT 1",
            (semana_inicio,),
        )
        existente = cursor.fetchone()
    finally:
        conn.close()

    data_guardar = {
        "semana_inicio": semana_inicio,
        "ventas_local": float(existente["ventas_local"] or 0) if existente else 0.0,
        "ventas_uber": ventas_uber,
        "ventas_pedidosya": ventas_pedidosya,
        "marketing_monto": float(existente["marketing_monto"] or 0) if existente else 0.0,
        "otros_descuentos_monto": float(existente["otros_descuentos_monto"] or 0) if existente else 0.0,
        "tasa_servicio_pct": float(existente["tasa_servicio_pct"] or 30) if existente else 30,
        "impuesto_tasa_servicio_pct": float(existente["impuesto_tasa_servicio_pct"] or 19) if existente else 19,
        "notas": notas if notas is not None else (str(existente["notas"] or "").strip()[:500] if existente else None),
    }
    resultado = guardar_venta_semanal(data_guardar)
    if not resultado.get("success"):
        raise ValueError(resultado.get("error", "No se pudo guardar"))
    try:
        crear_backup()
    except Exception as backup_error:
        print(f"[WARN] No se pudo crear backup tras guardar ventas apps manuales: {backup_error}")
    return resultado.get("registro")


@app.route('/api/reportes/version-prueba/canales-resumen/manual-apps', methods=['POST'])
def api_reportes_version_prueba_manual_apps():
    try:
        payload = request.get_json(silent=True) or {}
        registro = _guardar_resumen_manual_apps_semanal(payload)
        return jsonify({"success": True, "registro": registro})
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/finanzas/resumen-canales')
def api_finanzas_resumen_canales():
    try:
        data = _construir_reporte_version_prueba_canales(
            fecha_desde_raw=request.args.get("desde"),
            fecha_hasta_raw=request.args.get("hasta"),
        )
        return jsonify({"success": True, "data": data})
    except ValueError as e:
        return jsonify({"success": False, "data": {}, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"success": False, "data": {}, "error": str(e)}), 500


@app.route('/api/finanzas/resumen-canales/manual-apps', methods=['POST'])
def api_finanzas_resumen_canales_manual_apps():
    try:
        payload = request.get_json(silent=True) or {}
        registro = _guardar_resumen_manual_apps_semanal(payload)
        return jsonify({"success": True, "registro": registro})
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/finanzas/apps-manuales-desglose', methods=['GET'])
def api_finanzas_apps_manuales_desglose():
    try:
        fecha_hasta = _parse_fecha_iso_ymd(
            request.args.get("hasta") or datetime.now().strftime("%Y-%m-%d"),
            "fecha hasta",
        )
        fecha_desde = _parse_fecha_iso_ymd(
            request.args.get("desde") or (fecha_hasta - timedelta(days=29)).strftime("%Y-%m-%d"),
            "fecha desde",
        )
        if fecha_desde > fecha_hasta:
            return jsonify({"success": False, "error": "fecha desde no puede ser mayor que fecha hasta", "rows": []}), 400

        conn = get_db()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    semana_inicio,
                    semana_fin,
                    COALESCE(ventas_uber, 0) AS ventas_uber,
                    COALESCE(ventas_pedidosya, 0) AS ventas_pedidosya
                FROM ventas_semanales
                WHERE date(semana_inicio) >= date(?)
                  AND date(semana_inicio) <= date(?)
                ORDER BY date(semana_inicio) DESC, id DESC
                LIMIT 260
                """,
                (fecha_desde.strftime("%Y-%m-%d"), fecha_hasta.strftime("%Y-%m-%d")),
            )
            rows = []
            for r in cursor.fetchall():
                uber = round(max(0.0, float(r["ventas_uber"] or 0)), 2)
                pedidos = round(max(0.0, float(r["ventas_pedidosya"] or 0)), 2)
                rows.append(
                    {
                        "semana_inicio": r["semana_inicio"],
                        "semana_fin": r["semana_fin"],
                        "ventas_uber": uber,
                        "ventas_pedidosya": pedidos,
                        "total_apps": round(uber + pedidos, 2),
                    }
                )
        finally:
            conn.close()

        resumen = {
            "desde": fecha_desde.strftime("%Y-%m-%d"),
            "hasta": fecha_hasta.strftime("%Y-%m-%d"),
            "registros": len(rows),
            "total_uber": round(sum(float(x.get("ventas_uber") or 0) for x in rows), 2),
            "total_pedidosya": round(sum(float(x.get("ventas_pedidosya") or 0) for x in rows), 2),
            "total_apps": round(sum(float(x.get("total_apps") or 0) for x in rows), 2),
        }
        return jsonify({"success": True, "rows": rows, "resumen": resumen})
    except ValueError as e:
        return jsonify({"success": False, "error": str(e), "rows": []}), 400
    except Exception as e:
        return jsonify({"success": False, "error": str(e), "rows": []}), 500


@app.route('/api/finanzas/canal-historial', methods=['GET'])
def api_finanzas_canal_historial():
    try:
        canal = str(request.args.get("canal") or "presencial").strip().lower()
        agrupar = str(request.args.get("agrupar") or "semana").strip().lower()
        if canal not in {"presencial", "tienda_online"}:
            return jsonify({"success": False, "error": "canal invalido. Usa presencial o tienda_online", "rows": []}), 400
        if agrupar not in {"semana", "mes"}:
            return jsonify({"success": False, "error": "agrupar invalido. Usa semana o mes", "rows": []}), 400

        fecha_hasta = _parse_fecha_iso_ymd(
            request.args.get("hasta") or datetime.now().strftime("%Y-%m-%d"),
            "fecha hasta",
        )
        fecha_desde = _parse_fecha_iso_ymd(
            request.args.get("desde") or (fecha_hasta - timedelta(days=29)).strftime("%Y-%m-%d"),
            "fecha desde",
        )
        if fecha_desde > fecha_hasta:
            return jsonify({"success": False, "error": "fecha desde no puede ser mayor que fecha hasta", "rows": []}), 400

        conn = get_db()
        cursor = conn.cursor()
        try:
            if canal == "tienda_online":
                where_canal = "LOWER(TRIM(COALESCE(canal_venta, ''))) = 'tienda_online'"
            else:
                where_canal = """
                    LOWER(TRIM(COALESCE(canal_venta, 'presencial'))) <> 'tienda_online'
                    AND LOWER(TRIM(COALESCE(canal_venta, 'presencial'))) NOT IN ('uber_eats', 'pedidosya')
                """

            cursor.execute(
                f"""
                SELECT
                    date(fecha_hora) AS fecha_base,
                    COUNT(*) AS operaciones,
                    COALESCE(SUM(COALESCE(total_monto, 0)), 0) AS total
                FROM ventas
                WHERE date(fecha_hora) >= date(?)
                  AND date(fecha_hora) <= date(?)
                  AND LOWER(COALESCE(estado, '')) NOT IN ('anulada', 'anulado', 'cancelada', 'cancelado')
                  AND {where_canal}
                GROUP BY date(fecha_hora)
                ORDER BY date(fecha_hora) DESC
                LIMIT 500
                """,
                (fecha_desde.strftime("%Y-%m-%d"), fecha_hasta.strftime("%Y-%m-%d")),
            )
            diarios = cursor.fetchall()
        finally:
            conn.close()

        acumulado = {}
        for row in diarios:
            fecha_base = str(row["fecha_base"] or "").strip()
            if len(fecha_base) != 10:
                continue
            try:
                fecha_obj = datetime.strptime(fecha_base, "%Y-%m-%d").date()
            except ValueError:
                continue
            operaciones = int(row["operaciones"] or 0)
            total = round(max(0.0, float(row["total"] or 0)), 2)

            if agrupar == "mes":
                periodo_inicio_obj = fecha_obj.replace(day=1)
                if periodo_inicio_obj.month == 12:
                    prox_mes = periodo_inicio_obj.replace(year=periodo_inicio_obj.year + 1, month=1, day=1)
                else:
                    prox_mes = periodo_inicio_obj.replace(month=periodo_inicio_obj.month + 1, day=1)
                periodo_fin_obj = prox_mes - timedelta(days=1)
                periodo_etiqueta = periodo_inicio_obj.strftime("%Y-%m")
            else:
                periodo_inicio_obj = _semana_lunes(fecha_obj)
                periodo_fin_obj = periodo_inicio_obj + timedelta(days=6)
                periodo_etiqueta = f"Semana {periodo_inicio_obj.strftime('%d/%m')} - {periodo_fin_obj.strftime('%d/%m')}"

            key = periodo_inicio_obj.strftime("%Y-%m-%d")
            if key not in acumulado:
                acumulado[key] = {
                    "periodo_inicio": periodo_inicio_obj.strftime("%Y-%m-%d"),
                    "periodo_fin": periodo_fin_obj.strftime("%Y-%m-%d"),
                    "periodo_etiqueta": periodo_etiqueta,
                    "operaciones": 0,
                    "total": 0.0,
                }
            acumulado[key]["operaciones"] += operaciones
            acumulado[key]["total"] = round(float(acumulado[key]["total"]) + total, 2)

        rows = [acumulado[k] for k in sorted(acumulado.keys(), reverse=True)]
        resumen = {
            "canal": canal,
            "agrupar": agrupar,
            "desde": fecha_desde.strftime("%Y-%m-%d"),
            "hasta": fecha_hasta.strftime("%Y-%m-%d"),
            "registros": len(rows),
            "operaciones": int(sum(int(x.get("operaciones") or 0) for x in rows)),
            "total": round(sum(float(x.get("total") or 0) for x in rows), 2),
        }
        return jsonify({"success": True, "rows": rows, "resumen": resumen})
    except ValueError as e:
        return jsonify({"success": False, "error": str(e), "rows": []}), 400
    except Exception as e:
        return jsonify({"success": False, "error": str(e), "rows": []}), 500


@app.route('/api/finanzas/movimientos-manuales', methods=['POST'])
def api_finanzas_movimientos_manuales():
    try:
        payload = request.get_json(silent=True) or {}
        fecha = _parse_fecha_iso_ymd(payload.get("fecha") or datetime.now().strftime("%Y-%m-%d"), "fecha").strftime("%Y-%m-%d")
        tipo = str(payload.get("tipo") or "").strip().lower()
        if tipo not in {"ingreso", "egreso"}:
            return jsonify({"success": False, "error": "Tipo invalido. Usa ingreso o egreso"}), 400
        monto = max(0.0, float(payload.get("monto") or 0))
        if monto <= 0:
            return jsonify({"success": False, "error": "Monto debe ser mayor a 0"}), 400
        categoria = str(payload.get("categoria") or "").strip()[:80] or None
        descripcion = str(payload.get("descripcion") or "").strip()[:300] or None

        conn = get_db()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO finanzas_movimientos_manuales (fecha, tipo, monto, categoria, descripcion)
                VALUES (?, ?, ?, ?, ?)
                """,
                (fecha, tipo, monto, categoria, descripcion),
            )
            mov_id = int(cursor.lastrowid or 0)
            conn.commit()
        finally:
            conn.close()

        try:
            crear_backup()
        except Exception as backup_error:
            print(f"[WARN] No se pudo crear backup tras registrar movimiento manual de finanzas: {backup_error}")

        return jsonify({
            "success": True,
            "movimiento": {
                "id": mov_id,
                "fecha": fecha,
                "tipo": tipo,
                "monto": round(monto, 2),
                "categoria": categoria,
                "descripcion": descripcion,
            },
        })
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/finanzas/movimientos-manuales', methods=['GET'])
def api_finanzas_movimientos_manuales_listar():
    try:
        fecha_hasta = _parse_fecha_iso_ymd(
            request.args.get("hasta") or datetime.now().strftime("%Y-%m-%d"),
            "fecha hasta",
        ).strftime("%Y-%m-%d")
        fecha_desde = _parse_fecha_iso_ymd(
            request.args.get("desde") or (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
            "fecha desde",
        ).strftime("%Y-%m-%d")
        tipo = str(request.args.get("tipo") or "").strip().lower()
        where_tipo = ""
        params = [fecha_desde, fecha_hasta]
        if tipo in {"ingreso", "egreso"}:
            where_tipo = "AND LOWER(TRIM(COALESCE(tipo, ''))) = ?"
            params.append(tipo)
        conn = get_db()
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
                SELECT id, fecha, tipo, monto, categoria, descripcion, creado
                FROM finanzas_movimientos_manuales
                WHERE date(fecha) >= date(?)
                  AND date(fecha) <= date(?)
                  {where_tipo}
                ORDER BY date(fecha) DESC, id DESC
                LIMIT 500
                """,
                tuple(params),
            )
            rows = [dict(r) for r in cursor.fetchall()]
        finally:
            conn.close()
        return jsonify({"success": True, "movimientos": rows})
    except ValueError as e:
        return jsonify({"success": False, "movimientos": [], "error": str(e)}), 400
    except Exception as e:
        return jsonify({"success": False, "movimientos": [], "error": str(e)}), 500


@app.route('/api/finanzas/movimientos-manuales/<int:movimiento_id>/actualizar', methods=['POST'])
def api_finanzas_movimientos_manuales_actualizar(movimiento_id):
    try:
        payload = request.get_json(silent=True) or {}
        fecha = _parse_fecha_iso_ymd(payload.get("fecha") or datetime.now().strftime("%Y-%m-%d"), "fecha").strftime("%Y-%m-%d")
        tipo = str(payload.get("tipo") or "").strip().lower()
        if tipo not in {"ingreso", "egreso"}:
            return jsonify({"success": False, "error": "Tipo invalido. Usa ingreso o egreso"}), 400
        monto = max(0.0, float(payload.get("monto") or 0))
        if monto <= 0:
            return jsonify({"success": False, "error": "Monto debe ser mayor a 0"}), 400
        categoria = str(payload.get("categoria") or "").strip()[:80] or None
        descripcion = str(payload.get("descripcion") or "").strip()[:300] or None

        conn = get_db()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT id FROM finanzas_movimientos_manuales WHERE id = ?",
                (int(movimiento_id),),
            )
            if not cursor.fetchone():
                return jsonify({"success": False, "error": "Movimiento no encontrado"}), 404
            cursor.execute(
                """
                UPDATE finanzas_movimientos_manuales
                SET fecha = ?, tipo = ?, monto = ?, categoria = ?, descripcion = ?
                WHERE id = ?
                """,
                (fecha, tipo, monto, categoria, descripcion, int(movimiento_id)),
            )
            conn.commit()
        finally:
            conn.close()

        try:
            crear_backup()
        except Exception as backup_error:
            print(f"[WARN] No se pudo crear backup tras actualizar movimiento manual de finanzas: {backup_error}")

        return jsonify({"success": True})
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/finanzas/movimientos-manuales/<int:movimiento_id>/eliminar', methods=['POST'])
def api_finanzas_movimientos_manuales_eliminar(movimiento_id):
    try:
        conn = get_db()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT id FROM finanzas_movimientos_manuales WHERE id = ?",
                (int(movimiento_id),),
            )
            if not cursor.fetchone():
                return jsonify({"success": False, "error": "Movimiento no encontrado"}), 404
            cursor.execute(
                "DELETE FROM finanzas_movimientos_manuales WHERE id = ?",
                (int(movimiento_id),),
            )
            conn.commit()
        finally:
            conn.close()

        try:
            crear_backup()
        except Exception as backup_error:
            print(f"[WARN] No se pudo crear backup tras eliminar movimiento manual de finanzas: {backup_error}")

        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/finanzas/gastos-detalle', methods=['GET'])
def api_finanzas_gastos_detalle():
    try:
        modo = str(request.args.get("modo") or "general").strip().lower()
        if modo not in {"general", "total"}:
            return jsonify({"success": False, "error": "modo invalido. Usa general o total", "rows": []}), 400

        fecha_hasta = _parse_fecha_iso_ymd(
            request.args.get("hasta") or datetime.now().strftime("%Y-%m-%d"),
            "fecha hasta",
        )
        fecha_desde = _parse_fecha_iso_ymd(
            request.args.get("desde") or (fecha_hasta - timedelta(days=29)).strftime("%Y-%m-%d"),
            "fecha desde",
        )
        if fecha_desde > fecha_hasta:
            return jsonify({"success": False, "error": "fecha desde no puede ser mayor que fecha hasta", "rows": []}), 400

        desde_iso = fecha_desde.strftime("%Y-%m-%d")
        hasta_iso = fecha_hasta.strftime("%Y-%m-%d")
        rows = []

        conn = get_db()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, fecha_factura, proveedor, numero_factura, monto_total, observacion
                FROM facturas_archivo
                WHERE COALESCE(eliminado, 0) = 0
                  AND date(fecha_factura) >= date(?)
                  AND date(fecha_factura) <= date(?)
                ORDER BY date(fecha_factura) DESC, id DESC
                LIMIT 800
                """,
                (desde_iso, hasta_iso),
            )
            for r in cursor.fetchall():
                rows.append(
                    {
                        "id": int(r["id"] or 0),
                        "fecha": str(r["fecha_factura"] or "").strip(),
                        "origen": "factura",
                        "categoria": str(r["proveedor"] or "Proveedor").strip() or "Proveedor",
                        "referencia": str(r["numero_factura"] or "").strip() or f"Factura #{int(r['id'] or 0)}",
                        "detalle": str(r["observacion"] or "").strip(),
                        "monto": round(max(0.0, float(r["monto_total"] or 0)), 2),
                    }
                )

            if modo == "total":
                cursor.execute(
                    """
                    SELECT id, fecha, categoria, descripcion, monto
                    FROM finanzas_movimientos_manuales
                    WHERE tipo = 'egreso'
                      AND date(fecha) >= date(?)
                      AND date(fecha) <= date(?)
                    ORDER BY date(fecha) DESC, id DESC
                    LIMIT 800
                    """,
                    (desde_iso, hasta_iso),
                )
                for r in cursor.fetchall():
                    rows.append(
                        {
                            "id": int(r["id"] or 0),
                            "fecha": str(r["fecha"] or "").strip(),
                            "origen": "egreso_manual",
                            "categoria": str(r["categoria"] or "Egreso manual").strip() or "Egreso manual",
                            "referencia": f"Egreso #{int(r['id'] or 0)}",
                            "detalle": str(r["descripcion"] or "").strip(),
                            "monto": round(max(0.0, float(r["monto"] or 0)), 2),
                        }
                    )
        finally:
            conn.close()

        rows.sort(key=lambda x: (x.get("fecha") or "", int(x.get("id") or 0)), reverse=True)
        total = round(sum(float(x.get("monto") or 0) for x in rows), 2)

        return jsonify(
            {
                "success": True,
                "rows": rows,
                "resumen": {
                    "modo": modo,
                    "desde": desde_iso,
                    "hasta": hasta_iso,
                    "registros": len(rows),
                    "total": total,
                },
            }
        )
    except ValueError as e:
        return jsonify({"success": False, "error": str(e), "rows": []}), 400
    except Exception as e:
        return jsonify({"success": False, "error": str(e), "rows": []}), 500


@app.route('/api/ventas/desactivaciones-pendientes')
def api_desactivaciones_pendientes_venta():
    try:
        productos = _obtener_productos_para_venta()
        pendientes = [
            {
                "id": int(p.get("id") or 0),
                "nombre": p.get("nombre") or "Producto",
                "stock_label": p.get("stock_visual_label") or p.get("stock_label") or _formatear_numero_simple(p.get("stock")),
                "unidad": p.get("stock_visual_unidad") or p.get("unidad") or "unidad",
                "porcion_cantidad_label": p.get("porcion_cantidad_label") or _formatear_numero_simple(p.get("porcion_cantidad") or 1),
                "porcion_unidad": p.get("porcion_unidad") or "unidad",
                "motivo": p.get("dependencias_alerta_texto") or None,
            }
            for p in productos
            if p.get("desactivacion_manual_requiere_confirmacion")
        ]
        return jsonify({"success": True, "pendientes": pendientes, "total": len(pendientes)})
    except Exception as e:
        return jsonify({"success": False, "pendientes": [], "total": 0, "error": str(e)}), 500


@app.route('/api/dashboard/productos-por-agotar', methods=['GET'])
def api_dashboard_productos_por_agotar():
    try:
        productos = _obtener_productos_para_venta()
        items = []
        for p in productos:
            stock = float(p.get("stock") or 0)
            stock_min = float(p.get("stock_minimo") or 0)
            critico = bool(
                p.get("bajo_minimo_total")
                or p.get("baja_porcion_total")
                or p.get("desactivacion_manual_requiere_confirmacion")
                or stock <= 0
            )
            if not critico:
                continue
            faltante = max(0.0, stock_min - stock)
            sugerido = faltante if faltante > 0 else max(1.0, float(p.get("porcion_cantidad") or 1))
            items.append(
                {
                    "id": int(p.get("id") or 0),
                    "nombre": str(p.get("nombre") or "Producto"),
                    "foto_url": str(p.get("foto_url") or "").strip(),
                    "stock": stock,
                    "stock_label": p.get("stock_visual_label") or p.get("stock_label") or _formatear_numero_simple(stock),
                    "stock_minimo": stock_min,
                    "stock_minimo_label": _formatear_numero_simple(stock_min),
                    "unidad": str(p.get("stock_visual_unidad") or p.get("unidad") or "unidad"),
                    "sugerido_cantidad": round(float(sugerido), 3),
                    "motivo": str(p.get("dependencias_alerta_texto") or "").strip(),
                }
            )
        items.sort(key=lambda x: (x.get("stock", 0), str(x.get("nombre") or "").lower()))
        return jsonify({"success": True, "items": items, "total": len(items)})
    except Exception as e:
        return jsonify({"success": False, "items": [], "total": 0, "error": str(e)}), 500


@app.route('/api/dashboard/productos-por-agotar/<int:producto_id>/agregar-compra', methods=['POST'])
def api_dashboard_producto_agregar_compra(producto_id):
    try:
        payload = request.get_json(silent=True) or {}
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, nombre, unidad, stock, stock_minimo
            FROM productos
            WHERE id = ?
            LIMIT 1
            """,
            (int(producto_id),),
        )
        row = cursor.fetchone()
        conn.close()
        if not row:
            return jsonify({"success": False, "error": "Producto no encontrado."}), 404

        nombre = str(row["nombre"] or "Producto").strip()
        unidad = str(row["unidad"] or "unidad").strip() or "unidad"
        stock = float(row["stock"] or 0)
        stock_min = float(row["stock_minimo"] or 0)
        suggested = payload.get("cantidad")
        try:
            suggested = float(suggested) if suggested is not None else None
        except (TypeError, ValueError):
            suggested = None
        if suggested is None or suggested <= 0:
            suggested = max(1.0, stock_min - stock) if stock_min > 0 else 1.0
            if suggested <= 0:
                suggested = 1.0

        resultado = agregar_lote_compras_pendientes(
            [
                {
                    "nombre": nombre,
                    "cantidad": round(float(suggested), 3),
                    "unidad": unidad,
                    "precio_unitario": 0,
                    "precio_incluye_iva": True,
                    "nota": "Generado desde Dashboard Inicio (productos por agotar)",
                }
            ],
            combinar=True,
        )
        if not resultado.get("success"):
            return jsonify({"success": False, "error": resultado.get("error") or "No se pudo agregar."}), 400
        crear_backup()
        resumen = obtener_compras_pendientes(incluir_comprados=True)
        return jsonify(
            {
                "success": True,
                "message": f"'{nombre}' agregado a lista de compra.",
                "resumen": resumen.get("resumen") or {},
            }
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/dashboard/productos-por-vencer', methods=['GET'])
def api_dashboard_productos_por_vencer():
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(productos)")
        cols = {str(r["name"]).strip().lower() for r in (cursor.fetchall() or []) if r and r["name"]}

        sel_foto = "foto_url" if "foto_url" in cols else "'' AS foto_url"
        sel_eliminado = "COALESCE(eliminado, 0) = 0" if "eliminado" in cols else "1=1"
        sel_unidad = "unidad" if "unidad" in cols else "'unidad' AS unidad"
        sel_stock = "stock" if "stock" in cols else "0 AS stock"

        cursor.execute(
            f"""
            SELECT
                id,
                nombre,
                {sel_foto},
                {sel_unidad},
                {sel_stock},
                fecha_vencimiento,
                CAST(julianday(fecha_vencimiento) - julianday(date('now')) AS INTEGER) AS dias_restantes
            FROM productos
            WHERE {sel_eliminado}
              AND fecha_vencimiento IS NOT NULL
              AND TRIM(fecha_vencimiento) <> ''
              AND date(fecha_vencimiento) <= date('now', '+7 day')
            ORDER BY date(fecha_vencimiento) ASC, LOWER(nombre) ASC
            """
        )
        rows = cursor.fetchall()
        conn.close()

        items = []
        for r in rows:
            dias = int(r["dias_restantes"] or 0)
            if dias < 0:
                estado = "Vencido"
            elif dias == 0:
                estado = "Vence hoy"
            elif dias == 1:
                estado = "Vence mañana"
            else:
                estado = f"Vence en {dias} días"
            items.append(
                {
                    "id": int(r["id"] or 0),
                    "nombre": str(r["nombre"] or "Producto"),
                    "foto_url": str(r["foto_url"] or "").strip(),
                    "unidad": str(r["unidad"] or "unidad"),
                    "stock_label": _formatear_numero_simple(r["stock"]),
                    "fecha_vencimiento": str(r["fecha_vencimiento"] or "").strip(),
                    "dias_restantes": dias,
                    "estado_label": estado,
                }
            )

        return jsonify({"success": True, "items": items, "total": len(items)})
    except Exception as e:
        return jsonify({"success": False, "items": [], "total": 0, "error": str(e)}), 500


@app.route('/api/producto/<int:id>/desactivacion-manual', methods=['POST'])
def api_toggle_desactivacion_manual_producto(id):
    conn = None
    try:
        data = request.get_json(silent=True) or {}
        confirmar = bool(data.get("confirmado", True))

        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT *
            FROM productos
            WHERE id = ?
            """,
            (id,),
        )
        row = cursor.fetchone()
        if not row:
            return jsonify({"success": False, "error": "Producto no encontrado"}), 404

        producto = _armar_producto_base_para_venta(dict(row))
        _enriquecer_productos_con_dependencias_venta(cursor, [producto])
        es_critico = bool(_resolver_estado_disponibilidad_producto(producto).get("bloqueado"))
        if not es_critico:
            cursor.execute("DELETE FROM producto_desactivaciones_manuales WHERE producto_id = ?", (id,))
            conn.commit()
            return jsonify(
                {
                    "success": False,
                    "error": "Solo puedes confirmar desactivacion cuando el producto esta en rojo (bajo minimo o sin porcion disponible).",
                    "estado": {
                        "producto_id": id,
                        "desactivacion_manual_confirmada": False,
                        "desactivacion_manual_requiere_confirmacion": False,
                    },
                }
            ), 400

        if confirmar:
            cursor.execute(
                """
                INSERT INTO producto_desactivaciones_manuales (producto_id, confirmado_en)
                VALUES (?, CURRENT_TIMESTAMP)
                ON CONFLICT(producto_id) DO UPDATE SET confirmado_en = CURRENT_TIMESTAMP
                """,
                (id,),
            )
            confirmada = True
        else:
            cursor.execute("DELETE FROM producto_desactivaciones_manuales WHERE producto_id = ?", (id,))
            confirmada = False

        cursor.execute(
            "SELECT confirmado_en FROM producto_desactivaciones_manuales WHERE producto_id = ?",
            (id,),
        )
        row_confirm = cursor.fetchone()
        confirmado_en = row_confirm["confirmado_en"] if row_confirm else None

        conn.commit()
        crear_backup()
        return jsonify(
            {
                "success": True,
                "estado": {
                    "producto_id": id,
                    "desactivacion_manual_confirmada": bool(confirmada),
                    "desactivacion_manual_requiere_confirmacion": bool(es_critico and not confirmada),
                    "desactivacion_manual_confirmada_en": confirmado_en,
                },
            }
        )
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


def _procesar_venta_desde_payload(data, canal_por_defecto='presencial', permitir_canal_usuario=True, permitir_agenda=True):
    payload = data or {}
    items = payload.get('items', [])
    codigo_pedido = str(payload.get('codigo_pedido') or '').strip()[:80]
    fecha_venta = str(payload.get('fecha_venta') or '').strip() or None
    agenda_evento_id = payload.get('agenda_evento_id') if permitir_agenda else None
    canal_venta = canal_por_defecto
    if permitir_canal_usuario:
        canal_enviado = str(payload.get('canal_venta') or '').strip().lower()
        if canal_enviado:
            canal_venta = canal_enviado
    if not items:
        raise ValueError('Carrito vacio')

    resultado = procesar_venta_con_insumos(
        items,
        codigo_pedido=codigo_pedido,
        fecha_venta=fecha_venta,
        agenda_evento_id=agenda_evento_id,
        canal_venta=canal_venta,
    )
    if not resultado.get('success'):
        raise RuntimeError(resultado.get('error', 'No se pudo procesar la venta'))

    venta_id = resultado.get('venta_id')
    alertas = resultado.get('alertas', [])
    productos_actualizados = resultado.get('productos_actualizados', []) or []

    if productos_actualizados:
        conn_est = None
        try:
            conn_est = get_db()
            cursor_est = conn_est.cursor()
            _enriquecer_productos_con_dependencias_venta(cursor_est, productos_actualizados)
            _anotar_estado_desactivacion_manual(cursor_est, productos_actualizados, limpiar_resueltas=True)
            conn_est.commit()
        except Exception:
            if conn_est:
                conn_est.rollback()
        finally:
            if conn_est:
                conn_est.close()

    crear_backup()
    fecha_venta_resp = resultado.get('fecha_venta')
    segmento_fecha = f" el {fecha_venta_resp}" if fecha_venta_resp else ""
    return {
        'success': True,
        'venta_id': venta_id,
        'codigo_operacion': resultado.get('codigo_operacion'),
        'alertas': alertas,
        'productos_actualizados': productos_actualizados,
        'insumos_consumidos': resultado.get('insumos_consumidos', []),
        'agenda_evento_id': resultado.get('agenda_evento_id'),
        'codigo_pedido': codigo_pedido or None,
        'fecha_venta': fecha_venta_resp,
        'canal_venta': resultado.get('canal_venta') or canal_venta,
        'total_monto': resultado.get('total_monto'),
        'mensaje': f"Venta #{venta_id} procesada{(' (pedido ' + codigo_pedido + ')') if codigo_pedido else ''}{segmento_fecha}: {len(items)} productos"
    }


@app.route('/api/venta/procesar', methods=['POST'])
def procesar_venta():
    try:
        data = request.get_json(silent=True) or {}
        respuesta = _procesar_venta_desde_payload(
            data,
            canal_por_defecto='presencial',
            permitir_canal_usuario=True,
            permitir_agenda=True,
        )
        return jsonify(respuesta)
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except RuntimeError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/tienda/checkout', methods=['POST'])
def api_tienda_checkout():
    try:
        estado_tienda = _evaluar_estado_tienda(_obtener_tienda_config())
        if not bool(estado_tienda.get("abierta")):
            return jsonify({'success': False, 'error': 'La tienda esta cerrada por el momento'}), 403
        data = request.get_json(silent=True) or {}
        flow_cfg = _flow_cfg()
        metodo_pago_preferido = str(data.get("metodo_pago") or "transferencia").strip().lower()
        if metodo_pago_preferido not in {"transferencia", "flow"}:
            metodo_pago_preferido = "transferencia"
        flow_sim_status = str(data.get("flow_simulation_status") or "").strip().lower()
        flow_sim_status = flow_sim_status if flow_sim_status in {"paid", "pending", "error"} else ""
        flow_sim_enabled = bool(flow_sim_status and session.get(_ADMIN_SESSION_KEY))
        if metodo_pago_preferido == "flow" and not bool(flow_cfg.get("enabled")):
            return jsonify({'success': False, 'error': 'La pasarela Flow no esta disponible en este momento'}), 400
        items_req = data.get('items') or []
        if not isinstance(items_req, list) or not items_req:
            return jsonify({'success': False, 'error': 'Carrito vacio'}), 400
        cliente_nombre = str(data.get("cliente_nombre") or "").strip()
        if len(cliente_nombre) < 2:
            return jsonify({'success': False, 'error': 'Nombre invalido'}), 400
        checkout_modo = str(data.get("checkout_modo") or "online").strip().lower()
        es_modo_presencial = checkout_modo in {"presencial", "tablet", "local", "presencial_tablet"}
        cliente_email = str(data.get("cliente_email") or "").strip().lower()
        cliente_telefono = str(data.get("cliente_telefono") or "").strip()
        telefono_norm = _normalizar_telefono_cl(cliente_telefono)
        if not telefono_norm:
            return jsonify({'success': False, 'error': 'Telefono invalido. Debe tener 8 digitos.'}), 400
        cliente_telefono = telefono_norm
        if es_modo_presencial:
            tel_digits = re.sub(r"\D+", "", str(cliente_telefono or ""))
            suffix = tel_digits[-8:] if tel_digits else "cliente"
            cliente_email = f"presencial.{suffix}@local.sucree"
        elif not cliente_email or not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", cliente_email):
            return jsonify({'success': False, 'error': 'Correo electronico invalido'}), 400
        cupon_codigo = _normalizar_cupon_codigo(data.get("codigo_descuento"))
        cliente_ref = _normalizar_cliente_ref(cliente_email, cliente_telefono)
        entrega_tipo = str(data.get("entrega_tipo") or "retiro").strip().lower()
        if entrega_tipo not in {"retiro", "despacho"}:
            entrega_tipo = "retiro"
        if es_modo_presencial:
            entrega_tipo = "retiro"
        hora_retiro = str(data.get("hora_retiro") or "").strip()
        if es_modo_presencial:
            hora_retiro = ""
        elif not _parse_hora_hhmm(hora_retiro):
            return jsonify({'success': False, 'error': 'Selecciona una hora valida para retiro/entrega (HH:MM).'}), 400
        direccion_entrega = str(data.get("direccion") or "").strip()[:240]
        direccion_confirmada = bool(data.get("direccion_confirmada"))
        try:
            entrega_lat = float(data.get("lat")) if data.get("lat") not in (None, "") else None
            entrega_lng = float(data.get("lng")) if data.get("lng") not in (None, "") else None
        except (TypeError, ValueError):
            entrega_lat, entrega_lng = None, None

        now_local = datetime.now(ZoneInfo("America/Santiago"))
        if not es_modo_presencial:
            min_retiro_dt = now_local + timedelta(minutes=30)
            try:
                hh, mm = [int(x) for x in hora_retiro.split(":")]
                retiro_dt = now_local.replace(hour=hh, minute=mm, second=0, microsecond=0)
            except Exception:
                return jsonify({'success': False, 'error': 'Hora de retiro invalida.'}), 400
            if retiro_dt < min_retiro_dt:
                return jsonify({'success': False, 'error': 'La hora de retiro/entrega debe ser al menos 30 minutos desde ahora.'}), 400

        categorias = _cargar_categorias_tienda()
        categorias_map = {str(c.get("nombre") or "").strip().lower(): c for c in categorias}
        mapa = {
            int(p.get("id") or 0): _serializar_producto_tienda(p, categorias_map=categorias_map, now_local=now_local)
            for p in _obtener_productos_para_venta()
        }
        conn_pack = None
        pack_rules_by_product = {}
        try:
            item_ids = sorted({int((x or {}).get("id") or 0) for x in items_req if isinstance(x, dict)})
            item_ids = [x for x in item_ids if x > 0]
            if item_ids:
                conn_pack = get_db()
                cur_pack = conn_pack.cursor()
                _ensure_producto_pack_subopciones_table(cur_pack)
                placeholders = ",".join(["?"] * len(item_ids))
                cur_pack.execute(
                    f"""
                    SELECT s.producto_pack_id, s.subproducto_id, s.max_cantidad,
                           COALESCE(p.nombre, 'Producto #' || s.subproducto_id) AS subproducto_nombre,
                           COALESCE(p.activo_tienda, 1) AS subproducto_activo_tienda
                    FROM producto_pack_subopciones s
                    LEFT JOIN productos p ON p.id = s.subproducto_id
                    WHERE s.producto_pack_id IN ({placeholders})
                    ORDER BY s.orden ASC, s.id ASC
                    """,
                    tuple(item_ids),
                )
                for rr in cur_pack.fetchall():
                    pack_id = int(rr["producto_pack_id"] or 0)
                    pack_rules_by_product.setdefault(pack_id, {"max_total": 0, "items": {}})
                    pack_rules_by_product[pack_id]["items"][int(rr["subproducto_id"] or 0)] = {
                        "max_cantidad": int(rr["max_cantidad"] or 1),
                        "activo_tienda": bool(rr["subproducto_activo_tienda"]),
                        "nombre": str(rr["subproducto_nombre"] or "").strip() or f"Producto #{int(rr['subproducto_id'] or 0)}",
                    }
                cur_pack.execute(
                    f"""
                    SELECT producto_pack_id, max_total
                    FROM producto_pack_subopciones_config
                    WHERE producto_pack_id IN ({placeholders})
                    """,
                    tuple(item_ids),
                )
                for rr in cur_pack.fetchall():
                    pack_id = int(rr["producto_pack_id"] or 0)
                    pack_rules_by_product.setdefault(pack_id, {"max_total": 0, "items": {}})
                    pack_rules_by_product[pack_id]["max_total"] = int(rr["max_total"] or 0)
        finally:
            if conn_pack:
                conn_pack.close()
        fee_cfg = _flow_fee_cfg()
        items_limpios = []
        items_serializados = []
        items_notificacion = []
        pack_detalle_por_producto = {}
        for idx, raw in enumerate(items_req, start=1):
            if not isinstance(raw, dict):
                return jsonify({'success': False, 'error': f'Item #{idx} invalido'}), 400
            try:
                pid = int(raw.get("id") or 0)
                cantidad = int(raw.get("cantidad") or 0)
            except (TypeError, ValueError):
                return jsonify({'success': False, 'error': f'Item #{idx}: formato invalido'}), 400
            if pid <= 0 or cantidad <= 0:
                return jsonify({'success': False, 'error': f'Item #{idx}: datos invalidos'}), 400
            prod = mapa.get(pid)
            if not prod:
                return jsonify({'success': False, 'error': f'Producto #{pid} no disponible'}), 400
            pack_items_final = []
            pack_rule = pack_rules_by_product.get(pid) or {"max_total": 0, "items": {}}
            pack_items_input = raw.get("pack_items")
            if pack_rule.get("items"):
                if not isinstance(pack_items_input, list) or not pack_items_input:
                    return jsonify({'success': False, 'error': f'{prod.get("nombre")}: debes elegir subitems del pack'}), 400
                resumen_pack = {}
                for pidx, pick in enumerate(pack_items_input, start=1):
                    if not isinstance(pick, dict):
                        return jsonify({'success': False, 'error': f'{prod.get("nombre")}: subitem #{pidx} invalido'}), 400
                    sid = int(pick.get("subproducto_id") or 0)
                    sqty = int(pick.get("cantidad") or 0)
                    if sid <= 0 or sqty <= 0:
                        return jsonify({'success': False, 'error': f'{prod.get("nombre")}: subitem #{pidx} invalido'}), 400
                    if sid not in pack_rule["items"]:
                        return jsonify({'success': False, 'error': f'{prod.get("nombre")}: subitem no permitido en este pack'}), 400
                    if not bool(pack_rule["items"][sid].get("activo_tienda")):
                        return jsonify({'success': False, 'error': f'{prod.get("nombre")}: {pack_rule["items"][sid].get("nombre")} esta apagado en tienda'}), 400
                    resumen_pack[sid] = int(resumen_pack.get(sid, 0)) + sqty
                total_sel = sum(int(v or 0) for v in resumen_pack.values())
                if total_sel <= 0:
                    return jsonify({'success': False, 'error': f'{prod.get("nombre")}: selecciona al menos 1 subitem'}), 400
                max_total_pack = int(pack_rule.get("max_total") or 0)
                if max_total_pack > 0 and total_sel > max_total_pack:
                    return jsonify({'success': False, 'error': f'{prod.get("nombre")}: maximo total {max_total_pack} subitems'}), 400
                partes = []
                for sid, sqty in resumen_pack.items():
                    max_item = int(pack_rule["items"][sid].get("max_cantidad") or 1)
                    if sqty > max_item:
                        return jsonify({'success': False, 'error': f'{prod.get("nombre")}: {pack_rule["items"][sid].get("nombre")} maximo {max_item}'}), 400
                    nombre_sub = str(pack_rule["items"][sid].get("nombre") or f"Producto #{sid}")
                    pack_items_final.append({"subproducto_id": int(sid), "cantidad": int(sqty), "nombre": nombre_sub})
                    partes.append(f"{nombre_sub} x{int(sqty)}")
                pack_detalle_por_producto[pid] = "Pack: " + ", ".join(partes)
            if not bool(prod.get("categoria_activa", True)):
                return jsonify({'success': False, 'error': f'{prod.get("nombre")}: categoria no disponible en este horario'}), 400
            max_compra = int(prod.get("max_compra") or 0)
            if max_compra <= 0:
                return jsonify({'success': False, 'error': f'{prod.get("nombre")} sin stock disponible'}), 400
            if cantidad > max_compra:
                return jsonify({'success': False, 'error': f'{prod.get("nombre")}: maximo {max_compra} unidad(es)'}), 400

            precio_final_base = float(prod.get("precio_final") or 0)
            precio_final = precio_final_base
            if metodo_pago_preferido == "flow":
                precio_final = _flow_gross_from_net(precio_final_base, fee_cfg=fee_cfg, apply_fixed=False)
            items_limpios.append(
                {
                    "id": pid,
                    "cantidad": cantidad,
                    "precio_unitario": precio_final,
                }
            )
            items_serializados.append(
                {
                    "id": pid,
                    "cantidad": cantidad,
                    "precio_unitario": precio_final,
                    "precio_unitario_base": precio_final_base,
                    "descuento_tienda_pct": float(prod.get("descuento_tienda_pct") or 0),
                    "pack_items": pack_items_final,
                }
            )
            nombre_noti = str(prod.get("nombre") or "").strip() or f"Producto #{pid}"
            if pack_detalle_por_producto.get(pid):
                nombre_noti = f"{nombre_noti} [{pack_detalle_por_producto[pid]}]"
            items_notificacion.append(
                {
                    "id": pid,
                    "nombre": nombre_noti,
                    "cantidad": cantidad,
                    "precio_unitario": precio_final,
                }
            )

        subtotal = sum(float(it["precio_unitario"]) * int(it["cantidad"]) for it in items_limpios)
        shipping_quote = None
        despacho_monto = 0.0
        if entrega_tipo == "despacho":
            if len(direccion_entrega) < 8:
                return jsonify({'success': False, 'error': 'Ingresa una direccion valida para despacho.'}), 400
            if not direccion_confirmada:
                return jsonify({'success': False, 'error': 'Debes confirmar la direccion con el pin del mapa.'}), 400
            if entrega_lat is None or entrega_lng is None:
                return jsonify({'success': False, 'error': 'Coordenadas de despacho invalidas.'}), 400
            if not (-90 <= float(entrega_lat) <= 90 and -180 <= float(entrega_lng) <= 180):
                return jsonify({'success': False, 'error': 'Coordenadas de despacho fuera de rango.'}), 400
            shipping_quote = _cotizar_envio_checkout_tienda(
                float(entrega_lat),
                float(entrega_lng),
                cfg_tienda=_obtener_tienda_personalizacion(),
                hora_inicio=hora_retiro,
            )
            if bool(shipping_quote.get("inside_maipu")) and bool(shipping_quote.get("visible_to_client")):
                despacho_monto = float(shipping_quote.get("shipping_fee") or 0)

        descuento_monto = 0.0
        # Descuento por nivel desactivado temporalmente por negocio.
        descuento_nivel_monto = 0.0
        descuento_nivel_pct = 0.0
        cupon_aplicado = None
        cliente_prev = None
        conn_cli = None
        try:
            conn_cli = get_db()
            cur_cli = conn_cli.cursor()
            cliente_prev = _obtener_cliente_por_contacto_cursor(cur_cli, cliente_email, cliente_telefono)
        except Exception:
            cliente_prev = None
        finally:
            if conn_cli:
                conn_cli.close()
        # Nota: el sistema de nivel se mantiene en BD pero no aplica en checkout.
        if cupon_codigo:
            cupon = _obtener_cupon_por_codigo(cupon_codigo)
            valid = _validar_cupon_y_calcular_descuento(cupon, subtotal, items_serializados, cliente_ref)
            if not valid.get("ok"):
                return jsonify({'success': False, 'error': valid.get("error", "Cupon invalido")}), 400
            descuento_monto = float(valid.get("descuento_monto") or 0)
            cupon_aplicado = cupon

        payload_seguro = {
            "items": items_limpios,
            "codigo_pedido": str(data.get("codigo_pedido") or "").strip()[:80],
            "fecha_venta": None,
        }
        respuesta = _procesar_venta_desde_payload(
            payload_seguro,
            canal_por_defecto='tienda_online',
            permitir_canal_usuario=False,
            permitir_agenda=False,
        )
        venta_id = int(respuesta.get("venta_id") or 0)
        total_productos = subtotal - descuento_monto - descuento_nivel_monto
        if total_productos < 0:
            total_productos = 0
        total_neto = float(total_productos) + float(despacho_monto or 0)
        conn = None
        try:
            _ensure_ventas_metodo_pago_column()
            _ensure_ventas_flow_admin_alert_column()
            _ensure_ventas_flow_return_column()
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE ventas
                SET cliente_nombre = ?, cliente_email = ?, cliente_telefono = ?, descuento_codigo = ?, descuento_monto = ?, total_monto = ?,
                    pedido_estado = 'recibido', pedido_estado_actualizado = CURRENT_TIMESTAMP,
                    pedido_timer_minutos = NULL, pedido_timer_inicio = NULL,
                    metodo_pago = ?,
                    canal_venta = ?,
                    flow_admin_alertado = ?,
                    flow_cliente_regreso = ?,
                    entrega_tipo = ?, hora_retiro = ?, direccion_entrega = ?, entrega_lat = ?, entrega_lng = ?, despacho_monto = ?, entrega_detalle_json = ?
                WHERE id = ?
                """,
                (
                    cliente_nombre,
                    cliente_email,
                    cliente_telefono,
                    (cupon_codigo or None),
                    (descuento_monto + descuento_nivel_monto),
                    total_neto,
                    ("flow_pendiente" if metodo_pago_preferido == "flow" else "transferencia"),
                    ("tienda_online_flow_pendiente" if metodo_pago_preferido == "flow" else "tienda_online"),
                    (0 if metodo_pago_preferido == "flow" else 1),
                    (0 if metodo_pago_preferido == "flow" else 1),
                    entrega_tipo,
                    hora_retiro,
                    (direccion_entrega if entrega_tipo == "despacho" else None),
                    (float(entrega_lat) if entrega_tipo == "despacho" and entrega_lat is not None else None),
                    (float(entrega_lng) if entrega_tipo == "despacho" and entrega_lng is not None else None),
                    (float(despacho_monto) if entrega_tipo == "despacho" else 0),
                    (json.dumps(shipping_quote, ensure_ascii=False) if isinstance(shipping_quote, dict) else None),
                    venta_id,
                ),
            )
            for pid_det, det_txt in (pack_detalle_por_producto or {}).items():
                pid_det = int(pid_det or 0)
                if pid_det <= 0 or not det_txt:
                    continue
                nombre_base = str((mapa.get(pid_det) or {}).get("nombre") or f"Producto #{pid_det}")
                nombre_final = f"{nombre_base} [{det_txt}]"
                cursor.execute(
                    """
                    UPDATE venta_items
                    SET producto_nombre = ?
                    WHERE venta_id = ? AND producto_id = ?
                    """,
                    (nombre_final, int(venta_id), int(pid_det)),
                )
            if cupon_aplicado and descuento_monto > 0:
                cursor.execute(
                    """
                    INSERT INTO tienda_cupon_usos (cupon_id, venta_id, cliente_ref, descuento_aplicado)
                    VALUES (?, ?, ?, ?)
                    """,
                    (int(cupon_aplicado["id"]), venta_id, (cliente_ref or None), descuento_monto),
                )
            cliente = _upsert_cliente_tienda_cursor(
                cursor,
                nombre=cliente_nombre,
                email=cliente_email,
                telefono=cliente_telefono,
                email_confirmado=1,
            )
            if cupon_aplicado and descuento_monto > 0 and cliente:
                _marcar_cupon_regalado_usado_cursor(
                    cursor,
                    cupon_id=int(cupon_aplicado.get("id") or 0),
                    cliente_ref=_normalizar_cliente_ref(cliente.get("email"), cliente.get("telefono")),
                    venta_id=venta_id,
                )
            if cliente:
                cfg_prog = _obtener_config_programa_clientes(conn)
                puntos_compra = _puntos_compra(total_neto, cfg_prog)
                _registrar_puntos_cliente_cursor(
                    cursor,
                    cliente_id=int(cliente.get("id") or 0),
                    puntos=puntos_compra,
                    tipo="compra",
                    origen_tipo="venta_tienda",
                    origen_id=venta_id,
                    detalle=f"Compra tienda #{venta_id}",
                )
            conn.commit()
        finally:
            if conn:
                conn.close()

        respuesta["subtotal"] = round(subtotal, 2)
        respuesta["descuento_monto"] = round(descuento_monto, 2)
        respuesta["descuento_nivel_monto"] = round(descuento_nivel_monto, 2)
        respuesta["descuento_nivel_pct"] = round(descuento_nivel_pct, 2)
        respuesta["codigo_descuento"] = cupon_codigo or None
        respuesta["cliente_nombre"] = cliente_nombre
        respuesta["cliente_email"] = cliente_email
        respuesta["cliente_telefono"] = cliente_telefono
        respuesta["entrega_tipo"] = entrega_tipo
        respuesta["checkout_modo"] = ("presencial" if es_modo_presencial else "online")
        respuesta["hora_retiro"] = hora_retiro
        respuesta["direccion_entrega"] = (direccion_entrega if entrega_tipo == "despacho" else "")
        respuesta["despacho_monto"] = round(float(despacho_monto or 0), 2)
        respuesta["shipping_quote"] = shipping_quote if isinstance(shipping_quote, dict) else None
        respuesta["total_monto"] = round(total_neto, 2)
        respuesta["metodo_pago"] = metodo_pago_preferido

        # Simulacion QA interna de Flow (solo admin autenticado).
        if metodo_pago_preferido == "flow" and flow_sim_enabled:
            sim_token = f"SIM-{venta_id}-{int(time.time())}"
            sim_order = f"SIM-ORDER-{venta_id}-{int(time.time())}"
            flow_backup = {
                "venta_id": int(venta_id),
                "cliente_nombre": cliente_nombre,
                "cliente_email": cliente_email,
                "cliente_telefono": cliente_telefono,
                "entrega_tipo": entrega_tipo,
                "hora_retiro": hora_retiro,
                "direccion_entrega": (direccion_entrega if entrega_tipo == "despacho" else ""),
                "despacho_monto": float(despacho_monto or 0),
                "subtotal": float(subtotal or 0),
                "descuento_monto": float(descuento_monto or 0),
                "total_monto": float(total_neto or 0),
                "items": items_notificacion,
                "created_at": datetime.now(ZoneInfo("America/Santiago")).isoformat(),
            }
            _guardar_flow_pago(
                venta_id=venta_id,
                commerce_order=sim_order,
                flow_token=sim_token,
                amount=total_neto,
                flow_redirect_url=f"{_public_base_url(request.url_root)}/tienda?flow={flow_sim_status}&venta_id={venta_id}",
                checkout_backup=flow_backup,
            )
            if flow_sim_status == "paid":
                _actualizar_flow_pago(venta_id=venta_id, estado="pagado", flow_order=f"SIMPAID-{venta_id}", payment_data={"simulated": True, "status": "paid"})
                conn = get_db()
                cur = conn.cursor()
                cur.execute(
                    "UPDATE ventas SET metodo_pago = 'flow_pagado', canal_venta = 'tienda_online', flow_admin_alertado = 0 WHERE id = ?",
                    (venta_id,),
                )
                conn.commit()
                conn.close()
                _notificar_whatsapp_pedido_tienda_async(
                    venta_id=venta_id,
                    cliente_nombre=cliente_nombre,
                    cliente_email=cliente_email,
                    cliente_telefono=cliente_telefono,
                    items=items_notificacion,
                    subtotal=float(subtotal),
                    descuento=float(descuento_monto),
                    total=float(total_neto),
                    host_url=_public_base_url(request.url_root),
                    entrega_tipo=entrega_tipo,
                    hora_retiro=hora_retiro,
                    direccion_entrega=(direccion_entrega if entrega_tipo == "despacho" else ""),
                    despacho_monto=float(despacho_monto or 0),
                )
            else:
                _actualizar_flow_pago(
                    venta_id=venta_id,
                    estado="pendiente",
                    flow_order=f"SIM-{flow_sim_status.upper()}-{venta_id}",
                    payment_data={"simulated": True, "status": flow_sim_status},
                )
                conn = get_db()
                cur = conn.cursor()
                cur.execute(
                    "UPDATE ventas SET metodo_pago = 'flow_pendiente', canal_venta = 'tienda_online_flow_pendiente' WHERE id = ?",
                    (venta_id,),
                )
                conn.commit()
                conn.close()
            respuesta["flow_simulated"] = True
            respuesta["flow_result"] = flow_sim_status
            respuesta["flow_token"] = sim_token
            respuesta["requires_flow_redirect"] = False
            return jsonify(respuesta)

        # Si el cliente selecciona Flow, creamos orden y devolvemos URL de redireccion.
        if metodo_pago_preferido == "flow":
            base = _public_base_url(request.url_root)
            commerce_order = f"VENTA-{venta_id}-{int(time.time())}"
            subject = _flow_subject_safe(f"Compra tienda Sucree #{venta_id}")
            flow_email = str(cliente_email or "").strip().lower()
            if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", flow_email or ""):
                flow_email = f"pedido{venta_id}@pasteleriasucree.cl"
            flow_params = {
                "commerceOrder": commerce_order,
                "subject": subject,
                "currency": "CLP",
                "amount": int(round(float(total_neto or 0))),
                "email": flow_email,
                "urlConfirmation": f"{base}/api/tienda/flow/confirm",
                "urlReturn": f"{base}/tienda/flow/retorno?venta_id={int(venta_id)}",
            }
            try:
                flow_create = _flow_post("/payment/create", flow_params, flow_cfg)
            except Exception:
                # Reintento conservador ante validaciones estrictas de Flow con email.
                flow_params_alt = dict(flow_params)
                flow_params_alt["email"] = f"pedido{venta_id}@pasteleriasucree.cl"
                flow_create = _flow_post("/payment/create", flow_params_alt, flow_cfg)
            flow_token = str(flow_create.get("token") or "").strip()
            flow_url_base = str(flow_create.get("url") or "").strip().rstrip("/")
            if not flow_token or not flow_url_base:
                return jsonify({'success': False, 'error': 'Flow no retorno URL de pago valida'}), 502
            flow_redirect_url = f"{flow_url_base}?token={quote(flow_token)}"
            flow_backup = {
                "venta_id": int(venta_id),
                "cliente_nombre": cliente_nombre,
                "cliente_email": cliente_email,
                "cliente_telefono": cliente_telefono,
                "entrega_tipo": entrega_tipo,
                "hora_retiro": hora_retiro,
                "direccion_entrega": (direccion_entrega if entrega_tipo == "despacho" else ""),
                "despacho_monto": float(despacho_monto or 0),
                "subtotal": float(subtotal or 0),
                "descuento_monto": float(descuento_monto or 0),
                "total_monto": float(total_neto or 0),
                "items": items_notificacion,
                "created_at": datetime.now(ZoneInfo("America/Santiago")).isoformat(),
            }
            _guardar_flow_pago(
                venta_id=venta_id,
                commerce_order=commerce_order,
                flow_token=flow_token,
                amount=total_neto,
                flow_redirect_url=flow_redirect_url,
                checkout_backup=flow_backup,
            )
            respuesta["flow_redirect_url"] = flow_redirect_url
            respuesta["flow_token"] = flow_token
            respuesta["requires_flow_redirect"] = True
            return jsonify(respuesta)

        _notificar_whatsapp_pedido_tienda_async(
            venta_id=venta_id,
            cliente_nombre=cliente_nombre,
            cliente_email=cliente_email,
            cliente_telefono=cliente_telefono,
            items=items_notificacion,
            subtotal=float(subtotal),
            descuento=float(descuento_monto),
            total=float(total_neto),
            host_url=_public_base_url(request.url_root),
            entrega_tipo=entrega_tipo,
            hora_retiro=hora_retiro,
            direccion_entrega=(direccion_entrega if entrega_tipo == "despacho" else ""),
            despacho_monto=float(despacho_monto or 0),
        )
        return jsonify(respuesta)
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except RuntimeError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/tienda/flow/confirm', methods=['POST'])
def api_tienda_flow_confirm():
    # Callback servidor-a-servidor de Flow
    try:
        token = str(request.form.get("token") or request.args.get("token") or "").strip()
        result = _flow_confirmar_token_y_actualizar(token)
        if not result.get("success"):
            return "ERROR", 400
        return "OK", 200
    except Exception:
        return "ERROR", 500


@app.route('/tienda/flow/retorno', methods=['GET', 'POST'])
def tienda_flow_retorno():
    # Retorno del navegador del cliente luego del checkout Flow
    def _redirigir_con_cookie(base_url, estado, venta_id=0):
        vid = int(venta_id or 0)
        if vid > 0:
            _marcar_flow_cliente_regreso(vid)
        if estado == "paid":
            destino = f"{base_url}/tienda?flow=paid&venta_id={vid}"
        elif estado == "pending":
            destino = f"{base_url}/tienda?flow=pending&venta_id={vid}"
        else:
            destino = f"{base_url}/tienda?flow=error&venta_id={vid}"
        resp = redirect(destino)
        # Fallback movil: si se pierde query/localStorage, tienda.html lee esta cookie.
        resp.set_cookie(
            "flow_return_status",
            f"{estado}:{vid}:{int(time.time())}",
            max_age=60 * 20,
            secure=True,
            httponly=False,
            samesite="Lax",
            path="/",
        )
        return resp

    try:
        token = str(request.values.get("token") or "").strip()
        venta_hint = int(request.values.get("venta_id") or 0)
        if not token:
            base = _public_base_url(request.url_root)
            return _redirigir_con_cookie(base, "pending", venta_hint)
        result = _flow_confirmar_token_y_actualizar(token)
        base = _public_base_url(request.url_root)
        if not result.get("success"):
            return _redirigir_con_cookie(base, "pending", venta_hint)
        if bool(result.get("paid")):
            return _redirigir_con_cookie(base, "paid", int(result.get("venta_id") or 0))
        return _redirigir_con_cookie(base, "pending", int(result.get("venta_id") or 0))
    except Exception:
        base = _public_base_url(request.url_root)
        venta_hint = int(request.values.get("venta_id") or 0)
        return _redirigir_con_cookie(base, "pending", venta_hint)


@app.route('/api/tienda/flow/estado', methods=['GET'])
def api_tienda_flow_estado():
    try:
        venta_id = int(request.args.get("venta_id") or 0)
        if venta_id <= 0:
            return jsonify({"success": False, "error": "venta_id invalido"}), 400
        _flow_reconciliar_pendientes(limit=20, horas=72)
        conn = get_db()
        cur = conn.cursor()
        _ensure_flow_pago_table(cur)
        cur.execute("SELECT * FROM tienda_flow_pagos WHERE venta_id = ? LIMIT 1", (venta_id,))
        row = cur.fetchone()
        row_data = dict(row) if row else None
        # Reconciliacion activa: si sigue pendiente, consultamos a Flow en vivo.
        if row_data and str(row_data.get("estado") or "").strip().lower() == "pendiente":
            token = str(row_data.get("flow_token") or "").strip()
            if token and _flow_cfg().get("enabled"):
                try:
                    _flow_confirmar_token_y_actualizar(token)
                    cur.execute("SELECT * FROM tienda_flow_pagos WHERE venta_id = ? LIMIT 1", (venta_id,))
                    row = cur.fetchone()
                    row_data = dict(row) if row else row_data
                except Exception:
                    pass
        conn.close()
        if not row_data:
            return jsonify({"success": True, "found": False, "estado": "no_registrado", "checkout_backup": None})
        data = row_data
        backup = None
        try:
            raw_backup = data.get("checkout_backup_json")
            if raw_backup:
                backup = json.loads(raw_backup)
        except Exception:
            backup = None
        return jsonify({"success": True, "found": True, "estado": str(data.get("estado") or "pendiente"), "data": data, "checkout_backup": backup})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/historial-ventas')
def historial_ventas():
    try:
        fecha_desde = request.args.get('desde', '')
        fecha_hasta = request.args.get('hasta', '')
        
        ventas = obtener_historial_ventas(
            fecha_desde if fecha_desde else None,
            fecha_hasta if fecha_hasta else None
        )
        
        # Formatear fechas para mostrar
        from database import formatear_fecha_chile
        for venta in ventas:
            venta['fecha_formateada'] = formatear_fecha_chile(venta['fecha_hora'])
        
        return render_template('historial_ventas.html', 
                             ventas=ventas,
                             fecha_desde=fecha_desde,
                             fecha_hasta=fecha_hasta)
    except Exception as e:
        print(f"Error en historial: {e}")
        import traceback
        traceback.print_exc()
        return f"Error: {str(e)}", 500

@app.route('/api/venta/<int:venta_id>')
def detalle_venta(venta_id):
    try:
        venta, items = obtener_detalle_venta(venta_id)
        if not venta:
            return jsonify({'success': False, 'error': 'Venta no encontrada'}), 404

        venta_payload = dict(venta)
        if not str(venta_payload.get("codigo_operacion") or "").strip():
            venta_payload["codigo_operacion"] = obtener_codigo_operacion_venta(venta_id)
        codigo_op = str(venta_payload.get("codigo_operacion") or "").strip()
        chat_mensajes = []
        try:
            conn_chat = get_db()
            cur_chat = conn_chat.cursor()
            cur_chat.execute(
                """
                SELECT id, remitente_tipo, mensaje, creado_en
                FROM tienda_pedido_chat
                WHERE origen_tipo = 'venta' AND origen_id = ?
                ORDER BY id ASC
                LIMIT 500
                """,
                (int(venta_id),),
            )
            chat_mensajes = [dict(r) for r in cur_chat.fetchall()]
        except Exception:
            chat_mensajes = []
        finally:
            try:
                conn_chat.close()
            except Exception:
                pass
        return jsonify({
            'success': True,
            'venta': venta_payload,
            'items': [dict(item) for item in items],
            'chat_mensajes': chat_mensajes,
            'timeline_url': f"/api/operaciones/{codigo_op}/timeline" if codigo_op else None,
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/venta/<int:venta_id>/eliminar', methods=['POST'])
def anular_venta(venta_id):
    try:
        eliminar_venta(venta_id)
        crear_backup()
        return jsonify({'success': True, 'mensaje': 'Venta anulada correctamente'})
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 404
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/finanzas')
def finanzas():
    try:
        fecha_hasta = request.args.get('hasta') or datetime.now().strftime('%Y-%m-%d')
        fecha_desde = request.args.get('desde') or (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        _parse_fecha_iso_ymd(fecha_desde, "fecha desde")
        _parse_fecha_iso_ymd(fecha_hasta, "fecha hasta")
        data = _construir_reporte_version_prueba_canales(
            fecha_desde_raw=fecha_desde,
            fecha_hasta_raw=fecha_hasta,
        )
        return render_template(
            'finanzas.html',
            fecha_desde=fecha_desde,
            fecha_hasta=fecha_hasta,
            resumen_finanzas=data.get("resumen") or {},
            semanas_finanzas=data.get("semanas") or [],
        )
    except ValueError as e:
        return _error_or_text(e, 400)
    except Exception as e:
        return _error_or_text(e, 500)


@app.route('/reportes')
def reportes():
    try:
        fecha_hasta = request.args.get('hasta') or datetime.now().strftime('%Y-%m-%d')
        fecha_desde = request.args.get('desde') or (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        dias_hist = _as_int(request.args.get('dias_hist', 30) or 30, "días históricos", min_value=7)
        dias_cov = _as_int(request.args.get('dias_cov', 14) or 14, "días cobertura", min_value=1)
        kardex_tipo = (request.args.get('kardex_tipo') or '').strip().lower()
        kardex_tipo = kardex_tipo if kardex_tipo in {'insumo', 'producto'} else None
        kardex_limit = _as_int(request.args.get('kardex_limit', 250) or 250, "límite de kardex", min_value=1)

        produccion = obtener_reporte_produccion(fecha_desde, fecha_hasta)
        insumos_agregados = obtener_reporte_insumos_agregados(fecha_desde, fecha_hasta)
        productos_agregados = obtener_reporte_productos_agregados(fecha_desde, fecha_hasta)
        mermas_productos = obtener_reporte_mermas_productos(fecha_desde, fecha_hasta)
        resumen_mermas = obtener_resumen_mermas_por_fecha(fecha_desde, fecha_hasta)
        kardex = obtener_kardex_movimientos(
            fecha_desde=fecha_desde,
            fecha_hasta=fecha_hasta,
            tipo_recurso=kardex_tipo,
            limit=kardex_limit,
        )
        sugerencias_compra = obtener_sugerencias_compra_insumos(
            dias_historico=dias_hist,
            dias_cobertura=dias_cov,
            limite=30,
        )
        margen = obtener_resumen_margen_ventas(fecha_desde, fecha_hasta)
        ventas_vs_compras = obtener_resumen_ventas_vs_compras(fecha_desde, fecha_hasta)

        total_producido = sum(float(r.get('cantidad_resultado') or r.get('cantidad') or 0) for r in produccion)
        insumos_unicos = {
            str(r.get('recurso_id') or '').strip()
            for r in insumos_agregados
            if str(r.get('recurso_id') or '').strip()
        }
        if not insumos_unicos:
            insumos_unicos = {
                str(r.get('nombre_recurso') or '').strip().lower()
                for r in insumos_agregados
                if str(r.get('nombre_recurso') or '').strip()
            }
        total_insumos = len(insumos_unicos)
        productos_unicos = {
            str(r.get('recurso_id') or '').strip()
            for r in productos_agregados
            if str(r.get('recurso_id') or '').strip()
        }
        if not productos_unicos:
            productos_unicos = {
                str(r.get('nombre_recurso') or '').strip().lower()
                for r in productos_agregados
                if str(r.get('nombre_recurso') or '').strip()
            }
        total_productos = len(productos_unicos)
        total_merma_bruta = sum(float(r.get('cantidad') or 0) for r in mermas_productos)
        total_merma_revertida = sum(float(r.get('cantidad') or 0) for r in mermas_productos if (r.get('estado') or 'activa') == 'revertida')
        total_merma_neta = max(0.0, total_merma_bruta - total_merma_revertida)
        sugerencias_urgentes = sum(1 for s in sugerencias_compra if s.get('prioridad') == 'alta')

        return render_template('reportes.html',
                             fecha_desde=fecha_desde,
                             fecha_hasta=fecha_hasta,
                             dias_hist=dias_hist,
                             dias_cov=dias_cov,
                             kardex_tipo=kardex_tipo or '',
                             kardex_limit=kardex_limit,
                             produccion=produccion,
                             insumos_agregados=insumos_agregados,
                             productos_agregados=productos_agregados,
                             mermas_productos=mermas_productos,
                             resumen_mermas=resumen_mermas,
                             kardex=kardex,
                             sugerencias_compra=sugerencias_compra,
                             total_producido=total_producido,
                             total_insumos=total_insumos,
                             total_productos=total_productos,
                             total_movimientos=len(kardex),
                             sugerencias_urgentes=sugerencias_urgentes,
                             margen=margen,
                             ventas_vs_compras=ventas_vs_compras,
                             total_merma_neta=total_merma_neta,
                             total_merma_bruta=total_merma_bruta,
                             total_merma_revertida=total_merma_revertida)
    except ValueError as e:
        return _error_or_text(e, 400)
    except Exception as e:
        print(f"Error en reportes: {e}")
        return f"Error: {str(e)}", 500


def _construir_panel_correlacion_operativa(horas=72):
    horizonte_horas = max(24, min(int(horas or 72), 240))
    fecha_desde = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    fecha_hasta = datetime.now().strftime('%Y-%m-%d')

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT id, fecha, hora_inicio, titulo, cliente, estado, tipo
            FROM agenda_eventos
            WHERE estado = 'pendiente'
              AND tipo <> 'bloqueo'
              AND datetime(fecha || ' ' || COALESCE(hora_inicio, '23:59') || ':00') >= datetime('now')
              AND datetime(fecha || ' ' || COALESCE(hora_inicio, '23:59') || ':00') <= datetime('now', ?)
            ORDER BY date(fecha) ASC, COALESCE(hora_inicio, '23:59') ASC, id ASC
            LIMIT 150
            """,
            (f"+{horizonte_horas} hours",),
        )
        agenda_proxima = [dict(r) for r in cursor.fetchall()]

        cursor.execute(
            """
            SELECT
                producto_nombre,
                insumo_nombre,
                COALESCE(insumo_lote_codigo, 'Sin lote') AS insumo_lote_codigo,
                insumo_fecha_vencimiento,
                producido_en
            FROM haccp_trazabilidad_insumos
            WHERE date(COALESCE(insumo_fecha_vencimiento, '9999-12-31')) <= date('now', '+3 day')
            ORDER BY date(COALESCE(insumo_fecha_vencimiento, '9999-12-31')) ASC, id DESC
            LIMIT 120
            """
        )
        trazas_haccp_riesgo = [dict(r) for r in cursor.fetchall()]
    finally:
        conn.close()

    productos = _obtener_productos_para_venta()
    productos_riesgo = [
        {
            "id": int(p.get("id") or 0),
            "nombre": p.get("nombre") or "Producto",
            "sin_porcion_disponible": bool(p.get("sin_porcion_disponible")),
            "bajo_minimo": bool(p.get("bajo_minimo")),
            "dependencias_criticas": bool(p.get("dependencias_criticas")),
            "dependencias_alerta_texto": p.get("dependencias_alerta_texto") or "",
            "desactivacion_manual_requiere_confirmacion": bool(p.get("desactivacion_manual_requiere_confirmacion")),
        }
        for p in productos
        if bool(p.get("sin_porcion_disponible"))
        or bool(p.get("bajo_minimo"))
        or bool(p.get("dependencias_criticas"))
    ]

    mapa_producto = {}
    for p in productos:
        nombre = str(p.get("nombre") or "").strip().lower()
        if nombre:
            mapa_producto[nombre] = p

    cruces_agenda_stock = []
    for evento in agenda_proxima:
        titulo = str(evento.get("titulo") or "").strip().lower()
        if not titulo:
            continue
        match = None
        for nombre, producto in mapa_producto.items():
            if nombre and nombre in titulo:
                match = producto
                break
        if not match:
            continue
        if bool(match.get("sin_porcion_disponible")) or bool(match.get("bajo_minimo")) or bool(match.get("dependencias_criticas")):
            cruces_agenda_stock.append(
                {
                    "evento_id": int(evento.get("id") or 0),
                    "fecha": evento.get("fecha"),
                    "hora_inicio": evento.get("hora_inicio"),
                    "titulo": evento.get("titulo"),
                    "cliente": evento.get("cliente"),
                    "producto_id": int(match.get("id") or 0),
                    "producto_nombre": match.get("nombre"),
                    "motivo": match.get("dependencias_alerta_texto") or "Stock crítico o porción insuficiente",
                }
            )

    ventas_vs_compras = obtener_resumen_ventas_vs_compras(fecha_desde=fecha_desde, fecha_hasta=fecha_hasta)
    totales = ventas_vs_compras.get("totales") or {}
    comparativo = {
        "ventas_local": float(totales.get("ventas_local") or 0),
        "ventas_uber": float(totales.get("ventas_uber") or 0),
        "ventas_pedidosya": float(totales.get("ventas_pedidosya") or 0),
        "ventas_brutas": float(totales.get("ventas_brutas") or 0),
        "compras_facturadas": float(totales.get("compras_facturadas") or 0),
        "margen_neto_estimado": float(totales.get("saldo_estimado") or 0),
    }

    return {
        "generado_en": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "horizonte_horas": horizonte_horas,
        "agenda_proxima_total": len(agenda_proxima),
        "cruces_agenda_stock": cruces_agenda_stock[:60],
        "productos_riesgo_dependencias": productos_riesgo[:80],
        "haccp_lotes_comprometidos": trazas_haccp_riesgo[:120],
        "comparativo_operativo": comparativo,
        "acciones_rapidas": [
            {"label": "Ir a ventas y desactivar", "href": "/ventas"},
            {"label": "Crear compra pendiente", "href": "/insumos"},
            {"label": "Reprogramar agenda", "href": "/agenda"},
        ],
    }


@app.route('/correlacion-operativa')
def correlacion_operativa():
    try:
        horas = _as_int(request.args.get('horas', 72) or 72, "horas", min_value=24)
        panel = _construir_panel_correlacion_operativa(horas=horas)
        return render_template('correlacion_operativa.html', panel=panel)
    except ValueError as e:
        return _error_or_text(e, 400)
    except Exception as e:
        return _error_or_text(e, 500)


@app.route('/api/correlacion-operativa')
def api_correlacion_operativa():
    try:
        horas = _as_int(request.args.get('horas', 72) or 72, "horas", min_value=24)
        panel = _construir_panel_correlacion_operativa(horas=horas)
        return jsonify({'success': True, 'data': panel})
    except ValueError as e:
        return jsonify({'success': False, 'data': {}, 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'data': {}, 'error': str(e)}), 500

@app.route('/api/reportes/ventas-semanal')
def reporte_ventas_semanal():
    """API para gráfico de ventas semanal"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # Verificar ventas totales
        cursor.execute("SELECT COUNT(*) as total FROM ventas")
        total = cursor.fetchone()['total']
        print(f"DEBUG - Total ventas en BD: {total}")
        
        if total == 0:
            conn.close()
            return jsonify({
                'labels': ['10/02', '11/02', '12/02', '13/02', '14/02', '15/02', '16/02'],
                'values': [0, 0, 0, 0, 0, 0, 0]
            })
        
        # Obtener TODAS las ventas recientes (incluyendo posibles fechas futuras por error de zona horaria)
        cursor.execute("""
            SELECT 
                substr(fecha_hora, 1, 10) as fecha,
                SUM(COALESCE(total_monto, 0)) as total
            FROM ventas
            WHERE fecha_hora >= datetime('now', '-10 days')
            GROUP BY substr(fecha_hora, 1, 10)
            ORDER BY fecha ASC
        """)
        
        rows = cursor.fetchall()
        conn.close()
        
        print(f"DEBUG - Filas encontradas: {len(rows)}")
        for row in rows:
            print(f"  Fecha: {row['fecha']}, Monto: {row['total']}")
        
        # Construir array de 7 días centrado en las fechas con datos
        from datetime import datetime, timedelta
        
        # Si hay datos, usar el rango que incluya las fechas con ventas
        if rows:
            fechas_con_datos = [row['fecha'] for row in rows]
            fecha_mas_reciente = max(fechas_con_datos)
            fecha_base = datetime.strptime(fecha_mas_reciente, '%Y-%m-%d')
        else:
            fecha_base = datetime.now()
        
        # Crear rango de 6 días antes hasta la fecha base
        fechas = []
        for i in range(6, -1, -1):
            fecha = fecha_base - timedelta(days=i)
            fechas.append(fecha.strftime('%Y-%m-%d'))
        
        # Mapear datos
        ventas_por_fecha = {}
        for row in rows:
            ventas_por_fecha[row['fecha']] = float(row['total'] or 0)
        
        # Construir respuesta
        labels = []
        values = []
        for fecha_str in fechas:
            fecha_obj = datetime.strptime(fecha_str, '%Y-%m-%d')
            labels.append(fecha_obj.strftime('%d/%m'))
            values.append(ventas_por_fecha.get(fecha_str, 0))
        
        print(f"DEBUG - Respuesta final: labels={labels}, values={values}")
        
        return jsonify({'labels': labels, 'values': values})
        
    except Exception as e:
        print(f"ERROR en reporte semanal: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'labels': [], 'values': [], 'error': str(e)})

@app.route('/api/reportes/top-productos')
def reporte_top_productos():
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT producto_nombre, SUM(cantidad) as total
            FROM venta_items
            GROUP BY producto_id
            ORDER BY total DESC
            LIMIT 5
        """)
        
        datos = cursor.fetchall()
        conn.close()
        
        return jsonify({
            'labels': [d['producto_nombre'] for d in datos],
            'values': [d['total'] for d in datos]
        })
    except Exception as e:
        return jsonify({'labels': [], 'values': []})


@app.route('/api/reportes/produccion')
def api_reporte_produccion():
    try:
        fecha_desde = request.args.get('desde')
        fecha_hasta = request.args.get('hasta')
        data = obtener_reporte_produccion(fecha_desde, fecha_hasta)
        return jsonify({'success': True, 'data': data})
    except Exception as e:
        return jsonify({'success': False, 'data': [], 'error': str(e)}), 500


@app.route('/api/reportes/insumos-agregados')
def api_reporte_insumos_agregados():
    try:
        fecha_desde = request.args.get('desde')
        fecha_hasta = request.args.get('hasta')
        data = obtener_reporte_insumos_agregados(fecha_desde, fecha_hasta)
        return jsonify({'success': True, 'data': data})
    except Exception as e:
        return jsonify({'success': False, 'data': [], 'error': str(e)}), 500


@app.route('/api/reportes/productos-agregados')
def api_reporte_productos_agregados():
    try:
        fecha_desde = request.args.get('desde')
        fecha_hasta = request.args.get('hasta')
        data = obtener_reporte_productos_agregados(fecha_desde, fecha_hasta)
        return jsonify({'success': True, 'data': data})
    except Exception as e:
        return jsonify({'success': False, 'data': [], 'error': str(e)}), 500


@app.route('/api/reportes/mermas-productos')
def api_reporte_mermas_productos():
    try:
        fecha_desde = request.args.get('desde')
        fecha_hasta = request.args.get('hasta')
        data = obtener_reporte_mermas_productos(fecha_desde, fecha_hasta)
        resumen = obtener_resumen_mermas_por_fecha(fecha_desde, fecha_hasta)
        return jsonify({'success': True, 'data': data, 'resumen': resumen})
    except Exception as e:
        return jsonify({'success': False, 'data': [], 'resumen': [], 'error': str(e)}), 500


@app.route('/api/reportes/kardex')
def api_reporte_kardex():
    try:
        fecha_desde = request.args.get('desde')
        fecha_hasta = request.args.get('hasta')
        tipo_recurso = (request.args.get('tipo') or '').strip().lower()
        if tipo_recurso not in {'insumo', 'producto'}:
            tipo_recurso = None
        recurso_id = request.args.get('recurso_id') or None
        limite = _as_int(request.args.get('limite', 300) or 300, "límite kardex", min_value=1)

        data = obtener_kardex_movimientos(
            fecha_desde=fecha_desde,
            fecha_hasta=fecha_hasta,
            tipo_recurso=tipo_recurso,
            recurso_id=recurso_id,
            limit=limite,
        )
        return jsonify({'success': True, 'data': data})
    except ValueError as e:
        return jsonify({'success': False, 'data': [], 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'data': [], 'error': str(e)}), 500


@app.route('/api/reportes/sugerencias-compra')
def api_reporte_sugerencias_compra():
    try:
        dias_hist = _as_int(request.args.get('dias_hist', 30) or 30, "días históricos", min_value=7)
        dias_cov = _as_int(request.args.get('dias_cov', 14) or 14, "días cobertura", min_value=1)
        limite = _as_int(request.args.get('limite', 30) or 30, "límite", min_value=1)
        data = obtener_sugerencias_compra_insumos(
            dias_historico=dias_hist,
            dias_cobertura=dias_cov,
            limite=limite,
        )
        return jsonify({'success': True, 'data': data})
    except ValueError as e:
        return jsonify({'success': False, 'data': [], 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'data': [], 'error': str(e)}), 500


@app.route('/api/reportes/margen')
def api_reporte_margen():
    try:
        fecha_desde = request.args.get('desde')
        fecha_hasta = request.args.get('hasta')
        data = obtener_resumen_margen_ventas(fecha_desde, fecha_hasta)
        return jsonify({'success': True, 'data': data})
    except Exception as e:
        return jsonify({'success': False, 'data': {}, 'error': str(e)}), 500


@app.route('/api/reportes/ventas-vs-compras')
def api_reporte_ventas_vs_compras():
    try:
        fecha_desde = request.args.get('desde')
        fecha_hasta = request.args.get('hasta')
        data = obtener_resumen_ventas_vs_compras(fecha_desde, fecha_hasta)
        return jsonify({'success': True, 'data': data})
    except Exception as e:
        return jsonify({'success': False, 'data': {}, 'error': str(e)}), 500


@app.route('/api/reportes/automatizaciones')
def api_reportes_automatizaciones():
    conn = None
    try:
        dias_hist = _as_int(request.args.get('dias_hist', 30) or 30, "dias historicos", min_value=7)
        dias_cov = _as_int(request.args.get('dias_cov', 14) or 14, "dias cobertura", min_value=1)

        conn = get_db()
        cursor = conn.cursor()
        alertas_productos = _obtener_alertas_productos(cursor)
        cursor.execute(
            """
            SELECT id,
                   nombre,
                   CAST(stock AS REAL) AS stock,
                   CAST(stock_minimo AS REAL) AS stock_minimo
            FROM insumos
            WHERE CAST(stock_minimo AS REAL) > 0
              AND CAST(stock AS REAL) <= CAST(stock_minimo AS REAL)
            """
        )
        insumos_bajos = [dict(r) for r in cursor.fetchall()]
        criticidad = _resumen_criticidad_alertas(
            alertas_productos["productos_stock_bajo"],
            alertas_productos["productos_baja_porcion"],
            insumos_bajos,
        )
        cursor.execute(
            """
            SELECT COUNT(*) AS total
            FROM agenda_eventos
            WHERE estado = 'pendiente'
              AND tipo <> 'bloqueo'
              AND date(fecha) = date('now', '+1 day')
            """
        )
        eventos_manana = int(cursor.fetchone()["total"] or 0)
        haccp_vencidos = contar_haccp_vencidos(conn=conn)
        conn.close()
        conn = None

        sugerencias_compra = obtener_sugerencias_compra_insumos(
            dias_historico=dias_hist,
            dias_cobertura=dias_cov,
            limite=50,
        )
        compras_urgentes = sum(1 for s in sugerencias_compra if (s.get("prioridad") or "") == "alta")
        recordatorios_pendientes = len(obtener_recordatorios_agenda_pendientes())
        ultimo_backup = obtener_ultimo_backup()

        checks = [
            {
                "id": "alertas_criticas",
                "label": "Alertas criticas",
                "status": "critical" if int(criticidad.get("critica", 0)) > 0 else "ok",
                "value": int(criticidad.get("critica", 0)),
                "detail": "Productos o insumos en estado critico de stock",
            },
            {
                "id": "productos_sin_porcion",
                "label": "Productos sin porcion",
                "status": "warning" if len(alertas_productos["ids_baja_porcion"]) > 0 else "ok",
                "value": len(alertas_productos["ids_baja_porcion"]),
                "detail": "No alcanzan a completar una porcion de venta",
            },
            {
                "id": "compras_urgentes",
                "label": "Compras urgentes",
                "status": "warning" if compras_urgentes > 0 else "ok",
                "value": compras_urgentes,
                "detail": "Insumos sugeridos con prioridad alta",
            },
            {
                "id": "haccp_vencidos",
                "label": "Controles HACCP vencidos",
                "status": "critical" if haccp_vencidos > 0 else "ok",
                "value": haccp_vencidos,
                "detail": "Puntos criticos sin control dentro de la frecuencia",
            },
            {
                "id": "agenda_manana",
                "label": "Agenda de manana",
                "status": "warning" if eventos_manana > 0 else "ok",
                "value": eventos_manana,
                "detail": "Eventos pendientes para el proximo dia",
            },
            {
                "id": "recordatorios_pendientes",
                "label": "Recordatorios pendientes",
                "status": "warning" if recordatorios_pendientes > 0 else "ok",
                "value": recordatorios_pendientes,
                "detail": "Recordatorios activos sin descartar",
            },
            {
                "id": "backup",
                "label": "Backup operativo",
                "status": "ok" if ultimo_backup else "critical",
                "value": ultimo_backup or "Nunca",
                "detail": "Fecha del ultimo respaldo automatico",
            },
        ]

        return jsonify(
            {
                "success": True,
                "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "checks": checks,
                "resumen": {
                    "total_checks": len(checks),
                    "critical": sum(1 for c in checks if c.get("status") == "critical"),
                    "warning": sum(1 for c in checks if c.get("status") == "warning"),
                    "ok": sum(1 for c in checks if c.get("status") == "ok"),
                },
                "actions": [
                    {"id": "go_alertas", "label": "Ir a alertas", "href": "/alertas"},
                    {"id": "go_haccp", "label": "Ir a HACCP", "href": "/haccp"},
                    {"id": "go_agenda", "label": "Ir a agenda", "href": "/agenda"},
                    {"id": "go_correlacion", "label": "Panel correlación", "href": "/correlacion-operativa"},
                    {"id": "go_reportes", "label": "Ver reportes", "href": "/reportes"},
                ],
                "backup_endpoint": "/api/backup/crear",
                "backup_open_endpoint": "/api/backup/abrir-carpeta",
            }
        )
    except ValueError as e:
        return jsonify({'success': False, 'checks': [], 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'checks': [], 'error': str(e)}), 500
    finally:
        if conn:
            conn.close()


TUYA_HA_CLIENT_ID = "HA_3y9q4ak7g4ephrvke"
TUYA_HA_SCHEMA = "haauthorize"
TUYA_QR_LOGIN_PREFIX = "tuyaSmart--qrLogin?token="
_TUYA_PENDING_QR_LOGINS = {}
_TUYA_TEMP_CODES = (
    "va_temperature",
    "temp_current",
    "sensor_temperature",
    "temp_indoor",
    "temp",
)
_TUYA_HUM_CODES = (
    "va_humidity",
    "humidity_value",
    "sensor_humidity",
    "humidity_indoor",
    "humidity_current",
    "humidity",
)
_TUYA_AUTO_SLEEP_SEGUNDOS = 60
_TUYA_AUTO_THREAD = None
_TUYA_AUTO_LOCK = threading.Lock()
_TUYA_AUTO_LAST_SYNC = {}


class _TuyaTokenListener:
    def update_token(self, token_info):
        try:
            if isinstance(token_info, dict):
                guardar_config_tuya_haccp({"token_info": token_info})
        except Exception:
            # El refresco de token no debe romper la lectura si falla persistencia.
            pass


_TUYA_TOKEN_LISTENER = _TuyaTokenListener()


def _tuya_sdk_disponible():
    return LoginControl is not None and Manager is not None


def _tuya_error_es_auth(error):
    raw = str(error or "").strip()
    if not raw:
        return False
    low = raw.lower()
    markers = (
        "-999999",
        "access token",
        "refresh token",
        "token invalid",
        "invalid token",
        "token expired",
    )
    return any(m in low for m in markers)


def _tuya_error_para_ui(error):
    raw = str(error or "").strip()
    if _tuya_error_es_auth(raw):
        return (
            "Tuya devolvió error de autenticación (-999999). "
            "Re-vincula la cuenta desde HACCP > Configurar sensor Tuya > Generar QR > Verificar escaneo."
        ), 401
    return raw or "Error Tuya no identificado.", 500


def _tuya_config_forzar_refresh(config):
    cfg = dict(config or {})
    token_info = dict(cfg.get("token_info") or {})
    if token_info:
        token_info["t"] = 0
        token_info["expire_time"] = 0
        cfg["token_info"] = token_info
    return cfg


def _tuya_mensaje_dependencia():
    if _tuya_sdk_disponible():
        return ""
    base = (
        "Este ejecutable no incluye el SDK de Tuya. Debes actualizar/recompilar la app."
        if getattr(sys, "frozen", False)
        else "Dependencia faltante: instala tuya-device-sharing-sdk, PyQRCode y cryptography."
    )
    if TUYA_IMPORT_ERROR:
        return f"{base} Detalle: {TUYA_IMPORT_ERROR}"
    return base


def _tuya_limpiar_qr_pendientes(ttl_segundos=900):
    ahora = time.time()
    expirados = []
    for token, data in _TUYA_PENDING_QR_LOGINS.items():
        creado = float(data.get("created_at") or 0)
        if (ahora - creado) > max(60, int(ttl_segundos or 900)):
            expirados.append(token)
    for token in expirados:
        _TUYA_PENDING_QR_LOGINS.pop(token, None)


def _tuya_to_float(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        raw = str(value or "").strip()
        if not raw:
            return None
        raw = raw.replace(",", ".")
        return float(raw)
    except Exception:
        return None


def _tuya_escalar_status(device, code):
    status_map = getattr(device, "status", {}) or {}
    if code not in status_map:
        return None
    raw = status_map.get(code)

    status_range = getattr(device, "status_range", {}) or {}
    spec = status_range.get(code)
    spec_type = str(getattr(spec, "type", "") or "").strip().lower()
    if spec and spec_type == "integer":
        try:
            values_raw = getattr(spec, "values", "") or "{}"
            values = json.loads(values_raw)
            scale = int(values.get("scale", 0) or 0)
            numeric = _tuya_to_float(raw)
            if numeric is None:
                return raw
            if scale > 0:
                return numeric / (10 ** scale)
            return numeric
        except Exception:
            return raw

    return raw


def _tuya_unidad_status(device, code):
    try:
        status_range = getattr(device, "status_range", {}) or {}
        spec = status_range.get(code)
        if not spec:
            return ""
        values_raw = getattr(spec, "values", "") or "{}"
        values = json.loads(values_raw)
        return str(values.get("unit") or "").strip()
    except Exception:
        return ""


def _tuya_extraer_metrica(device, codigos):
    status_map = getattr(device, "status", {}) or {}
    for code in codigos:
        if code not in status_map:
            continue
        valor = _tuya_escalar_status(device, code)
        numerico = _tuya_to_float(valor)
        if numerico is None:
            continue
        return {
            "codigo": code,
            "valor": round(numerico, 3),
            "unidad": _tuya_unidad_status(device, code),
        }
    return {"codigo": "", "valor": None, "unidad": ""}


def _tuya_dispositivo_con_metricas(device):
    status_map = getattr(device, "status", {}) or {}
    keys = set(status_map.keys())
    return bool(keys.intersection(_TUYA_TEMP_CODES) or keys.intersection(_TUYA_HUM_CODES))


def _tuya_config_publica(config):
    cfg = config if isinstance(config, dict) else {}
    bindings = cfg.get("device_bindings")
    if not isinstance(bindings, list):
        bindings = []
    return {
        "habilitado": bool(cfg.get("habilitado")),
        "user_code": str(cfg.get("user_code") or ""),
        "endpoint": str(cfg.get("endpoint") or ""),
        "terminal_id": str(cfg.get("terminal_id") or ""),
        "device_id": str(cfg.get("device_id") or ""),
        "device_name": str(cfg.get("device_name") or ""),
        "auto_interval_min": int(cfg.get("auto_interval_min") or 15),
        "device_bindings": bindings,
        "token_disponible": bool(cfg.get("token_disponible")),
        "ultimo_temp": cfg.get("ultimo_temp"),
        "ultima_humedad": cfg.get("ultima_humedad"),
        "ultima_lectura_en": cfg.get("ultima_lectura_en"),
    }


def _tuya_generar_qr_svg(payload):
    try:
        import pyqrcode
    except Exception:
        return ""
    try:
        qr = pyqrcode.create(payload)
        buffer = BytesIO()
        qr.svg(file=buffer, scale=4)
        svg_text = buffer.getvalue().decode("ascii", errors="ignore")
        return (
            str(svg_text)
            .replace("\n", "")
            .replace(
                '<?xml version="1.0" encoding="UTF-8"?><svg xmlns="http://www.w3.org/2000/svg"',
                "<svg",
            )
        )
    except Exception:
        return ""


def _tuya_manager_desde_config(config):
    if not _tuya_sdk_disponible():
        raise RuntimeError(_tuya_mensaje_dependencia())

    cfg = config if isinstance(config, dict) else {}
    user_code = str(cfg.get("user_code") or "").strip()
    endpoint = str(cfg.get("endpoint") or "").strip()
    terminal_id = str(cfg.get("terminal_id") or "").strip()
    token_info = cfg.get("token_info") if isinstance(cfg.get("token_info"), dict) else {}

    if not user_code:
        raise ValueError("Falta User Code de Smart Life.")
    if not endpoint:
        raise ValueError("Falta endpoint de Smart Life. Vincula con QR.")
    if not terminal_id:
        raise ValueError("Falta terminal_id de Smart Life. Vincula con QR.")
    if not token_info.get("access_token") or not token_info.get("refresh_token"):
        raise ValueError("No hay token válido. Vincula con QR en configuración Tuya.")

    return Manager(
        TUYA_HA_CLIENT_ID,
        user_code,
        terminal_id,
        endpoint,
        token_info,
        _TUYA_TOKEN_LISTENER,
    )


def _tuya_seleccionar_dispositivo(manager, config, requested_device_id=None):
    device_map = getattr(manager, "device_map", {}) or {}
    if not device_map:
        return None, False

    requested = str(requested_device_id or "").strip()
    if requested and requested in device_map:
        return device_map.get(requested), False

    saved_id = str((config or {}).get("device_id") or "").strip()
    if saved_id and saved_id in device_map:
        return device_map.get(saved_id), False

    candidates = [d for d in device_map.values() if _tuya_dispositivo_con_metricas(d)]
    if not candidates:
        return None, False

    candidates.sort(
        key=lambda d: (
            0 if str(getattr(d, "category", "")).lower() == "wsdcg" else 1,
            0 if bool(getattr(d, "online", False)) else 1,
            str(getattr(d, "name", "")).lower(),
        )
    )
    return candidates[0], True


def _tuya_listar_dispositivos(manager):
    items = []
    for device in (getattr(manager, "device_map", {}) or {}).values():
        status_keys = sorted(list((getattr(device, "status", {}) or {}).keys()))
        items.append(
            {
                "id": str(getattr(device, "id", "") or ""),
                "name": str(getattr(device, "name", "") or ""),
                "category": str(getattr(device, "category", "") or ""),
                "online": bool(getattr(device, "online", False)),
                "status_keys": status_keys,
                "has_temp_humidity": _tuya_dispositivo_con_metricas(device),
            }
        )
    items.sort(key=lambda x: ((0 if x["has_temp_humidity"] else 1), x["name"].lower(), x["id"]))
    return items


def _tuya_binding_por_device(config, device_id):
    cfg = config if isinstance(config, dict) else {}
    bindings = cfg.get("device_bindings")
    if not isinstance(bindings, list):
        return {}
    did = str(device_id or "").strip()
    if not did:
        return {}
    for item in bindings:
        if str((item or {}).get("device_id") or "").strip() == did:
            return item if isinstance(item, dict) else {}
    return {}


def _tuya_evaluar_alerta_temperatura(binding, temperatura):
    item = binding if isinstance(binding, dict) else {}
    min_temp = _tuya_to_float(item.get("alerta_min_temp"))
    max_temp = _tuya_to_float(item.get("alerta_max_temp"))
    if min_temp is not None and max_temp is not None and min_temp > max_temp:
        min_temp, max_temp = max_temp, min_temp
    configurada = (min_temp is not None) or (max_temp is not None)
    valor = _tuya_to_float(temperatura)
    fuera_rango = False
    motivo = ""
    if configurada and valor is not None:
        if min_temp is not None and valor < min_temp:
            fuera_rango = True
            motivo = f"Temperatura bajo minimo configurado ({valor:.2f} C < {min_temp:.2f} C)."
        elif max_temp is not None and valor > max_temp:
            fuera_rango = True
            motivo = f"Temperatura sobre maximo configurado ({valor:.2f} C > {max_temp:.2f} C)."
    return {
        "configurada": configurada,
        "min_temp": min_temp,
        "max_temp": max_temp,
        "temperatura": valor,
        "fuera_rango": fuera_rango,
        "motivo": motivo,
    }


def _tuya_lectura_desde_device(device):
    temp_data = _tuya_extraer_metrica(device, _TUYA_TEMP_CODES)
    hum_data = _tuya_extraer_metrica(device, _TUYA_HUM_CODES)
    if temp_data.get("valor") is None and hum_data.get("valor") is None:
        return None
    return {
        "temperatura": temp_data,
        "humedad": hum_data,
    }


def _tuya_registrar_control_auto(punto_id, temperatura, device_name):
    pid = int(punto_id or 0)
    if pid <= 0 or temperatura is None:
        return None
    payload = {
        "punto_id": pid,
        "valor": temperatura,
        "responsable": "Tuya Auto",
        "observacion": f"Lectura automática Tuya ({device_name or 'sensor'})",
        "accion_correctiva": "",
    }
    try:
        return registrar_haccp_control(payload)
    except ValueError as exc:
        if "accion correctiva" not in str(exc).lower():
            raise
        payload["accion_correctiva"] = (
            "Desvío detectado automáticamente por Tuya. Revisar refrigerador y corregir temperatura."
        )
        return registrar_haccp_control(payload)


def _tuya_sync_bindings(force=False, device_ids=None, origen="auto"):
    if not _tuya_sdk_disponible():
        raise RuntimeError(_tuya_mensaje_dependencia())

    config = obtener_config_tuya_haccp()
    if not bool(config.get("habilitado")) and str(origen or "").lower() == "auto":
        return {"items": [], "skipped": "disabled"}

    bindings = config.get("device_bindings")
    if not isinstance(bindings, list):
        bindings = []
    if device_ids:
        ids = {str(i).strip() for i in device_ids if str(i).strip()}
        bindings = [b for b in bindings if str((b or {}).get("device_id") or "").strip() in ids]
    else:
        bindings = [b for b in bindings if bool((b or {}).get("activo", 1))]

    if not bindings:
        return {"items": [], "skipped": "no_bindings"}

    ahora = time.time()
    resumen = []
    with _TUYA_AUTO_LOCK:
        manager = _tuya_manager_desde_config(config)
        try:
            manager.update_device_cache()
        except Exception as first_error:
            if not _tuya_error_es_auth(first_error):
                raise
            manager = _tuya_manager_desde_config(_tuya_config_forzar_refresh(config))
            manager.update_device_cache()
        device_map = getattr(manager, "device_map", {}) or {}

        for binding in bindings:
            item = binding if isinstance(binding, dict) else {}
            device_id = str(item.get("device_id") or "").strip()
            if not device_id:
                continue

            intervalo_min = int(
                item.get("intervalo_min")
                or config.get("auto_interval_min")
                or 15
            )
            intervalo_min = max(1, min(720, intervalo_min))
            if not force:
                last_sync = float(_TUYA_AUTO_LAST_SYNC.get(device_id) or 0)
                if last_sync and (ahora - last_sync) < (intervalo_min * 60):
                    continue

            device = device_map.get(device_id)
            if not device:
                resumen.append(
                    {
                        "device_id": device_id,
                        "ok": False,
                        "error": "Dispositivo no encontrado en Smart Life.",
                    }
                )
                continue

            lectura = _tuya_lectura_desde_device(device)
            if not lectura:
                resumen.append(
                    {
                        "device_id": device_id,
                        "device_name": str(getattr(device, "name", "") or ""),
                        "ok": False,
                        "error": "El dispositivo no expone temperatura/humedad por API.",
                    }
                )
                _TUYA_AUTO_LAST_SYNC[device_id] = ahora
                continue

            device_name = str(getattr(device, "name", "") or "")
            temp_val = (lectura.get("temperatura") or {}).get("valor")
            hum_val = (lectura.get("humedad") or {}).get("valor")
            punto_id = item.get("punto_id")
            alerta_temp = _tuya_evaluar_alerta_temperatura(item, temp_val)

            registrar_lectura_tuya_haccp(
                device_id=device_id,
                device_name=device_name,
                temperatura=temp_val,
                humedad=hum_val,
                punto_id=punto_id,
                origen=origen or "auto",
            )

            registro_haccp = None
            try:
                registro_haccp = _tuya_registrar_control_auto(punto_id, temp_val, device_name)
            except Exception as exc:
                registro_haccp = {"success": False, "error": str(exc)}

            resumen.append(
                {
                    "device_id": device_id,
                    "device_name": device_name,
                    "ok": True,
                    "temperatura": temp_val,
                    "humedad": hum_val,
                    "punto_id": int(punto_id or 0) if punto_id else None,
                    "alerta_temp": alerta_temp,
                    "registro_haccp": registro_haccp,
                }
            )
            _TUYA_AUTO_LAST_SYNC[device_id] = ahora

    return {"items": resumen}


def _tuya_sidebar_slot_por_texto(valor):
    txt = _normalizar_texto_busqueda(valor)
    if not txt:
        return ""
    if any(k in txt for k in ("vitrina", "exhibidor", "mostrador", "display")):
        return "vitrina"
    if any(
        k in txt
        for k in ("refrigerador", "frigorifico", "frigorifica", "camara de frio", "cadena de frio", "refrigeracion")
    ):
        return "refrigerador"
    return ""


def _tuya_ultimas_lecturas_por_device(device_ids):
    ids = []
    for raw in device_ids or []:
        did = str(raw or "").strip()
        if did and did not in ids:
            ids.append(did)
    if not ids:
        return {}

    conn = get_db()
    cursor = conn.cursor()
    out = {}
    try:
        for did in ids:
            cursor.execute(
                """
                SELECT device_id, device_name, punto_id, temperatura, humedad, origen, leida_en
                FROM haccp_tuya_lecturas
                WHERE device_id = ?
                ORDER BY datetime(leida_en) DESC, id DESC
                LIMIT 1
                """,
                (did,),
            )
            row = cursor.fetchone()
            if row:
                out[did] = dict(row)
    finally:
        conn.close()
    return out


def _tuya_sidebar_item_vacio(slot, titulo):
    return {
        "slot": slot,
        "titulo": titulo,
        "device_id": "",
        "device_name": "",
        "punto_id": None,
        "punto_nombre": "",
        "temperatura": None,
        "humedad": None,
        "leida_en": "",
        "alerta_temp": {"configurada": False, "fuera_rango": False, "motivo": ""},
    }


def _tuya_sidebar_serializar_item(binding, punto_map, lectura_map):
    item = binding if isinstance(binding, dict) else {}
    device_id = str(item.get("device_id") or "").strip()
    punto_id = item.get("punto_id")
    try:
        punto_id = int(punto_id) if punto_id not in (None, "", 0, "0") else None
        if punto_id is not None and punto_id <= 0:
            punto_id = None
    except Exception:
        punto_id = None
    punto = punto_map.get(punto_id) if punto_id else None
    punto_nombre = str((punto or {}).get("nombre") or "").strip()
    device_name_cfg = str(item.get("device_name") or "").strip()
    lectura = lectura_map.get(device_id) if device_id else None
    lectura = lectura if isinstance(lectura, dict) else {}
    device_name = str(lectura.get("device_name") or device_name_cfg).strip()
    temp = _tuya_to_float(lectura.get("temperatura"))
    hum = _tuya_to_float(lectura.get("humedad"))
    slot = _tuya_sidebar_slot_por_texto(punto_nombre) or _tuya_sidebar_slot_por_texto(device_name)
    alerta_temp = _tuya_evaluar_alerta_temperatura(item, temp)
    return {
        "slot": slot,
        "titulo": punto_nombre or device_name or device_id or "Sensor Tuya",
        "device_id": device_id,
        "device_name": device_name,
        "punto_id": punto_id,
        "punto_nombre": punto_nombre,
        "temperatura": temp,
        "humedad": hum,
        "leida_en": str(lectura.get("leida_en") or ""),
        "alerta_temp": alerta_temp,
    }


def _tuya_sidebar_elegir(items):
    pendientes = [i for i in items if isinstance(i, dict)]
    pendientes.sort(key=lambda x: str(x.get("leida_en") or ""), reverse=True)

    usados = set()

    def pick(slot_name):
        for idx, item in enumerate(pendientes):
            if idx in usados:
                continue
            if str(item.get("slot") or "") == slot_name:
                usados.add(idx)
                return item
        for idx, item in enumerate(pendientes):
            if idx in usados:
                continue
            usados.add(idx)
            return item
        return None

    refri = pick("refrigerador")
    vitrina = pick("vitrina")
    if not refri:
        refri = _tuya_sidebar_item_vacio("refrigerador", "Refrigerador")
    if not vitrina:
        vitrina = _tuya_sidebar_item_vacio("vitrina", "Vitrina")
    return refri, vitrina


def _tuya_background_worker():
    while True:
        try:
            _tuya_sync_bindings(force=False, origen="auto")
        except Exception:
            pass
        time.sleep(_TUYA_AUTO_SLEEP_SEGUNDOS)


def _tuya_ensure_background_worker():
    global _TUYA_AUTO_THREAD
    if _TUYA_AUTO_THREAD is not None and _TUYA_AUTO_THREAD.is_alive():
        return
    _TUYA_AUTO_THREAD = threading.Thread(
        target=_tuya_background_worker,
        name="tuya-haccp-auto-sync",
        daemon=True,
    )
    _TUYA_AUTO_THREAD.start()


@app.route('/api/haccp/tuya/config', methods=['GET'])
def api_haccp_tuya_config():
    try:
        _tuya_ensure_background_worker()
        config = obtener_config_tuya_haccp()
        return jsonify({
            'success': True,
            'sdk_disponible': _tuya_sdk_disponible(),
            'sdk_error': _tuya_mensaje_dependencia(),
            'app_frozen': bool(getattr(sys, "frozen", False)),
            'config': _tuya_config_publica(config),
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e), 'config': {}}), 500


@app.route('/api/haccp/tuya/config', methods=['POST'])
def api_haccp_tuya_guardar_config():
    try:
        data = request.get_json(silent=True) or {}
        payload = {}
        for key in ("habilitado", "user_code", "endpoint", "terminal_id", "device_id", "device_name", "auto_interval_min", "device_bindings"):
            if key in data:
                payload[key] = data.get(key)
        config = guardar_config_tuya_haccp(payload)
        crear_backup()
        return jsonify({'success': True, 'config': _tuya_config_publica(config)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/haccp/tuya/login/start', methods=['POST'])
def api_haccp_tuya_login_start():
    if not _tuya_sdk_disponible():
        return jsonify({'success': False, 'error': _tuya_mensaje_dependencia()}), 500
    try:
        data = request.get_json(silent=True) or {}
        current = obtener_config_tuya_haccp()
        user_code = str(data.get('user_code') or current.get('user_code') or '').strip()
        if not user_code:
            return jsonify({'success': False, 'error': 'Debes indicar el User Code de Smart Life.'}), 400

        login_control = LoginControl()
        response = login_control.qr_code(TUYA_HA_CLIENT_ID, TUYA_HA_SCHEMA, user_code)
        if not response.get('success'):
            return jsonify({
                'success': False,
                'error': response.get('msg') or 'No se pudo generar el QR de Smart Life.',
                'code': response.get('code'),
            }), 400

        qr_token = str((response.get('result') or {}).get('qrcode') or '').strip()
        if not qr_token:
            return jsonify({'success': False, 'error': 'Respuesta sin token QR de Smart Life.'}), 400

        qr_payload = f"{TUYA_QR_LOGIN_PREFIX}{qr_token}"
        qr_svg = _tuya_generar_qr_svg(qr_payload)

        _tuya_limpiar_qr_pendientes()
        _TUYA_PENDING_QR_LOGINS[qr_token] = {
            'created_at': time.time(),
            'user_code': user_code,
            'login_control': login_control,
        }
        guardar_config_tuya_haccp({'user_code': user_code})

        return jsonify({
            'success': True,
            'qr_token': qr_token,
            'qr_payload': qr_payload,
            'qr_svg': qr_svg,
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/haccp/tuya/login/poll', methods=['POST'])
def api_haccp_tuya_login_poll():
    if not _tuya_sdk_disponible():
        return jsonify({'success': False, 'error': _tuya_mensaje_dependencia()}), 500
    try:
        data = request.get_json(silent=True) or {}
        qr_token = str(data.get('qr_token') or '').strip()
        if not qr_token:
            return jsonify({'success': False, 'error': 'Falta qr_token para validar login.'}), 400

        _tuya_limpiar_qr_pendientes()
        pending = _TUYA_PENDING_QR_LOGINS.get(qr_token) or {}
        user_code = str(data.get('user_code') or pending.get('user_code') or '').strip()
        if not user_code:
            return jsonify({'success': False, 'error': 'Falta User Code para validar login.'}), 400

        login_control = pending.get('login_control') or LoginControl()
        ok, info = login_control.login_result(qr_token, TUYA_HA_CLIENT_ID, user_code)
        if not ok:
            return jsonify({
                'success': True,
                'authenticated': False,
                'code': (info or {}).get('code'),
                'message': (info or {}).get('msg') or 'Pendiente de escaneo/confirmación.',
            })

        token_info = {
            't': info.get('t'),
            'uid': info.get('uid'),
            'expire_time': info.get('expire_time'),
            'access_token': info.get('access_token'),
            'refresh_token': info.get('refresh_token'),
        }
        endpoint = str(info.get('endpoint') or '').strip()
        terminal_id = str(info.get('terminal_id') or '').strip()
        if not endpoint or not terminal_id:
            return jsonify({'success': False, 'error': 'Login incompleto: faltan endpoint/terminal_id.'}), 400

        config = guardar_auth_tuya_haccp(
            user_code=user_code,
            endpoint=endpoint,
            terminal_id=terminal_id,
            token_info=token_info,
        )
        _TUYA_PENDING_QR_LOGINS.pop(qr_token, None)
        crear_backup()

        return jsonify({
            'success': True,
            'authenticated': True,
            'username': info.get('username') or '',
            'config': _tuya_config_publica(config),
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/haccp/tuya/devices', methods=['GET'])
def api_haccp_tuya_devices():
    try:
        _tuya_ensure_background_worker()
        config = obtener_config_tuya_haccp()
        manager = _tuya_manager_desde_config(config)
        try:
            manager.update_device_cache()
        except Exception as first_error:
            if not _tuya_error_es_auth(first_error):
                raise
            # Reintento único forzando refresh de token si la API respondió auth inválida.
            manager = _tuya_manager_desde_config(_tuya_config_forzar_refresh(config))
            manager.update_device_cache()
        devices = _tuya_listar_dispositivos(manager)
        return jsonify({
            'success': True,
            'devices': devices,
            'selected_device_id': str(config.get('device_id') or ''),
            'bindings': config.get('device_bindings') if isinstance(config.get('device_bindings'), list) else [],
        })
    except ValueError as e:
        return jsonify({'success': False, 'devices': [], 'error': str(e)}), 400
    except RuntimeError as e:
        return jsonify({'success': False, 'devices': [], 'error': str(e)}), 500
    except Exception as e:
        message, status = _tuya_error_para_ui(e)
        if status == 401:
            try:
                guardar_config_tuya_haccp({'token_info': {}})
            except Exception:
                pass
        return jsonify({'success': False, 'devices': [], 'error': message}), status


@app.route('/api/haccp/tuya/lectura', methods=['GET'])
def api_haccp_tuya_lectura():
    try:
        _tuya_ensure_background_worker()
        requested_device_id = str(request.args.get('device_id') or '').strip() or None
        config = obtener_config_tuya_haccp()
        manager = _tuya_manager_desde_config(config)
        try:
            manager.update_device_cache()
        except Exception as first_error:
            if not _tuya_error_es_auth(first_error):
                raise
            manager = _tuya_manager_desde_config(_tuya_config_forzar_refresh(config))
            manager.update_device_cache()
        device, auto_device = _tuya_seleccionar_dispositivo(manager, config, requested_device_id)
        if not device:
            return jsonify({
                'success': False,
                'error': 'No se encontró un dispositivo con temperatura/humedad en Smart Life.',
            }), 404

        lectura = _tuya_lectura_desde_device(device)
        if not lectura:
            return jsonify({
                'success': False,
                'error': 'El dispositivo no expone temperatura/humedad por API.',
                'device': {
                    'id': str(getattr(device, 'id', '') or ''),
                    'name': str(getattr(device, 'name', '') or ''),
                    'category': str(getattr(device, 'category', '') or ''),
                    'status_keys': sorted(list((getattr(device, 'status', {}) or {}).keys())),
                },
            }), 422

        device_id = str(getattr(device, 'id', '') or '')
        device_name = str(getattr(device, 'name', '') or '')
        temp_data = lectura.get('temperatura') or {}
        hum_data = lectura.get('humedad') or {}
        binding = _tuya_binding_por_device(config, device_id)
        punto_id = binding.get('punto_id')
        alerta_temp = _tuya_evaluar_alerta_temperatura(binding, temp_data.get('valor'))
        guardar_config_tuya_haccp({'device_id': device_id, 'device_name': device_name})
        registrar_lectura_tuya_haccp(
            device_id=device_id,
            device_name=device_name,
            temperatura=temp_data.get('valor'),
            humedad=hum_data.get('valor'),
            punto_id=punto_id,
            origen='manual',
        )
        updated_cfg = obtener_config_tuya_haccp()

        return jsonify({
            'success': True,
            'device': {
                'id': device_id,
                'name': device_name,
                'category': str(getattr(device, 'category', '') or ''),
                'online': bool(getattr(device, 'online', False)),
            },
            'auto_device': bool(auto_device),
            'temperatura': {
                'valor': temp_data.get('valor'),
                'unidad': temp_data.get('unidad') or 'C',
                'codigo': temp_data.get('codigo') or '',
            },
            'humedad': {
                'valor': hum_data.get('valor'),
                'unidad': hum_data.get('unidad') or '%',
                'codigo': hum_data.get('codigo') or '',
            },
            'punto_id': int(punto_id or 0) if punto_id else None,
            'alerta_temp': alerta_temp,
            'ultima_lectura_en': updated_cfg.get('ultima_lectura_en'),
        })
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except RuntimeError as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    except Exception as e:
        message, status = _tuya_error_para_ui(e)
        if status == 401:
            try:
                guardar_config_tuya_haccp({'token_info': {}})
            except Exception:
                pass
        return jsonify({'success': False, 'error': message}), status


@app.route('/api/haccp/tuya/vinculaciones', methods=['GET'])
def api_haccp_tuya_vinculaciones():
    try:
        config = obtener_config_tuya_haccp()
        bindings = obtener_vinculaciones_tuya_haccp()
        puntos = [
            {
                'id': int(p.get('id') or 0),
                'nombre': str(p.get('nombre') or ''),
                'categoria': str(p.get('categoria') or ''),
                'tipo_control': str(p.get('tipo_control') or ''),
                'unidad': str(p.get('unidad') or ''),
                'activo': int(p.get('activo') or 0),
            }
            for p in listar_haccp_puntos(incluir_inactivos=False)
            if str(p.get('tipo_control') or '').lower() == 'rango'
        ]
        return jsonify({
            'success': True,
            'bindings': bindings,
            'auto_interval_min': int(config.get('auto_interval_min') or 15),
            'puntos': puntos,
        })
    except Exception as e:
        return jsonify({'success': False, 'bindings': [], 'puntos': [], 'error': str(e)}), 500


@app.route('/api/haccp/tuya/vinculaciones', methods=['POST'])
def api_haccp_tuya_guardar_vinculaciones():
    try:
        data = request.get_json(silent=True) or {}
        bindings = data.get('bindings') if isinstance(data.get('bindings'), list) else []
        auto_interval_min = data.get('auto_interval_min')
        config = guardar_vinculaciones_tuya_haccp(bindings, auto_interval_min=auto_interval_min)
        crear_backup()
        return jsonify({'success': True, 'config': _tuya_config_publica(config)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/haccp/tuya/sync', methods=['POST'])
def api_haccp_tuya_sync():
    if not _tuya_sdk_disponible():
        return jsonify({'success': False, 'error': _tuya_mensaje_dependencia()}), 500
    try:
        data = request.get_json(silent=True) or {}
        force = bool(data.get('force', True))
        requested = data.get('device_ids')
        if not isinstance(requested, list):
            requested = None
        resultado = _tuya_sync_bindings(force=force, device_ids=requested, origen='manual-sync')
        items = resultado.get('items') if isinstance(resultado, dict) else []
        if not isinstance(items, list):
            items = []
        alertas_fuera_rango = sum(
            1
            for item in items
            if bool(((item or {}).get('alerta_temp') or {}).get('fuera_rango'))
        )
        return jsonify({
            'success': True,
            'synced': items,
            'total': len(items),
            'alertas_fuera_rango': int(alertas_fuera_rango),
            'skipped': (resultado or {}).get('skipped'),
        })
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except RuntimeError as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    except Exception as e:
        message, status = _tuya_error_para_ui(e)
        if status == 401:
            try:
                guardar_config_tuya_haccp({'token_info': {}})
            except Exception:
                pass
        return jsonify({'success': False, 'error': message}), status


@app.route('/api/haccp/tuya/historial', methods=['GET'])
def api_haccp_tuya_historial():
    try:
        device_id = str(request.args.get('device_id') or '').strip() or None
        dias = int(request.args.get('dias', 7) or 7)
        agrupado_hora = str(request.args.get('agrupado_hora', '1')).strip() != '0'
        puntos = obtener_historial_tuya_haccp(
            device_id=device_id,
            dias=dias,
            agrupado_por_hora=agrupado_hora,
            limit=6000,
        )
        return jsonify({
            'success': True,
            'dias': max(1, min(30, dias)),
            'agrupado_hora': bool(agrupado_hora),
            'puntos': puntos,
        })
    except Exception as e:
        return jsonify({'success': False, 'puntos': [], 'error': str(e)}), 500


@app.route('/api/haccp/tuya/sidebar', methods=['GET'])
def api_haccp_tuya_sidebar():
    try:
        _tuya_ensure_background_worker()
        try:
            _tuya_sync_bindings(force=False, origen='auto-sidebar')
        except Exception:
            pass

        config = obtener_config_tuya_haccp()
        bindings = config.get("device_bindings")
        if not isinstance(bindings, list):
            bindings = []
        bindings_activos = [b for b in bindings if bool((b or {}).get("activo", 1))]

        puntos_map = {}
        for p in listar_haccp_puntos(incluir_inactivos=True):
            try:
                pid = int(p.get("id") or 0)
            except Exception:
                pid = 0
            if pid > 0:
                puntos_map[pid] = p

        device_ids = [str((b or {}).get("device_id") or "").strip() for b in bindings_activos]
        lecturas_map = _tuya_ultimas_lecturas_por_device(device_ids)
        items = [
            _tuya_sidebar_serializar_item(b, puntos_map, lecturas_map)
            for b in bindings_activos
            if str((b or {}).get("device_id") or "").strip()
        ]
        refrigerador, vitrina = _tuya_sidebar_elegir(items)

        return jsonify(
            {
                "success": True,
                "habilitado": bool(config.get("habilitado")),
                "sdk_disponible": _tuya_sdk_disponible(),
                "sdk_error": _tuya_mensaje_dependencia(),
                "vinculados": len(bindings_activos),
                "refrigerador": refrigerador,
                "vitrina": vitrina,
                "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/haccp')
def haccp():
    try:
        incluir_inactivos = request.args.get('inactivos', '0') == '1'
        puntos = listar_haccp_puntos(incluir_inactivos=incluir_inactivos)
        resumen = obtener_resumen_haccp()
        registros = obtener_haccp_registros(limit=120)
        return render_template(
            'haccp.html',
            puntos=puntos,
            resumen=resumen,
            registros=registros,
            incluir_inactivos=incluir_inactivos,
        )
    except Exception as e:
        return f"Error: {e}", 500


@app.route('/api/haccp/resumen')
def api_haccp_resumen():
    try:
        return jsonify({
            'success': True,
            'resumen': obtener_resumen_haccp(),
            'vencidos': obtener_haccp_puntos_vencidos(limit=30),
        })
    except Exception as e:
        return jsonify({'success': False, 'resumen': {}, 'vencidos': [], 'error': str(e)}), 500


@app.route('/api/haccp/puntos')
def api_haccp_puntos():
    try:
        incluir_inactivos = request.args.get('inactivos', '0') == '1'
        puntos = listar_haccp_puntos(incluir_inactivos=incluir_inactivos)
        return jsonify({'success': True, 'puntos': puntos})
    except Exception as e:
        return jsonify({'success': False, 'puntos': [], 'error': str(e)}), 500


@app.route('/api/haccp/punto', methods=['POST'])
def api_haccp_crear_punto():
    try:
        data = request.get_json(silent=True) or {}
        resultado = crear_haccp_punto(data)
        return jsonify({'success': True, 'id': resultado.get('id')})
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/operaciones/<codigo_operacion>/timeline')
def api_operacion_timeline(codigo_operacion):
    try:
        limit = _as_int(request.args.get('limit', 600) or 600, "límite", min_value=10)
        data = obtener_timeline_operacion(codigo_operacion, limit=limit)
        return jsonify({'success': True, **data})
    except ValueError as e:
        return jsonify({'success': False, 'timeline': [], 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'timeline': [], 'error': str(e)}), 500


@app.route('/api/venta/<int:venta_id>/timeline')
def api_venta_timeline(venta_id):
    try:
        codigo = obtener_codigo_operacion_venta(venta_id)
        if not codigo:
            return jsonify({'success': False, 'timeline': [], 'error': 'La venta no tiene codigo de operación'}), 404
        limit = _as_int(request.args.get('limit', 600) or 600, "límite", min_value=10)
        data = obtener_timeline_operacion(codigo, limit=limit)
        data["venta_id"] = int(venta_id)
        return jsonify({'success': True, **data})
    except ValueError as e:
        return jsonify({'success': False, 'timeline': [], 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'timeline': [], 'error': str(e)}), 500


@app.route('/api/haccp/punto/<int:punto_id>/actualizar', methods=['POST'])
def api_haccp_actualizar_punto(punto_id):
    try:
        data = request.get_json(silent=True) or {}
        resultado = actualizar_haccp_punto(punto_id, data)
        return jsonify({'success': True, 'id': resultado.get('id')})
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/haccp/punto/<int:punto_id>/estado', methods=['POST'])
def api_haccp_estado_punto(punto_id):
    try:
        data = request.get_json(silent=True) or {}
        cambiar_estado_haccp_punto(punto_id, data.get('activo', True))
        return jsonify({'success': True})
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/haccp/registro', methods=['POST'])
def api_haccp_registro():
    try:
        data = request.get_json(silent=True) or {}
        resultado = registrar_haccp_control(data)
        return jsonify({'success': True, 'registro': resultado})
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/haccp/registros')
def api_haccp_registros():
    try:
        limit = _as_int(request.args.get('limit', 120) or 120, "limite", min_value=1)
        punto_id = request.args.get('punto_id')
        punto_id = int(punto_id) if punto_id not in (None, "") else None
        registros = obtener_haccp_registros(limit=limit, punto_id=punto_id)
        return jsonify({'success': True, 'registros': registros})
    except ValueError as e:
        return jsonify({'success': False, 'registros': [], 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'registros': [], 'error': str(e)}), 500


@app.route('/api/haccp/trazabilidad-insumos')
def api_haccp_trazabilidad_insumos():
    try:
        limit = _as_int(request.args.get('limit', 250) or 250, "limite", min_value=1)
        mes = (request.args.get('mes') or '').strip() or None
        fecha_desde = (request.args.get('desde') or '').strip() or None
        fecha_hasta = (request.args.get('hasta') or '').strip() or None
        busqueda = (request.args.get('q') or '').strip() or None

        data = obtener_haccp_trazabilidad_insumos(
            limit=limit,
            mes=mes,
            fecha_desde=fecha_desde,
            fecha_hasta=fecha_hasta,
            busqueda=busqueda,
        )
        return jsonify({
            'success': True,
            'lotes': data.get('lotes', []),
            'meses_disponibles': data.get('meses_disponibles', []),
        })
    except ValueError as e:
        return jsonify({'success': False, 'lotes': [], 'meses_disponibles': [], 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'lotes': [], 'meses_disponibles': [], 'error': str(e)}), 500


def _tcp_port_open(host, port, timeout=1.5):
    try:
        with socket.create_connection((str(host).strip(), int(port)), timeout=timeout):
            return True
    except Exception:
        return False


def _rtsp_describe_status(host, port, path, user="", password="", timeout=1.6):
    host = str(host or "").strip()
    if not host:
        return None
    path = str(path or "").strip()
    if not path:
        return None
    if not path.startswith("/"):
        path = "/" + path
    try:
        port = int(port or 554)
    except Exception:
        port = 554

    auth = ""
    user = str(user or "").strip()
    password = str(password or "").strip()
    if user or password:
        token = base64.b64encode(f"{user}:{password}".encode("utf-8", errors="ignore")).decode("ascii", errors="ignore")
        auth = f"Authorization: Basic {token}\r\n"

    req = (
        f"DESCRIBE rtsp://{host}:{port}{path} RTSP/1.0\r\n"
        f"CSeq: 2\r\n"
        f"Accept: application/sdp\r\n"
        f"{auth}"
        f"User-Agent: SucreeStock/4.3\r\n\r\n"
    )
    try:
        with socket.create_connection((host, port), timeout=timeout) as s:
            s.sendall(req.encode("utf-8", errors="ignore"))
            data = s.recv(2048).decode("utf-8", errors="ignore")
        if not data:
            return None
        first = data.splitlines()[0] if data.splitlines() else ""
        m = re.search(r"RTSP/\d+\.\d+\s+(\d{3})", first)
        if not m:
            return None
        return int(m.group(1))
    except Exception:
        return None


def _to_int(value, default, min_value=None, max_value=None):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = int(default)
    if min_value is not None:
        parsed = max(min_value, parsed)
    if max_value is not None:
        parsed = min(max_value, parsed)
    return parsed


def _normalize_http_base_url(base_url, default="http://127.0.0.1:1984"):
    url = str(base_url or "").strip()
    if not url:
        url = default
    if not re.match(r"^https?://", url, flags=re.IGNORECASE):
        url = "http://" + url
    return url.rstrip("/")


def _build_go2rtc_embed_url(base_url, source_name):
    base = _normalize_http_base_url(base_url)
    src = quote(str(source_name or "").strip() or "cam1", safe="")
    return f"{base}/stream.html?src={src}&mode=webrtc"


def _go2rtc_status_probe(base_url, timeout=2.5):
    base = _normalize_http_base_url(base_url)
    parsed = urlparse(base)
    host = parsed.hostname or "127.0.0.1"
    if parsed.port:
        port = parsed.port
    else:
        port = 443 if (parsed.scheme or "http").lower() == "https" else 80

    tcp_ok = _tcp_port_open(host, port)
    api_ok = False
    http_status = None
    streams_count = 0
    error = ""

    if tcp_ok:
        req = UrlRequest(
            f"{base}/api/streams",
            headers={
                "User-Agent": "SucreeStock/4.3",
                "Accept": "application/json,*/*;q=0.8",
            },
        )
        try:
            with urlopen(req, timeout=timeout, context=ssl.create_default_context()) as resp:
                http_status = int(getattr(resp, "status", 200) or 200)
                body = (resp.read() or b"").decode("utf-8", errors="ignore")
                if http_status < 500:
                    api_ok = True
                streams_count = len(re.findall(r'"url"\s*:', body))
                if streams_count == 0:
                    streams_count = len(re.findall(r'"producers"\s*:', body))
        except ssl.SSLError:
            try:
                with urlopen(req, timeout=timeout, context=ssl._create_unverified_context()) as resp:
                    http_status = int(getattr(resp, "status", 200) or 200)
                    body = (resp.read() or b"").decode("utf-8", errors="ignore")
                    if http_status < 500:
                        api_ok = True
                    streams_count = len(re.findall(r'"url"\s*:', body))
                    if streams_count == 0:
                        streams_count = len(re.findall(r'"producers"\s*:', body))
            except Exception as ex_ssl:
                error = str(ex_ssl)
        except Exception as ex:
            error = str(ex)
    else:
        error = f"No responde {host}:{port}"

    return {
        "base_url": base,
        "host": host,
        "port": port,
        "tcp_ok": bool(tcp_ok),
        "api_ok": bool(api_ok),
        "http_status": http_status,
        "streams_count": int(streams_count or 0),
        "error": error,
    }


def _build_go2rtc_yaml(paneles):
    lines = [
        "api:",
        "  listen: \":1984\"",
        "webrtc:",
        "  listen: \":8555\"",
        "streams:",
    ]
    usados = 0
    for panel in paneles or []:
        rtsp = str((panel or {}).get("rtsp_url") or "").strip()
        if not rtsp:
            continue
        cam_id = _to_int((panel or {}).get("id"), 1, min_value=1, max_value=4)
        nombre = f"cam{cam_id}"
        lines.append(f"  {nombre}:")
        candidates = _build_rtsp_candidates(rtsp, "")
        if not candidates:
            candidates = [rtsp]
        for candidate in candidates[:6]:
            lines.append(f"    - \"{candidate}\"")
        usados += 1

    if usados == 0:
        lines.append("  cam1:")
        lines.append("    - \"rtsp://127.0.0.1:554/avstream/channel=1/stream=1.sdp\"")

    return "\n".join(lines) + "\n"


def _go2rtc_yaml_path():
    camaras_dir = os.path.join(DATA_DIR, "camaras")
    os.makedirs(camaras_dir, exist_ok=True)
    return os.path.join(camaras_dir, "go2rtc.yaml")


def _write_go2rtc_yaml(paneles):
    yaml_path = _go2rtc_yaml_path()
    with open(yaml_path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(_build_go2rtc_yaml(paneles))
    return yaml_path


def _find_go2rtc_binary():
    here = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(here, "go2rtc.exe"),
        os.path.join(here, "tools", "go2rtc.exe"),
        os.path.join(DATA_DIR, "camaras", "go2rtc.exe"),
        shutil.which("go2rtc.exe"),
        shutil.which("go2rtc"),
    ]
    for c in candidates:
        if c and os.path.isfile(c):
            return c
    return ""


def _start_go2rtc_process(base_url):
    global _GO2RTC_PROCESS

    if _GO2RTC_PROCESS is not None and _GO2RTC_PROCESS.poll() is None:
        return {"started": True, "already_running": True, "pid": _GO2RTC_PROCESS.pid, "error": ""}

    bin_path = _find_go2rtc_binary()
    if not bin_path:
        return {"started": False, "already_running": False, "pid": None, "error": "No se encontró go2rtc.exe"}

    cfg = obtener_config_camaras()
    paneles = (cfg or {}).get("paneles", []) if isinstance(cfg, dict) else []
    yaml_path = _write_go2rtc_yaml(paneles)
    cmd = [bin_path, "-config", yaml_path]

    popen_kwargs = {
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
        "stdin": subprocess.DEVNULL,
        "close_fds": True,
    }
    if os.name == "nt":
        popen_kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)

    try:
        _GO2RTC_PROCESS = subprocess.Popen(cmd, **popen_kwargs)
    except Exception as ex:
        return {"started": False, "already_running": False, "pid": None, "error": str(ex)}

    time.sleep(0.9)
    probe = _go2rtc_status_probe(base_url)
    started_ok = bool(probe.get("tcp_ok") and probe.get("api_ok"))
    return {
        "started": started_ok,
        "already_running": False,
        "pid": _GO2RTC_PROCESS.pid,
        "error": "" if started_ok else (probe.get("error") or "go2rtc no respondió tras iniciar"),
    }


def _stop_go2rtc_process():
    global _GO2RTC_PROCESS
    if _GO2RTC_PROCESS is None:
        return {"stopped": False, "error": "go2rtc no estaba iniciado por SucréeStock"}
    if _GO2RTC_PROCESS.poll() is not None:
        _GO2RTC_PROCESS = None
        return {"stopped": True, "error": ""}
    try:
        _GO2RTC_PROCESS.terminate()
        _GO2RTC_PROCESS.wait(timeout=3)
    except Exception:
        try:
            _GO2RTC_PROCESS.kill()
        except Exception:
            pass
    _GO2RTC_PROCESS = None
    return {"stopped": True, "error": ""}


def _build_rtsp_url(host, channel, port=554, stream=1, user="", password=""):
    host = str(host or "").strip()
    if not host:
        return ""

    user = str(user or "").strip()
    password = str(password or "").strip()
    auth = ""
    if user and password:
        auth = f"{quote(user, safe='')}:{quote(password, safe='')}@"
    elif user:
        auth = f"{quote(user, safe='')}@"

    return f"rtsp://{auth}{host}:{int(port)}/avstream/channel={int(channel)}/stream={int(stream)}.sdp"


def _build_rtsp_url_with_path(host, port, path_and_query, user="", password="", scheme="rtsp"):
    host = str(host or "").strip()
    if not host:
        return ""
    user = str(user or "").strip()
    password = str(password or "").strip()
    auth = ""
    if user and password:
        auth = f"{quote(user, safe='')}:{quote(password, safe='')}@"
    elif user:
        auth = f"{quote(user, safe='')}@"
    path = str(path_and_query or "").strip()
    if not path.startswith("/"):
        path = "/" + path
    return f"{str(scheme or 'rtsp').lower()}://{auth}{host}:{int(port)}{path}"


def _extract_rtsp_params(rtsp_url):
    url = str(rtsp_url or "").strip()
    if not url:
        return None
    try:
        parsed = urlparse(url)
    except Exception:
        return None
    scheme = (parsed.scheme or "").lower()
    if scheme not in ("rtsp", "rtsps"):
        return None
    host = parsed.hostname or ""
    if not host:
        return None
    port = int(parsed.port or 554)
    user = unquote(parsed.username or "")
    password = unquote(parsed.password or "")
    path = parsed.path or ""
    query = parsed.query or ""

    channel = None
    stream = None

    m = re.search(r"/avstream/channel=(\d+)/stream=(\d+)\.sdp", path, flags=re.IGNORECASE)
    if m:
        channel = _to_int(m.group(1), 1, min_value=1, max_value=64)
        stream = _to_int(m.group(2), 1, min_value=0, max_value=1)

    if channel is None:
        m_channel = re.search(r"(?:^|[?&])channel=(\d+)(?:&|$)", query, flags=re.IGNORECASE)
        m_subtype = re.search(r"(?:^|[?&])subtype=(\d+)(?:&|$)", query, flags=re.IGNORECASE)
        if m_channel:
            channel = _to_int(m_channel.group(1), 1, min_value=1, max_value=64)
        if m_subtype:
            stream = _to_int(m_subtype.group(1), 1, min_value=0, max_value=1)

    if channel is None:
        m = re.search(r"_channel=(\d+)_stream=(\d+)\.sdp", path, flags=re.IGNORECASE)
        if m:
            channel = _to_int(m.group(1), 1, min_value=0, max_value=64)
            stream = _to_int(m.group(2), 1, min_value=0, max_value=1)

    if channel is None:
        m = re.search(r"/h264/ch(\d+)/(main|sub)/av_stream", path, flags=re.IGNORECASE)
        if m:
            channel = _to_int(m.group(1), 1, min_value=1, max_value=64)
            stream = 0 if m.group(2).lower() == "main" else 1

    if channel is None:
        m = re.search(r"/Streaming/Channels/(\d{3,4})", path, flags=re.IGNORECASE)
        if m:
            code = _to_int(m.group(1), 101, min_value=1, max_value=9999)
            if code >= 100:
                channel = max(1, code // 100)
                tail = code % 100
                if tail == 1:
                    stream = 0
                elif tail == 2:
                    stream = 1

    if channel is None:
        return None
    if stream is None:
        stream = 1

    return {
        "scheme": scheme,
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "channel": channel,
        "stream": stream,
    }


def _append_unique(urls, candidate):
    c = str(candidate or "").strip()
    if not c:
        return
    if c not in urls:
        urls.append(c)


def _build_rtsp_candidates(rtsp_url, fallback_rtsp_url=""):
    urls = []
    _append_unique(urls, rtsp_url)
    _append_unique(urls, fallback_rtsp_url)

    base = _extract_rtsp_params(rtsp_url) or _extract_rtsp_params(fallback_rtsp_url)
    if not base:
        return urls

    host = base["host"]
    port = base["port"]
    user = base["user"]
    password = base["password"]
    scheme = base["scheme"]
    channel = base["channel"]
    stream = _to_int(base["stream"], 1, min_value=0, max_value=1)
    alt_stream = 0 if stream == 1 else 1

    # Para XVR tipo AVStream priorizamos solo rutas compatibles, reduciendo spam 451 en consola.
    is_avstream = "/avstream/" in str(rtsp_url or "").lower() or "/avstream/" in str(fallback_rtsp_url or "").lower()
    if is_avstream:
        user_path = str(user or "admin")
        pass_path = str(password or "")
        user_q = quote(user_path, safe="")
        pass_q = quote(pass_path, safe="")
        for ch in (channel, channel - 1, channel + 1):
            if ch < 0:
                continue
            for st in (stream, alt_stream):
                _append_unique(urls, _build_rtsp_url(host, ch, port=port, stream=st, user=user, password=password))
                _append_unique(
                    urls,
                    _build_rtsp_url_with_path(
                        host,
                        port,
                        f"/user={user_q}_password={pass_q}_channel={ch}_stream={st}.sdp?real_stream",
                        user="",
                        password="",
                        scheme=scheme,
                    ),
                )
                _append_unique(
                    urls,
                    _build_rtsp_url_with_path(
                        host,
                        port,
                        f"/user={user_q}&password={pass_q}&channel={ch}&stream={st}.sdp?real_stream",
                        user="",
                        password="",
                        scheme=scheme,
                    ),
                )
        return urls[:12]

    # Fallback genérico para otros modelos.
    for st in (stream, alt_stream):
        _append_unique(urls, _build_rtsp_url(host, channel, port=port, stream=st, user=user, password=password))

    _append_unique(
        urls,
        _build_rtsp_url_with_path(
            host,
            port,
            f"/cam/realmonitor?channel={channel}&subtype={stream}",
            user=user,
            password=password,
            scheme=scheme,
        ),
    )
    _append_unique(
        urls,
        _build_rtsp_url_with_path(
            host,
            port,
            f"/cam/realmonitor?channel={channel}&subtype={alt_stream}",
            user=user,
            password=password,
            scheme=scheme,
        ),
    )

    return urls[:6]


def _replace_rtsp_stream(rtsp_url, stream_idx):
    url = str(rtsp_url or "").strip()
    if not url:
        return ""
    stream_idx = _to_int(stream_idx, 1, min_value=0, max_value=1)
    if re.search(r"stream=\d+", url, flags=re.IGNORECASE):
        return re.sub(r"stream=\d+", f"stream={stream_idx}", url, flags=re.IGNORECASE)
    return url


def _mjpeg_frame_bytes(jpeg_bytes):
    size = len(jpeg_bytes or b"")
    return (
        b"--frame\r\n"
        b"Content-Type: image/jpeg\r\n"
        b"Cache-Control: no-cache\r\n"
        + f"Content-Length: {size}\r\n\r\n".encode("ascii")
        + (jpeg_bytes or b"")
        + b"\r\n"
    )


_OPENCV_DIAG_LOCK = threading.Lock()
_OPENCV_DIAG_CACHE = {"ready": False, "payload": {}}


def _opencv_import_diagnostic(force=False):
    with _OPENCV_DIAG_LOCK:
        if _OPENCV_DIAG_CACHE.get("ready") and not force:
            payload = _OPENCV_DIAG_CACHE.get("payload") or {}
            return dict(payload)

        payload = {}
        try:
            import cv2  # type: ignore
            payload = {
                "ok": True,
                "version": str(getattr(cv2, "__version__", "") or ""),
                "error": "",
                "hint": "",
            }
        except Exception as exc:
            detail = f"{exc.__class__.__name__}: {exc}"
            detail_low = detail.lower()
            if "more than once per process" in detail_low or "mas de una vez por proceso" in detail_low:
                hint = (
                    "Conflicto de DLL OpenCV cargada mas de una vez. "
                    "Cierra todas las instancias de la app y usa solo el paquete portable mas nuevo."
                )
            elif "openh264" in detail_low:
                hint = "Falta OpenH264 en Windows. Instala OpenH264 64-bit o usa substream H.265/H.264 alternativo."
            elif "dll load failed" in detail_low or "no module named cv2" in detail_low:
                hint = "Instala Microsoft Visual C++ 2015-2022 Redistributable (x64) y vuelve a abrir la app."
            else:
                hint = "Reinstala el paquete portable completo y verifica dependencias de Windows."
            payload = {
                "ok": False,
                "version": "",
                "error": detail[:260],
                "hint": hint,
            }

        _OPENCV_DIAG_CACHE["payload"] = payload
        _OPENCV_DIAG_CACHE["ready"] = True
        return dict(payload)


def _opencv_missing_status_text():
    diag = _opencv_import_diagnostic()
    if diag.get("ok"):
        return ""
    msg = "OpenCV no disponible"
    if diag.get("error"):
        msg = f"{msg}: {diag['error']}"
    return msg[:180]


def _status_mjpeg_jpeg(text):
    try:
        import cv2
        import numpy as np
    except Exception:
        return b""

    canvas = np.zeros((240, 426, 3), dtype=np.uint8)
    canvas[:] = (28, 32, 45)
    cv2.putText(canvas, "Camara sin senal", (22, 94), cv2.FONT_HERSHEY_SIMPLEX, 0.9, (255, 255, 255), 2, cv2.LINE_AA)
    cv2.putText(canvas, str(text or "RTSP no disponible")[:34], (22, 138), cv2.FONT_HERSHEY_SIMPLEX, 0.62, (189, 201, 219), 1, cv2.LINE_AA)
    ok, encoded = cv2.imencode(".jpg", canvas, [int(cv2.IMWRITE_JPEG_QUALITY), 80])
    if not ok:
        return b""
    return encoded.tobytes()


def _iter_mjpeg_rtsp(rtsp_url, jpeg_quality=88, target_fps=12, fallback_rtsp_url="", frame_drop=0, render_mode="quality"):
    try:
        import cv2
    except Exception:
        frame = _status_mjpeg_jpeg(_opencv_missing_status_text() or "OpenCV no disponible")
        while True:
            yield _mjpeg_frame_bytes(frame)
            time.sleep(1.4)
        return

    rtsp_url = str(rtsp_url or "").strip()
    fallback_rtsp_url = str(fallback_rtsp_url or "").strip()
    urls = _build_rtsp_candidates(rtsp_url, fallback_rtsp_url)
    if not urls:
        urls = [rtsp_url]

    active_url_idx = 0
    active_url = urls[active_url_idx]
    cap = None
    jpeg_quality = _to_int(jpeg_quality, 86, min_value=55, max_value=95)
    target_fps = _to_int(target_fps, 12, min_value=3, max_value=30)
    frame_drop = _to_int(frame_drop, 0, min_value=0, max_value=6)
    render_mode = str(render_mode or "quality").strip().lower()
    if render_mode not in ("quality", "realtime"):
        render_mode = "quality"
    frame_sleep = max(0.01, 1.0 / float(target_fps))
    quality = [int(cv2.IMWRITE_JPEG_QUALITY), int(jpeg_quality)]
    open_failures = 0
    read_failures = 0
    try:
        # Reduce ruido de logs FFmpeg/HEVC en consola cuando el stream llega inestable.
        os.environ.setdefault("OPENCV_VIDEOIO_DEBUG", "0")
        os.environ.setdefault("OPENCV_LOG_LEVEL", "SILENT")
        os.environ.setdefault("OPENCV_FFMPEG_LOGLEVEL", "-8")
        try:
            if hasattr(cv2, "setLogLevel"):
                cv2.setLogLevel(0)
            elif hasattr(cv2, "utils") and hasattr(cv2.utils, "logging"):
                cv2.utils.logging.setLogLevel(cv2.utils.logging.LOG_LEVEL_SILENT)
        except Exception:
            pass

        while True:
            if cap is None or not cap.isOpened():
                # quality: prioriza imagen limpia y decodificacion estable
                # realtime: prioriza menor latencia (puede introducir artefactos)
                if render_mode == "realtime":
                    os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = (
                        "rtsp_transport;tcp|fflags;nobuffer|flags;low_delay|err_detect;ignore_err|"
                        "stimeout;5000000|rw_timeout;5000000|max_delay;300000"
                    )
                else:
                    os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = (
                        "rtsp_transport;tcp|fflags;discardcorrupt|err_detect;ignore_err|"
                        "stimeout;7000000|rw_timeout;7000000|"
                        "max_delay;1200000"
                    )
                ffmpeg_backend = getattr(cv2, "CAP_FFMPEG", 0)
                cap = cv2.VideoCapture(active_url, ffmpeg_backend) if ffmpeg_backend else cv2.VideoCapture(active_url)
                if not cap.isOpened():
                    cap = cv2.VideoCapture(active_url)
                if hasattr(cv2, "CAP_PROP_BUFFERSIZE"):
                    try:
                        cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
                    except Exception:
                        pass
                if hasattr(cv2, "CAP_PROP_FPS"):
                    try:
                        cap.set(cv2.CAP_PROP_FPS, float(target_fps))
                    except Exception:
                        pass
                if not cap.isOpened():
                    open_failures += 1
                    if open_failures >= 2 and active_url_idx + 1 < len(urls):
                        active_url_idx += 1
                        active_url = urls[active_url_idx]
                        open_failures = 0
                        yield _mjpeg_frame_bytes(_status_mjpeg_jpeg(f"Probando ruta RTSP {active_url_idx + 1}/{len(urls)}"))
                    else:
                        yield _mjpeg_frame_bytes(_status_mjpeg_jpeg("No conecta RTSP"))
                    time.sleep(1.3)
                    try:
                        cap.release()
                    except Exception:
                        pass
                    cap = None
                    continue

            if frame_drop > 0:
                dropped = 0
                while dropped < frame_drop:
                    grabbed = cap.grab()
                    if not grabbed:
                        break
                    dropped += 1

            ok, frame = cap.read()
            if not ok or frame is None:
                read_failures += 1
                if read_failures >= 5 and active_url_idx + 1 < len(urls):
                    active_url_idx += 1
                    active_url = urls[active_url_idx]
                    read_failures = 0
                    yield _mjpeg_frame_bytes(_status_mjpeg_jpeg(f"Probando ruta RTSP {active_url_idx + 1}/{len(urls)}"))
                else:
                    yield _mjpeg_frame_bytes(_status_mjpeg_jpeg("Reconectando..."))
                try:
                    cap.release()
                except Exception:
                    pass
                cap = None
                time.sleep(0.6)
                continue

            open_failures = 0
            read_failures = 0
            ok, encoded = cv2.imencode(".jpg", frame, quality)
            if not ok:
                continue

            yield _mjpeg_frame_bytes(encoded.tobytes())
            time.sleep(frame_sleep)
    except GeneratorExit:
        pass
    finally:
        if cap is not None:
            try:
                cap.release()
            except Exception:
                pass


def _payload_camaras_xvr_local(ip="192.168.1.10", rtsp_port=554, stream_idx=1, user="", password=""):
    host = str(ip or "").strip()
    if not host:
        raise ValueError("IP local invalida")

    rtsp_port = _to_int(rtsp_port, 554, min_value=1, max_value=65535)
    stream_idx = _to_int(stream_idx, 1, min_value=0, max_value=1)
    user = str(user or "").strip()
    password = str(password or "").strip()

    actual = obtener_config_camaras()
    config_actual = (actual or {}).get("config", {}) if isinstance(actual, dict) else {}
    payload = {
        "plataforma": config_actual.get("plataforma") or "XVRview",
        "modo": "local",
        "device_id": config_actual.get("device_id") or "rjphdn5bniqq",
        "user_id": user or (config_actual.get("user_id") or "admin"),
        "servidor_1": config_actual.get("servidor_1") or host,
        "servidor_2": config_actual.get("servidor_2") or "",
        "paneles": [],
    }

    def _stream_valido(status_code):
        return status_code in (200, 401)

    def _resolver_stream_canal(ch):
        pref = stream_idx
        alt = 0 if pref == 1 else 1
        status_pref = _rtsp_describe_status(
            host,
            rtsp_port,
            f"/avstream/channel={ch}/stream={pref}.sdp",
            user=user,
            password=password,
        )
        status_alt = _rtsp_describe_status(
            host,
            rtsp_port,
            f"/avstream/channel={ch}/stream={alt}.sdp",
            user=user,
            password=password,
        )
        if _stream_valido(status_pref):
            return pref
        if _stream_valido(status_alt):
            return alt
        return pref

    for channel in range(1, 5):
        stream_channel = _resolver_stream_canal(channel)
        payload["paneles"].append(
            {
                "id": channel,
                "activa": True,
                "nombre": f"Camara {channel}",
                "abrir_url": f"http://{host}",
                "embed_url": f"/api/camaras/mjpeg/{channel}",
                "rtsp_url": _build_rtsp_url(host, channel=channel, port=rtsp_port, stream=stream_channel, user=user, password=password),
                "orden": channel,
            }
        )
    return payload


@app.route('/camaras')
def camaras():
    try:
        data = obtener_config_camaras()
    except Exception:
        data = {"config": {}, "paneles": []}

    config = (data or {}).get("config", {}) if isinstance(data, dict) else {}
    paneles = [dict(p) for p in ((data or {}).get("paneles") or [])]
    try:
        CAMERA_HUB.sync_paneles(paneles)
        estados = CAMERA_HUB.get_statuses()
    except Exception:
        estados = {}
    opencv_diag = _opencv_import_diagnostic()

    return render_template(
        'camaras.html',
        camaras_config=config,
        camaras_paneles=paneles,
        camaras_estado=estados,
        opencv_diag=opencv_diag,
    )


@app.route('/api/camaras/config', methods=['GET'])
def api_camaras_config():
    try:
        data = obtener_config_camaras()
        return jsonify(data)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e), 'config': {}, 'paneles': []}), 500


@app.route('/api/camaras/diagnostico', methods=['GET'])
def api_camaras_diagnostico():
    try:
        diag = _opencv_import_diagnostic()
        return jsonify({"success": True, "opencv": diag})
    except Exception as e:
        return jsonify({"success": False, "error": str(e), "opencv": {"ok": False}}), 500


@app.route('/api/camaras/config/auto-xvr-local', methods=['POST'])
def api_camaras_config_auto_xvr_local():
    try:
        payload = request.get_json(silent=True) or {}
        ip = str(payload.get("ip") or "192.168.1.10").strip()
        rtsp_port = _to_int(payload.get("rtsp_port"), 554, min_value=1, max_value=65535)
        stream_idx = _to_int(payload.get("stream_idx"), 1, min_value=0, max_value=1)
        user = str(payload.get("user") or "").strip()
        password = str(payload.get("password") or "").strip()

        config_payload = _payload_camaras_xvr_local(
            ip=ip,
            rtsp_port=rtsp_port,
            stream_idx=stream_idx,
            user=user,
            password=password,
        )
        data = guardar_config_camaras(config_payload)
        CAMERA_HUB.sync_paneles((data or {}).get("paneles", []))
        crear_backup()
        data["tcp_http_ok"] = _tcp_port_open(ip, 80)
        data["tcp_rtsp_ok"] = _tcp_port_open(ip, rtsp_port)
        return jsonify(data)
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e), 'config': {}, 'paneles': []}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e), 'config': {}, 'paneles': []}), 500


@app.route('/api/camaras/go2rtc/status', methods=['POST'])
def api_camaras_go2rtc_status():
    try:
        payload = request.get_json(silent=True) or {}
        base_url = _normalize_http_base_url(payload.get("base_url") or "http://127.0.0.1:1984")
        probe = _go2rtc_status_probe(base_url)
        return jsonify({
            "success": True,
            **probe,
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/camaras/config/auto-go2rtc-local', methods=['POST'])
def api_camaras_config_auto_go2rtc_local():
    try:
        payload = request.get_json(silent=True) or {}
        ip = str(payload.get("ip") or "192.168.1.10").strip()
        rtsp_port = _to_int(payload.get("rtsp_port"), 554, min_value=1, max_value=65535)
        stream_idx = _to_int(payload.get("stream_idx"), 1, min_value=0, max_value=1)
        user = str(payload.get("user") or "").strip()
        password = str(payload.get("password") or "").strip()
        base_url = _normalize_http_base_url(payload.get("go2rtc_base_url") or "http://127.0.0.1:1984")

        config_payload = _payload_camaras_xvr_local(
            ip=ip,
            rtsp_port=rtsp_port,
            stream_idx=stream_idx,
            user=user,
            password=password,
        )
        config_payload["modo"] = "local_go2rtc"
        for panel in config_payload.get("paneles", []):
            cam_id = _to_int(panel.get("id"), 1, min_value=1, max_value=4)
            panel["embed_url"] = _build_go2rtc_embed_url(base_url, f"cam{cam_id}")

        data = guardar_config_camaras(config_payload)
        CAMERA_HUB.sync_paneles((data or {}).get("paneles", []))
        crear_backup()

        yaml_path = _write_go2rtc_yaml((data or {}).get("paneles", []))
        start_info = _start_go2rtc_process(base_url)

        probe = _go2rtc_status_probe(base_url)
        data["go2rtc"] = {
            **probe,
            "yaml_path": yaml_path,
            "binary_path": _find_go2rtc_binary(),
            "start_info": start_info,
        }
        data["tcp_http_ok"] = _tcp_port_open(ip, 80)
        data["tcp_rtsp_ok"] = _tcp_port_open(ip, rtsp_port)
        return jsonify(data)
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e), 'config': {}, 'paneles': []}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e), 'config': {}, 'paneles': []}), 500


@app.route('/api/camaras/go2rtc/start', methods=['POST'])
def api_camaras_go2rtc_start():
    try:
        payload = request.get_json(silent=True) or {}
        base_url = _normalize_http_base_url(payload.get("base_url") or "http://127.0.0.1:1984")
        yaml_path = _write_go2rtc_yaml((obtener_config_camaras() or {}).get("paneles", []))
        start_info = _start_go2rtc_process(base_url)
        probe = _go2rtc_status_probe(base_url)
        return jsonify({
            "success": True,
            "yaml_path": yaml_path,
            "binary_path": _find_go2rtc_binary(),
            "start_info": start_info,
            **probe,
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/camaras/go2rtc/stop', methods=['POST'])
def api_camaras_go2rtc_stop():
    try:
        result = _stop_go2rtc_process()
        return jsonify({"success": True, **result})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/camaras/mjpeg/<int:camara_id>')
def api_camaras_mjpeg(camara_id):
    try:
        data = obtener_config_camaras()
        paneles = (data or {}).get("paneles", []) if isinstance(data, dict) else []
        CAMERA_HUB.sync_paneles(paneles)
        panel = next((p for p in paneles if int(p.get("id") or 0) == int(camara_id)), None)
        if not panel:
            return jsonify({"success": False, "error": "Camara no encontrada"}), 404

        rtsp_url = str(panel.get("rtsp_url") or "").strip()
        if not rtsp_url:
            return jsonify({"success": False, "error": "Camara sin URL RTSP configurada"}), 400

        fps = _to_int(request.args.get("fps"), 10, min_value=3, max_value=20)

        headers = {
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
        }
        return Response(
            CAMERA_HUB.mjpeg_generator(camara_id, fps=fps),
            mimetype="multipart/x-mixed-replace; boundary=frame",
            headers=headers,
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/camaras/perfil/<int:camara_id>', methods=['POST'])
def api_camaras_perfil(camara_id):
    try:
        payload = request.get_json(silent=True) or {}
        perfil = str(payload.get("perfil") or "").strip().lower()
        if perfil in ("estable", "stability", "stable"):
            stream_idx = 1
            perfil_norm = "estable"
        elif perfil in ("alta", "alta_calidad", "high", "hq"):
            stream_idx = 0
            perfil_norm = "alta_calidad"
        else:
            raise ValueError("Perfil invalido. Usa 'estable' o 'alta_calidad'")

        data = obtener_config_camaras()
        paneles = [dict(p) for p in ((data or {}).get("paneles") or [])]
        if not paneles:
            raise ValueError("No hay paneles de camara configurados")

        objetivo = None
        for panel in paneles:
            if int(panel.get("id") or 0) == int(camara_id):
                objetivo = panel
                break
        if not objetivo:
            raise ValueError("Camara no encontrada")

        rtsp_actual = str(objetivo.get("rtsp_url") or "").strip()
        if not rtsp_actual:
            raise ValueError("Camara sin RTSP. Usa 'Auto RTSP local' primero")

        objetivo["rtsp_url"] = _replace_rtsp_stream(rtsp_actual, stream_idx)

        config = dict((data or {}).get("config") or {})
        payload_save = {
            "plataforma": config.get("plataforma") or "XVRview",
            "modo": config.get("modo") or "local",
            "device_id": config.get("device_id") or "",
            "user_id": config.get("user_id") or "",
            "servidor_1": config.get("servidor_1") or "",
            "servidor_2": config.get("servidor_2") or "",
            "paneles": paneles,
        }
        saved = guardar_config_camaras(payload_save)
        CAMERA_HUB.sync_paneles((saved or {}).get("paneles", []))
        saved["perfil_aplicado"] = perfil_norm
        saved["camara_id"] = int(camara_id)
        return jsonify(saved)
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/camaras/config', methods=['POST'])
def api_camaras_config_guardar():
    try:
        payload = request.get_json(silent=True) or {}
        data = guardar_config_camaras(payload)
        CAMERA_HUB.sync_paneles((data or {}).get("paneles", []))
        crear_backup()
        return jsonify(data)
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e), 'config': {}, 'paneles': []}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e), 'config': {}, 'paneles': []}), 500


@app.route('/settings')
def settings():
    try:
        config_alertas = obtener_config_alertas()
        config_clima_sidebar = obtener_config_clima_sidebar()
        config_updater = obtener_config_updater()
        config_backup_orquestacion = _leer_backup_orquestacion()
        recordatorios = obtener_recordatorios_agenda_pendientes()
    except Exception:
        config_alertas = {}
        config_clima_sidebar = {}
        config_updater = {}
        config_backup_orquestacion = _backup_orquestacion_default()
        recordatorios = []
    return render_template(
        'settings.html',
        config_alertas=config_alertas,
        config_clima_sidebar=config_clima_sidebar,
        config_updater=config_updater,
        config_backup_orquestacion=config_backup_orquestacion,
        admin_users_count=_admin_users_count(),
        admin_legacy_user=_obtener_admin_legacy_username(),
        app_version=APP_VERSION,
        recordatorios=recordatorios,
        data_dir=DATA_DIR,
        backup_dir=BACKUP_DIR,
    )


def _normalizar_telefono_contacto(raw):
    texto = str(raw or "").strip()
    dig = re.sub(r"\D+", "", texto)
    if not dig:
        return ""
    if dig.startswith("56"):
        return dig
    if len(dig) == 9 and dig.startswith("9"):
        return f"56{dig}"
    if len(dig) >= 8:
        return dig
    return ""


def _parse_fecha_iso_segura(raw):
    valor = str(raw or "").strip()
    if not valor:
        return None
    base = valor.split(" ")[0].strip()
    try:
        return datetime.strptime(base, "%Y-%m-%d").date()
    except ValueError:
        return None


def _label_origen_evento(item):
    fuente = str(item.get("fuente") or "").strip().lower()
    origen = str(item.get("origen") or "").strip().lower()
    tipo = str(item.get("tipo") or "").strip().lower()
    if fuente == "agenda":
        if origen == "tienda_online":
            return "Agendo torta (tienda online)"
        return "Agendo torta (manual)" if tipo == "torta" else "Agenda manual"
    if fuente == "venta":
        if origen == "tienda_online":
            return "Compra tienda online"
        if origen == "presencial":
            return "Compra presencial"
        if origen == "uber_eats":
            return "Compra Uber Eats"
        if origen == "pedidosya":
            return "Compra PedidosYa"
        return f"Compra {origen}" if origen else "Compra"
    if fuente == "cliente_tienda":
        return "Registro cliente tienda"
    return "Registro"


def _split_detalle_lineas(raw_texto, titulo_default="Detalle"):
    texto = str(raw_texto or "").replace("\r", "\n")
    if not texto.strip():
        return [{"titulo": titulo_default, "items": ["-"]}]
    secciones = []
    actual = {"titulo": titulo_default, "items": []}
    for linea in [ln.strip() for ln in texto.split("\n") if ln.strip()]:
        if linea.endswith(":") and len(linea) <= 90:
            if actual["items"]:
                secciones.append(actual)
            actual = {"titulo": linea[:-1].strip() or titulo_default, "items": []}
            continue
        if str(actual.get("titulo") or "").strip().lower() == "builder json":
            continue
        actual["items"].append(linea)
    if actual["items"]:
        secciones.append(actual)
    return secciones or [{"titulo": titulo_default, "items": ["-"]}]


@app.route('/settings/clientes')
def settings_clientes():
    return render_template('settings_clientes.html', app_version=APP_VERSION)


@app.route('/api/clientes/registro', methods=['GET'])
def api_clientes_registro():
    try:
        q = str(request.args.get('q') or '').strip().lower()
        fecha_desde = str(request.args.get('desde') or '').strip()
        fecha_hasta = str(request.args.get('hasta') or '').strip()
        tipo_filtro = str(request.args.get('tipo') or 'todos').strip().lower()

        desde_dt = _parse_fecha_iso_segura(fecha_desde) if fecha_desde else None
        hasta_dt = _parse_fecha_iso_segura(fecha_hasta) if fecha_hasta else None
        if desde_dt and hasta_dt and desde_dt > hasta_dt:
            desde_dt, hasta_dt = hasta_dt, desde_dt

        conn = get_db()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                'agenda' AS fuente,
                id,
                COALESCE(NULLIF(TRIM(cliente), ''), '') AS nombre,
                LOWER(TRIM(COALESCE(cliente_email, ''))) AS email,
                TRIM(COALESCE(cliente_telefono, telefono, '')) AS telefono,
                COALESCE(NULLIF(TRIM(tipo), ''), 'torta') AS tipo,
                CASE
                    WHEN LOWER(COALESCE(ingredientes, '')) LIKE '%reserva desde tienda online%' THEN 'tienda_online'
                    ELSE 'manual'
                END AS origen,
                COALESCE(NULLIF(TRIM(fecha), ''), '') AS fecha_objetivo,
                COALESCE(NULLIF(TRIM(creado), ''), '') AS fecha_registro,
                COALESCE(NULLIF(TRIM(codigo_pedido), ''), '') AS codigo_ref,
                COALESCE(total, 0) AS total_ref
            FROM agenda_eventos
            WHERE COALESCE(NULLIF(TRIM(cliente), ''), NULLIF(TRIM(telefono), ''), NULLIF(TRIM(cliente_telefono), ''), NULLIF(TRIM(cliente_email), '')) IS NOT NULL
            """
        )
        agenda_rows = [dict(r) for r in cursor.fetchall()]

        cursor.execute(
            """
            SELECT
                'venta' AS fuente,
                id,
                COALESCE(NULLIF(TRIM(cliente_nombre), ''), '') AS nombre,
                LOWER(TRIM(COALESCE(cliente_email, ''))) AS email,
                TRIM(COALESCE(cliente_telefono, '')) AS telefono,
                'venta' AS tipo,
                LOWER(TRIM(COALESCE(canal_venta, 'presencial'))) AS origen,
                COALESCE(SUBSTR(fecha_hora, 1, 10), '') AS fecha_objetivo,
                COALESCE(NULLIF(TRIM(fecha_hora), ''), '') AS fecha_registro,
                COALESCE(NULLIF(TRIM(codigo_pedido), ''), '') AS codigo_ref,
                COALESCE(total_monto, 0) AS total_ref
            FROM ventas
            WHERE COALESCE(NULLIF(TRIM(cliente_nombre), ''), NULLIF(TRIM(cliente_telefono), ''), NULLIF(TRIM(cliente_email), '')) IS NOT NULL
            """
        )
        venta_rows = [dict(r) for r in cursor.fetchall()]

        cursor.execute(
            """
            SELECT
                'cliente_tienda' AS fuente,
                id,
                COALESCE(NULLIF(TRIM(nombre), ''), '') AS nombre,
                LOWER(TRIM(COALESCE(email, ''))) AS email,
                TRIM(COALESCE(telefono, '')) AS telefono,
                'registro' AS tipo,
                'tienda_online' AS origen,
                COALESCE(SUBSTR(creado_en, 1, 10), '') AS fecha_objetivo,
                COALESCE(NULLIF(TRIM(creado_en), ''), '') AS fecha_registro,
                '' AS codigo_ref,
                0 AS total_ref
            FROM tienda_clientes
            WHERE COALESCE(NULLIF(TRIM(nombre), ''), NULLIF(TRIM(telefono), ''), NULLIF(TRIM(email), '')) IS NOT NULL
            """
        )
        cliente_rows = [dict(r) for r in cursor.fetchall()]
        conn.close()

        interacciones_raw = agenda_rows + venta_rows + cliente_rows
        interacciones = []
        for row in interacciones_raw:
            nombre = str(row.get('nombre') or '').strip()
            email = str(row.get('email') or '').strip().lower()
            tel = str(row.get('telefono') or '').strip()
            tel_norm = _normalizar_telefono_contacto(tel)
            fecha_obj = str(row.get('fecha_objetivo') or '').strip()
            fecha_reg = str(row.get('fecha_registro') or '').strip() or fecha_obj
            label = _label_origen_evento(row)
            row_tipo = str(row.get('tipo') or '').strip().lower()
            tipo_filter_key = 'otro'
            if row.get('fuente') == 'agenda':
                tipo_filter_key = 'agenda_torta'
            elif row.get('fuente') == 'venta':
                origen = str(row.get('origen') or '').strip().lower()
                tipo_filter_key = 'compra_presencial' if origen == 'presencial' else ('compra_online' if origen == 'tienda_online' else 'compra_app')
            elif row.get('fuente') == 'cliente_tienda':
                tipo_filter_key = 'registro_tienda'

            fecha_ref_dt = _parse_fecha_iso_segura(fecha_obj) or _parse_fecha_iso_segura(fecha_reg)
            if desde_dt and (not fecha_ref_dt or fecha_ref_dt < desde_dt):
                continue
            if hasta_dt and (not fecha_ref_dt or fecha_ref_dt > hasta_dt):
                continue

            searchable = " ".join([
                nombre,
                email,
                tel,
                tel_norm,
                label,
                str(row.get('codigo_ref') or ''),
            ]).lower()
            if q and q not in searchable:
                continue
            if tipo_filtro != 'todos' and tipo_filtro != tipo_filter_key:
                continue

            interacciones.append({
                'fuente': row.get('fuente'),
                'id': int(row.get('id') or 0),
                'nombre': nombre,
                'email': email,
                'telefono': tel,
                'telefono_norm': tel_norm,
                'tipo': row_tipo,
                'origen': str(row.get('origen') or '').strip().lower(),
                'tipo_label': label,
                'tipo_filtro': tipo_filter_key,
                'fecha_objetivo': fecha_obj,
                'fecha_registro': fecha_reg,
                'codigo_ref': str(row.get('codigo_ref') or '').strip(),
                'total_ref': float(row.get('total_ref') or 0),
            })

        interacciones.sort(key=lambda x: (x.get('fecha_registro') or x.get('fecha_objetivo') or '', x.get('id') or 0), reverse=True)

        clientes_map = {}
        for it in interacciones:
            key = (it.get('email') or '').strip().lower() or (it.get('telefono_norm') or '').strip() or f"tmp-{it.get('fuente')}-{it.get('id')}"
            c = clientes_map.get(key)
            if not c:
                c = {
                    'key': key,
                    'nombre': it.get('nombre') or '',
                    'email': it.get('email') or '',
                    'telefono': it.get('telefono') or '',
                    'telefono_norm': it.get('telefono_norm') or '',
                    'ultima_fecha': it.get('fecha_registro') or it.get('fecha_objetivo') or '',
                    'ult_tipo': it.get('tipo_label') or '',
                    'total_interacciones': 0,
                    'interacciones': [],
                }
                clientes_map[key] = c
            if not c['nombre'] and it.get('nombre'):
                c['nombre'] = it.get('nombre')
            if not c['email'] and it.get('email'):
                c['email'] = it.get('email')
            if not c['telefono'] and it.get('telefono'):
                c['telefono'] = it.get('telefono')
                c['telefono_norm'] = it.get('telefono_norm') or c['telefono_norm']
            c['total_interacciones'] += 1
            c['interacciones'].append(it)

        clientes = list(clientes_map.values())
        clientes.sort(key=lambda x: x.get('ultima_fecha') or '', reverse=True)
        for c in clientes:
            phone = str(c.get('telefono_norm') or '').strip()
            c['whatsapp_url'] = f"https://wa.me/{phone}" if phone else ''

        return jsonify({
            'success': True,
            'clientes': clientes,
            'resumen': {
                'total_clientes': len(clientes),
                'total_interacciones': len(interacciones),
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e), 'clientes': [], 'resumen': {'total_clientes': 0, 'total_interacciones': 0}}), 500


@app.route('/api/clientes/registro/detalle', methods=['GET'])
def api_clientes_registro_detalle():
    conn = None
    try:
        fuente = str(request.args.get('fuente') or '').strip().lower()
        item_id = int(request.args.get('id') or 0)
        if fuente not in ('agenda', 'venta') or item_id <= 0:
            return jsonify({"success": False, "error": "Parametros invalidos"}), 400

        conn = get_db()
        cursor = conn.cursor()

        if fuente == 'agenda':
            cursor.execute(
                """
                SELECT id, fecha, hora_inicio, hora_fin, hora_entrega, tipo, titulo, cliente, telefono,
                       cliente_email, cliente_telefono, direccion, es_envio, ingredientes, total, abono,
                       motivo, estado, codigo_pedido, creado
                FROM agenda_eventos
                WHERE id = ?
                LIMIT 1
                """,
                (item_id,),
            )
            row = cursor.fetchone()
            if not row:
                return jsonify({"success": False, "error": "Evento no encontrado"}), 404
            ev = dict(row)
            nombre = str(ev.get("cliente") or "").strip() or "Cliente"
            telefono = str(ev.get("cliente_telefono") or ev.get("telefono") or "").strip()
            email = str(ev.get("cliente_email") or "").strip()
            total = float(ev.get("total") or 0)
            abono = float(ev.get("abono") or 0)
            secciones = _split_detalle_lineas(ev.get("ingredientes") or "", "Detalle pedido")
            resumen = [
                {"label": "Cliente", "value": nombre},
                {"label": "Telefono", "value": telefono or "-"},
                {"label": "Correo", "value": email or "-"},
                {"label": "Codigo pedido", "value": str(ev.get("codigo_pedido") or "-")},
                {"label": "Fecha solicitada", "value": str(ev.get("fecha") or "-")},
                {"label": "Hora inicio", "value": str(ev.get("hora_inicio") or "-")},
                {"label": "Hora entrega/retiro", "value": str(ev.get("hora_entrega") or "-")},
                {"label": "Entrega", "value": "Despacho" if int(ev.get("es_envio") or 0) == 1 else "Retiro"},
                {"label": "Direccion", "value": str(ev.get("direccion") or "-")},
                {"label": "Estado", "value": str(ev.get("estado") or "-")},
            ]
            return jsonify({
                "success": True,
                "detalle": {
                    "fuente": "agenda",
                    "titulo": str(ev.get("titulo") or "Pedido agenda"),
                    "subtitulo": str(ev.get("creado") or ""),
                    "total": total,
                    "abono": abono,
                    "resumen": resumen,
                    "secciones": secciones,
                }
            })

        cursor.execute(
            """
            SELECT id, fecha_hora, canal_venta, cliente_nombre, cliente_email, cliente_telefono,
                   codigo_pedido, codigo_operacion, total_monto, descuento_codigo, descuento_monto,
                   metodo_pago, observaciones
            FROM ventas
            WHERE id = ?
            LIMIT 1
            """,
            (item_id,),
        )
        row = cursor.fetchone()
        if not row:
            return jsonify({"success": False, "error": "Venta no encontrada"}), 404
        venta = dict(row)
        venta_id = int(venta.get("id") or 0)

        cursor.execute(
            """
            SELECT vd.producto_id, COALESCE(p.nombre, '') AS producto_nombre, vd.cantidad, vd.precio_unitario
            FROM venta_detalles vd
            LEFT JOIN productos p ON p.id = vd.producto_id
            WHERE vd.venta_id = ?
            ORDER BY vd.id ASC
            """,
            (venta_id,),
        )
        items_rows = [dict(r) for r in cursor.fetchall()]
        if not items_rows:
            cursor.execute(
                """
                SELECT vi.producto_id, COALESCE(vi.producto_nombre, '') AS producto_nombre, vi.cantidad, 0 AS precio_unitario
                FROM venta_items vi
                WHERE vi.venta_id = ?
                ORDER BY vi.id ASC
                """,
                (venta_id,),
            )
            items_rows = [dict(r) for r in cursor.fetchall()]

        items = []
        for it in items_rows:
            nombre = str(it.get("producto_nombre") or "").strip() or f"Producto #{int(it.get('producto_id') or 0)}"
            cantidad = int(it.get("cantidad") or 0)
            precio_u = float(it.get("precio_unitario") or 0)
            subtotal = float(max(0, cantidad) * max(0.0, precio_u))
            items.append({
                "nombre": nombre,
                "cantidad": cantidad,
                "precio_unitario": precio_u,
                "subtotal": subtotal,
            })

        total = float(venta.get("total_monto") or 0)
        descuento = float(venta.get("descuento_monto") or 0)
        resumen = [
            {"label": "Cliente", "value": str(venta.get("cliente_nombre") or "Cliente")},
            {"label": "Telefono", "value": str(venta.get("cliente_telefono") or "-")},
            {"label": "Correo", "value": str(venta.get("cliente_email") or "-")},
            {"label": "Canal", "value": str(venta.get("canal_venta") or "-")},
            {"label": "Fecha", "value": str(venta.get("fecha_hora") or "-")},
            {"label": "Codigo pedido", "value": str(venta.get("codigo_pedido") or "-")},
            {"label": "Codigo operacion", "value": str(venta.get("codigo_operacion") or "-")},
            {"label": "Metodo pago", "value": str(venta.get("metodo_pago") or "-")},
        ]
        secciones = [{
            "titulo": "Detalle de compra",
            "items": [
                f"{it['nombre']} | Cant: {it['cantidad']} | P/U: ${int(round(it['precio_unitario'])):,} | Subtotal: ${int(round(it['subtotal'])):,}".replace(",", ".")
                for it in items
            ] or ["-"]
        }]
        obs = str(venta.get("observaciones") or "").strip()
        if obs:
            secciones.append({"titulo": "Observaciones", "items": [obs]})

        return jsonify({
            "success": True,
            "detalle": {
                "fuente": "venta",
                "titulo": f"Compra #{venta_id}",
                "subtitulo": str(venta.get("fecha_hora") or ""),
                "total": total,
                "descuento": descuento,
                "resumen": resumen,
                "secciones": secciones,
            }
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


def _sanitize_admin_username(valor):
    raw = str(valor or "").strip().lower()
    raw = re.sub(r"[^a-z0-9._-]", "", raw)
    return raw[:50]


def _sanitize_admin_display_name(valor):
    return str(valor or "").strip()[:80]


@app.route('/api/admin/users', methods=['GET'])
def api_admin_users_list():
    try:
        conn = get_db()
        rows = conn.execute(
            """
            SELECT id, username, display_name, activo, creado_en, actualizado_en, last_login_at, last_login_ip
            FROM admin_users
            ORDER BY id ASC
            """
        ).fetchall()
        conn.close()
        users = []
        for row in rows:
            item = dict(row)
            item["activo"] = bool(item.get("activo"))
            users.append(item)
        return jsonify({
            "success": True,
            "legacy_user": _obtener_admin_legacy_username(),
            "legacy_pin_enabled": True,
            "items": users,
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e), "items": []}), 500


@app.route('/api/admin/users', methods=['POST'])
def api_admin_users_create():
    try:
        payload = request.get_json(silent=True) or {}
        username = _sanitize_admin_username(payload.get("username"))
        display_name = _sanitize_admin_display_name(payload.get("display_name") or username)
        password = str(payload.get("password") or "").strip()
        if len(username) < 3:
            return jsonify({"success": False, "error": "Usuario invalido (minimo 3 caracteres)."}), 400
        if len(password) < 6:
            return jsonify({"success": False, "error": "Clave invalida (minimo 6 caracteres)."}), 400

        conn = get_db()
        exists = conn.execute("SELECT id FROM admin_users WHERE LOWER(username)=? LIMIT 1", (username.lower(),)).fetchone()
        if exists:
            conn.close()
            return jsonify({"success": False, "error": "Ese usuario ya existe."}), 400
        hashed = generate_password_hash(password)
        conn.execute(
            """
            INSERT INTO admin_users (username, display_name, password_hash, activo)
            VALUES (?, ?, ?, 1)
            """,
            (username, display_name, hashed),
        )
        conn.commit()
        conn.close()
        _admin_audit_login(username=username, success=True, reason="admin_user_created")
        return jsonify({"success": True, "message": "Usuario admin creado."})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/admin/users/<int:user_id>/password', methods=['POST'])
def api_admin_users_change_password(user_id):
    try:
        payload = request.get_json(silent=True) or {}
        password = str(payload.get("password") or "").strip()
        if len(password) < 6:
            return jsonify({"success": False, "error": "Clave invalida (minimo 6 caracteres)."}), 400
        conn = get_db()
        row = conn.execute("SELECT id, username FROM admin_users WHERE id=? LIMIT 1", (user_id,)).fetchone()
        if not row:
            conn.close()
            return jsonify({"success": False, "error": "Usuario no encontrado."}), 404
        conn.execute(
            "UPDATE admin_users SET password_hash=?, actualizado_en=CURRENT_TIMESTAMP WHERE id=?",
            (generate_password_hash(password), user_id),
        )
        conn.commit()
        conn.close()
        _admin_audit_login(username=str(row["username"]), success=True, reason="admin_user_password_changed")
        return jsonify({"success": True, "message": "Clave actualizada."})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/admin/users/<int:user_id>/toggle', methods=['POST'])
def api_admin_users_toggle(user_id):
    try:
        conn = get_db()
        row = conn.execute("SELECT id, username, activo FROM admin_users WHERE id=? LIMIT 1", (user_id,)).fetchone()
        if not row:
            conn.close()
            return jsonify({"success": False, "error": "Usuario no encontrado."}), 404
        nuevo = 0 if int(row["activo"] or 0) == 1 else 1
        conn.execute("UPDATE admin_users SET activo=?, actualizado_en=CURRENT_TIMESTAMP WHERE id=?", (nuevo, user_id))
        conn.commit()
        conn.close()
        _admin_audit_login(username=str(row["username"]), success=True, reason=f"admin_user_toggle_{nuevo}")
        return jsonify({"success": True, "activo": bool(nuevo)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/admin/users/<int:user_id>', methods=['DELETE'])
def api_admin_users_delete(user_id):
    try:
        conn = get_db()
        total_row = conn.execute("SELECT COUNT(*) AS c FROM admin_users").fetchone()
        total = int((total_row["c"] if total_row else 0) or 0)
        row = conn.execute("SELECT id, username FROM admin_users WHERE id=? LIMIT 1", (user_id,)).fetchone()
        if not row:
            conn.close()
            return jsonify({"success": False, "error": "Usuario no encontrado."}), 404
        if total <= 1:
            conn.close()
            return jsonify({"success": False, "error": "Debe quedar al menos un usuario admin activo."}), 400
        conn.execute("DELETE FROM admin_users WHERE id=?", (user_id,))
        conn.commit()
        conn.close()
        _admin_audit_login(username=str(row["username"]), success=True, reason="admin_user_deleted")
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


_GITHUB_REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")


def _normalizar_repo_github(valor):
    repo = str(valor or "").strip().strip("/")
    if not repo:
        return ""
    return repo


def _version_tuple(valor):
    texto = str(valor or "").strip().lower().lstrip("v")
    if not texto:
        return ()
    match = re.search(r"\d+(?:\.\d+){0,3}", texto)
    if not match:
        return ()
    try:
        return tuple(int(p) for p in match.group(0).split("."))
    except Exception:
        return ()


def _github_json(url, token=""):
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": f"{APP_DISPLAY_NAME}-Updater/{APP_VERSION}",
    }
    token_txt = str(token or "").strip()
    if token_txt:
        headers["Authorization"] = f"Bearer {token_txt}"
    req = UrlRequest(url, headers=headers)
    with urlopen(req, timeout=20, context=ssl.create_default_context()) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
        return json.loads(raw)


def _github_request_json(method, url, token="", payload=None, headers=None):
    base_headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": f"{APP_DISPLAY_NAME}-Updater/{APP_VERSION}",
    }
    token_txt = str(token or "").strip()
    if token_txt:
        base_headers["Authorization"] = f"Bearer {token_txt}"
    if isinstance(headers, dict):
        base_headers.update(headers)

    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        base_headers.setdefault("Content-Type", "application/json; charset=utf-8")

    req = UrlRequest(url, data=data, headers=base_headers, method=method)
    try:
        with urlopen(req, timeout=30, context=ssl.create_default_context()) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            if not raw.strip():
                return {}
            return json.loads(raw)
    except HTTPError as e:
        detail = ""
        try:
            detail_raw = e.read().decode("utf-8", errors="replace")
            parsed = json.loads(detail_raw) if detail_raw else {}
            detail = parsed.get("message") or detail_raw
        except Exception:
            detail = str(e)
        raise ValueError(f"GitHub {e.code}: {detail}") from None


def _github_request_bytes(method, url, data_bytes, token="", headers=None):
    base_headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": f"{APP_DISPLAY_NAME}-Updater/{APP_VERSION}",
        "Content-Type": "application/octet-stream",
    }
    token_txt = str(token or "").strip()
    if token_txt:
        base_headers["Authorization"] = f"Bearer {token_txt}"
    if isinstance(headers, dict):
        base_headers.update(headers)

    req = UrlRequest(url, data=data_bytes, headers=base_headers, method=method)
    try:
        with urlopen(req, timeout=90, context=ssl.create_default_context()) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            if not raw.strip():
                return {}
            return json.loads(raw)
    except HTTPError as e:
        detail = ""
        try:
            detail_raw = e.read().decode("utf-8", errors="replace")
            parsed = json.loads(detail_raw) if detail_raw else {}
            detail = parsed.get("message") or detail_raw
        except Exception:
            detail = str(e)
        raise ValueError(f"GitHub {e.code}: {detail}") from None


def _obtener_release_github(repo, permitir_prerelease=False, token=""):
    repo_enc = quote(repo, safe="/")
    if permitir_prerelease:
        data = _github_json(f"https://api.github.com/repos/{repo_enc}/releases?per_page=10", token=token)
        if not isinstance(data, list):
            raise ValueError("Respuesta inesperada de GitHub")
        for item in data:
            if isinstance(item, dict) and not bool(item.get("draft")):
                return item
        raise ValueError("No se encontraron releases publicadas en el repositorio")
    release = _github_json(f"https://api.github.com/repos/{repo_enc}/releases/latest", token=token)
    if not isinstance(release, dict):
        raise ValueError("Respuesta inesperada de GitHub")
    return release


def _seleccionar_asset_release(release, asset_preferido=""):
    assets = release.get("assets") or []
    if not isinstance(assets, list) or not assets:
        return None

    preferido = str(asset_preferido or "").strip().lower()
    if preferido:
        for asset in assets:
            nombre = str(asset.get("name") or "").strip().lower()
            if nombre == preferido:
                return asset
        for asset in assets:
            nombre = str(asset.get("name") or "").strip().lower()
            if preferido in nombre:
                return asset

    for ext in (".exe", ".msi", ".zip"):
        for asset in assets:
            nombre = str(asset.get("name") or "").strip().lower()
            if nombre.endswith(ext):
                return asset
    return assets[0]


def _resolver_estado_updater(config=None):
    cfg = dict(obtener_config_updater())
    if isinstance(config, dict):
        cfg.update(config)

    repo = _normalizar_repo_github(cfg.get("github_repo"))
    if not repo:
        raise ValueError("Configura el repositorio GitHub (formato: usuario/repositorio)")
    if not _GITHUB_REPO_RE.match(repo):
        raise ValueError("Repositorio GitHub inválido. Usa formato usuario/repositorio")

    permitir_prerelease = bool(int(cfg.get("permitir_prerelease") or 0))
    token = str(cfg.get("github_token") or "").strip()
    asset_preferido = str(cfg.get("release_asset") or "").strip()

    release = _obtener_release_github(repo, permitir_prerelease=permitir_prerelease, token=token)
    asset = _seleccionar_asset_release(release, asset_preferido=asset_preferido)

    tag = str(release.get("tag_name") or "").strip()
    titulo = str(release.get("name") or "").strip() or tag
    published_at = str(release.get("published_at") or "").strip()

    current_v = _version_tuple(APP_VERSION)
    latest_v = _version_tuple(tag or titulo)
    update_available = False
    if latest_v:
        update_available = (not current_v) or (latest_v > current_v)
    elif (tag or titulo):
        update_available = str(tag or titulo).strip().lower() != str(APP_VERSION).strip().lower()

    return {
        "repo": repo,
        "tag": tag,
        "titulo": titulo,
        "published_at": published_at,
        "url_release": str(release.get("html_url") or "").strip(),
        "asset": asset or {},
        "asset_name": str((asset or {}).get("name") or "").strip(),
        "asset_api_url": str((asset or {}).get("url") or "").strip(),
        "asset_url": str((asset or {}).get("browser_download_url") or "").strip(),
        "asset_size": int((asset or {}).get("size") or 0),
        "update_available": bool(update_available),
        "current_version": str(APP_VERSION),
    }


@app.route('/api/updater/config', methods=['GET'])
def api_updater_config_get():
    try:
        cfg = obtener_config_updater()
        has_token = bool(str(cfg.get("github_token") or "").strip())
        cfg["github_token"] = "***" if has_token else ""
        cfg["has_token"] = has_token
        return jsonify({"success": True, "config": cfg, "app_version": APP_VERSION})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/updater/config', methods=['POST'])
def api_updater_config_save():
    try:
        payload = request.get_json(silent=True) or {}
        actual = obtener_config_updater()
        token_in = payload.get("github_token")
        if token_in == "__KEEP__":
            payload["github_token"] = actual.get("github_token") or ""
        cfg = guardar_config_updater(payload)
        cfg["github_token"] = "***" if str(cfg.get("github_token") or "").strip() else ""
        return jsonify({"success": True, "config": cfg})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400


@app.route('/api/updater/check', methods=['POST'])
def api_updater_check():
    try:
        payload = request.get_json(silent=True) or {}
        estado = _resolver_estado_updater(config=payload)
        return jsonify({"success": True, **estado})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400


@app.route('/api/updater/download', methods=['POST'])
def api_updater_download():
    try:
        payload = request.get_json(silent=True) or {}
        estado = _resolver_estado_updater(config=payload)
        cfg = dict(obtener_config_updater())
        token = str(cfg.get("github_token") or "").strip()
        asset_api_url = str(estado.get("asset_api_url") or "").strip()
        asset_browser_url = str(estado.get("asset_url") or "").strip()
        # Para repos privados, browser_download_url suele devolver 404 sin sesión web.
        # Priorizamos la URL API del asset, que acepta Authorization con PAT.
        asset_url = asset_api_url or asset_browser_url
        if not asset_url:
            raise ValueError("La release no tiene un archivo descargable")

        updater_dir = os.path.join(DATA_DIR, "updater_cache")
        os.makedirs(updater_dir, exist_ok=True)
        file_name = secure_filename(estado.get("asset_name") or "update.bin") or "update.bin"
        file_path = os.path.abspath(os.path.join(updater_dir, file_name))

        headers = {
            "Accept": "application/octet-stream",
            "User-Agent": f"{APP_DISPLAY_NAME}-Updater/{APP_VERSION}",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        req = UrlRequest(asset_url, headers=headers)
        with urlopen(req, timeout=60, context=ssl.create_default_context()) as resp:
            with open(file_path, "wb") as out:
                shutil.copyfileobj(resp, out)

        return jsonify(
            {
                "success": True,
                "file_path": file_path,
                "file_name": file_name,
                "file_size": os.path.getsize(file_path),
                "tag": estado.get("tag"),
                "update_available": bool(estado.get("update_available")),
            }
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400


@app.route('/api/updater/apply', methods=['POST'])
def api_updater_apply():
    try:
        payload = request.get_json(silent=True) or {}
        file_path = os.path.abspath(str(payload.get("file_path") or "").strip())
        if not file_path or not os.path.isfile(file_path):
            raise ValueError("No se encontró el archivo descargado para actualizar")

        if not getattr(sys, "frozen", False):
            os.startfile(file_path)
            return jsonify(
                {
                    "success": True,
                    "manual": True,
                    "message": "Actualización descargada. Se abrió el archivo para instalación manual.",
                }
            )

        updater_dir = os.path.abspath(os.path.join(DATA_DIR, "updater_cache"))
        if not file_path.startswith(updater_dir + os.sep):
            raise ValueError("Ruta de actualización inválida")

        target_exe = os.path.abspath(sys.executable)
        script_path = os.path.join(updater_dir, "apply_update.cmd")
        current_pid = os.getpid()

        script = (
            "@echo off\n"
            "setlocal\n"
            f"set \"TARGET={target_exe}\"\n"
            f"set \"SOURCE={file_path}\"\n"
            f"set \"PID={current_pid}\"\n"
            "timeout /t 2 /nobreak >nul\n"
            ":wait_loop\n"
            "tasklist /FI \"PID eq %PID%\" 2>NUL | find \"%PID%\" >NUL\n"
            "if not errorlevel 1 (\n"
            "  timeout /t 1 /nobreak >nul\n"
            "  goto wait_loop\n"
            ")\n"
            "copy /Y \"%SOURCE%\" \"%TARGET%\" >nul\n"
            "start \"\" \"%TARGET%\"\n"
            "exit /b 0\n"
        )
        with open(script_path, "w", encoding="utf-8") as f:
            f.write(script)

        creation_flags = 0
        creation_flags |= int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
        creation_flags |= int(getattr(subprocess, "DETACHED_PROCESS", 0))
        subprocess.Popen(
            ["cmd", "/c", "start", "", script_path],
            creationflags=creation_flags,
            close_fds=True,
        )

        def _close_app_delayed():
            time.sleep(1.2)
            os._exit(0)

        threading.Thread(target=_close_app_delayed, daemon=True).start()
        return jsonify({"success": True, "restarting": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400


@app.route('/api/updater/publish', methods=['POST'])
def api_updater_publish():
    try:
        payload = request.get_json(silent=True) or {}
        cfg = dict(obtener_config_updater())
        if isinstance(payload, dict):
            payload_norm = dict(payload)
            if str(payload_norm.get("github_token") or "").strip() == "__KEEP__":
                payload_norm.pop("github_token", None)
            cfg.update(payload_norm)

        repo = _normalizar_repo_github(cfg.get("github_repo"))
        if not repo or not _GITHUB_REPO_RE.match(repo):
            raise ValueError("Repositorio GitHub inválido. Usa formato usuario/repositorio")

        token = str(cfg.get("github_token") or "").strip()
        if not token:
            raise ValueError("Falta GitHub Token en Configuración para publicar releases")

        asset_name = secure_filename(str(cfg.get("release_asset") or "").strip()) or "GestionStockPro.exe"
        tag_name = str(payload.get("tag") or f"v{APP_VERSION}").strip()
        if not tag_name:
            tag_name = f"v{APP_VERSION}"
        release_name = str(payload.get("name") or tag_name).strip() or tag_name
        release_body = str(payload.get("notes") or f"Release automática {tag_name}").strip()
        prerelease = bool(payload.get("prerelease", False))

        file_path_in = os.path.abspath(str(payload.get("file_path") or "").strip()) if payload.get("file_path") else ""
        candidates = [
            file_path_in,
            os.path.abspath(os.path.join(os.path.dirname(__file__), "dist", asset_name)),
            os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dist", asset_name)),
            os.path.abspath(os.path.join(os.getcwd(), "dist", asset_name)),
        ]
        file_path = next((p for p in candidates if p and os.path.isfile(p)), "")
        if not file_path:
            raise ValueError(
                f"No se encontró el instalador para publicar ({asset_name}). Genera build primero."
            )

        repo_enc = quote(repo, safe="/")
        release = None
        try:
            release = _github_request_json(
                "GET",
                f"https://api.github.com/repos/{repo_enc}/releases/tags/{quote(tag_name, safe='')}",
                token=token,
            )
        except ValueError as e:
            if "404" not in str(e):
                raise

        if not release:
            release = _github_request_json(
                "POST",
                f"https://api.github.com/repos/{repo_enc}/releases",
                token=token,
                payload={
                    "tag_name": tag_name,
                    "name": release_name,
                    "body": release_body,
                    "draft": False,
                    "prerelease": prerelease,
                },
            )

        assets = release.get("assets") or []
        for asset in assets:
            if str(asset.get("name") or "").strip().lower() == asset_name.lower():
                asset_id = int(asset.get("id") or 0)
                if asset_id > 0:
                    _github_request_json(
                        "DELETE",
                        f"https://api.github.com/repos/{repo_enc}/releases/assets/{asset_id}",
                        token=token,
                    )

        upload_url_tpl = str(release.get("upload_url") or "").strip()
        if not upload_url_tpl:
            raise ValueError("No se obtuvo upload_url de la release en GitHub")
        upload_url = upload_url_tpl.split("{", 1)[0] + f"?name={quote(asset_name, safe='')}"

        with open(file_path, "rb") as f:
            data_bytes = f.read()
        uploaded = _github_request_bytes("POST", upload_url, data_bytes, token=token)

        return jsonify(
            {
                "success": True,
                "repo": repo,
                "tag": tag_name,
                "release_url": str(release.get("html_url") or "").strip(),
                "asset_name": str(uploaded.get("name") or asset_name),
                "asset_size": int(uploaded.get("size") or len(data_bytes)),
                "file_path": file_path,
            }
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400


def _resolver_contexto_sii_facturas():
    anios_sii = obtener_anios_tributarios_disponibles()
    if not anios_sii:
        anios_sii = [int(datetime.now().year)]

    sii_anio = request.args.get('sii_anio')
    sii_iva_pct = request.args.get('sii_iva_pct')
    sii_comision_apps_pct = request.args.get('sii_comision_apps_pct')
    sii_ppm_pct = request.args.get('sii_ppm_pct')

    if sii_anio in (None, ""):
        sii_anio = str(anios_sii[0])
    if sii_iva_pct in (None, ""):
        sii_iva_pct = "19"
    if sii_comision_apps_pct in (None, ""):
        sii_comision_apps_pct = "30"
    if sii_ppm_pct in (None, ""):
        sii_ppm_pct = "0"

    sii_resumen = obtener_resumen_sii_facturas(
        anio=sii_anio,
        iva_pct=sii_iva_pct,
        comision_apps_pct=sii_comision_apps_pct,
        ppm_pct=sii_ppm_pct,
    )

    return {
        "sii_anios": anios_sii,
        "sii_params": {
            "anio": int(sii_resumen.get("anio") or int(sii_anio)),
            "iva_pct": float(sii_resumen.get("tasas", {}).get("iva_pct") or float(sii_iva_pct)),
            "comision_apps_pct": float(
                sii_resumen.get("tasas", {}).get("comision_apps_pct") or float(sii_comision_apps_pct)
            ),
            "ppm_pct": float(sii_resumen.get("tasas", {}).get("ppm_pct") or float(sii_ppm_pct)),
        },
        "sii_resumen": sii_resumen,
    }


@app.route('/facturas')
def facturas():
    try:
        proveedor = (request.args.get('proveedor') or '').strip()
        mes = (request.args.get('mes') or '').strip()
        buscar = (request.args.get('q') or '').strip()

        facturas_data = obtener_facturas_archivadas(proveedor=proveedor, mes=mes, busqueda=buscar)
        filtros = obtener_filtros_facturas()
        total_archivos = len(facturas_data)
        total_bytes = sum(int(f.get('archivo_bytes') or 0) for f in facturas_data)
        total_monto = sum(float(f.get('monto_total') or 0) for f in facturas_data)

        return render_template(
            'facturas.html',
            modo_sii=False,
            facturas=facturas_data,
            proveedores=filtros.get('proveedores', []),
            meses=filtros.get('meses', []),
            filtro_proveedor=proveedor,
            filtro_mes=mes,
            filtro_q=buscar,
            total_archivos=total_archivos,
            total_bytes=total_bytes,
            total_monto=total_monto,
            facturas_dir=FACTURAS_DIR,
        )
    except Exception as e:
        return f"Error: {e}", 500


@app.route('/facturas/sii')
def facturas_sii():
    try:
        sii_ctx = _resolver_contexto_sii_facturas()
        return render_template(
            'facturas.html',
            modo_sii=True,
            facturas=[],
            proveedores=[],
            meses=[],
            filtro_proveedor='',
            filtro_mes='',
            filtro_q='',
            total_archivos=0,
            total_bytes=0,
            total_monto=0,
            facturas_dir=FACTURAS_DIR,
            **sii_ctx,
        )
    except Exception as e:
        return f"Error: {e}", 500


@app.route('/api/facturas/listado')
def api_listado_facturas():
    try:
        proveedor = (request.args.get('proveedor') or '').strip()
        mes = (request.args.get('mes') or '').strip()
        buscar = (request.args.get('q') or '').strip()
        facturas_data = obtener_facturas_archivadas(proveedor=proveedor, mes=mes, busqueda=buscar)
        total_archivos = len(facturas_data)
        total_bytes = sum(int(f.get('archivo_bytes') or 0) for f in facturas_data)
        total_monto = sum(float(f.get('monto_total') or 0) for f in facturas_data)
        return jsonify({
            'success': True,
            'facturas': facturas_data,
            'totales': {
                'archivos': total_archivos,
                'bytes': total_bytes,
                'monto': total_monto,
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'facturas': [], 'error': str(e)}), 500


@app.route('/api/facturas/sii-resumen')
def api_facturas_sii_resumen():
    try:
        anio = request.args.get('anio')
        iva_pct = request.args.get('iva_pct')
        comision_apps_pct = request.args.get('comision_apps_pct')
        ppm_pct = request.args.get('ppm_pct')

        resumen = obtener_resumen_sii_facturas(
            anio=anio,
            iva_pct=iva_pct,
            comision_apps_pct=comision_apps_pct,
            ppm_pct=ppm_pct,
        )
        return jsonify(
            {
                'success': True,
                'resumen': resumen,
                'anios_disponibles': obtener_anios_tributarios_disponibles(),
            }
        )
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/facturas/sii-ajustes', methods=['POST'])
def api_facturas_sii_ajustes_guardar():
    try:
        payload = request.get_json(silent=True) or {}
        anio = payload.get('anio')
        iva_pct = payload.get('iva_pct')
        comision_apps_pct = payload.get('comision_apps_pct')
        ppm_pct = payload.get('ppm_pct')
        ajustes = payload.get('ajustes') or []

        resultado = guardar_ajustes_sii_facturas(anio=anio, ajustes=ajustes)
        resumen = obtener_resumen_sii_facturas(
            anio=anio,
            iva_pct=iva_pct,
            comision_apps_pct=comision_apps_pct,
            ppm_pct=ppm_pct,
        )
        return jsonify(
            {
                'success': True,
                'resultado': resultado,
                'resumen': resumen,
            }
        )
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/facturas/sii-ajustes/limpiar', methods=['POST'])
def api_facturas_sii_ajustes_limpiar():
    try:
        payload = request.get_json(silent=True) or {}
        anio = payload.get('anio')
        iva_pct = payload.get('iva_pct')
        comision_apps_pct = payload.get('comision_apps_pct')
        ppm_pct = payload.get('ppm_pct')

        resultado = limpiar_ajustes_sii_facturas(anio=anio)
        resumen = obtener_resumen_sii_facturas(
            anio=anio,
            iva_pct=iva_pct,
            comision_apps_pct=comision_apps_pct,
            ppm_pct=ppm_pct,
        )
        return jsonify(
            {
                'success': True,
                'resultado': resultado,
                'resumen': resumen,
            }
        )
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/facturas/sii-resumen.csv')
def api_facturas_sii_resumen_csv():
    try:
        anio = request.args.get('anio')
        iva_pct = request.args.get('iva_pct')
        comision_apps_pct = request.args.get('comision_apps_pct')
        ppm_pct = request.args.get('ppm_pct')

        resumen = obtener_resumen_sii_facturas(
            anio=anio,
            iva_pct=iva_pct,
            comision_apps_pct=comision_apps_pct,
            ppm_pct=ppm_pct,
        )
        mensual = resumen.get('mensual') or []

        buffer = StringIO()
        writer = csv.writer(buffer)
        writer.writerow(
            [
                "Mes",
                "Documentos compra",
                "Ventas local",
                "Ventas Uber",
                "Ventas PedidosYa",
                "Ventas apps",
                "Ventas brutas",
                "Comision apps",
                "Ventas netas comision",
                "Ventas netas sin IVA",
                "Compras con IVA",
                "Compras sin IVA",
                "IVA debito estimado",
                "IVA credito estimado",
                "IVA neto estimado",
                "Remanente credito",
                "PPM estimado",
                "Resultado operacional",
                "Flujo post impuestos",
                "Estado IVA",
            ]
        )
        for row in mensual:
            writer.writerow(
                [
                    row.get("mes_label"),
                    row.get("documentos_compra"),
                    row.get("ventas_local"),
                    row.get("ventas_uber"),
                    row.get("ventas_pedidosya"),
                    row.get("ventas_apps"),
                    row.get("ventas_brutas"),
                    row.get("comision_apps"),
                    row.get("ventas_netas_comision"),
                    row.get("ventas_netas_sin_iva"),
                    row.get("compras_con_iva"),
                    row.get("compras_sin_iva"),
                    row.get("iva_debito_estimado"),
                    row.get("iva_credito_estimado"),
                    row.get("iva_neto_estimado"),
                    row.get("remanente_credito"),
                    row.get("ppm_estimado"),
                    row.get("resultado_operacional"),
                    row.get("flujo_post_impuestos"),
                    row.get("estado_iva"),
                ]
            )

        writer.writerow([])
        writer.writerow(["Resumen anual"])
        for key, value in (resumen.get("totales") or {}).items():
            writer.writerow([key, value])

        csv_content = buffer.getvalue()
        buffer.close()

        nombre = f"sii_resumen_{int(resumen.get('anio') or datetime.now().year)}.csv"
        resp = make_response(csv_content)
        resp.headers["Content-Type"] = "text/csv; charset=utf-8"
        resp.headers["Content-Disposition"] = f"attachment; filename={nombre}"
        return resp
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/facturas/<int:factura_id>/auditoria')
def api_auditoria_factura(factura_id):
    try:
        limite = _as_int(request.args.get('limit', 100) or 100, "límite", min_value=1)
        data = obtener_auditoria_factura(factura_id=factura_id, limite=limite)
        return jsonify({'success': True, 'auditoria': data})
    except ValueError as e:
        return jsonify({'success': False, 'auditoria': [], 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'auditoria': [], 'error': str(e)}), 500


@app.route('/api/facturas/subir', methods=['POST'])
def api_subir_facturas():
    try:
        proveedor = (request.form.get('proveedor') or '').strip()
        if not proveedor:
            return jsonify({'success': False, 'error': 'El proveedor es obligatorio'}), 400

        try:
            fecha_factura = _parse_fecha_factura(request.form.get('fecha_factura'))
        except ValueError as e:
            return jsonify({'success': False, 'error': str(e)}), 400

        numero_factura = (request.form.get('numero_factura') or '').strip()
        observacion = (request.form.get('observacion') or '').strip()
        monto_total = request.form.get('monto_total') or 0

        archivos = request.files.getlist('archivos')
        if not archivos or all(not a or not a.filename for a in archivos):
            return jsonify({'success': False, 'error': 'Debes seleccionar al menos un archivo'}), 400

        fecha_dt = datetime.strptime(fecha_factura, '%Y-%m-%d')
        proveedor_slug = _normalizar_nombre_carpeta(proveedor)
        base_dir = os.path.join(
            FACTURAS_DIR,
            f"{fecha_dt.year}",
            f"{fecha_dt.month:02d}",
            f"{fecha_dt.day:02d}",
            proveedor_slug,
        )
        os.makedirs(base_dir, exist_ok=True)

        creadas = []
        for archivo in archivos:
            if not archivo or not archivo.filename:
                continue

            original_name = os.path.basename(archivo.filename)
            ext = os.path.splitext(original_name)[1].lower()
            if ext not in ALLOWED_FACTURA_EXTENSIONS:
                return jsonify({'success': False, 'error': f'Formato no permitido: {original_name}'}), 400

            base_name = secure_filename(os.path.splitext(original_name)[0]) or 'factura'
            unique_name = f"{fecha_dt.strftime('%Y%m%d')}_{uuid.uuid4().hex[:8]}_{base_name}{ext}"
            abs_path = os.path.abspath(os.path.join(base_dir, unique_name))
            archivo.save(abs_path)
            _registrar_respaldo_local_factura(
                abs_path=abs_path,
                proveedor=proveedor,
                fecha_factura=fecha_factura,
                numero_factura=numero_factura,
                original_name=original_name,
            )

            archivo_bytes = os.path.getsize(abs_path) if os.path.exists(abs_path) else 0
            ruta_relativa = os.path.relpath(abs_path, FACTURAS_DIR).replace('\\', '/')

            registro = guardar_factura_archivo(
                {
                    'proveedor': proveedor,
                    'fecha_factura': fecha_factura,
                    'numero_factura': numero_factura,
                    'monto_total': monto_total,
                    'observacion': observacion,
                    'archivo_nombre_original': original_name,
                    'archivo_nombre_guardado': unique_name,
                    'archivo_ruta_relativa': ruta_relativa,
                    'archivo_extension': ext,
                    'archivo_mime': archivo.mimetype,
                    'archivo_bytes': archivo_bytes,
                }
            )
            if not registro.get('success'):
                try:
                    if os.path.exists(abs_path):
                        os.remove(abs_path)
                except Exception:
                    pass
                return jsonify({'success': False, 'error': registro.get('error', 'No se pudo registrar archivo')}), 500

            creadas.append(registro.get('id'))

        if not creadas:
            return jsonify({'success': False, 'error': 'No se subieron archivos válidos'}), 400

        crear_backup()
        return jsonify({'success': True, 'creadas': len(creadas)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/facturas/archivo/<int:factura_id>')
def ver_factura_archivo(factura_id):
    factura = obtener_factura_archivo(factura_id)
    if not factura:
        return "Factura no encontrada", 404

    try:
        abs_path, _base_factura = _resolver_ruta_factura(
            factura.get('archivo_ruta_relativa'),
            incluir_legadas=True,
        )
    except ValueError:
        return "Ruta inválida", 400

    if not os.path.exists(abs_path):
        return "Archivo no disponible", 404

    descargar = request.args.get('download') == '1'
    nombre_descarga = factura.get('archivo_nombre_original') or os.path.basename(abs_path)
    mime = factura.get('archivo_mime') or None
    return send_file(abs_path, as_attachment=descargar, download_name=nombre_descarga, mimetype=mime)


@app.route('/api/facturas/<int:factura_id>/eliminar', methods=['POST'])
def api_eliminar_factura(factura_id):
    resultado = eliminar_factura_archivo(factura_id)
    if not resultado.get('success'):
        msg = str(resultado.get('error') or '').lower()
        status = 404 if 'no encontrada' in msg or 'no encontrado' in msg else 400
        return jsonify(resultado), status

    factura = resultado.get('factura') or {}
    ruta_relativa = factura.get('archivo_ruta_relativa')
    if ruta_relativa:
        try:
            abs_path, base_dir = _resolver_ruta_factura(
                ruta_relativa,
                incluir_legadas=True,
            )
            if os.path.exists(abs_path):
                os.remove(abs_path)

            base = os.path.abspath(base_dir)
            carpeta = os.path.dirname(abs_path)
            while carpeta.startswith(base + os.sep):
                if os.path.isdir(carpeta) and not os.listdir(carpeta):
                    os.rmdir(carpeta)
                    carpeta = os.path.dirname(carpeta)
                    continue
                break
        except Exception:
            pass

    crear_backup()
    return jsonify({'success': True})


@app.route('/api/facturas/<int:factura_id>/actualizar', methods=['POST'])
def api_actualizar_factura(factura_id):
    payload = request.get_json(silent=True) or {}
    resultado = actualizar_factura_archivo(factura_id, payload)
    if not resultado.get('success'):
        mensaje = str(resultado.get('error') or '')
        status = 404 if 'no encontrada' in mensaje.lower() else 400
        return jsonify(resultado), status

    crear_backup()
    return jsonify({'success': True, 'factura': resultado.get('factura')})


@app.route('/api/backup/crear', methods=['POST'])
def crear_backup_manual():
    try:
        path = crear_backup(force=True)
        if path:
            return jsonify({'success': True, 'mensaje': 'Backup creado correctamente'})
        else:
            return jsonify({'success': False, 'error': 'No se pudo crear el backup'}), 500
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/backup/ultimo')
def obtener_ultimo_backup_api():
    try:
        ultimo = obtener_ultimo_backup()
        return jsonify({'ultimo': ultimo or 'Nunca'})
    except Exception:
        return jsonify({'ultimo': 'Nunca'})


@app.route('/api/backup/directorio')
def obtener_directorio_backup_api():
    try:
        return jsonify(
            {
                'success': True,
                'data_dir': os.path.abspath(DATA_DIR),
                'backup_dir': os.path.abspath(BACKUP_DIR),
            }
        )
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/backup/orquestacion-config', methods=['GET'])
def api_backup_orquestacion_get():
    try:
        cfg = _leer_backup_orquestacion()
        return jsonify({"success": True, "config": cfg})
    except Exception as e:
        return jsonify({"success": False, "error": str(e), "config": _backup_orquestacion_default()}), 500


@app.route('/api/backup/orquestacion-config', methods=['POST'])
def api_backup_orquestacion_save():
    try:
        payload = request.get_json(silent=True) or {}
        cfg = _guardar_backup_orquestacion(payload)
        return jsonify({"success": True, "config": cfg})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/backup/abrir-carpeta', methods=['POST'])
def abrir_carpeta_backup_api():
    try:
        tipo = (request.args.get('tipo') or '').strip().lower()
        target_dir = BACKUP_DIR if tipo == 'backup' else DATA_DIR
        target_dir = os.path.abspath(target_dir)
        os.makedirs(target_dir, exist_ok=True)

        if os.name == 'nt':
            os.startfile(target_dir)  # type: ignore[attr-defined]
        elif sys.platform == 'darwin':
            subprocess.Popen(['open', target_dir])
        else:
            subprocess.Popen(['xdg-open', target_dir])

        return jsonify({'success': True, 'path': target_dir})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/producto/<int:id>/agregar-lote', methods=['POST'])
def agregar_lote(id):
    """Agrega un lote nuevo a un producto existente"""
    try:
        cantidad = _as_int(request.form.get('cantidad') or 0, "cantidad de lote", min_value=1)
        
        # Calcular fecha de vencimiento
        vencimiento_cantidad = request.form.get('vencimiento_cantidad')
        vencimiento_tipo = request.form.get('vencimiento_tipo')
        
        fecha_vencimiento = None
        if vencimiento_cantidad and vencimiento_tipo:
            from datetime import datetime, timedelta
            cantidad_dias = _as_int(vencimiento_cantidad, "vencimiento de lote", min_value=1)
            hoy = datetime.now()
            
            if vencimiento_tipo == 'dias':
                fecha_venc = hoy + timedelta(days=cantidad_dias)
            elif vencimiento_tipo == 'semanas':
                fecha_venc = hoy + timedelta(weeks=cantidad_dias)
            elif vencimiento_tipo == 'meses':
                fecha_venc = hoy + timedelta(days=cantidad_dias * 30)
            else:
                return _error_or_text("Tipo de vencimiento inválido", 400)
            
            fecha_vencimiento = fecha_venc.strftime('%Y-%m-%d')
        
        resultado = agregar_lote_producto(id, cantidad, fecha_vencimiento)
        
        if resultado['success']:
            crear_backup()
            return _ok_or_redirect(
                {
                    'success': True,
                    'message': 'Lote agregado correctamente'
                },
                'productos'
            )
        else:
            return _error_or_text(resultado['error'], 400)
             
    except ValueError as e:
        return _error_or_text(e, 400)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return _error_or_text(e, 500)

@app.route('/api/producto/<int:id>/lotes')
def api_obtener_lotes(id):
    """API para obtener lotes de un producto (usado por AJAX)"""
    try:
        lotes = obtener_lotes_por_producto(id)
        producto = obtener_producto_detalle(id) or {}
        unidad_producto = _normalizar_unidad_producto(producto.get('unidad', 'unidad'))
        resultado = []
        
        for lote in lotes:
            dias = calcular_dias_restantes(lote['fecha_vencimiento'])
            estado = obtener_estado_lote(dias)
            
            resultado.append({
                'id': lote['id'],
                'cantidad': lote['cantidad'],
                'unidad': unidad_producto,
                'fecha_vencimiento': lote['fecha_vencimiento'],
                'dias_restantes': dias,
                'estado': estado['estado'],
                'emoji': estado['emoji'],
                'color': estado['color']
            })
        
        return jsonify({'success': True, 'lotes': resultado})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/lote/<int:lote_id>/eliminar', methods=['POST'])
def api_eliminar_lote(lote_id):
    """API para eliminar un lote específico"""
    try:
        resultado = eliminar_lote(lote_id)
        if resultado['success']:
            crear_backup()
            return jsonify({'success': True})
        else:
            msg = str(resultado.get('error') or '').lower()
            status = 404 if 'no encontrado' in msg else 400
            return jsonify({'success': False, 'error': resultado['error']}), status
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    
@app.route('/api/insumo/agregar', methods=['POST'])
def agregar_insumo():
    try:
        def _parse_optional_float(raw_value, field_name, min_value=None):
            raw = "" if raw_value is None else str(raw_value).strip()
            if raw == "":
                return None
            valor = _as_float(raw, field_name)
            if min_value is not None and valor < min_value:
                raise ValueError(f"{field_name} debe ser mayor o igual a {min_value}")
            return valor

        codigo_barra = request.form.get('codigo_barra', '').strip()
        nombre = (request.form.get('nombre_insumo') or '').strip()
        if not nombre:
            return _error_or_text("El nombre del insumo es obligatorio", 400)

        stock = _as_float(request.form.get('stock_insumo', 0) or 0, "stock inicial", min_value=0)
        stock_minimo = _as_float(request.form.get('stock_minimo', 1.0) or 1.0, "stock mínimo", min_value=0)
        unidad = _normalizar_unidad_producto(request.form.get('unidad', 'unidad'))
        
        precio_unitario = _as_float(request.form.get('precio_unitario', 0) or 0, "precio de compra", min_value=0)
        cantidad_comprada = _as_float(request.form.get('cantidad_comprada', 1) or 1, "cantidad comprada", min_value=0.0001)
        unidad_compra = _normalizar_unidad_producto(request.form.get('unidad_compra', unidad))
        precio_incluye_iva = 1 if request.form.get('precio_incluye_iva') == 'on' else 0
        cantidad_por_scan = _as_float(
            request.form.get('cantidad_por_scan', stock if stock > 0 else 1) or 1,
            "cantidad por escaneo",
            min_value=0.0001,
        )
        unidad_por_scan = _normalizar_unidad_producto(request.form.get('unidad_por_scan', unidad))
        nutricion_ref_cantidad = _parse_optional_float(
            request.form.get('nutricion_ref_cantidad'),
            "referencia nutricional",
            min_value=0.0001,
        )
        if nutricion_ref_cantidad is None:
            nutricion_ref_cantidad = 100.0

        nutricion_ref_unidad_raw = request.form.get('nutricion_ref_unidad')
        nutricion_ref_unidad = (
            _normalizar_unidad_producto(nutricion_ref_unidad_raw)
            if str(nutricion_ref_unidad_raw or '').strip()
            else None
        )
        if nutricion_ref_unidad and not _son_unidades_compatibles_porcion(unidad, nutricion_ref_unidad):
            return _error_or_text(
                f"La unidad nutricional ({nutricion_ref_unidad}) no es compatible con la unidad de stock ({unidad})",
                400,
            )

        nutricion_kcal = _parse_optional_float(request.form.get('nutricion_kcal'), "kcal", min_value=0)
        nutricion_proteinas_g = _parse_optional_float(request.form.get('nutricion_proteinas_g'), "proteínas", min_value=0)
        nutricion_carbohidratos_g = _parse_optional_float(request.form.get('nutricion_carbohidratos_g'), "carbohidratos", min_value=0)
        nutricion_grasas_g = _parse_optional_float(request.form.get('nutricion_grasas_g'), "grasas", min_value=0)
        nutricion_azucares_g = _parse_optional_float(request.form.get('nutricion_azucares_g'), "azúcares", min_value=0)
        nutricion_sodio_mg = _parse_optional_float(request.form.get('nutricion_sodio_mg'), "sodio", min_value=0)
        lote_codigo = str(request.form.get('lote_codigo') or '').strip() or None
        fecha_elaboracion = _as_optional_date(request.form.get('fecha_elaboracion'), "fecha de elaboración")
        fecha_vencimiento = _as_optional_date(request.form.get('fecha_vencimiento'), "fecha de vencimiento")
        if fecha_elaboracion and fecha_vencimiento and fecha_vencimiento < fecha_elaboracion:
            return _error_or_text("La fecha de vencimiento no puede ser anterior a la fecha de elaboración", 400)
        
        conn = get_db()
        cursor = conn.cursor()
        insumo_por_codigo = None
        if codigo_barra:
            insumo_por_codigo, _ = _buscar_insumo_por_codigo_cursor(cursor, codigo_barra)

        insumo_por_nombre = _buscar_insumo_por_nombre_cursor(cursor, nombre)

        if insumo_por_codigo and insumo_por_nombre and int(insumo_por_codigo["id"]) != int(insumo_por_nombre["id"]):
            conn.close()
            return _error_or_text(
                "El código y el nombre apuntan a insumos distintos. Revisa el registro para evitar mezclar lotes.",
                400,
            )

        insumo_existente = insumo_por_codigo or insumo_por_nombre
        if insumo_existente:
            insumo_id = int(insumo_existente["id"])
            unidad_existente = _normalizar_unidad_producto(insumo_existente["unidad"] or unidad)
            stock_convertido = float(stock or 0)
            if stock_convertido > 0 and unidad_existente != unidad:
                conversion = convert_amount(stock_convertido, unidad, unidad_existente, convertir_a_base)
                if not conversion["success"]:
                    conn.close()
                    return _error_or_text(
                        f"No se pudo agregar stock al insumo existente: {conversion['error']}",
                        400,
                    )
                stock_convertido = float(conversion["cantidad"] or 0)

            stock_anterior = float(insumo_existente["stock"] or 0)
            stock_nuevo = stock_anterior + stock_convertido
            codigo_guardado = str(insumo_existente["codigo_barra"] or "").strip()
            if not codigo_guardado and codigo_barra:
                codigo_guardado = codigo_barra

            cursor.execute(
                "UPDATE insumos SET stock = ?, codigo_barra = ? WHERE id = ?",
                (stock_nuevo, codigo_guardado or None, insumo_id),
            )
            if codigo_barra:
                _asociar_codigo_insumo_cursor(cursor, insumo_id, codigo_barra)

            if stock_convertido > 0:
                registrar_lote_insumo(
                    insumo_id,
                    stock_convertido,
                    lote_codigo=lote_codigo,
                    fecha_elaboracion=fecha_elaboracion,
                    fecha_vencimiento=fecha_vencimiento,
                    merge=False,
                    conn=conn,
                )
                registrar_movimiento_stock(
                    'insumo',
                    insumo_id,
                    'entrada_manual',
                    stock_convertido,
                    stock_anterior=stock_anterior,
                    stock_nuevo=stock_nuevo,
                    referencia_tipo='lote_manual',
                    detalle='Ingreso manual como nuevo lote de insumo existente',
                    conn=conn
                )

            conn.commit()
            conn.close()
            crear_backup()
            return _ok_or_redirect(
                {
                    'success': True,
                    'insumo_id': insumo_id,
                    'lote_agregado': bool(stock_convertido > 0),
                    'message': 'Lote agregado al insumo existente'
                    if stock_convertido > 0
                    else 'Insumo encontrado. No se agregó stock porque la cantidad es 0.'
                },
                'insumos'
            )

        if not codigo_barra:
            import time
            codigo_barra = f"GEN{int(time.time())}"

        cursor.execute(
            """INSERT INTO insumos (codigo_barra, nombre, stock, stock_minimo, unidad,
                                     precio_unitario, cantidad_comprada, unidad_compra, precio_incluye_iva,
                                     cantidad_por_scan, unidad_por_scan,
                                     nutricion_ref_cantidad, nutricion_ref_unidad,
                                     nutricion_kcal, nutricion_proteinas_g, nutricion_carbohidratos_g,
                                     nutricion_grasas_g, nutricion_azucares_g, nutricion_sodio_mg)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (codigo_barra, nombre, stock, stock_minimo, unidad,
             precio_unitario, cantidad_comprada, unidad_compra, precio_incluye_iva,
             cantidad_por_scan, unidad_por_scan,
             nutricion_ref_cantidad, nutricion_ref_unidad,
             nutricion_kcal, nutricion_proteinas_g, nutricion_carbohidratos_g,
             nutricion_grasas_g, nutricion_azucares_g, nutricion_sodio_mg)
        )
        insumo_id = cursor.lastrowid
        if codigo_barra:
            _asociar_codigo_insumo_cursor(cursor, insumo_id, codigo_barra)
        if stock > 0:
            registrar_lote_insumo(
                insumo_id,
                stock,
                lote_codigo=lote_codigo,
                fecha_elaboracion=fecha_elaboracion,
                fecha_vencimiento=fecha_vencimiento,
                merge=False,
                conn=conn,
            )
        registrar_historial_cambio(
            recurso_tipo='insumo',
            recurso_id=insumo_id,
            recurso_nombre=nombre,
            accion='agregado',
            detalle='Alta manual de insumo',
            origen_modulo='insumos',
            metadata={
                'stock_inicial': stock,
                'unidad': unidad,
                'stock_minimo': stock_minimo,
            },
            conn=conn,
        )
        registrar_movimiento_stock(
            'insumo',
            insumo_id,
            'entrada_manual',
            stock,
            stock_anterior=0,
            stock_nuevo=stock,
            referencia_tipo='alta_manual',
            detalle='Alta manual de insumo',
            conn=conn
        )
        conn.commit()
        conn.close()
        crear_backup()
        return _ok_or_redirect(
            {
                'success': True,
                'insumo_id': insumo_id,
                'message': 'Insumo agregado correctamente'
            },
            'insumos'
        )
    except ValueError as e:
        return _error_or_text(e, 400)
    except Exception as e:
        return _error_or_text(e, 500)
@app.route('/api/insumo/buscar')
def buscar_insumo_por_codigo():
    """Busca un insumo por codigo de barras o por nombre."""
    try:
        termino = (request.args.get('codigo') or '').strip()
        if not termino:
            termino = (request.args.get('q') or '').strip()
        if not termino:
            termino = (request.args.get('nombre') or '').strip()
        if not termino:
            return jsonify({'encontrado': False, 'error': 'Código o nombre vacío'})

        conn = get_db()
        cursor = conn.cursor()
        sql_base = '''
            SELECT id, codigo_barra, nombre, stock, unidad, stock_minimo,
                   precio_unitario, cantidad_comprada, unidad_compra, precio_incluye_iva,
                   cantidad_por_scan, unidad_por_scan,
                   nutricion_ref_cantidad, nutricion_ref_unidad,
                   nutricion_kcal, nutricion_proteinas_g, nutricion_carbohidratos_g,
                   nutricion_grasas_g, nutricion_azucares_g, nutricion_sodio_mg
            FROM insumos
        '''
        insumo, coincidencia = _buscar_insumo_por_codigo_cursor(cursor, termino)
        if not insumo:
            insumo = _buscar_insumo_por_nombre_cursor(cursor, termino)
            coincidencia = 'nombre' if insumo else None
        if not insumo:
            like = f"%{termino}%"
            cursor.execute(
                f"""
                {sql_base}
                WHERE nombre LIKE ? COLLATE NOCASE
                ORDER BY
                    CASE WHEN LOWER(TRIM(nombre)) = LOWER(TRIM(?)) THEN 0 ELSE 1 END ASC,
                    LENGTH(nombre) ASC,
                    id ASC
                LIMIT 1
                """,
                (like, termino),
            )
            insumo = cursor.fetchone()
            coincidencia = 'nombre_parcial' if insumo else None
        lote_ref = None
        if insumo:
            cursor.execute(
                """
                SELECT lote_codigo, fecha_elaboracion, fecha_vencimiento
                FROM insumo_lotes
                WHERE insumo_id = ?
                ORDER BY CASE WHEN cantidad > 0 THEN 0 ELSE 1 END ASC, id DESC
                LIMIT 1
                """,
                (insumo['id'],),
            )
            lote_ref = cursor.fetchone()
        conn.close()
        
        if insumo:
            return jsonify({
                'encontrado': True,
                'coincidencia': coincidencia,
                'insumo': {
                    'id': insumo['id'],
                    'codigo_barra': insumo['codigo_barra'],
                    'nombre': insumo['nombre'],
                    'stock': insumo['stock'],
                    'unidad': insumo['unidad'],
                    'stock_minimo': insumo['stock_minimo'],
                    'precio_unitario': insumo['precio_unitario'],
                    'cantidad_comprada': insumo['cantidad_comprada'],
                    'unidad_compra': insumo['unidad_compra'],
                    'precio_incluye_iva': insumo['precio_incluye_iva'],
                    'cantidad_por_scan': insumo['cantidad_por_scan'],
                    'unidad_por_scan': insumo['unidad_por_scan'] or insumo['unidad'],
                    'nutricion_ref_cantidad': insumo['nutricion_ref_cantidad'],
                    'nutricion_ref_unidad': insumo['nutricion_ref_unidad'],
                    'nutricion_kcal': insumo['nutricion_kcal'],
                    'nutricion_proteinas_g': insumo['nutricion_proteinas_g'],
                    'nutricion_carbohidratos_g': insumo['nutricion_carbohidratos_g'],
                    'nutricion_grasas_g': insumo['nutricion_grasas_g'],
                    'nutricion_azucares_g': insumo['nutricion_azucares_g'],
                    'nutricion_sodio_mg': insumo['nutricion_sodio_mg'],
                    'lote_codigo': lote_ref['lote_codigo'] if lote_ref else None,
                    'fecha_elaboracion': lote_ref['fecha_elaboracion'] if lote_ref else None,
                    'fecha_vencimiento': lote_ref['fecha_vencimiento'] if lote_ref else None,
                }
            })
        else:
            return jsonify({'encontrado': False})
            
    except Exception as e:
        return jsonify({'encontrado': False, 'error': str(e)})

@app.route('/api/insumo/crear-desde-escaner', methods=['POST'])
def crear_insumo_desde_escaner():
    """Crea un nuevo insumo desde el escáner con datos completos"""
    try:
        data = request.get_json()
        
        codigo_barra = data.get('codigo_barra', '').strip()
        nombre = data['nombre']
        stock = float(data.get('stock', 0))
        stock_minimo = float(data.get('stock_minimo', 1))
        unidad = data.get('unidad', 'unidad')
        lote_codigo = str(data.get('lote_codigo') or '').strip() or None
        fecha_elaboracion = _as_optional_date(data.get('fecha_elaboracion'), "fecha de elaboración")
        fecha_vencimiento = _as_optional_date(data.get('fecha_vencimiento'), "fecha de vencimiento")
        if fecha_elaboracion and fecha_vencimiento and fecha_vencimiento < fecha_elaboracion:
            return jsonify({'success': False, 'error': 'La fecha de vencimiento no puede ser anterior a la fecha de elaboración'}), 400
        
        conn = get_db()
        cursor = conn.cursor()

        insumo_por_codigo = None
        if codigo_barra:
            insumo_por_codigo, _ = _buscar_insumo_por_codigo_cursor(cursor, codigo_barra)
        insumo_por_nombre = _buscar_insumo_por_nombre_cursor(cursor, nombre)
        if insumo_por_codigo and insumo_por_nombre and int(insumo_por_codigo["id"]) != int(insumo_por_nombre["id"]):
            conn.close()
            return jsonify({'success': False, 'error': 'Código y nombre pertenecen a insumos distintos'}), 400

        insumo_existente = insumo_por_codigo or insumo_por_nombre
        if insumo_existente:
            unidad_existente = _normalizar_unidad_producto(insumo_existente["unidad"] or unidad)
            cantidad_sumar = float(stock or 0)
            if cantidad_sumar > 0 and unidad_existente != unidad:
                conversion = convert_amount(cantidad_sumar, unidad, unidad_existente, convertir_a_base)
                if not conversion["success"]:
                    conn.close()
                    return jsonify({'success': False, 'error': conversion["error"]}), 400
                cantidad_sumar = float(conversion["cantidad"] or 0)

            stock_anterior = float(insumo_existente["stock"] or 0)
            stock_nuevo = stock_anterior + cantidad_sumar
            codigo_guardado = str(insumo_existente["codigo_barra"] or "").strip()
            if not codigo_guardado and codigo_barra:
                codigo_guardado = codigo_barra

            cursor.execute(
                """
                UPDATE insumos
                SET codigo_barra = ?,
                    stock = ?,
                    precio_unitario = ?,
                    cantidad_comprada = ?,
                    unidad_compra = ?,
                    precio_incluye_iva = ?
                WHERE id = ?
                """,
                (
                    codigo_guardado or None,
                    stock_nuevo,
                    float(data.get('precio_unitario', insumo_existente['precio_unitario'] or 0)),
                    float(data.get('cantidad_comprada', insumo_existente['cantidad_comprada'] or 1)),
                    data.get('unidad_compra', insumo_existente['unidad_compra'] or unidad_existente),
                    data.get('precio_incluye_iva', insumo_existente['precio_incluye_iva'] or 1),
                    insumo_existente['id'],
                ),
            )
            if codigo_barra:
                _asociar_codigo_insumo_cursor(cursor, insumo_existente['id'], codigo_barra)

            if cantidad_sumar > 0:
                registrar_lote_insumo(
                    insumo_existente['id'],
                    cantidad_sumar,
                    lote_codigo=lote_codigo,
                    fecha_elaboracion=fecha_elaboracion,
                    fecha_vencimiento=fecha_vencimiento,
                    merge=False,
                    conn=conn,
                )
                registrar_movimiento_stock(
                    'insumo',
                    insumo_existente['id'],
                    'entrada_scanner',
                    cantidad_sumar,
                    stock_anterior=stock_anterior,
                    stock_nuevo=stock_nuevo,
                    referencia_tipo='scanner',
                    detalle='Ingreso por escáner como nuevo lote',
                    conn=conn
                )

            conn.commit()
            cursor.execute('SELECT * FROM insumos WHERE id = ?', (insumo_existente['id'],))
            insumo = cursor.fetchone()
            conn.close()
            crear_backup()
            return jsonify({
                'success': True,
                'id': insumo_existente['id'],
                'nombre': insumo['nombre'],
                'stock': insumo['stock'],
                'unidad': insumo['unidad'],
                'lote_agregado': bool(cantidad_sumar > 0)
            })
        
        # Insertar insumo con precio si existe
        precio_unitario = data.get('precio_unitario', 0)
        if precio_unitario:
            cursor.execute('''
                INSERT INTO insumos (codigo_barra, nombre, stock, stock_minimo, unidad,
                                    precio_unitario, cantidad_comprada, unidad_compra, precio_incluye_iva,
                                    cantidad_por_scan, unidad_por_scan)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                codigo_barra, nombre, stock, stock_minimo, unidad,
                float(precio_unitario),
                float(data.get('cantidad_comprada', 1)),
                data.get('unidad_compra', unidad),
                data.get('precio_incluye_iva', 1),
                float(data.get('cantidad_por_scan', stock if stock > 0 else 1)),
                data.get('unidad_por_scan', unidad),
            ))
        else:
            cursor.execute('''
                INSERT INTO insumos (codigo_barra, nombre, stock, stock_minimo, unidad, cantidad_por_scan, unidad_por_scan)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (codigo_barra, nombre, stock, stock_minimo, unidad, float(data.get('cantidad_por_scan', stock if stock > 0 else 1)), data.get('unidad_por_scan', unidad)))
        
        insumo_id = cursor.lastrowid
        if codigo_barra:
            _asociar_codigo_insumo_cursor(cursor, insumo_id, codigo_barra)
        if stock > 0:
            registrar_lote_insumo(
                insumo_id,
                stock,
                lote_codigo=lote_codigo,
                fecha_elaboracion=fecha_elaboracion,
                fecha_vencimiento=fecha_vencimiento,
                merge=False,
                conn=conn,
            )
        registrar_historial_cambio(
            recurso_tipo='insumo',
            recurso_id=insumo_id,
            recurso_nombre=nombre,
            accion='agregado',
            detalle='Alta desde escaner',
            origen_modulo='insumos',
            metadata={
                'stock_inicial': stock,
                'unidad': unidad,
            },
            conn=conn,
        )
        registrar_movimiento_stock(
            'insumo',
            insumo_id,
            'alta_scanner',
            stock,
            stock_anterior=0,
            stock_nuevo=stock,
            referencia_tipo='scanner',
            detalle='Alta desde escáner',
            conn=conn
        )
        conn.commit()

        # Obtener el insumo creado
        cursor.execute('SELECT * FROM insumos WHERE id = ?', (insumo_id,))
        insumo = cursor.fetchone()
        conn.close()
        
        crear_backup()
        
        return jsonify({
            'success': True,
            'id': insumo_id,
            'nombre': insumo['nombre'],
            'stock': insumo['stock'],
            'unidad': insumo['unidad']
        })
        
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/insumo/lote-rapido/confirmar', methods=['POST'])
def api_confirmar_lote_rapido_insumos():
    try:
        data = request.get_json(silent=True) or {}
        items = data.get('items') or []
        if not items:
            return jsonify({'success': False, 'error': 'No hay líneas para procesar'}), 400

        resultado = procesar_lote_rapido_insumos(items)
        if not resultado.get('success'):
            return jsonify(resultado), 400

        crear_backup()
        return jsonify(resultado)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/insumo/<int:id>/scan-default', methods=['POST'])
def api_actualizar_scan_default_insumo(id):
    try:
        data = request.get_json(silent=True) or {}
        actualizar_preferencias_scan_insumo(
            id,
            data.get('cantidad_por_scan'),
            data.get('unidad_por_scan'),
        )
        crear_backup()
        return jsonify({'success': True})
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    
@app.route('/api/receta/<int:id>/costo')
def api_costo_receta(id):
    """Obtiene el costo calculado de una receta en tiempo real"""
    try:
        from database import calcular_costo_receta
        costo_info = calcular_costo_receta(id)
        
        return jsonify({
            'success': True,
            'costo_total': costo_info['costo_total'],
            'detalle': costo_info['detalle']
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    
   
# ============================================================================
# API AGENDA - Persistencia en SQLite
# ============================================================================

@app.route('/api/agenda/evento/<int:id>', methods=['DELETE'])
def api_agenda_eliminar(id):
    """Elimina un evento de la agenda"""
    try:
        from database import eliminar_evento_agenda
        resultado = eliminar_evento_agenda(id)
        if resultado.get('success'):
            crear_backup()
            return jsonify(resultado)
        msg = str(resultado.get('error') or '').lower()
        status = 404 if 'no encontrado' in msg else 400
        return jsonify(resultado), status
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/agenda/eventos/pasados/eliminar', methods=['POST'])
def api_agenda_eliminar_pasados():
    """Elimina eventos de agenda anteriores a hoy (o a fecha_hasta si se envía)."""
    try:
        data = request.get_json(silent=True) or {}
        fecha_hasta = str(data.get('fecha_hasta') or '').strip() or None
        resultado = eliminar_eventos_agenda_pasados(fecha_hasta=fecha_hasta)
        if resultado.get('success'):
            crear_backup()
            return jsonify(resultado)
        return jsonify(resultado), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# API AGENDA - Persistencia en SQLite
# ============================================================================

@app.route('/api/agenda/eventos', methods=['GET'])
def api_agenda_eventos():
    """Obtiene todos los eventos de la agenda"""
    try:
        from database import obtener_eventos_agenda
        eventos = obtener_eventos_agenda()
        return jsonify({'success': True, 'eventos': eventos})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/agenda/evento', methods=['POST'])
def api_agenda_guardar():
    """Guarda un evento de la agenda"""
    try:
        from database import guardar_evento_agenda
        data = request.get_json()
        
        resultado = guardar_evento_agenda(data)
        if resultado.get('success'):
            crear_backup()
        
        return jsonify(resultado)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/agenda/pedido/buscar', methods=['GET'])
def api_agenda_buscar_pedido_por_codigo():
    try:
        from database import obtener_evento_agenda_por_codigo
        codigo = str(request.args.get('codigo') or '').strip()
        if not codigo:
            return jsonify({'success': False, 'error': 'Ingresa un codigo de pedido'}), 400
        evento = obtener_evento_agenda_por_codigo(codigo)
        if not evento:
            return jsonify({'success': False, 'error': 'No se encontro un pedido con ese codigo'}), 404
        return jsonify({'success': True, 'evento': evento})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/agenda/pedidos/rango', methods=['GET'])
def api_agenda_pedidos_por_rango():
    try:
        from database import obtener_pedidos_agenda_por_rango
        fecha_desde = str(request.args.get('desde') or '').strip()
        fecha_hasta = str(request.args.get('hasta') or '').strip()
        if not fecha_desde and not fecha_hasta:
            return jsonify({'success': False, 'error': 'Debes indicar al menos una fecha'}), 400
        pedidos = obtener_pedidos_agenda_por_rango(fecha_desde=fecha_desde, fecha_hasta=fecha_hasta, limite=500)
        return jsonify({'success': True, 'pedidos': pedidos, 'desde': fecha_desde or fecha_hasta, 'hasta': fecha_hasta or fecha_desde})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e), 'pedidos': []}), 500


@app.route('/api/agenda/evento/<int:id>/pdf', methods=['GET'])
def api_agenda_evento_pdf(id):
    conn = None
    try:
        evento_id = int(id or 0)
        if evento_id <= 0:
            return jsonify({'success': False, 'error': 'ID de evento invalido'}), 400
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, tipo, fecha, hora_inicio, hora_fin, hora_entrega, cliente, telefono, direccion, ingredientes, total, abono, codigo_operacion, codigo_pedido
            FROM agenda_eventos
            WHERE id = ?
            LIMIT 1
            """,
            (evento_id,),
        )
        row = cur.fetchone()
        if not row:
            return jsonify({'success': False, 'error': 'Evento no encontrado'}), 404
        evento = dict(row)
        filename = _crear_pdf_reserva_agenda_tienda(evento)
        media_url = f"{_public_base_url(request.url_root)}/static/tienda_pedidos_pdf/{quote(filename)}"
        return jsonify({'success': True, 'media_url': media_url, 'filename': filename, 'codigo_pedido': str(evento.get('codigo_pedido') or '').strip()})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/agenda/backfill-builder-json', methods=['POST'])
def api_agenda_backfill_builder_json():
    conn = None
    try:
        def _norm_text(value):
            txt = str(value or "").strip().lower()
            txt = txt.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u").replace("ü", "u")
            return " ".join(txt.split())

        def _parse_resumen_cliente_catalogo(txt):
            lines = [str(ln or "").strip() for ln in str(txt or "").replace("\r\n", "\n").replace("\r", "\n").split("\n")]
            lines = [ln for ln in lines if ln]
            start = -1
            for i, ln in enumerate(lines):
                if _norm_text(ln.strip("- ").strip()) == "resumen de cotizacion (cliente)":
                    start = i + 1
                    break
            if start < 0:
                return None
            data = {"categoria": "", "tamano": "", "sabores": [], "extras": [], "topper": "", "nota": "", "referencias": []}
            mode = ""
            for ln in lines[start:]:
                if ln.startswith("---") and ln.endswith("---") and len(ln) > 6:
                    break
                low = _norm_text(ln)
                if low.startswith("categoria:"):
                    data["categoria"] = ln.split(":", 1)[1].strip()
                    mode = ""
                    continue
                if low.startswith("tamano:"):
                    data["tamano"] = ln.split(":", 1)[1].strip()
                    mode = ""
                    continue
                if low == "sabores:":
                    mode = "sabores"
                    continue
                if low == "extras:":
                    mode = "extras"
                    continue
                if low == "topper:":
                    mode = "topper"
                    continue
                if low.startswith("nota catalogo:"):
                    data["nota"] = ln.split(":", 1)[1].strip()
                    mode = ""
                    continue
                if low.startswith("referencias:"):
                    val = ln.split(":", 1)[1].strip()
                    if val and val != "-":
                        data["referencias"].append(val)
                    mode = "referencias"
                    continue
                if mode in {"sabores", "extras", "topper", "referencias"} and ln.startswith("-"):
                    v = ln[1:].strip()
                    if not v or v == "-":
                        continue
                    if mode == "sabores":
                        data["sabores"].append(v)
                    elif mode == "extras":
                        data["extras"].append(v)
                    elif mode == "topper":
                        data["topper"] = v
                    else:
                        data["referencias"].append(v)
            return data

        def _id_by_name(rows, query_name):
            want = _norm_text(query_name)
            if not want:
                return ""
            for r in (rows or []):
                rid = str((r or {}).get("id") or "").strip()
                nm = _norm_text((r or {}).get("nombre"))
                if rid and nm and (nm == want or nm in want or want in nm):
                    return rid
            return ""

        def _name_qty(row_txt):
            txt_row = str(row_txt or "").strip()
            qty = 1
            m = re.search(r"\bx\s*(\d+)\b", txt_row, re.IGNORECASE)
            if m:
                try:
                    qty = max(1, int(m.group(1)))
                except (TypeError, ValueError):
                    qty = 1
            txt_row = re.sub(r"\bx\s*\d+\b", "", txt_row, flags=re.IGNORECASE).strip()
            txt_row = re.sub(r"\(.*?\)", "", txt_row).strip(" -")
            return txt_row, qty

        cfg_tienda = _obtener_tienda_personalizacion(apply_programacion=True, editor_mode="live")
        catalogo = _catalogo_torta_publico_desde_personalizacion(cfg_tienda)

        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, ingredientes
            FROM agenda_eventos
            WHERE lower(coalesce(tipo,''))='torta'
              AND lower(coalesce(motivo,'')) LIKE '%reserva cliente tienda online%'
              AND instr(coalesce(ingredientes,''), '--- Builder JSON ---') = 0
              AND instr(coalesce(ingredientes,''), '--- Resumen de cotizacion (cliente) ---') > 0
            ORDER BY id DESC
            """
        )
        rows = cur.fetchall() or []
        actualizados = 0
        omitidos = 0
        for row in rows:
            rid = int((dict(row) or {}).get("id") or 0)
            ingredientes = str((dict(row) or {}).get("ingredientes") or "")
            parsed = _parse_resumen_cliente_catalogo(ingredientes)
            if not parsed:
                omitidos += 1
                continue
            categoria_id = _id_by_name(catalogo.get("categorias"), parsed.get("categoria"))
            tamano_name = re.sub(r"\(.*?\)", "", str(parsed.get("tamano") or "")).strip()
            size_id = _id_by_name(catalogo.get("sizes"), tamano_name or parsed.get("tamano"))
            sabor_ids = []
            for s in (parsed.get("sabores") or []):
                s_name, _ = _name_qty(s)
                sid = _id_by_name(catalogo.get("sabores"), s_name)
                if sid and sid not in sabor_ids:
                    sabor_ids.append(sid)
            extra_items = []
            for ex in (parsed.get("extras") or []):
                ex_name, qty = _name_qty(ex)
                ex_id = _id_by_name(catalogo.get("extras"), ex_name)
                if ex_id:
                    extra_items.append({"id": ex_id, "qty": int(max(1, qty))})
            topper_name, _ = _name_qty(parsed.get("topper"))
            topper_id = _id_by_name(catalogo.get("toppers"), topper_name) if topper_name and _norm_text(topper_name) != "sin topper" else ""
            builder = {
                "categoria_id": categoria_id,
                "size_id": size_id,
                "sabor_ids": sabor_ids,
                "extra_items": extra_items,
                "topper_id": topper_id,
                "referencia_urls": [str(x).strip() for x in (parsed.get("referencias") or []) if str(x).strip()],
                "nota": str(parsed.get("nota") or "").strip(),
            }
            if not any([builder["categoria_id"], builder["size_id"], builder["sabor_ids"], builder["extra_items"], builder["topper_id"], builder["referencia_urls"], builder["nota"]]):
                omitidos += 1
                continue
            ingredientes_new = f"{ingredientes}\n--- Builder JSON ---\n{json.dumps(builder, ensure_ascii=False, separators=(',', ':'))}"
            cur.execute("UPDATE agenda_eventos SET ingredientes=? WHERE id=?", (ingredientes_new, rid))
            if cur.rowcount:
                actualizados += 1
        conn.commit()
        return jsonify({"success": True, "actualizados": actualizados, "omitidos": omitidos, "evaluados": len(rows)})
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/agenda/evento/<int:id>/whatsapp-cliente-pdf', methods=['POST'])
def api_agenda_evento_whatsapp_cliente_pdf(id):
    conn = None
    try:
        evento_id = int(id or 0)
        if evento_id <= 0:
            return jsonify({'success': False, 'error': 'ID de evento invalido'}), 400

        data = request.get_json(silent=True) or {}
        telefono_req = str(data.get('telefono') or '').strip()
        cliente_req = str(data.get('cliente') or '').strip()

        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, tipo, fecha, hora_inicio, hora_fin, hora_entrega, cliente, telefono, direccion, ingredientes, total, abono, codigo_operacion, codigo_pedido
            FROM agenda_eventos
            WHERE id = ?
            LIMIT 1
            """,
            (evento_id,),
        )
        row = cur.fetchone()
        if not row:
            return jsonify({'success': False, 'error': 'Evento no encontrado'}), 404
        evento = dict(row)

        telefono_cliente = telefono_req or str(evento.get('telefono') or '').strip()
        destino_twilio = _normalizar_numero_whatsapp(telefono_cliente)
        destino_digits = re.sub(r"\D+", "", telefono_cliente)
        if destino_digits.startswith("56") and len(destino_digits) >= 10:
            destino_wa = destino_digits
        elif len(destino_digits) == 9 and destino_digits.startswith("9"):
            destino_wa = f"56{destino_digits}"
        else:
            destino_wa = ""
        if not destino_twilio and not destino_wa:
            return jsonify({'success': False, 'error': 'Telefono del cliente invalido o faltante'}), 400

        filename = _crear_pdf_reserva_agenda_tienda(evento)
        media_url = f"{_public_base_url(request.url_root)}/static/tienda_pedidos_pdf/{quote(filename)}"

        cliente_txt = cliente_req or str(evento.get('cliente') or '').strip() or 'cliente'
        tipo_txt = str(evento.get('tipo') or '').strip().capitalize() or 'Reserva'
        codigo_txt = str(evento.get('codigo_pedido') or '').strip()
        fecha_txt = str(evento.get('fecha') or '-').strip()
        hora_txt = str(evento.get('hora_entrega') or evento.get('hora_inicio') or '-').strip()
        body = (
            f"Hola {cliente_txt}, te compartimos la cotizacion de tu pedido.\n"
            f"Codigo pedido: {codigo_txt or ('#' + str(evento_id))}\n"
            f"Tipo: {tipo_txt}\n"
            f"Fecha: {fecha_txt} {hora_txt}\n"
            "Adjunto PDF con el detalle y total."
        )

        if _bool_env("GESTIONSTOCK_WHATSAPP_ENABLED", default=False) and _twilio_whatsapp_configurado() and destino_twilio:
            ok, err = _enviar_whatsapp_twilio(body, media_url=media_url, to_number=destino_twilio)
            if ok:
                return jsonify({'success': True, 'via': 'twilio', 'media_url': media_url})
            return jsonify({'success': False, 'error': err or 'No se pudo enviar por WhatsApp (Twilio)'}), 502

        mensaje_manual = (
            f"Hola {cliente_txt}, te compartimos la cotizacion de tu pedido. "
            f"Codigo: {codigo_txt or ('#' + str(evento_id))}. Tipo: {tipo_txt}. Fecha: {fecha_txt} {hora_txt}. "
            f"PDF: {media_url}"
        )
        whatsapp_url = f"https://wa.me/{destino_wa}?text={quote(mensaje_manual)}" if destino_wa else ""
        if not whatsapp_url:
            return jsonify({'success': False, 'error': 'No se pudo generar enlace WhatsApp para envio manual'}), 400
        return jsonify({'success': True, 'via': 'manual', 'media_url': media_url, 'whatsapp_url': whatsapp_url})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/agenda/evento/<int:id>/estado', methods=['POST'])
def api_agenda_estado(id):
    try:
        data = request.get_json(silent=True) or {}
        estado = data.get('estado')
        resultado = actualizar_estado_evento_agenda(id, estado)
        if resultado.get('success'):
            crear_backup()
            return jsonify(resultado)
        return jsonify(resultado), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/agenda/eventos/proximos')
def api_agenda_proximos():
    """Obtiene eventos próximos para notificaciones"""
    try:
        from database import obtener_eventos_proximos_agenda
        eventos = obtener_eventos_proximos_agenda()
        return jsonify({'success': True, 'eventos': eventos})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/agenda/notas', methods=['GET'])
def api_agenda_notas_listar():
    try:
        notas = obtener_notas_agenda()
        return jsonify({'success': True, 'notas': notas})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e), 'notas': []}), 500


@app.route('/api/agenda/nota', methods=['POST'])
def api_agenda_nota_guardar():
    try:
        data = request.get_json() or {}
        resultado = guardar_nota_agenda(data)
        if resultado.get('success'):
            crear_backup()
            return jsonify(resultado)
        return jsonify(resultado), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/agenda/nota/<int:nota_id>', methods=['DELETE'])
def api_agenda_nota_eliminar(nota_id):
    try:
        resultado = eliminar_nota_agenda(nota_id)
        if resultado.get('success'):
            crear_backup()
            return jsonify(resultado)
        msg = str(resultado.get('error') or '').lower()
        status = 404 if 'no encontrada' in msg or 'no encontrado' in msg else 400
        return jsonify(resultado), status
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    
@app.route('/api/mapa/static')
def api_mapa_static():
    """Proxy de mapa estático para evitar bloqueos CORS en el navegador."""
    try:
        lat = float(request.args.get('lat', ''))
        lon = float(request.args.get('lon', ''))
    except (TypeError, ValueError):
        return jsonify({'success': False, 'error': 'Parámetros lat/lon inválidos'}), 400

    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return jsonify({'success': False, 'error': 'Coordenadas fuera de rango'}), 400

    try:
        zoom = int(request.args.get('zoom', 16))
    except (TypeError, ValueError):
        zoom = 16
    zoom = max(1, min(18, zoom))

    try:
        width = int(request.args.get('w', 700))
        height = int(request.args.get('h', 320))
    except (TypeError, ValueError):
        width, height = 700, 320
    width = max(200, min(1024, width))
    height = max(120, min(1024, height))

    def _descargar_imagen(url, timeout=2):
        req = UrlRequest(
            url,
            headers={
                'User-Agent': 'SucreeStock/3.0 (desktop-app)',
                'Accept': 'image/png,image/*;q=0.9,*/*;q=0.8',
                'Referer': 'https://www.openstreetmap.org/'
            }
        )

        try:
            with urlopen(req, timeout=timeout, context=ssl.create_default_context()) as resp:
                content_type = (resp.headers.get('Content-Type') or 'image/png').split(';')[0].strip()
                data = resp.read()
                if data:
                    return content_type, data
        except ssl.SSLError:
            # Algunos entornos Windows congelados fallan con validación SSL estricta.
            with urlopen(req, timeout=timeout, context=ssl._create_unverified_context()) as resp:
                content_type = (resp.headers.get('Content-Type') or 'image/png').split(';')[0].strip()
                data = resp.read()
                if data:
                    return content_type, data
        raise RuntimeError('No se pudo descargar imagen')

    def _lat_lon_a_tile(lat_v, lon_v, z):
        lat_rad = math.radians(lat_v)
        n = 2.0 ** z
        x = (lon_v + 180.0) / 360.0 * n
        y = (1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n
        return x, y

    def _construir_por_tiles(lat_v, lon_v, z, w, h):
        try:
            from PIL import Image, ImageDraw
        except Exception as exc:
            raise RuntimeError(f'Pillow no disponible: {exc}')

        tile_size = 256
        n_tiles = 2 ** z
        center_x, center_y = _lat_lon_a_tile(lat_v, lon_v, z)
        base_x = int(center_x) - 1
        base_y = int(center_y) - 1

        canvas = Image.new('RGB', (tile_size * 3, tile_size * 3), (240, 240, 240))
        tiles_ok = 0

        for gx in range(3):
            for gy in range(3):
                tx = (base_x + gx) % n_tiles
                ty = base_y + gy
                if ty < 0 or ty >= n_tiles:
                    continue
                tile_url = f'https://tile.openstreetmap.org/{z}/{tx}/{ty}.png'
                try:
                    _ctype, raw = _descargar_imagen(tile_url, timeout=1)
                    tile = Image.open(BytesIO(raw)).convert('RGB')
                    canvas.paste(tile, (gx * tile_size, gy * tile_size))
                    tiles_ok += 1
                except Exception:
                    continue

        if tiles_ok == 0:
            raise RuntimeError('No se pudieron descargar tiles')

        px = (center_x - base_x) * tile_size
        py = (center_y - base_y) * tile_size
        left = int(round(px - w / 2))
        top = int(round(py - h / 2))
        left = max(0, min(canvas.width - w, left))
        top = max(0, min(canvas.height - h, top))
        recorte = canvas.crop((left, top, left + w, top + h))

        marker_x = px - left
        marker_y = py - top
        draw = ImageDraw.Draw(recorte)
        draw.ellipse((marker_x - 8, marker_y - 8, marker_x + 8, marker_y + 8), fill=(220, 38, 38), outline=(255, 255, 255), width=2)

        out = BytesIO()
        recorte.save(out, format='PNG', optimize=True)
        return 'image/png', out.getvalue()

    def _placeholder_imagen(texto):
        try:
            from PIL import Image, ImageDraw
        except Exception:
            return None, None
        img = Image.new('RGB', (width, height), (243, 244, 246))
        draw = ImageDraw.Draw(img)
        draw.rectangle((8, 8, width - 8, height - 8), outline=(203, 213, 225), width=2)
        draw.text((16, 18), 'Mapa no disponible', fill=(15, 23, 42))
        draw.text((16, 40), texto, fill=(51, 65, 85))
        draw.text((16, 62), f'Lat: {lat:.6f}  Lon: {lon:.6f}', fill=(51, 65, 85))
        out = BytesIO()
        img.save(out, format='PNG')
        return 'image/png', out.getvalue()

    params = urlencode({
        'center': f'{lat:.6f},{lon:.6f}',
        'zoom': zoom,
        'size': f'{width}x{height}',
        'markers': f'{lat:.6f},{lon:.6f},red-pushpin'
    })

    candidatos = [
        f'https://staticmap.openstreetmap.de/staticmap.php?{params}',
        f'http://staticmap.openstreetmap.de/staticmap.php?{params}',
        f'https://static-maps.yandex.ru/1.x/?lang=es_ES&ll={lon:.6f},{lat:.6f}&z={zoom}&size={width},{height}&l=map&pt={lon:.6f},{lat:.6f},pm2rdm',
    ]

    content_type = None
    data = None
    for url in candidatos:
        try:
            ctype, raw = _descargar_imagen(url, timeout=2)
            if raw and len(raw) > 1000:
                content_type, data = ctype, raw
                break
        except Exception:
            continue

    if not data:
        try:
            content_type, data = _construir_por_tiles(lat, lon, zoom, width, height)
        except Exception:
            content_type, data = _placeholder_imagen('Sin conexión al proveedor de mapa estático.')

    if not data:
        return jsonify({'success': False, 'error': 'No se pudo generar imagen de mapa'}), 502

    response = make_response(data)
    response.headers['Content-Type'] = content_type if str(content_type).startswith('image/') else 'image/png'
    response.headers['Cache-Control'] = 'no-store'
    return response
@app.route('/api/lista-compras/pdf', methods=['POST'])
def generar_lista_compras_pdf():
    """Genera PDF de lista de compras desde datos enviados por POST"""
    try:
        try:
            from reportlab.lib import colors
            from reportlab.lib.pagesizes import letter
            from reportlab.platypus import (
                SimpleDocTemplate,
                Table,
                TableStyle,
                Paragraph,
                Spacer,
            )
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib.units import inch
        except Exception as import_error:
            return jsonify({
                'success': False,
                'error': f'No se pudo cargar ReportLab: {import_error}'
            }), 500

        data = request.get_json()
        items = data.get('items', [])
        
        if not items:
            return jsonify({'success': False, 'error': 'No hay items'}), 400
        
        # Crear PDF en memoria
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter,
                               rightMargin=72, leftMargin=72,
                               topMargin=72, bottomMargin=18)
        
        # Contenedor para elementos
        elementos = []
        styles = getSampleStyleSheet()
        
        # Título
        titulo_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            textColor=colors.HexColor('#d97706'),
            spaceAfter=30
        )
        elementos.append(Paragraph("Lista de Compras - Sucrée Pastelería", titulo_style))
        
        # Fecha
        fecha_style = ParagraphStyle(
            'Fecha',
            parent=styles['Normal'],
            fontSize=10,
            textColor=colors.grey
        )
        from datetime import datetime
        elementos.append(Paragraph(f"Generado: {datetime.now().strftime('%d/%m/%Y %H:%M')}", fecha_style))
        elementos.append(Spacer(1, 20))
        
        # Tabla de items
        tabla_datos = [['Insumo', 'Cantidad', 'Unidad', 'Precio Unit.', 'Total']]
        
        total_sin_iva = 0
        total_con_iva = 0
        
        for item in items:
            precio_incluye_iva = item.get('precio_incluye_iva', True)
            total_item = item.get('total', 0)
            
            if precio_incluye_iva:
                base = total_item / 1.19
                total_sin_iva += base
                total_con_iva += total_item
            else:
                total_sin_iva += total_item
                total_con_iva += total_item * 1.19
            
            tabla_datos.append([
                item.get('nombre', ''),
                str(item.get('cantidad', '')),
                item.get('unidad', ''),
                f"${item.get('precio', 0):,.0f}",
                f"${total_item:,.0f}"
            ])
        
        # Totales
        iva = total_con_iva - total_sin_iva
        
        tabla = Table(tabla_datos, colWidths=[3*inch, 0.8*inch, 0.8*inch, 1.2*inch, 1.2*inch])
        tabla.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#f59e0b')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, -1), (-1, -1), 10),
        ]))
        
        elementos.append(tabla)
        elementos.append(Spacer(1, 30))
        
        # Tabla de totales
        totales_datos = [
            ['', '', '', 'Subtotal:', f"${total_sin_iva:,.0f}"],
            ['', '', '', 'IVA (19%):', f"${iva:,.0f}"],
            ['', '', '', 'TOTAL:', f"${total_con_iva:,.0f}"]
        ]
        
        tabla_totales = Table(totales_datos, colWidths=[3*inch, 0.8*inch, 0.8*inch, 1.2*inch, 1.2*inch])
        tabla_totales.setStyle(TableStyle([
            ('ALIGN', (3, 0), (-1, -1), 'RIGHT'),
            ('FONTNAME', (3, 0), (3, -1), 'Helvetica-Bold'),
            ('FONTNAME', (4, 0), (4, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 11),
            ('TEXTCOLOR', (3, 2), (4, 2), colors.HexColor('#d97706')),
            ('FONTNAME', (3, 2), (4, 2), 'Helvetica-Bold'),
            ('FONTSIZE', (3, 2), (4, 2), 14),
        ]))
        
        elementos.append(tabla_totales)
        
        # Construir PDF
        doc.build(elementos)
        
        # Preparar respuesta
        buffer.seek(0)
        response = make_response(buffer.getvalue())
        response.headers['Content-Type'] = 'application/pdf'
        response.headers['Content-Disposition'] = 'attachment; filename="lista_compras.pdf"'
        response.headers['Content-Length'] = len(buffer.getvalue())
        
        return response
        
    except Exception as e:
        print(f"Error generando PDF: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500
# ESTO DEBE IR AL FINAL, DESPUÉS DE TODAS LAS RUTAS
if __name__ == '__main__':
    app.run(debug=False, host='127.0.0.1', port=5000, threaded=True)
