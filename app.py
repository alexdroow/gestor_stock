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
import subprocess
import threading
import sqlite3
import imghdr
from urllib.parse import urlencode, quote, unquote, urlparse
from urllib.request import Request as UrlRequest, urlopen
from urllib.error import HTTPError
from datetime import datetime, timedelta
from io import BytesIO, StringIO
from zoneinfo import ZoneInfo
from werkzeug.utils import secure_filename
from camera_hub import CameraHub
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4

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

ADMIN_PIN_ENV = "GESTIONSTOCK_ADMIN_PIN"
DEFAULT_ADMIN_PIN = "1234"
_ADMIN_SESSION_KEY = "admin_autenticado"

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


def _ruta_es_publica(path):
    ruta = str(path or "").strip()
    if not ruta:
        return False
    if ruta.startswith("/static/"):
        return True
    if ruta in {"/tienda", "/tienda/", "/admin/login", "/admin/logout", "/favicon.ico"}:
        return True
    if ruta.startswith("/api/tienda/"):
        return True
    return False


def _normalizar_next_admin(destino):
    raw = str(destino or "").strip()
    if not raw:
        return url_for("index")
    parsed = urlparse(raw)
    if parsed.scheme or parsed.netloc:
        return url_for("index")
    path = parsed.path or "/"
    if not path.startswith("/"):
        path = f"/{path}"
    query = f"?{parsed.query}" if parsed.query else ""
    return f"{path}{query}"


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
    destino = request.full_path if request.query_string else request.path
    return redirect(url_for("admin_login", next=destino))

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
    agendar_produccion_manual, eliminar_produccion_agendada,
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
    marcar_compras_pendientes_completadas,
    registrar_historial_cambio, listar_historial_cambios, eliminar_historial_cambio,
    descartar_insumos_masivo,
    obtener_evento_agenda_por_id, actualizar_estado_evento_agenda,
    obtener_notas_agenda, guardar_nota_agenda, eliminar_nota_agenda,
    guardar_factura_archivo, obtener_facturas_archivadas, obtener_factura_archivo,
    eliminar_factura_archivo, actualizar_factura_archivo, obtener_filtros_facturas, obtener_auditoria_factura,
    obtener_anios_tributarios_disponibles, obtener_resumen_sii_facturas,
    guardar_ajustes_sii_facturas, limpiar_ajustes_sii_facturas,
    guardar_venta_semanal, listar_ventas_semanales, eliminar_venta_semanal, obtener_resumen_ventas_vs_compras,
    convertir_a_base
)
from backup import crear_backup, obtener_ultimo_backup
from config import DATA_DIR, LEGACY_DATA_DIRS, BACKUP_DIR, APP_VERSION, APP_DISPLAY_NAME
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


init_db()

# Migrar base de datos (agregar columnas nuevas)
from database import migrar_db
migrar_db()

FACTURAS_DIR = os.path.join(DATA_DIR, "facturas")
ALLOWED_FACTURA_EXTENSIONS = {".pdf", ".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tif", ".tiff"}
os.makedirs(FACTURAS_DIR, exist_ok=True)
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


def _normalizar_texto_busqueda(valor):
    texto = str(valor or "").strip().lower()
    if not texto:
        return ""
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(ch for ch in texto if not unicodedata.combining(ch))
    return " ".join(texto.split())


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
    return render_template('index.html')


@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    if session.get(_ADMIN_SESSION_KEY):
        return redirect(url_for("index"))

    error = None
    if request.method == 'POST':
        pin = str(request.form.get('pin') or '').strip()
        next_url = _normalizar_next_admin(request.form.get('next'))
        if pin == _obtener_admin_pin():
            session[_ADMIN_SESSION_KEY] = True
            return redirect(next_url)
        error = "PIN incorrecto."

    next_url = _normalizar_next_admin(request.args.get('next'))
    return render_template('admin_login.html', error=error, next_url=next_url)


@app.route('/admin/logout', methods=['GET', 'POST'])
def admin_logout():
    session.pop(_ADMIN_SESSION_KEY, None)
    return redirect(url_for('admin_login'))


@app.route('/tienda')
def tienda_publica():
    try:
        personalizacion = _obtener_tienda_personalizacion()
    except Exception:
        personalizacion = _default_tienda_personalizacion()
    return render_template('tienda.html', tienda_personalizacion=personalizacion)


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
        "hour_start": _minutos_a_hhmm(start_m),
        "hour_end": _minutos_a_hhmm(end_m),
        "start_minutes": start_m,
        "end_minutes": end_m,
    }


def _rangos_ocupados_evento_agenda(evento, slot_minutes):
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
    return {"bloqueo_dia": False, "rangos": [(hora_inicio, hora_fin, False)]}


def _calcular_disponibilidad_agenda_tienda(cursor, cfg_agenda, fecha_desde, fecha_hasta):
    slot_minutes = int(cfg_agenda["slot_minutes"])
    slot_capacity = int(cfg_agenda["slot_capacity"])
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
        bloqueo_dia = False
        for ev in eventos_dia:
            occ = _rangos_ocupados_evento_agenda(ev, slot_minutes)
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
            if bloqueo_dia or slot["bloqueado"]:
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


def _minutos_anticipacion_reserva(tipo, topper_requiere_96h=False):
    t = _normalizar_tipo_reserva_tienda(tipo)
    if t == "torta":
        if bool(topper_requiere_96h):
            return 96 * 60
        return 48 * 60
    # Pastel: 24h.
    return 24 * 60


def _min_datetime_anticipacion_reserva(tipo, cfg_agenda=None, now_local=None, topper_requiere_96h=False):
    tz = ZoneInfo("America/Santiago")
    now_dt = now_local or datetime.now(tz)
    t = _normalizar_tipo_reserva_tienda(tipo)
    # Regla horaria exacta por tipo:
    # - torta: 48h (96h si incluye topper distinto a "sin topper")
    # - pastel: 24h
    return now_dt + timedelta(minutes=_minutos_anticipacion_reserva(t, topper_requiere_96h=topper_requiere_96h))


def _cumple_anticipacion_reserva(fecha_iso, hora_inicio, tipo, cfg_agenda=None, now_local=None, topper_requiere_96h=False):
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


def _crear_pdf_resumen_pedido_tienda(venta_id, cliente_nombre, cliente_email, cliente_telefono, items, subtotal, descuento, total):
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


def _enviar_whatsapp_twilio(body_text, media_url=None):
    account_sid = str(os.environ.get("TWILIO_ACCOUNT_SID") or "").strip()
    auth_token = str(os.environ.get("TWILIO_AUTH_TOKEN") or "").strip()
    from_number = _normalizar_numero_whatsapp(os.environ.get("TWILIO_WHATSAPP_FROM"))
    to_number = _normalizar_numero_whatsapp(os.environ.get("GESTIONSTOCK_WHATSAPP_TO", "+56964330546"))
    if not account_sid or not auth_token or not from_number or not to_number:
        return False, "Twilio no configurado"

    payload = {
        "To": to_number,
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


def _notificar_whatsapp_pedido_tienda_async(venta_id, cliente_nombre, cliente_email, cliente_telefono, items, subtotal, descuento, total, host_url):
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
            {"id": "bizcocho", "nombre": "Tortas Bizcocho", "activo": True},
            {"id": "panqueque", "nombre": "Tortas Panqueque", "activo": True},
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
        "agenda_days_ahead": 14,
        "agenda_hour_start": "09:00",
        "agenda_hour_end": "19:00",
        "agenda_slot_minutes": 60,
        "agenda_slot_capacity": 1,
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

    clean["agenda_card_bg"] = _normalizar_color_hex(data.get("agenda_card_bg"), base["agenda_card_bg"])
    clean["agenda_card_border"] = _normalizar_color_hex(data.get("agenda_card_border"), base["agenda_card_border"])
    clean["agenda_slot_available_bg"] = _normalizar_color_hex(data.get("agenda_slot_available_bg"), base["agenda_slot_available_bg"])
    clean["agenda_slot_unavailable_bg"] = _normalizar_color_hex(data.get("agenda_slot_unavailable_bg"), base["agenda_slot_unavailable_bg"])
    clean["agenda_slot_unavailable_text"] = _normalizar_color_hex(data.get("agenda_slot_unavailable_text"), base["agenda_slot_unavailable_text"])
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
    for item in cats_in[:20]:
        cat = dict(item or {})
        cid = re.sub(r"[^a-z0-9\-]+", "-", str(cat.get("id") or "").strip().lower()).strip("-")[:60] or _slug_simple(cat.get("nombre") or "categoria")
        nombre = str(cat.get("nombre") or "").strip()[:80] or "Categoria"
        out["categorias"].append({"id": cid, "nombre": nombre, "activo": bool(cat.get("activo", True))})
    if not out["categorias"]:
        out["categorias"] = list(base.get("categorias") or [{"id": "general", "nombre": "General", "activo": True}])

    categorias_validas = {str(c.get("id") or "") for c in out["categorias"]}

    sizes_in = data.get("sizes")
    if not isinstance(sizes_in, list):
        sizes_in = list(base.get("sizes") or [])
    for item in sizes_in[:25]:
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
    for item in sabores_in[:60]:
        out["sabores"].append(
            _normalizar_catalogo_torta_item(
                item,
                {"id": "sabor", "nombre": "Sabor", "precio": 0, "activo": True},
            )
        )

    extras_in = data.get("extras")
    if not isinstance(extras_in, list):
        extras_in = list(base.get("extras") or [])
    for item in extras_in[:80]:
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
    for item in toppers_in[:40]:
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
    return out


def _catalogo_torta_publico(cfg):
    cat = _normalizar_catalogo_torta_cfg(cfg)
    categorias_activas = [x for x in (cat.get("categorias") or []) if bool(x.get("activo"))]
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
        "sabores": [x for x in (cat.get("sabores") or []) if bool(x.get("activo"))],
        "extras": [x for x in (cat.get("extras") or []) if bool(x.get("activo"))],
        "toppers": [x for x in (cat.get("toppers") or []) if bool(x.get("activo"))],
    }


def _validar_payload_catalogo_torta(payload, catalogo_publico):
    data = dict(payload or {})
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
        sabores_limpios.append(sabor)
        seen_flavors.add(sid)
        if len(sabores_limpios) >= max_sabores:
            break
    if not sabores_limpios:
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
        "size": {"id": size.get("id"), "nombre": size.get("nombre"), "precio": float(size.get("precio") or 0), "max_sabores": max_sabores},
        "sabores": [{"id": s.get("id"), "nombre": s.get("nombre"), "precio": float(s.get("precio") or 0)} for s in sabores_limpios],
        "extras": extras_final,
        "topper": {"id": topper.get("id"), "nombre": topper.get("nombre"), "precio": float(topper.get("precio") or 0)} if topper else None,
        "referencia_urls": refs_limpias,
        "nota": nota,
        "subtotal": round(subtotal, 2),
    }


def _obtener_tienda_personalizacion():
    conn = None
    try:
        conn = get_db()
        _asegurar_presets_personalizacion(conn)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT config_json
            FROM tienda_personalizacion
            WHERE id = 1
            LIMIT 1
            """
        )
        row = cursor.fetchone()
        if not row:
            return _default_tienda_personalizacion()
        raw_json = str(row["config_json"] or "").strip()
        if not raw_json:
            return _default_tienda_personalizacion()
        try:
            payload = json.loads(raw_json)
        except Exception:
            payload = {}
        base = _normalizar_tienda_personalizacion(payload)
        return _aplicar_programacion_personalizacion(conn, base)
    finally:
        if conn:
            conn.close()


def _guardar_tienda_personalizacion(payload):
    actual = _obtener_tienda_personalizacion()
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
            ("manual", json.dumps(actual, ensure_ascii=False)),
        )
        cursor.execute(
            """
            INSERT INTO tienda_personalizacion (id, config_json, actualizado_en)
            VALUES (1, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(id) DO UPDATE SET
                config_json = excluded.config_json,
                actualizado_en = CURRENT_TIMESTAMP
            """,
            (json.dumps(config, ensure_ascii=False),),
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
        if not email:
            return jsonify({"success": False, "error": "Correo electronico invalido"}), 400
        telefono = _normalizar_telefono_cl(data.get("telefono"))
        if not telefono:
            return jsonify({"success": False, "error": "Telefono invalido. Debe tener 8 digitos"}), 400
        nombre = str(data.get("nombre") or "").strip()[:80] or _nombre_desde_email(email)

        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO tienda_clientes (nombre, email, telefono, activo, actualizado_en, ultimo_login)
            VALUES (?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(email, telefono) DO UPDATE SET
                nombre = excluded.nombre,
                activo = 1,
                actualizado_en = CURRENT_TIMESTAMP,
                ultimo_login = CURRENT_TIMESTAMP
            """,
            (nombre, email, telefono),
        )
        conn.commit()
        cursor.execute(
            """
            SELECT id, nombre, email, telefono, activo, creado_en, actualizado_en, ultimo_login
            FROM tienda_clientes
            WHERE email = ? AND telefono = ?
            LIMIT 1
            """,
            (email, telefono),
        )
        row = cursor.fetchone()
        cliente = dict(row) if row else {"nombre": nombre, "email": email, "telefono": telefono}
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
        cursor.execute(
            """
            SELECT id, nombre, email, telefono, activo
            FROM tienda_clientes
            WHERE email = ? AND telefono = ? AND activo = 1
            LIMIT 1
            """,
            (email, telefono),
        )
        cli = cursor.fetchone()
        if not cli:
            return jsonify({"success": False, "error": "Cliente no registrado"}), 404

        cursor.execute(
            """
            SELECT id, fecha_hora, codigo_pedido, codigo_operacion, total_monto, descuento_codigo, descuento_monto,
                   COALESCE(NULLIF(TRIM(pedido_estado), ''), 'recibido') AS pedido_estado,
                   pedido_estado_actualizado
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
            ventas.append(venta)

        return jsonify({"success": True, "cliente": dict(cli), "ventas": ventas})
    except Exception as e:
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
                   pedido_estado_actualizado
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


@app.route('/ventas/admin-catalogo-torta')
def ventas_admin_catalogo_torta():
    return render_template('tienda_catalogo_torta_admin.html')


@app.route('/ventas/cupones')
def ventas_admin_cupones():
    return render_template('cupones_admin.html')


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


@app.route('/api/tienda/admin/catalogo-torta', methods=['GET', 'POST'])
def api_tienda_admin_catalogo_torta():
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    try:
        if request.method == "GET":
            cfg = _obtener_tienda_personalizacion()
            cat = _normalizar_catalogo_torta_cfg(cfg.get("catalogo_torta"))
            return jsonify({"success": True, "catalogo": cat})
        data = request.get_json(silent=True) or {}
        payload = data.get("catalogo") if isinstance(data.get("catalogo"), dict) else data
        cfg = _guardar_tienda_personalizacion({"catalogo_torta": payload})
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

        conn = get_db()
        cursor = conn.cursor()
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


@app.route('/api/tienda/admin/pedidos-nuevos', methods=['GET'])
def api_tienda_admin_pedidos_nuevos():
    conn = None
    try:
        since_id = int(request.args.get("since_id") or 0)
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COALESCE(MAX(id), 0) AS max_online_id
            FROM ventas
            WHERE canal_venta = 'tienda_online'
            """
        )
        max_online_id = int(cursor.fetchone()["max_online_id"] or 0)
        cursor.execute(
            """
            SELECT v.id, v.fecha_hora, v.total_monto, v.cliente_nombre, v.cliente_email, v.cliente_telefono, v.codigo_pedido,
                   COALESCE(NULLIF(TRIM(v.pedido_estado), ''), 'recibido') AS pedido_estado,
                   v.pedido_estado_actualizado,
                   COALESCE(vp.productos, '') AS productos
            FROM ventas v
            LEFT JOIN (
                SELECT venta_id,
                       GROUP_CONCAT(producto_nombre || ' (x' || cantidad || ')', ', ') AS productos
                FROM venta_items
                GROUP BY venta_id
            ) vp ON vp.venta_id = v.id
            WHERE v.canal_venta = 'tienda_online' AND v.id > ?
            ORDER BY v.id ASC
            LIMIT 50
            """,
            (since_id,),
        )
        rows = [dict(r) for r in cursor.fetchall()]
        max_id = max(since_id, max_online_id)
        return jsonify({"success": True, "pedidos": rows, "max_id": max_id})
    except Exception as e:
        return jsonify({"success": False, "pedidos": [], "max_id": 0, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/admin/pedido/<int:venta_id>/estado', methods=['POST'])
def api_tienda_admin_pedido_estado(venta_id):
    if not session.get(_ADMIN_SESSION_KEY):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    conn = None
    try:
        data = request.get_json(silent=True) or {}
        nuevo_estado = _normalizar_pedido_estado(data.get("estado"))
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE ventas
            SET pedido_estado = ?, pedido_estado_actualizado = CURRENT_TIMESTAMP
            WHERE id = ? AND canal_venta = 'tienda_online'
            """,
            (nuevo_estado, int(venta_id)),
        )
        if cursor.rowcount <= 0:
            return jsonify({"success": False, "error": "Pedido no encontrado"}), 404
        conn.commit()
        return jsonify({"success": True, "estado": nuevo_estado, "estado_label": _pedido_estado_label(nuevo_estado)})
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/tienda/pedido/<int:venta_id>/estado', methods=['GET'])
def api_tienda_pedido_estado(venta_id):
    conn = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, fecha_hora, total_monto,
                   COALESCE(NULLIF(TRIM(pedido_estado), ''), 'recibido') AS pedido_estado,
                   pedido_estado_actualizado
            FROM ventas
            WHERE id = ? AND canal_venta = 'tienda_online'
            LIMIT 1
            """,
            (int(venta_id),),
        )
        row = cursor.fetchone()
        if not row:
            return jsonify({"success": False, "error": "Pedido no encontrado"}), 404
        item = dict(row)
        estado = _normalizar_pedido_estado(item.get("pedido_estado"))
        return jsonify(
            {
                "success": True,
                "pedido": {
                    "id": int(item.get("id") or 0),
                    "estado": estado,
                    "estado_label": _pedido_estado_label(estado),
                    "estado_actualizado": item.get("pedido_estado_actualizado"),
                    "fecha_hora": item.get("fecha_hora"),
                    "total_monto": float(item.get("total_monto") or 0),
                },
            }
        )
    except Exception as e:
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
        tipo_reserva = _normalizar_tipo_reserva_tienda(request.args.get("tipo"))
        topper_id = str(request.args.get("topper_id") or "").strip().lower()
        topper_96h = tipo_reserva == "torta" and _topper_requiere_96h(topper_id=topper_id)
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
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", fecha_desde):
            fecha_desde = hoy_iso
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", fecha_hasta):
            fecha_hasta = (datetime.strptime(fecha_desde, "%Y-%m-%d") + timedelta(days=int(cfg["days_ahead"]) - 1)).strftime("%Y-%m-%d")

        conn = get_db()
        cursor = conn.cursor()
        data = _calcular_disponibilidad_agenda_tienda(cursor, cfg, fecha_desde, fecha_hasta)
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
                "cfg": {
                    "days_ahead": int(cfg["days_ahead"]),
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

        catalogo_torta_resumen = None
        if tipo_pedido == "torta":
            cfg_tienda = _obtener_tienda_personalizacion()
            catalogo_publico = _catalogo_torta_publico((cfg_tienda or {}).get("catalogo_torta") or {})
            if not bool(catalogo_publico.get("enabled")):
                return jsonify({"success": False, "error": "El armado de tortas no esta habilitado"}), 400
            try:
                catalogo_torta_resumen = _validar_payload_catalogo_torta(catalogo_torta_payload, catalogo_publico)
            except ValueError as ve:
                return jsonify({"success": False, "error": str(ve)}), 400

        if entrega_tipo == "despacho":
            if len(direccion) < 8:
                return jsonify({"success": False, "error": "Ingresa una direccion valida para despacho"}), 400
            if not direccion_confirmada:
                return jsonify({"success": False, "error": "Debes confirmar la direccion con el pin del mapa"}), 400
            if not (-90 <= latitud <= 90 and -180 <= longitud <= 180):
                return jsonify({"success": False, "error": "Coordenadas de despacho invalidas"}), 400

        fecha_hoy = datetime.now(ZoneInfo("America/Santiago")).date()
        fecha_req = datetime.strptime(fecha, "%Y-%m-%d").date()
        if fecha_req < fecha_hoy:
            return jsonify({"success": False, "error": "No puedes reservar fechas pasadas"}), 400
        topper_96h = False
        if tipo_pedido == "torta" and catalogo_torta_resumen:
            topper = catalogo_torta_resumen.get("topper") or {}
            topper_96h = _topper_requiere_96h(topper_id=topper.get("id"), topper_nombre=topper.get("nombre"))

        if not _cumple_anticipacion_reserva(
            fecha,
            hora_inicio,
            tipo_pedido,
            cfg_agenda=cfg,
            topper_requiere_96h=topper_96h,
        ):
            minutos = _minutos_anticipacion_reserva(tipo_pedido, topper_requiere_96h=topper_96h)
            if tipo_pedido == "torta":
                msg = "Las tortas con topper requieren minimo 96 horas de anticipacion" if topper_96h else "Las tortas requieren minimo 48 horas de anticipacion"
            elif tipo_pedido == "pastel":
                msg = "Los pasteles requieren minimo 24 horas de anticipacion"
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
        ingredientes = (
            f"Reserva desde tienda online\n"
            f"Email: {email}\n"
            f"Entrega: {'Despacho' if entrega_tipo == 'despacho' else 'Retiro'}"
        )
        if catalogo_torta_resumen:
            ingredientes = f"{ingredientes}\nTamano: {catalogo_torta_resumen['size']['nombre']}"
            ingredientes = f"{ingredientes}\nSabores: {', '.join([s['nombre'] for s in (catalogo_torta_resumen.get('sabores') or [])])}"
            for ex in (catalogo_torta_resumen.get("extras") or []):
                ingredientes = f"{ingredientes}\nExtra: {ex.get('nombre')} x{int(ex.get('qty') or 0)}"
            if catalogo_torta_resumen.get("topper"):
                ingredientes = f"{ingredientes}\nTopper: {catalogo_torta_resumen['topper']['nombre']}"
            if catalogo_torta_resumen.get("nota"):
                ingredientes = f"{ingredientes}\nNota catalogo: {catalogo_torta_resumen['nota']}"
            refs = catalogo_torta_resumen.get("referencia_urls") or []
            for idx, ref in enumerate(refs, start=1):
                ingredientes = f"{ingredientes}\nRef {idx}: {ref}"
        if detalle:
            ingredientes = f"{ingredientes}\nDetalle: {detalle}"
        if entrega_tipo == "despacho":
            ingredientes = f"{ingredientes}\nMapa pin: {latitud:.6f},{longitud:.6f}"
        codigo_op = f"OPA-TI-{int(time.time())}-{uuid.uuid4().hex[:6].upper()}"[:80]

        cursor.execute(
            """
            INSERT INTO agenda_eventos (
                tipo, titulo, fecha, hora_inicio, hora_fin, hora_entrega,
                cliente, telefono, es_envio, direccion, ingredientes,
                total, abono, motivo, alerta_minutos, estado, codigo_operacion
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, 1440, 'pendiente', ?)
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
                1 if entrega_tipo == "despacho" else 0,
                direccion if entrega_tipo == "despacho" else None,
                ingredientes,
                "Reserva cliente tienda online",
                codigo_op,
            ),
        )
        reserva_id = int(cursor.lastrowid or 0)
        conn.commit()
        crear_backup()
        return jsonify(
            {
                "success": True,
                "reserva": {
                    "id": reserva_id,
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
        
        cursor.execute("SELECT COUNT(*) FROM productos WHERE COALESCE(eliminado, 0) = 0")
        total_productos = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM insumos")
        total_insumos = cursor.fetchone()[0]

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
        alertas_ins = cursor.fetchone()[0]
        haccp_vencidos = contar_haccp_vencidos(conn=conn)
        
        cursor.execute("SELECT COUNT(*) FROM ventas WHERE date(fecha_hora) = date('now')")
        ventas_hoy = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(*) FROM productos 
            WHERE COALESCE(eliminado, 0) = 0
              AND fecha_vencimiento IS NOT NULL 
              AND fecha_vencimiento <= date('now', '+2 days')
        """)
        por_vencer = cursor.fetchone()[0]
        
        conn.close()
        
        return jsonify({
            'productos': total_productos,
            'insumos': total_insumos,
            'alertas': alertas_prod + alertas_ins + haccp_vencidos,
            'haccp_vencidos': haccp_vencidos,
            'ventas_hoy': ventas_hoy,
            'por_vencer': por_vencer
        })
    except Exception as e:
        print(f"Error estadisticas: {e}")
        return jsonify({'productos': 0, 'insumos': 0, 'alertas': 0, 'haccp_vencidos': 0, 'ventas_hoy': 0, 'por_vencer': 0})

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
        if "oferta_inicio_tienda" in data:
            data["oferta_inicio_tienda"] = str(data.get("oferta_inicio_tienda") or "").strip() or None
        if "oferta_fin_tienda" in data:
            data["oferta_fin_tienda"] = str(data.get("oferta_fin_tienda") or "").strip() or None
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
        crear_backup()
        return jsonify({'success': True})
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
        
        return render_template('insumos.html',
                             insumos=insumos,
                             orden=orden,
                             direccion=direccion,
                             solo_cero=solo_cero)
    except Exception as e:
        print(f"Error en insumos: {e}")
        return f"Error: {str(e)}", 500


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

        tipo = imghdr.what(None, data)
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
        productos = _obtener_productos_para_venta()
        agenda_evento_id = request.args.get('agenda_evento', type=int)
        agenda_evento = obtener_evento_agenda_por_id(agenda_evento_id) if agenda_evento_id else None
        return render_template('ventas.html', productos=productos, agenda_evento=agenda_evento)
    except Exception as e:
        print(f"Error en ventas: {e}")
        return f"Error: {str(e)}", 500


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
        productos = _obtener_productos_para_venta()
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
        items_req = data.get('items') or []
        if not isinstance(items_req, list) or not items_req:
            return jsonify({'success': False, 'error': 'Carrito vacio'}), 400
        cliente_nombre = str(data.get("cliente_nombre") or "").strip()
        if len(cliente_nombre) < 2:
            return jsonify({'success': False, 'error': 'Nombre invalido'}), 400
        cliente_email = str(data.get("cliente_email") or "").strip().lower()
        cliente_telefono = str(data.get("cliente_telefono") or "").strip()
        if not cliente_email or not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", cliente_email):
            return jsonify({'success': False, 'error': 'Correo electronico invalido'}), 400
        telefono_norm = _normalizar_telefono_cl(cliente_telefono)
        if not telefono_norm:
            return jsonify({'success': False, 'error': 'Telefono invalido. Debe tener 8 digitos.'}), 400
        cliente_telefono = telefono_norm
        cupon_codigo = _normalizar_cupon_codigo(data.get("codigo_descuento"))
        cliente_ref = _normalizar_cliente_ref(cliente_email, cliente_telefono)

        now_local = datetime.now(ZoneInfo("America/Santiago"))
        categorias = _cargar_categorias_tienda()
        categorias_map = {str(c.get("nombre") or "").strip().lower(): c for c in categorias}
        mapa = {
            int(p.get("id") or 0): _serializar_producto_tienda(p, categorias_map=categorias_map, now_local=now_local)
            for p in _obtener_productos_para_venta()
        }
        items_limpios = []
        items_serializados = []
        items_notificacion = []
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
            if not bool(prod.get("categoria_activa", True)):
                return jsonify({'success': False, 'error': f'{prod.get("nombre")}: categoria no disponible en este horario'}), 400
            max_compra = int(prod.get("max_compra") or 0)
            if max_compra <= 0:
                return jsonify({'success': False, 'error': f'{prod.get("nombre")} sin stock disponible'}), 400
            if cantidad > max_compra:
                return jsonify({'success': False, 'error': f'{prod.get("nombre")}: maximo {max_compra} unidad(es)'}), 400

            precio_final = float(prod.get("precio_final") or 0)
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
                    "descuento_tienda_pct": float(prod.get("descuento_tienda_pct") or 0),
                }
            )
            items_notificacion.append(
                {
                    "id": pid,
                    "nombre": str(prod.get("nombre") or "").strip() or f"Producto #{pid}",
                    "cantidad": cantidad,
                    "precio_unitario": precio_final,
                }
            )

        subtotal = sum(float(it["precio_unitario"]) * int(it["cantidad"]) for it in items_limpios)
        descuento_monto = 0.0
        cupon_aplicado = None
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
            "fecha_venta": str(data.get("fecha_venta") or "").strip() or None,
        }
        respuesta = _procesar_venta_desde_payload(
            payload_seguro,
            canal_por_defecto='tienda_online',
            permitir_canal_usuario=False,
            permitir_agenda=False,
        )
        venta_id = int(respuesta.get("venta_id") or 0)
        total_neto = subtotal - descuento_monto
        if total_neto < 0:
            total_neto = 0
        conn = None
        try:
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE ventas
                SET cliente_nombre = ?, cliente_email = ?, cliente_telefono = ?, descuento_codigo = ?, descuento_monto = ?, total_monto = ?,
                    pedido_estado = 'recibido', pedido_estado_actualizado = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    cliente_nombre,
                    cliente_email,
                    cliente_telefono,
                    (cupon_codigo or None),
                    descuento_monto,
                    total_neto,
                    venta_id,
                ),
            )
            if cupon_aplicado and descuento_monto > 0:
                cursor.execute(
                    """
                    INSERT INTO tienda_cupon_usos (cupon_id, venta_id, cliente_ref, descuento_aplicado)
                    VALUES (?, ?, ?, ?)
                    """,
                    (int(cupon_aplicado["id"]), venta_id, (cliente_ref or None), descuento_monto),
                )
            cursor.execute(
                """
                INSERT INTO tienda_clientes (nombre, email, telefono, activo, actualizado_en, ultimo_login)
                VALUES (?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT(email, telefono) DO UPDATE SET
                    nombre = excluded.nombre,
                    activo = 1,
                    actualizado_en = CURRENT_TIMESTAMP,
                    ultimo_login = CURRENT_TIMESTAMP
                """,
                (cliente_nombre, cliente_email, cliente_telefono),
            )
            conn.commit()
        finally:
            if conn:
                conn.close()

        respuesta["subtotal"] = round(subtotal, 2)
        respuesta["descuento_monto"] = round(descuento_monto, 2)
        respuesta["codigo_descuento"] = cupon_codigo or None
        respuesta["cliente_nombre"] = cliente_nombre
        respuesta["cliente_email"] = cliente_email
        respuesta["cliente_telefono"] = cliente_telefono
        respuesta["total_monto"] = round(total_neto, 2)
        _notificar_whatsapp_pedido_tienda_async(
            venta_id=venta_id,
            cliente_nombre=cliente_nombre,
            cliente_email=cliente_email,
            cliente_telefono=cliente_telefono,
            items=items_notificacion,
            subtotal=float(subtotal),
            descuento=float(descuento_monto),
            total=float(total_neto),
            host_url=request.url_root,
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
        return jsonify({
            'success': True,
            'venta': venta_payload,
            'items': [dict(item) for item in items],
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
                COUNT(*) as total
            FROM ventas
            WHERE fecha_hora >= datetime('now', '-10 days')
            GROUP BY substr(fecha_hora, 1, 10)
            ORDER BY fecha ASC
        """)
        
        rows = cursor.fetchall()
        conn.close()
        
        print(f"DEBUG - Filas encontradas: {len(rows)}")
        for row in rows:
            print(f"  Fecha: {row['fecha']}, Ventas: {row['total']}")
        
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
            ventas_por_fecha[row['fecha']] = row['total']
        
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
        recordatorios = obtener_recordatorios_agenda_pendientes()
    except Exception:
        config_alertas = {}
        config_clima_sidebar = {}
        config_updater = {}
        recordatorios = []
    return render_template(
        'settings.html',
        config_alertas=config_alertas,
        config_clima_sidebar=config_clima_sidebar,
        config_updater=config_updater,
        app_version=APP_VERSION,
        recordatorios=recordatorios,
        data_dir=DATA_DIR,
        backup_dir=BACKUP_DIR,
    )


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
