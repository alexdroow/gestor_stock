import json
import sqlite3
import math
import unicodedata
import time
import random
import uuid
from collections import deque
import pytz
from datetime import datetime, timedelta

from config import DB_PATH
from unit_utils import (
    normalize_unit,
    unit_type,
    units_compatible,
    convert_amount,
)

ZONA_HORARIA_CHILE = pytz.timezone('America/Santiago')
DB_LOCK_RETRY_ATTEMPTS = 4
DB_LOCK_RETRY_BASE_DELAY_MS = 80

def obtener_hora_chile():
    """Retorna la hora actual en Chile"""
    return datetime.now(ZONA_HORARIA_CHILE)

def formatear_fecha_chile(fecha_str):
    """Convierte fecha UTC a hora Chile y formatea"""
    if not fecha_str:
        return "Sin fecha"
    
    try:
        # Parsear fecha de SQLite (UTC)
        if isinstance(fecha_str, str):
            fecha_utc = datetime.strptime(fecha_str, '%Y-%m-%d %H:%M:%S')
        else:
            fecha_utc = fecha_str
        
        # Asumir que es UTC y convertir a Chile
        utc = pytz.utc
        fecha_utc = utc.localize(fecha_utc)
        fecha_chile = fecha_utc.astimezone(ZONA_HORARIA_CHILE)
        
        return fecha_chile.strftime('%d/%m/%Y %H:%M')
    except Exception as e:
        print(f"Error convirtiendo fecha: {e}")
        return str(fecha_str)


def _normalizar_unidad_producto(unidad_raw):
    return normalize_unit(unidad_raw)


def _tipo_unidad(unidad_raw):
    return unit_type(unidad_raw)


def _unidades_compatibles_porcion(unidad_1, unidad_2):
    return units_compatible(unidad_1, unidad_2)


def _convertir_cantidad_unidad_db(cantidad, unidad_origen, unidad_destino):
    return convert_amount(cantidad, unidad_origen, unidad_destino, convertir_a_base)


def _esta_cerca_minimo_db(stock_actual, stock_minimo):
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


def _calcular_info_porciones_producto_db(producto):
    unidad_stock = _normalizar_unidad_producto(producto.get("unidad") or "unidad")
    stock_actual = float(producto.get("stock") or 0)
    stock_minimo = float(producto.get("stock_minimo") or 0)
    porcion_cantidad = float(producto.get("porcion_cantidad") or 1)
    porcion_unidad = _normalizar_unidad_producto(producto.get("porcion_unidad") or unidad_stock)

    conversion = _convertir_cantidad_unidad_db(porcion_cantidad, porcion_unidad, unidad_stock)
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
            "cerca_minimo": _esta_cerca_minimo_db(stock_actual, stock_minimo),
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
        "cerca_minimo": _esta_cerca_minimo_db(stock_actual, stock_minimo),
    }


def _normalizar_fecha_iso(valor, campo):
    raw = str(valor or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError(f"{campo} invalida")


def _normalizar_lote_codigo(valor):
    raw = str(valor or "").strip()
    if not raw:
        return None
    return raw[:80]


def _validar_fechas_lote(fecha_elaboracion, fecha_vencimiento):
    if not fecha_elaboracion or not fecha_vencimiento:
        return
    try:
        fecha_elab_dt = datetime.strptime(fecha_elaboracion, "%Y-%m-%d")
        fecha_venc_dt = datetime.strptime(fecha_vencimiento, "%Y-%m-%d")
    except ValueError:
        return
    if fecha_venc_dt < fecha_elab_dt:
        raise ValueError("La fecha de vencimiento no puede ser anterior a la fecha de elaboracion")


def _normalizar_nombre_insumo_busqueda(valor):
    texto = str(valor or "").strip().lower()
    if not texto:
        return ""
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(ch for ch in texto if not unicodedata.combining(ch))
    return " ".join(texto.split())


def _buscar_insumo_por_nombre_cursor(cursor, nombre):
    objetivo = _normalizar_nombre_insumo_busqueda(nombre)
    if not objetivo:
        return None
    cursor.execute("SELECT * FROM insumos ORDER BY id ASC")
    for row in cursor.fetchall():
        if _normalizar_nombre_insumo_busqueda(row["nombre"]) == objetivo:
            return row
    return None


def _buscar_insumo_por_codigo_cursor(cursor, codigo_barra):
    codigo = str(codigo_barra or "").strip()
    if not codigo:
        return None

    cursor.execute("SELECT * FROM insumos WHERE codigo_barra = ? LIMIT 1", (codigo,))
    insumo = cursor.fetchone()
    if insumo:
        return insumo

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
        return cursor.fetchone()
    except sqlite3.OperationalError:
        # Compatibilidad para esquemas antiguos antes de migración.
        return None


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
        # Compatibilidad para esquemas antiguos antes de migración.
        return

    if existente and int(existente["insumo_id"] or 0) != insumo_id_int:
        raise ValueError(f"El código '{codigo}' ya está asociado a otro insumo")

    if not existente:
        cursor.execute(
            "INSERT INTO insumo_codigos (insumo_id, codigo_barra) VALUES (?, ?)",
            (insumo_id_int, codigo),
        )

def _sumar_cantidad_lotes_insumo_cursor(cursor, insumo_id):
    cursor.execute(
        "SELECT COALESCE(SUM(cantidad), 0) AS total FROM insumo_lotes WHERE insumo_id = ?",
        (insumo_id,),
    )
    row = cursor.fetchone()
    return float(row["total"] or 0)


def _obtener_lote_referencia_insumo_cursor(cursor, insumo_id, incluir_cero=True):
    filtro = "" if incluir_cero else "AND cantidad > 0"
    cursor.execute(
        f"""
        SELECT id, insumo_id, lote_codigo, fecha_elaboracion, fecha_vencimiento, cantidad, fecha_ingreso
        FROM insumo_lotes
        WHERE insumo_id = ? {filtro}
        ORDER BY
            CASE WHEN cantidad > 0 THEN 0 ELSE 1 END ASC,
            id DESC
        LIMIT 1
        """,
        (insumo_id,),
    )
    row = cursor.fetchone()
    return dict(row) if row else None


def _crear_o_sumar_lote_insumo_cursor(
    cursor,
    insumo_id,
    cantidad,
    lote_codigo=None,
    fecha_elaboracion=None,
    fecha_vencimiento=None,
    merge=True,
):
    cantidad_val = float(cantidad or 0)
    if cantidad_val <= 0:
        return None

    lote_val = _normalizar_lote_codigo(lote_codigo)
    fecha_elab_val = _normalizar_fecha_iso(fecha_elaboracion, "Fecha de elaboracion")
    fecha_venc_val = _normalizar_fecha_iso(fecha_vencimiento, "Fecha de vencimiento")
    _validar_fechas_lote(fecha_elab_val, fecha_venc_val)

    if merge:
        cursor.execute(
            """
            SELECT id, cantidad
            FROM insumo_lotes
            WHERE insumo_id = ?
              AND COALESCE(lote_codigo, '') = COALESCE(?, '')
              AND COALESCE(fecha_elaboracion, '') = COALESCE(?, '')
              AND COALESCE(fecha_vencimiento, '') = COALESCE(?, '')
            ORDER BY id DESC
            LIMIT 1
            """,
            (insumo_id, lote_val, fecha_elab_val, fecha_venc_val),
        )
        existente = cursor.fetchone()
        if existente:
            nuevo_total = float(existente["cantidad"] or 0) + cantidad_val
            cursor.execute(
                """
                UPDATE insumo_lotes
                SET cantidad = ?, actualizado = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (nuevo_total, existente["id"]),
            )
            return int(existente["id"])

    cursor.execute(
        """
        INSERT INTO insumo_lotes (
            insumo_id, lote_codigo, fecha_elaboracion, fecha_vencimiento, cantidad
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (insumo_id, lote_val, fecha_elab_val, fecha_venc_val, cantidad_val),
    )
    return int(cursor.lastrowid)


def _descontar_lotes_insumo_cursor(cursor, insumo_id, cantidad_objetivo):
    restante = float(cantidad_objetivo or 0)
    detalle = []
    if restante <= 0:
        return {"detalle": detalle, "restante_sin_lote": 0.0}

    cursor.execute(
        """
        SELECT id, cantidad, lote_codigo, fecha_elaboracion, fecha_vencimiento, fecha_ingreso
        FROM insumo_lotes
        WHERE insumo_id = ? AND cantidad > 0
        ORDER BY COALESCE(fecha_vencimiento, '9999-12-31') ASC,
                 COALESCE(fecha_elaboracion, fecha_ingreso, '9999-12-31') ASC,
                 fecha_ingreso ASC,
                 id ASC
        """,
        (insumo_id,),
    )
    lotes = cursor.fetchall()
    for lote in lotes:
        if restante <= 1e-9:
            restante = 0.0
            break

        disponible = float(lote["cantidad"] or 0)
        if disponible <= 0:
            continue
        usado = min(disponible, restante)
        nuevo_lote = max(disponible - usado, 0.0)
        cursor.execute(
            """
            UPDATE insumo_lotes
            SET cantidad = ?, actualizado = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (nuevo_lote, lote["id"]),
        )
        detalle.append(
            {
                "insumo_lote_id": int(lote["id"]),
                "cantidad_usada": float(usado),
                "lote_codigo": lote["lote_codigo"],
                "fecha_elaboracion": lote["fecha_elaboracion"],
                "fecha_vencimiento": lote["fecha_vencimiento"],
                "fecha_ingreso": lote["fecha_ingreso"],
            }
        )
        restante -= usado

    if abs(restante) < 1e-9:
        restante = 0.0
    return {"detalle": detalle, "restante_sin_lote": max(restante, 0.0)}


def _sincronizar_lotes_insumo_a_stock_cursor(
    cursor,
    insumo_id,
    stock_objetivo,
    lote_codigo=None,
    fecha_elaboracion=None,
    fecha_vencimiento=None,
):
    stock_meta = max(0.0, float(stock_objetivo or 0))
    total_lotes = _sumar_cantidad_lotes_insumo_cursor(cursor, insumo_id)
    delta = stock_meta - total_lotes

    if abs(delta) < 1e-9:
        return
    if delta > 0:
        _crear_o_sumar_lote_insumo_cursor(
            cursor,
            insumo_id,
            delta,
            lote_codigo=lote_codigo,
            fecha_elaboracion=fecha_elaboracion,
            fecha_vencimiento=fecha_vencimiento,
            merge=True,
        )
        return
    _descontar_lotes_insumo_cursor(cursor, insumo_id, abs(delta))


def _actualizar_metadata_lote_referencia_cursor(
    cursor,
    insumo_id,
    stock_actual,
    lote_codigo,
    fecha_elaboracion,
    fecha_vencimiento,
):
    lote_val = _normalizar_lote_codigo(lote_codigo)
    fecha_elab_val = _normalizar_fecha_iso(fecha_elaboracion, "Fecha de elaboracion")
    fecha_venc_val = _normalizar_fecha_iso(fecha_vencimiento, "Fecha de vencimiento")
    _validar_fechas_lote(fecha_elab_val, fecha_venc_val)

    referencia = _obtener_lote_referencia_insumo_cursor(cursor, insumo_id, incluir_cero=True)
    if referencia:
        cursor.execute(
            """
            UPDATE insumo_lotes
            SET lote_codigo = ?, fecha_elaboracion = ?, fecha_vencimiento = ?, actualizado = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (lote_val, fecha_elab_val, fecha_venc_val, referencia["id"]),
        )
        return int(referencia["id"])

    stock_val = float(stock_actual or 0)
    if stock_val > 0:
        return _crear_o_sumar_lote_insumo_cursor(
            cursor,
            insumo_id,
            stock_val,
            lote_codigo=lote_val,
            fecha_elaboracion=fecha_elab_val,
            fecha_vencimiento=fecha_venc_val,
            merge=False,
        )
    return None


def registrar_lote_insumo(
    insumo_id,
    cantidad,
    lote_codigo=None,
    fecha_elaboracion=None,
    fecha_vencimiento=None,
    merge=True,
    conn=None,
):
    propia = conn is None
    if propia:
        conn = get_db()
    cursor = conn.cursor()
    try:
        lote_id = _crear_o_sumar_lote_insumo_cursor(
            cursor,
            insumo_id,
            cantidad,
            lote_codigo=lote_codigo,
            fecha_elaboracion=fecha_elaboracion,
            fecha_vencimiento=fecha_vencimiento,
            merge=merge,
        )
        if propia:
            conn.commit()
        return lote_id
    except Exception:
        if propia:
            conn.rollback()
        raise
    finally:
        if propia:
            conn.close()


def sincronizar_lotes_insumo_stock(
    insumo_id,
    stock_objetivo,
    lote_codigo=None,
    fecha_elaboracion=None,
    fecha_vencimiento=None,
    actualizar_metadata=False,
    conn=None,
):
    propia = conn is None
    if propia:
        conn = get_db()
    cursor = conn.cursor()
    try:
        _sincronizar_lotes_insumo_a_stock_cursor(
            cursor,
            insumo_id,
            stock_objetivo,
            lote_codigo=lote_codigo,
            fecha_elaboracion=fecha_elaboracion,
            fecha_vencimiento=fecha_vencimiento,
        )
        if actualizar_metadata:
            _actualizar_metadata_lote_referencia_cursor(
                cursor,
                insumo_id,
                stock_objetivo,
                lote_codigo=lote_codigo,
                fecha_elaboracion=fecha_elaboracion,
                fecha_vencimiento=fecha_vencimiento,
            )
        if propia:
            conn.commit()
        return True
    except Exception:
        if propia:
            conn.rollback()
        raise
    finally:
        if propia:
            conn.close()


def obtener_lotes_insumo(insumo_id, incluir_cero=False, limit=50):
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT id, lote_codigo, fecha_elaboracion, fecha_vencimiento, cantidad, fecha_ingreso
            FROM insumo_lotes
            WHERE insumo_id = ?
              AND (? = 1 OR cantidad > 0)
            ORDER BY COALESCE(fecha_vencimiento, '9999-12-31') ASC,
                     COALESCE(fecha_elaboracion, fecha_ingreso, '9999-12-31') ASC,
                     id DESC
            LIMIT ?
            """,
            (insumo_id, 1 if incluir_cero else 0, int(limit or 50)),
        )
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def init_db():
    """Inicializa la base de datos si no existe"""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("PRAGMA journal_mode=WAL")  # Activar WAL
        
        cursor = conn.cursor()  # <-- CREAR CURSOR AQUÍ
        
        # Tabla de productos
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS productos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT NOT NULL,
                foto TEXT,
                icono TEXT DEFAULT 'cupcake',
                stock REAL DEFAULT 0,
                stock_minimo REAL DEFAULT 2,
                unidad TEXT DEFAULT 'unidad',
                porcion_cantidad REAL DEFAULT 1,
                porcion_unidad TEXT DEFAULT 'unidad',
                eliminado INTEGER DEFAULT 0,
                stock_dependencia_tipo TEXT,
                stock_dependencia_id INTEGER,
                stock_dependencia_cantidad REAL DEFAULT 1,
                fecha_vencimiento TEXT,
                alerta_dias INTEGER DEFAULT 2,
                precio REAL DEFAULT 0,
                vida_util_dias INTEGER DEFAULT 0
            )
        ''')
        
        # Tabla de insumos
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS insumos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                codigo_barra TEXT,
                nombre TEXT NOT NULL,
                stock REAL DEFAULT 0,
                stock_minimo REAL DEFAULT 1.0,
                unidad TEXT DEFAULT 'unidad',
                precio_unitario REAL DEFAULT 0,
                cantidad_comprada REAL DEFAULT 1,
                unidad_compra TEXT DEFAULT 'unidad',
                precio_incluye_iva BOOLEAN DEFAULT 1,
                cantidad_por_scan REAL DEFAULT 1,
                unidad_por_scan TEXT DEFAULT NULL,
                nutricion_ref_cantidad REAL DEFAULT 100,
                nutricion_ref_unidad TEXT DEFAULT NULL,
                nutricion_kcal REAL DEFAULT NULL,
                nutricion_proteinas_g REAL DEFAULT NULL,
                nutricion_carbohidratos_g REAL DEFAULT NULL,
                nutricion_grasas_g REAL DEFAULT NULL,
                nutricion_azucares_g REAL DEFAULT NULL,
                nutricion_sodio_mg REAL DEFAULT NULL
            )
        ''')

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS insumo_codigos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                insumo_id INTEGER NOT NULL,
                codigo_barra TEXT NOT NULL UNIQUE,
                creado TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (insumo_id) REFERENCES insumos(id) ON DELETE CASCADE
            )
            '''
        )
            # Tabla de agenda/eventos
        cursor.execute('''
             CREATE TABLE IF NOT EXISTS agenda_eventos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tipo TEXT NOT NULL DEFAULT 'torta',
            titulo TEXT NOT NULL,
            fecha TEXT NOT NULL,
            hora_inicio TEXT,
            hora_fin TEXT,
            hora_entrega TEXT,
            cliente TEXT,
            telefono TEXT,
            es_envio INTEGER DEFAULT 0,
            direccion TEXT,
            ingredientes TEXT,
            total REAL DEFAULT 0,
            abono REAL DEFAULT 0,
            motivo TEXT,
            alerta_minutos INTEGER DEFAULT 1440,
            estado TEXT DEFAULT 'pendiente',
            codigo_operacion TEXT,
            creado TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
        # Tabla de lotes de productos
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS producto_lotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                producto_id INTEGER NOT NULL,
                cantidad INTEGER NOT NULL,
                fecha_vencimiento TEXT,
                fecha_ingreso TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (producto_id) REFERENCES productos(id)
            )
        ''')

        # Tabla de lotes de insumos para trazabilidad de ingreso/consumo
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS insumo_lotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                insumo_id INTEGER NOT NULL,
                lote_codigo TEXT,
                fecha_elaboracion TEXT,
                fecha_vencimiento TEXT,
                cantidad REAL NOT NULL,
                fecha_ingreso TEXT DEFAULT CURRENT_TIMESTAMP,
                actualizado TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (insumo_id) REFERENCES insumos(id) ON DELETE CASCADE
            )
            '''
        )
        
        # Tabla de ventas
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ventas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha_hora TEXT DEFAULT CURRENT_TIMESTAMP,
                codigo_pedido TEXT,
                canal_venta TEXT DEFAULT 'presencial',
                codigo_operacion TEXT,
                total_items INTEGER DEFAULT 0,
                total_monto REAL DEFAULT 0,
                estado TEXT DEFAULT 'completada'
            )
        ''')

        # Ventas semanales manuales para comparativo con compras facturadas
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS ventas_semanales (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                semana_inicio TEXT NOT NULL,
                semana_fin TEXT NOT NULL,
                ventas_local REAL DEFAULT 0,
                ventas_uber REAL DEFAULT 0,
                ventas_pedidosya REAL DEFAULT 0,
                ventas_monto REAL DEFAULT 0,
                marketing_monto REAL DEFAULT 0,
                otros_descuentos_monto REAL DEFAULT 0,
                tasa_servicio_pct REAL DEFAULT 30,
                impuesto_tasa_servicio_pct REAL DEFAULT 19,
                notas TEXT,
                creado TEXT DEFAULT CURRENT_TIMESTAMP,
                actualizado TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(semana_inicio)
            )
            '''
        )
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS resumen_mensual (
                anio INTEGER NOT NULL,
                mes INTEGER NOT NULL,
                mes_clave TEXT NOT NULL,
                ventas_local REAL DEFAULT 0,
                ventas_uber REAL DEFAULT 0,
                ventas_pedidosya REAL DEFAULT 0,
                compras_con_iva REAL DEFAULT 0,
                documentos_compra INTEGER DEFAULT 0,
                semanas_consideradas INTEGER DEFAULT 0,
                dirty INTEGER DEFAULT 1,
                actualizado TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (anio, mes)
            )
            '''
        )
        
        # Tabla de items por venta (compatibilidad)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS venta_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                venta_id INTEGER NOT NULL,
                producto_id INTEGER NOT NULL,
                producto_nombre TEXT NOT NULL,
                cantidad INTEGER NOT NULL,
                FOREIGN KEY (venta_id) REFERENCES ventas(id) ON DELETE CASCADE
            )
        ''')
        
        # NUEVA: Tabla de detalles de venta (para sistema FIFO)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS venta_detalles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                venta_id INTEGER NOT NULL,
                producto_id INTEGER NOT NULL,
                cantidad INTEGER NOT NULL,
                precio_unitario REAL DEFAULT 0,
                subtotal REAL DEFAULT 0,
                FOREIGN KEY (venta_id) REFERENCES ventas(id)
            )
        ''')
        
        # NUEVA: Tabla para trazabilidad de lotes en ventas
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS venta_lotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                venta_id INTEGER NOT NULL,
                producto_id INTEGER NOT NULL,
                lote_id INTEGER,
                cantidad_usada INTEGER NOT NULL,
                FOREIGN KEY (venta_id) REFERENCES ventas(id)
            )
        ''')

        # Insumos que se descuentan automáticamente al vender un producto
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS producto_insumos_venta (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                producto_id INTEGER NOT NULL,
                insumo_id INTEGER NOT NULL,
                cantidad REAL NOT NULL,
                unidad TEXT DEFAULT 'unidad',
                creado TEXT DEFAULT CURRENT_TIMESTAMP,
                actualizado TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(producto_id, insumo_id),
                FOREIGN KEY (producto_id) REFERENCES productos(id) ON DELETE CASCADE,
                FOREIGN KEY (insumo_id) REFERENCES insumos(id)
            )
        ''')

        # Productos que se descuentan automáticamente al vender un producto
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS producto_productos_venta (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                producto_id INTEGER NOT NULL,
                producto_asociado_id INTEGER NOT NULL,
                cantidad REAL NOT NULL DEFAULT 1,
                creado TEXT DEFAULT CURRENT_TIMESTAMP,
                actualizado TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(producto_id, producto_asociado_id),
                FOREIGN KEY (producto_id) REFERENCES productos(id) ON DELETE CASCADE,
                FOREIGN KEY (producto_asociado_id) REFERENCES productos(id) ON DELETE CASCADE
            )
        ''')

        # Confirmación manual de desactivación externa para productos en estado crítico de venta
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS producto_desactivaciones_manuales (
                producto_id INTEGER PRIMARY KEY,
                confirmado_en TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (producto_id) REFERENCES productos(id) ON DELETE CASCADE
            )
        ''')

        # Trazabilidad de descuentos de insumos por venta
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS venta_insumos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                venta_id INTEGER NOT NULL,
                producto_id INTEGER NOT NULL,
                insumo_id INTEGER NOT NULL,
                cantidad_por_producto REAL NOT NULL,
                unidad_asociada TEXT DEFAULT 'unidad',
                cantidad_total_asociada REAL NOT NULL,
                cantidad_descontada_stock REAL NOT NULL,
                unidad_stock TEXT DEFAULT 'unidad',
                FOREIGN KEY (venta_id) REFERENCES ventas(id) ON DELETE CASCADE,
                FOREIGN KEY (producto_id) REFERENCES productos(id),
                FOREIGN KEY (insumo_id) REFERENCES insumos(id)
            )
        ''')

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS venta_insumo_lotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                venta_id INTEGER NOT NULL,
                producto_id INTEGER NOT NULL,
                insumo_id INTEGER NOT NULL,
                insumo_lote_id INTEGER,
                cantidad_usada_stock REAL NOT NULL,
                unidad_stock TEXT DEFAULT 'unidad',
                lote_codigo TEXT,
                fecha_elaboracion TEXT,
                fecha_vencimiento TEXT,
                fecha_ingreso TEXT,
                creado TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (venta_id) REFERENCES ventas(id) ON DELETE CASCADE,
                FOREIGN KEY (producto_id) REFERENCES productos(id),
                FOREIGN KEY (insumo_id) REFERENCES insumos(id),
                FOREIGN KEY (insumo_lote_id) REFERENCES insumo_lotes(id)
            )
            '''
        )
        
        # Tablas de producción
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS recetas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT NOT NULL,
                producto_id INTEGER,
                rendimiento REAL DEFAULT 1,
                fecha_ingreso TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (producto_id) REFERENCES productos(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS receta_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                receta_id INTEGER NOT NULL,
                tipo TEXT DEFAULT 'insumo',
                insumo_id INTEGER,
                producto_id INTEGER,
                cantidad REAL NOT NULL,
                unidad TEXT DEFAULT 'unidad',
                FOREIGN KEY (receta_id) REFERENCES recetas(id) ON DELETE CASCADE,
                FOREIGN KEY (insumo_id) REFERENCES insumos(id),
                FOREIGN KEY (producto_id) REFERENCES productos(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS producciones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                receta_id INTEGER NOT NULL,
                cantidad INTEGER NOT NULL,
                cantidad_resultado REAL DEFAULT 0,
                fecha_hora TEXT DEFAULT CURRENT_TIMESTAMP,
                codigo_operacion TEXT,
                metadata_json TEXT,
                FOREIGN KEY (receta_id) REFERENCES recetas(id)
            )
        ''')

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS produccion_agendada (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha TEXT NOT NULL,
                receta_id INTEGER,
                receta_nombre TEXT,
                producto_id INTEGER,
                producto_nombre TEXT,
                cantidad REAL NOT NULL DEFAULT 1,
                nota TEXT,
                estado TEXT DEFAULT 'pendiente',
                creado TEXT DEFAULT CURRENT_TIMESTAMP,
                actualizado TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (receta_id) REFERENCES recetas(id) ON DELETE SET NULL,
                FOREIGN KEY (producto_id) REFERENCES productos(id) ON DELETE SET NULL
            )
            '''
        )

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS produccion_movimientos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                produccion_id INTEGER NOT NULL,
                tipo_recurso TEXT NOT NULL,
                recurso_id INTEGER,
                lote_id INTEGER,
                accion TEXT NOT NULL,
                cantidad REAL NOT NULL,
                unidad TEXT,
                metadata_json TEXT,
                creado TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (produccion_id) REFERENCES producciones(id) ON DELETE CASCADE
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_movimientos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tipo_recurso TEXT NOT NULL,
                recurso_id INTEGER NOT NULL,
                accion TEXT NOT NULL,
                cantidad REAL NOT NULL,
                stock_anterior REAL,
                stock_nuevo REAL,
                referencia_tipo TEXT,
                referencia_id INTEGER,
                detalle TEXT,
                creado TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS stock_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha TEXT DEFAULT CURRENT_TIMESTAMP,
                tipo_operacion TEXT NOT NULL,
                recurso_tipo TEXT NOT NULL,
                recurso_id INTEGER NOT NULL,
                cantidad_delta REAL NOT NULL,
                stock_anterior REAL,
                stock_nuevo REAL,
                unidad TEXT,
                lote_id INTEGER,
                origen_modulo TEXT,
                codigo_operacion TEXT NOT NULL,
                usuario TEXT,
                metadata_json TEXT,
                referencia_tipo TEXT,
                referencia_id INTEGER,
                detalle TEXT,
                legacy_movimiento_id INTEGER UNIQUE,
                creado TEXT DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS historial_cambios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recurso_tipo TEXT NOT NULL,
                recurso_id INTEGER,
                recurso_nombre TEXT NOT NULL,
                accion TEXT NOT NULL,
                detalle TEXT,
                origen_modulo TEXT,
                metadata_json TEXT,
                fecha_hora TEXT DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS alertas_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                notificaciones_activas INTEGER DEFAULT 1,
                repetir_minutos INTEGER DEFAULT 15,
                dias_anticipacion INTEGER DEFAULT 2,
                incluir_stock_bajo INTEGER DEFAULT 1,
                incluir_vencimientos INTEGER DEFAULT 1,
                incluir_agenda INTEGER DEFAULT 1,
                inicio_windows INTEGER DEFAULT 1,
                actualizado TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS sidebar_clima_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                habilitado INTEGER DEFAULT 1,
                ubicacion TEXT DEFAULT 'Santiago, Chile',
                latitud REAL,
                longitud REAL,
                nombre_mostrado TEXT DEFAULT '',
                timezone TEXT DEFAULT '',
                actualizado TEXT DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS updater_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                habilitado INTEGER DEFAULT 0,
                github_repo TEXT DEFAULT '',
                release_asset TEXT DEFAULT 'GestionStockPro.exe',
                permitir_prerelease INTEGER DEFAULT 0,
                github_token TEXT DEFAULT '',
                actualizado TEXT DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        cursor.execute(
            """
            INSERT OR IGNORE INTO updater_config (
                id, habilitado, github_repo, release_asset, permitir_prerelease, github_token
            )
            VALUES (1, 0, '', 'GestionStockPro.exe', 0, '')
            """
        )

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS agenda_recordatorios_descartados (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                evento_id INTEGER NOT NULL,
                ventana_clave TEXT NOT NULL,
                descartado_en TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(evento_id, ventana_clave)
            )
        ''')

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS compras_pendientes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                insumo_id INTEGER,
                nombre TEXT NOT NULL,
                cantidad REAL DEFAULT 0,
                unidad TEXT DEFAULT 'unidad',
                precio_unitario REAL DEFAULT 0,
                precio_incluye_iva INTEGER DEFAULT 1,
                estado TEXT DEFAULT 'pendiente',
                nota TEXT,
                creado TEXT DEFAULT CURRENT_TIMESTAMP,
                actualizado TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (insumo_id) REFERENCES insumos(id)
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS agenda_notas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                titulo TEXT NOT NULL,
                contenido TEXT,
                tipo TEXT DEFAULT 'texto',
                checklist_json TEXT,
                color TEXT DEFAULT 'amarilla',
                fijada INTEGER DEFAULT 0,
                recordatorio TEXT,
                estado TEXT DEFAULT 'activa',
                creada TEXT DEFAULT CURRENT_TIMESTAMP,
                actualizada TEXT DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS producto_mermas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                producto_id INTEGER NOT NULL,
                cantidad REAL NOT NULL,
                motivo TEXT NOT NULL,
                detalle TEXT,
                lotes_json TEXT,
                stock_anterior REAL,
                stock_nuevo REAL,
                codigo_operacion TEXT,
                estado TEXT DEFAULT 'activa',
                creado TEXT DEFAULT CURRENT_TIMESTAMP,
                revertida_en TEXT,
                FOREIGN KEY (producto_id) REFERENCES productos(id)
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS facturas_archivo (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                proveedor TEXT NOT NULL,
                fecha_factura TEXT NOT NULL,
                mes_clave TEXT NOT NULL,
                numero_factura TEXT,
                monto_total REAL DEFAULT 0,
                observacion TEXT,
                archivo_nombre_original TEXT NOT NULL,
                archivo_nombre_guardado TEXT NOT NULL,
                archivo_ruta_relativa TEXT NOT NULL,
                archivo_extension TEXT,
                archivo_mime TEXT,
                archivo_bytes INTEGER DEFAULT 0,
                eliminado INTEGER DEFAULT 0,
                eliminado_en TEXT,
                eliminado_motivo TEXT,
                creado TEXT DEFAULT CURRENT_TIMESTAMP,
                actualizado TEXT DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS facturas_auditoria (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                factura_id INTEGER,
                accion TEXT NOT NULL,
                snapshot_antes TEXT,
                snapshot_despues TEXT,
                metadata TEXT,
                creado TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (factura_id) REFERENCES facturas_archivo(id)
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS haccp_puntos_control (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT NOT NULL,
                categoria TEXT DEFAULT 'General',
                tipo_control TEXT DEFAULT 'rango',
                frecuencia_horas INTEGER DEFAULT 4,
                limite_min REAL,
                limite_max REAL,
                unidad TEXT DEFAULT '',
                activo INTEGER DEFAULT 1,
                orden INTEGER DEFAULT 100,
                creado TEXT DEFAULT CURRENT_TIMESTAMP,
                actualizado TEXT DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS haccp_registros (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                punto_id INTEGER NOT NULL,
                valor REAL,
                cumple INTEGER NOT NULL,
                observacion TEXT,
                accion_correctiva TEXT,
                responsable TEXT,
                registrado_en TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (punto_id) REFERENCES haccp_puntos_control(id) ON DELETE CASCADE
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS haccp_trazabilidad_insumos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                produccion_id INTEGER,
                receta_id INTEGER,
                producto_id INTEGER NOT NULL,
                producto_nombre TEXT,
                producto_lote_id INTEGER,
                producto_fecha_elaboracion TEXT,
                producto_fecha_vencimiento TEXT,
                cantidad_producto_lote REAL DEFAULT 0,
                insumo_id INTEGER NOT NULL,
                insumo_nombre TEXT,
                insumo_lote_id INTEGER,
                insumo_lote_codigo TEXT,
                insumo_fecha_elaboracion TEXT,
                insumo_fecha_vencimiento TEXT,
                insumo_fecha_ingreso TEXT,
                cantidad_insumo_usada REAL NOT NULL,
                unidad_insumo TEXT DEFAULT 'unidad',
                producido_en TEXT DEFAULT CURRENT_TIMESTAMP,
                mes_clave TEXT,
                creado TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (produccion_id) REFERENCES producciones(id) ON DELETE CASCADE,
                FOREIGN KEY (producto_id) REFERENCES productos(id),
                FOREIGN KEY (producto_lote_id) REFERENCES producto_lotes(id),
                FOREIGN KEY (insumo_id) REFERENCES insumos(id),
                FOREIGN KEY (insumo_lote_id) REFERENCES insumo_lotes(id)
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS haccp_tuya_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                habilitado INTEGER DEFAULT 0,
                user_code TEXT DEFAULT '',
                endpoint TEXT DEFAULT '',
                terminal_id TEXT DEFAULT '',
                device_id TEXT DEFAULT '',
                device_name TEXT DEFAULT '',
                device_bindings_json TEXT DEFAULT '',
                auto_interval_min INTEGER DEFAULT 15,
                token_info_json TEXT DEFAULT '',
                ultimo_temp REAL,
                ultima_humedad REAL,
                ultima_lectura_en TEXT,
                actualizado TEXT DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS haccp_tuya_lecturas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                device_name TEXT DEFAULT '',
                punto_id INTEGER,
                temperatura REAL,
                humedad REAL,
                origen TEXT DEFAULT 'manual',
                leida_en TEXT DEFAULT CURRENT_TIMESTAMP,
                creado TEXT DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )

        cursor.execute('''
            INSERT OR IGNORE INTO alertas_config (id) VALUES (1)
        ''')
        cursor.execute(
            '''
            INSERT OR IGNORE INTO sidebar_clima_config (id, habilitado, ubicacion)
            VALUES (1, 1, 'Santiago, Chile')
            '''
        )
        cursor.execute('''
            INSERT OR IGNORE INTO haccp_tuya_config (id, habilitado) VALUES (1, 0)
        ''')

        _sembrar_haccp_base(conn)
        
        conn.commit()  # <-- COMMIT ANTES DE CERRAR
        conn.close()   # <-- CERRAR AL FINAL
        print(f"[OK] Base de datos inicializada en: {DB_PATH}")
    except Exception as e:
        print(f"[ERROR] {e}")
        raise
    
def get_db():
    """Obtiene conexión a la base de datos con configuración optimizada"""
    conn = sqlite3.connect(DB_PATH, timeout=30)  # Aumentar timeout a 30 segundos
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")  # Modo WAL para mejor concurrencia
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")  # 30 segundos de espera
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def generar_codigo_operacion(prefijo="OP", fecha_base=None):
    base = fecha_base or obtener_hora_chile()
    if not isinstance(base, datetime):
        base = obtener_hora_chile()
    token = uuid.uuid4().hex[:4].upper()
    return f"{prefijo}-{base.strftime('%Y%m%d-%H%M%S')}-{token}"


def _normalizar_codigo_operacion(codigo_operacion=None, prefijo="OP"):
    raw = str(codigo_operacion or "").strip()[:80]
    if raw:
        return raw
    return generar_codigo_operacion(prefijo=prefijo)


def _normalizar_canal_venta(valor):
    raw = str(valor or "").strip().lower()
    if raw in {"uber", "uber_eats", "ubereats", "uber eats"}:
        return "uber_eats"
    if raw in {"pedidosya", "pedidos_ya", "pedidos ya", "py"}:
        return "pedidosya"
    if raw in {"tienda_online", "tienda", "online", "web", "ecommerce", "e-commerce"}:
        return "tienda_online"
    return "presencial"


def _safe_json_dumps(payload):
    if payload is None:
        return None
    if isinstance(payload, str):
        texto = payload.strip()
        return texto[:5000] if texto else None
    try:
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)
    except Exception:
        return json.dumps({"valor": str(payload)}, ensure_ascii=False, sort_keys=True)


def registrar_historial_cambio(
    recurso_tipo,
    recurso_nombre,
    accion,
    recurso_id=None,
    detalle=None,
    origen_modulo=None,
    metadata=None,
    conn=None,
):
    propia = conn is None
    db = conn or get_db()
    try:
        tipo = str(recurso_tipo or "").strip().lower() or "sistema"
        nombre = str(recurso_nombre or "").strip() or "Recurso"
        accion_norm = str(accion or "").strip().lower() or "actualizado"
        detalle_txt = str(detalle or "").strip() or None
        origen = str(origen_modulo or "").strip() or None
        rid = None
        if recurso_id is not None and str(recurso_id).strip() != "":
            rid = int(recurso_id)

        _ejecutar_write_con_reintento(
            db,
            """
            INSERT INTO historial_cambios (
                recurso_tipo, recurso_id, recurso_nombre, accion,
                detalle, origen_modulo, metadata_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tipo,
                rid,
                nombre[:180],
                accion_norm[:40],
                detalle_txt[:500] if detalle_txt else None,
                origen[:80] if origen else None,
                _safe_json_dumps(metadata),
            ),
        )
        if propia:
            db.commit()
        return True
    except Exception:
        if propia:
            db.rollback()
        raise
    finally:
        if propia:
            db.close()


def listar_historial_cambios(limit=500):
    conn = get_db()
    cursor = conn.cursor()
    try:
        limite = int(limit or 500)
        limite = max(1, min(limite, 5000))
        cursor.execute(
            """
            SELECT
                id, recurso_tipo, recurso_id, recurso_nombre,
                accion, detalle, origen_modulo, metadata_json, fecha_hora
            FROM historial_cambios
            ORDER BY datetime(fecha_hora) DESC, id DESC
            LIMIT ?
            """,
            (limite,),
        )
        return [dict(r) for r in cursor.fetchall()]
    finally:
        conn.close()


def eliminar_historial_cambio(cambio_id):
    conn = get_db()
    cursor = conn.cursor()
    try:
        cambio_id_int = int(cambio_id)
        cursor.execute("DELETE FROM historial_cambios WHERE id = ?", (cambio_id_int,))
        eliminado = cursor.rowcount > 0
        conn.commit()
        if not eliminado:
            return {"success": False, "error": "Movimiento no encontrado"}
        return {"success": True}
    except Exception as e:
        conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        conn.close()


def _es_error_db_bloqueo(exc):
    texto = str(exc or "").lower()
    return "database is locked" in texto or "database table is locked" in texto or "database schema is locked" in texto


def _ejecutar_write_con_reintento(conn, sql, params=(), intentos=DB_LOCK_RETRY_ATTEMPTS):
    total = max(1, int(intentos or 1))
    for intento in range(total):
        try:
            return conn.execute(sql, params)
        except sqlite3.OperationalError as exc:
            if not _es_error_db_bloqueo(exc) or intento >= total - 1:
                raise
            espera_ms = DB_LOCK_RETRY_BASE_DELAY_MS * (2 ** intento) + random.randint(0, 60)
            time.sleep(espera_ms / 1000.0)

def _table_columns(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    return {row["name"] for row in cursor.fetchall()}


def _column_exists(conn, table_name, column_name):
    return column_name in _table_columns(conn, table_name)


def _ensure_column(conn, table_name, column_name, ddl):
    if _column_exists(conn, table_name, column_name):
        return
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl}")


def _table_exists(conn, table_name):
    cursor = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    )
    return cursor.fetchone() is not None


def _delete_orphan_rows(conn, child_table, child_column, parent_table, parent_column="id"):
    if not _table_exists(conn, child_table) or not _table_exists(conn, parent_table):
        return 0
    sql = f"""
        DELETE FROM {child_table}
        WHERE {child_column} IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM {parent_table} p
              WHERE p.{parent_column} = {child_table}.{child_column}
          )
    """
    return conn.execute(sql).rowcount


def _nullify_orphan_fk(conn, child_table, child_column, parent_table, parent_column="id"):
    if not _table_exists(conn, child_table) or not _table_exists(conn, parent_table):
        return 0
    sql = f"""
        UPDATE {child_table}
        SET {child_column} = NULL
        WHERE {child_column} IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM {parent_table} p
              WHERE p.{parent_column} = {child_table}.{child_column}
          )
    """
    return conn.execute(sql).rowcount


def _limpiar_huerfanos_fk(conn):
    """
    Corrige registros huérfanos acumulados por versiones anteriores con FK desactivadas.
    Es idempotente y se ejecuta durante migración.
    """
    cambios = {}

    def _add(nombre, cantidad):
        if cantidad:
            cambios[nombre] = cambios.get(nombre, 0) + int(cantidad)

    # Referencias opcionales: se conserva el registro, se nulifica FK.
    _add(
        "recetas.producto_id",
        _nullify_orphan_fk(conn, "recetas", "producto_id", "productos"),
    )

    # Referencias obligatorias: se eliminan filas huérfanas.
    _add("venta_lotes.venta_id", _delete_orphan_rows(conn, "venta_lotes", "venta_id", "ventas"))
    _add("venta_items.venta_id", _delete_orphan_rows(conn, "venta_items", "venta_id", "ventas"))
    _add("venta_detalles.venta_id", _delete_orphan_rows(conn, "venta_detalles", "venta_id", "ventas"))
    _add("producto_lotes.producto_id", _delete_orphan_rows(conn, "producto_lotes", "producto_id", "productos"))
    _add("producciones.receta_id", _delete_orphan_rows(conn, "producciones", "receta_id", "recetas"))
    _add(
        "produccion_movimientos.produccion_id",
        _delete_orphan_rows(conn, "produccion_movimientos", "produccion_id", "producciones"),
    )
    _add("receta_items.receta_id", _delete_orphan_rows(conn, "receta_items", "receta_id", "recetas"))
    _add("receta_items.insumo_id", _delete_orphan_rows(conn, "receta_items", "insumo_id", "insumos"))
    _add("receta_items.producto_id", _delete_orphan_rows(conn, "receta_items", "producto_id", "productos"))
    _add(
        "producto_insumos_venta.producto_id",
        _delete_orphan_rows(conn, "producto_insumos_venta", "producto_id", "productos"),
    )
    _add(
        "producto_insumos_venta.insumo_id",
        _delete_orphan_rows(conn, "producto_insumos_venta", "insumo_id", "insumos"),
    )
    _add(
        "producto_productos_venta.producto_id",
        _delete_orphan_rows(conn, "producto_productos_venta", "producto_id", "productos"),
    )
    _add(
        "producto_productos_venta.producto_asociado_id",
        _delete_orphan_rows(conn, "producto_productos_venta", "producto_asociado_id", "productos"),
    )
    _add(
        "producto_desactivaciones_manuales.producto_id",
        _delete_orphan_rows(conn, "producto_desactivaciones_manuales", "producto_id", "productos"),
    )
    _add("venta_insumos.venta_id", _delete_orphan_rows(conn, "venta_insumos", "venta_id", "ventas"))
    _add("venta_insumos.producto_id", _delete_orphan_rows(conn, "venta_insumos", "producto_id", "productos"))
    _add("venta_insumos.insumo_id", _delete_orphan_rows(conn, "venta_insumos", "insumo_id", "insumos"))
    _add("insumo_lotes.insumo_id", _delete_orphan_rows(conn, "insumo_lotes", "insumo_id", "insumos"))
    _add("venta_insumo_lotes.venta_id", _delete_orphan_rows(conn, "venta_insumo_lotes", "venta_id", "ventas"))
    _add("venta_insumo_lotes.producto_id", _delete_orphan_rows(conn, "venta_insumo_lotes", "producto_id", "productos"))
    _add("venta_insumo_lotes.insumo_id", _delete_orphan_rows(conn, "venta_insumo_lotes", "insumo_id", "insumos"))
    _add(
        "venta_insumo_lotes.insumo_lote_id",
        _nullify_orphan_fk(conn, "venta_insumo_lotes", "insumo_lote_id", "insumo_lotes"),
    )
    _add(
        "haccp_trazabilidad_insumos.produccion_id",
        _delete_orphan_rows(conn, "haccp_trazabilidad_insumos", "produccion_id", "producciones"),
    )
    _add(
        "haccp_trazabilidad_insumos.producto_id",
        _delete_orphan_rows(conn, "haccp_trazabilidad_insumos", "producto_id", "productos"),
    )
    _add(
        "haccp_trazabilidad_insumos.insumo_id",
        _delete_orphan_rows(conn, "haccp_trazabilidad_insumos", "insumo_id", "insumos"),
    )
    _add(
        "haccp_trazabilidad_insumos.producto_lote_id",
        _nullify_orphan_fk(conn, "haccp_trazabilidad_insumos", "producto_lote_id", "producto_lotes"),
    )
    _add(
        "haccp_trazabilidad_insumos.insumo_lote_id",
        _nullify_orphan_fk(conn, "haccp_trazabilidad_insumos", "insumo_lote_id", "insumo_lotes"),
    )
    _add("haccp_registros.punto_id", _delete_orphan_rows(conn, "haccp_registros", "punto_id", "haccp_puntos_control"))

    # Tabla intermedia de migración antigua que no debe permanecer en producción.
    if _table_exists(conn, "receta_items_old"):
        conn.execute("DROP TABLE receta_items_old")
        _add("receta_items_old.drop", 1)

    if cambios:
        detalle = ", ".join(f"{k}: {v}" for k, v in sorted(cambios.items()))
        print(f"[WARN] Limpieza de integridad aplicada -> {detalle}")

    return sum(cambios.values())


def registrar_movimiento_stock(
    tipo_recurso,
    recurso_id,
    accion,
    cantidad,
    stock_anterior=None,
    stock_nuevo=None,
    referencia_tipo=None,
    referencia_id=None,
    detalle=None,
    unidad=None,
    lote_id=None,
    origen_modulo=None,
    codigo_operacion=None,
    usuario=None,
    metadata=None,
    conn=None,
):
    """Registra un movimiento en ledger central y replica compatibilidad en stock_movimientos."""
    own_conn = False
    if conn is None:
        conn = get_db()
        own_conn = True

    tipo_recurso_norm = str(tipo_recurso or "").strip().lower()
    if tipo_recurso_norm not in {"producto", "insumo"}:
        tipo_recurso_norm = "producto" if "prod" in tipo_recurso_norm else "insumo"

    tipo_operacion = str(accion or "").strip()[:80] or "movimiento"
    origen_modulo = str(origen_modulo or referencia_tipo or "sistema").strip()[:80] or "sistema"
    codigo_op = _normalizar_codigo_operacion(codigo_operacion)
    metadata_json = _safe_json_dumps(metadata)
    usuario_val = str(usuario or "").strip()[:80] or None
    fecha_mov = obtener_hora_chile().strftime("%Y-%m-%d %H:%M:%S")

    cantidad_raw = float(cantidad or 0)
    if stock_anterior is not None and stock_nuevo is not None:
        try:
            cantidad_delta = float(stock_nuevo) - float(stock_anterior)
        except (TypeError, ValueError):
            cantidad_delta = cantidad_raw
    else:
        cantidad_delta = cantidad_raw
        accion_norm = tipo_operacion.lower()
        if (
            ("salida" in accion_norm or "consumo" in accion_norm or "merma" in accion_norm or "descuento" in accion_norm)
            and "reversion" not in accion_norm
            and "entrada" not in accion_norm
            and "alta" not in accion_norm
        ):
            cantidad_delta = -abs(cantidad_raw)

    try:
        row_ledger = _ejecutar_write_con_reintento(
            conn,
            """
            INSERT INTO stock_ledger (
                fecha, tipo_operacion, recurso_tipo, recurso_id, cantidad_delta, stock_anterior, stock_nuevo, unidad,
                lote_id, origen_modulo, codigo_operacion, usuario, metadata_json,
                referencia_tipo, referencia_id, detalle, creado
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                fecha_mov,
                tipo_operacion,
                tipo_recurso_norm,
                recurso_id,
                float(cantidad_delta or 0),
                None if stock_anterior is None else float(stock_anterior),
                None if stock_nuevo is None else float(stock_nuevo),
                unidad,
                lote_id,
                origen_modulo,
                codigo_op,
                usuario_val,
                metadata_json,
                referencia_tipo,
                referencia_id,
                detalle,
                fecha_mov,
            ),
        )
        ledger_id = row_ledger.lastrowid if row_ledger is not None else None

        _ejecutar_write_con_reintento(
            conn,
            """
            INSERT INTO stock_movimientos (
                tipo_recurso, recurso_id, accion, cantidad, stock_anterior, stock_nuevo,
                referencia_tipo, referencia_id, detalle,
                tipo_operacion, recurso_tipo, cantidad_delta, unidad, lote_id,
                origen_modulo, codigo_operacion, usuario, metadata_json, fecha, creado
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tipo_recurso_norm,
                recurso_id,
                tipo_operacion,
                cantidad_raw,
                None if stock_anterior is None else float(stock_anterior),
                None if stock_nuevo is None else float(stock_nuevo),
                referencia_tipo,
                referencia_id,
                detalle,
                tipo_operacion,
                tipo_recurso_norm,
                float(cantidad_delta or 0),
                unidad,
                lote_id,
                origen_modulo,
                codigo_op,
                usuario_val,
                metadata_json,
                fecha_mov,
                fecha_mov,
            ),
        )
        if own_conn:
            conn.commit()
        return {"codigo_operacion": codigo_op, "ledger_id": ledger_id}
    finally:
        if own_conn:
            conn.close()


def calcular_dias_para_vencer(producto):
    """Calcula días para vencer - SIN USAR STRFTIME"""
    fecha_str = producto.get('fecha_vencimiento')
    if not fecha_str:
        return None
    
    try:
        # fecha_vencimiento ya es string en formato YYYY-MM-DD
        if isinstance(fecha_str, str):
            # Parsear año, mes, día manualmente
            partes = fecha_str.split('-')
            if len(partes) == 3:
                ano = int(partes[0])
                mes = int(partes[1])
                dia = int(partes[2])
                
                from datetime import date
                fecha_venc = date(ano, mes, dia)
                hoy = date.today()
                dias = (fecha_venc - hoy).days
                return dias
        return None
    except Exception as e:
        print(f"Error calculando días: {e}, valor: {fecha_str}")
        return None

def obtener_productos_con_dias(orden='nombre', direccion='asc', solo_cero=False):
    """Obtiene productos con días para vencer"""
    conn = get_db()
    cursor = conn.cursor()
    
    orden_valido = 'nombre' if orden == 'nombre' else 'stock'
    dir_valido = 'DESC' if direccion == 'desc' else 'ASC'
    
    query = "SELECT * FROM productos WHERE COALESCE(eliminado, 0) = 0"
    if solo_cero:
        query += " AND stock = 0"
    query += f" ORDER BY {orden_valido} {dir_valido}"
    
    cursor.execute(query)
    filas = cursor.fetchall()
    conn.close()
    
    resultado = []
    for fila in filas:
        # Convertir explícitamente a dict
        prod = {
            'id': fila['id'],
            'nombre': fila['nombre'],
            'stock': fila['stock'],
            'stock_minimo': fila['stock_minimo'],
            'unidad': fila['unidad'] or 'unidad',
            'porcion_cantidad': float(fila['porcion_cantidad'] or 1) if 'porcion_cantidad' in fila.keys() else 1.0,
            'porcion_unidad': _normalizar_unidad_producto(
                (fila['porcion_unidad'] if 'porcion_unidad' in fila.keys() else None) or fila['unidad'] or 'unidad'
            ),
            'stock_dependencia_tipo': (
                str(fila['stock_dependencia_tipo']).strip().lower()
                if 'stock_dependencia_tipo' in fila.keys() and fila['stock_dependencia_tipo'] is not None
                else None
            ),
            'stock_dependencia_id': (
                int(fila['stock_dependencia_id'] or 0)
                if 'stock_dependencia_id' in fila.keys() and fila['stock_dependencia_id'] is not None
                else None
            ),
            'stock_dependencia_cantidad': (
                float(fila['stock_dependencia_cantidad'] or 1)
                if 'stock_dependencia_cantidad' in fila.keys()
                else 1.0
            ),
            'fecha_vencimiento': fila['fecha_vencimiento'],
            'alerta_dias': fila['alerta_dias'] or 2
        }
        if prod['stock_dependencia_tipo'] not in ('producto', 'insumo'):
            prod['stock_dependencia_tipo'] = None
            prod['stock_dependencia_id'] = None
            prod['stock_dependencia_cantidad'] = 1.0
        prod['dias_para_vencer'] = calcular_dias_para_vencer(prod)
        resultado.append(prod)
    
    return resultado


def obtener_producto_detalle(producto_id):
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT id, nombre, stock, stock_minimo, unidad, fecha_vencimiento,
                   alerta_dias, precio, vida_util_dias, icono, foto,
                   porcion_cantidad, porcion_unidad,
                    stock_dependencia_tipo, stock_dependencia_id, stock_dependencia_cantidad
            FROM productos
            WHERE id = ?
              AND COALESCE(eliminado, 0) = 0
            """,
            (producto_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        item = dict(row)
        item["porcion_cantidad"] = float(item.get("porcion_cantidad") or 1)
        item["porcion_unidad"] = _normalizar_unidad_producto(item.get("porcion_unidad") or item.get("unidad") or "unidad")
        item["icono"] = str(item.get("icono") or "cupcake").strip().lower() or "cupcake"
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
            dep_cantidad = 1.0
        if dep_cantidad <= 0:
            dep_cantidad = 1.0
        if not tipo_dep:
            dep_id = 0
            dep_cantidad = 1.0

        item["stock_dependencia_tipo"] = tipo_dep
        item["stock_dependencia_id"] = dep_id if dep_id > 0 else None
        item["stock_dependencia_cantidad"] = dep_cantidad
        cursor.execute(
            """
            SELECT piv.insumo_id, piv.cantidad, piv.unidad,
                   i.nombre AS insumo_nombre,
                   COALESCE(NULLIF(TRIM(i.unidad), ''), 'unidad') AS insumo_unidad
            FROM producto_insumos_venta piv
            LEFT JOIN insumos i ON i.id = piv.insumo_id
            WHERE piv.producto_id = ?
            ORDER BY i.nombre COLLATE NOCASE ASC, piv.id ASC
            """,
            (producto_id,),
        )
        item["insumos_venta"] = [
            {
                "insumo_id": int(r["insumo_id"]),
                "insumo_nombre": r["insumo_nombre"] or "Insumo",
                "cantidad": float(r["cantidad"] or 0),
                "unidad": _normalizar_unidad_producto(r["unidad"] or "unidad"),
                "insumo_unidad": _normalizar_unidad_producto(r["insumo_unidad"] or "unidad"),
            }
            for r in cursor.fetchall()
        ]
        cursor.execute(
            """
            SELECT ppv.producto_asociado_id, ppv.cantidad,
                   p.nombre AS producto_nombre
            FROM producto_productos_venta ppv
            LEFT JOIN productos p ON p.id = ppv.producto_asociado_id
            WHERE ppv.producto_id = ?
            ORDER BY p.nombre COLLATE NOCASE ASC, ppv.id ASC
            """,
            (producto_id,),
        )
        item["productos_venta"] = [
            {
                "producto_id": int(r["producto_asociado_id"]),
                "producto_nombre": r["producto_nombre"] or "Producto",
                "cantidad": float(r["cantidad"] or 0),
            }
            for r in cursor.fetchall()
        ]
        if tipo_dep == "producto" and dep_id > 0:
            cursor.execute("SELECT nombre FROM productos WHERE id = ?", (dep_id,))
            dep_row = cursor.fetchone()
            item["stock_dependencia_nombre"] = dep_row["nombre"] if dep_row else None
        elif tipo_dep == "insumo" and dep_id > 0:
            cursor.execute("SELECT nombre FROM insumos WHERE id = ?", (dep_id,))
            dep_row = cursor.fetchone()
            item["stock_dependencia_nombre"] = dep_row["nombre"] if dep_row else None
        else:
            item["stock_dependencia_nombre"] = None
        return item
    finally:
        conn.close()


def actualizar_producto(producto_id, data):
    """Actualiza datos de producto y sincroniza stock con FIFO/lotes."""
    conn = get_db()
    cursor = conn.cursor()
    actual = None
    try:
        cursor.execute("SELECT * FROM productos WHERE id = ?", (producto_id,))
        actual = cursor.fetchone()
        if not actual:
            raise ValueError("Producto no encontrado")

        nombre = str(data.get("nombre", actual["nombre"])).strip()
        if not nombre:
            raise ValueError("El nombre del producto es obligatorio")

        unidad = _normalizar_unidad_producto(data.get("unidad", actual["unidad"] or "unidad"))
        icono = str(data.get("icono", actual["icono"] if "icono" in actual.keys() else "cupcake") or "cupcake").strip().lower()
        stock_minimo = float(data.get("stock_minimo", actual["stock_minimo"] or 0) or 0)
        alerta_dias = int(data.get("alerta_dias", actual["alerta_dias"] or 2) or 2)
        precio = float(data.get("precio", actual["precio"] or 0) or 0)
        vida_util_dias = int(data.get("vida_util_dias", actual["vida_util_dias"] or 0) or 0)
        porcion_actual = actual["porcion_cantidad"] if "porcion_cantidad" in actual.keys() else 1
        porcion_unidad_actual = (actual["porcion_unidad"] if "porcion_unidad" in actual.keys() else None) or unidad
        porcion_cantidad = float(data.get("porcion_cantidad", porcion_actual) or 1)
        porcion_unidad = _normalizar_unidad_producto(data.get("porcion_unidad", porcion_unidad_actual))
        categoria_tienda_actual = (actual["categoria_tienda"] if "categoria_tienda" in actual.keys() else None) or "General"
        descripcion_tienda_actual = (actual["descripcion_tienda"] if "descripcion_tienda" in actual.keys() else None) or ""
        descuento_tienda_actual = float(actual["descuento_tienda_pct"] or 0) if "descuento_tienda_pct" in actual.keys() else 0.0
        foto_fit_tienda_actual = (actual["foto_fit_tienda"] if "foto_fit_tienda" in actual.keys() else None) or "cover"
        foto_pos_tienda_actual = (actual["foto_pos_tienda"] if "foto_pos_tienda" in actual.keys() else None) or "center"
        foto_pos_x_tienda_actual = float(actual["foto_pos_x_tienda"] or 50) if "foto_pos_x_tienda" in actual.keys() else 50.0
        foto_pos_y_tienda_actual = float(actual["foto_pos_y_tienda"] or 50) if "foto_pos_y_tienda" in actual.keys() else 50.0
        foto_zoom_tienda_actual = float(actual["foto_zoom_tienda"] or 100) if "foto_zoom_tienda" in actual.keys() else 100.0
        destacado_tienda_actual = int(actual["destacado_tienda"] or 0) if "destacado_tienda" in actual.keys() else 0
        orden_tienda_actual = int(actual["orden_tienda"] or 0) if "orden_tienda" in actual.keys() else 0
        activo_tienda_actual = int(actual["activo_tienda"] or 0) if "activo_tienda" in actual.keys() else 1
        categoria_tienda = str(data.get("categoria_tienda", categoria_tienda_actual) or "").strip()[:60] or "General"
        descripcion_tienda = str(data.get("descripcion_tienda", descripcion_tienda_actual) or "").strip()[:800]
        descuento_tienda_pct = float(data.get("descuento_tienda_pct", descuento_tienda_actual) or 0)
        foto_fit_tienda = str(data.get("foto_fit_tienda", foto_fit_tienda_actual) or "cover").strip().lower()
        foto_pos_tienda = str(data.get("foto_pos_tienda", foto_pos_tienda_actual) or "center").strip().lower()
        foto_pos_x_tienda = float(data.get("foto_pos_x_tienda", foto_pos_x_tienda_actual) or 50)
        foto_pos_y_tienda = float(data.get("foto_pos_y_tienda", foto_pos_y_tienda_actual) or 50)
        foto_zoom_tienda = float(data.get("foto_zoom_tienda", foto_zoom_tienda_actual) or 100)
        destacado_raw = data.get("destacado_tienda", destacado_tienda_actual)
        if isinstance(destacado_raw, str):
            destacado_tienda = 1 if destacado_raw.strip().lower() in {"1", "true", "si", "yes", "on"} else 0
        else:
            destacado_tienda = 1 if bool(destacado_raw) else 0
        orden_tienda = int(data.get("orden_tienda", orden_tienda_actual) or 0)
        activo_raw = data.get("activo_tienda", activo_tienda_actual)
        if isinstance(activo_raw, str):
            activo_tienda = 1 if activo_raw.strip().lower() in {"1", "true", "si", "yes", "on"} else 0
        else:
            activo_tienda = 1 if bool(activo_raw) else 0
        insumos_venta = data.get("insumos_venta", None)
        productos_venta = data.get("productos_venta", None)
        tipo_dep_actual = str(actual["stock_dependencia_tipo"] or "").strip().lower() if "stock_dependencia_tipo" in actual.keys() else ""
        tipo_dep_raw = data.get("stock_dependencia_tipo", tipo_dep_actual)
        tipo_dep = str(tipo_dep_raw or "").strip().lower()
        if tipo_dep in {"", "none", "ninguna", "null", "sin"}:
            tipo_dep = None
        dep_id_actual = int(actual["stock_dependencia_id"] or 0) if "stock_dependencia_id" in actual.keys() else 0
        dep_id_raw = data.get("stock_dependencia_id", dep_id_actual)
        try:
            dep_id = int(dep_id_raw or 0)
        except (TypeError, ValueError):
            dep_id = 0
        dep_cantidad_actual = float(actual["stock_dependencia_cantidad"] or 1) if "stock_dependencia_cantidad" in actual.keys() else 1
        dep_cantidad_raw = data.get("stock_dependencia_cantidad", dep_cantidad_actual)
        try:
            dep_cantidad = float(dep_cantidad_raw or 1)
        except (TypeError, ValueError):
            dep_cantidad = 1.0

        if stock_minimo < 0:
            raise ValueError("El stock mínimo no puede ser negativo")
        if alerta_dias < 0:
            raise ValueError("La alerta previa no puede ser negativa")
        if precio < 0:
            raise ValueError("El precio no puede ser negativo")
        if vida_util_dias < 0:
            raise ValueError("La vida útil no puede ser negativa")
        if porcion_cantidad <= 0:
            raise ValueError("La porción debe ser mayor a 0")
        if not _unidades_compatibles_porcion(unidad, porcion_unidad):
            raise ValueError(f"La unidad de porción ({porcion_unidad}) no es compatible con la unidad del stock ({unidad})")
        if descuento_tienda_pct < 0 or descuento_tienda_pct > 100:
            raise ValueError("El descuento de tienda debe estar entre 0 y 100")
        if foto_fit_tienda not in {"cover", "contain"}:
            raise ValueError("El ajuste de foto de tienda es invalido")
        if foto_pos_tienda not in {"center", "top", "bottom"}:
            raise ValueError("La posicion de foto de tienda es invalida")
        if foto_pos_x_tienda < 0 or foto_pos_x_tienda > 100:
            raise ValueError("La posicion horizontal de foto debe estar entre 0 y 100")
        if foto_pos_y_tienda < 0 or foto_pos_y_tienda > 100:
            raise ValueError("La posicion vertical de foto debe estar entre 0 y 100")
        if foto_zoom_tienda < 50 or foto_zoom_tienda > 220:
            raise ValueError("El zoom de foto debe estar entre 50 y 220")
        if orden_tienda < 0:
            raise ValueError("El orden de tienda no puede ser negativo")
        if not icono:
            icono = "cupcake"
        if dep_cantidad <= 0:
            raise ValueError("La cantidad de dependencia de stock debe ser mayor a 0")
        if tipo_dep not in {None, "producto", "insumo"}:
            raise ValueError("Tipo de dependencia de stock inválido")
        if tipo_dep == "producto":
            if dep_id <= 0:
                raise ValueError("Selecciona un producto válido para dependencia de stock")
            if dep_id == int(producto_id):
                raise ValueError("No puedes depender del mismo producto")
            cursor.execute("SELECT id FROM productos WHERE id = ?", (dep_id,))
            if not cursor.fetchone():
                raise ValueError("El producto de dependencia no existe")
        elif tipo_dep == "insumo":
            if dep_id <= 0:
                raise ValueError("Selecciona un insumo válido para dependencia de stock")
            cursor.execute("SELECT id FROM insumos WHERE id = ?", (dep_id,))
            if not cursor.fetchone():
                raise ValueError("El insumo de dependencia no existe")
        else:
            dep_id = 0
            dep_cantidad = 1.0

        insumos_venta_normalizados = None
        if insumos_venta is not None:
            if not isinstance(insumos_venta, list):
                raise ValueError("Los insumos asociados deben enviarse en una lista")

            insumos_venta_normalizados = []
            insumos_repetidos = set()
            for idx, fila in enumerate(insumos_venta, start=1):
                if not isinstance(fila, dict):
                    raise ValueError(f"Insumo asociado #{idx} inválido")

                try:
                    insumo_id = int(fila.get("insumo_id") or 0)
                except (TypeError, ValueError):
                    raise ValueError(f"Insumo asociado #{idx}: ID inválido")

                cantidad_raw = fila.get("cantidad", 0)
                try:
                    cantidad = float(cantidad_raw)
                except (TypeError, ValueError):
                    raise ValueError(f"Insumo asociado #{idx}: cantidad inválida")

                unidad_asociada = _normalizar_unidad_producto(fila.get("unidad") or "unidad")

                if insumo_id <= 0:
                    raise ValueError(f"Insumo asociado #{idx}: selecciona un insumo válido")
                if cantidad <= 0:
                    raise ValueError(f"Insumo asociado #{idx}: la cantidad debe ser mayor a 0")
                if insumo_id in insumos_repetidos:
                    raise ValueError("No puedes repetir el mismo insumo asociado en un producto")

                cursor.execute(
                    "SELECT id, nombre, unidad FROM insumos WHERE id = ?",
                    (insumo_id,),
                )
                insumo_db = cursor.fetchone()
                if not insumo_db:
                    raise ValueError(f"Insumo asociado #{idx}: no existe en la base de datos")

                unidad_insumo = _normalizar_unidad_producto(insumo_db["unidad"] or "unidad")
                if not _unidades_compatibles_porcion(unidad_insumo, unidad_asociada):
                    raise ValueError(
                        f"{insumo_db['nombre']}: unidad incompatible ({unidad_asociada}) con el stock del insumo ({unidad_insumo})"
                    )

                insumos_venta_normalizados.append(
                    {
                        "insumo_id": insumo_id,
                        "cantidad": cantidad,
                        "unidad": unidad_asociada,
                    }
                )
                insumos_repetidos.add(insumo_id)

        productos_venta_normalizados = None
        if productos_venta is not None:
            if not isinstance(productos_venta, list):
                raise ValueError("Los productos asociados deben enviarse en una lista")

            productos_venta_normalizados = []
            productos_repetidos = set()
            for idx, fila in enumerate(productos_venta, start=1):
                if not isinstance(fila, dict):
                    raise ValueError(f"Producto asociado #{idx} inválido")

                try:
                    producto_asociado_id = int(fila.get("producto_id") or fila.get("producto_asociado_id") or 0)
                except (TypeError, ValueError):
                    raise ValueError(f"Producto asociado #{idx}: ID inválido")

                try:
                    cantidad_asociada = float(fila.get("cantidad") or 0)
                except (TypeError, ValueError):
                    raise ValueError(f"Producto asociado #{idx}: cantidad inválida")

                if producto_asociado_id <= 0:
                    raise ValueError(f"Producto asociado #{idx}: selecciona un producto válido")
                if producto_asociado_id == int(producto_id):
                    raise ValueError("No puedes asociar el mismo producto a sí mismo")
                if cantidad_asociada <= 0:
                    raise ValueError(f"Producto asociado #{idx}: la cantidad debe ser mayor a 0")
                if producto_asociado_id in productos_repetidos:
                    raise ValueError("No puedes repetir el mismo producto asociado")

                cursor.execute(
                    "SELECT id, nombre FROM productos WHERE id = ?",
                    (producto_asociado_id,),
                )
                producto_db = cursor.fetchone()
                if not producto_db:
                    raise ValueError(f"Producto asociado #{idx}: no existe en la base de datos")

                productos_venta_normalizados.append(
                    {
                        "producto_asociado_id": producto_asociado_id,
                        "cantidad": cantidad_asociada,
                    }
                )
                productos_repetidos.add(producto_asociado_id)

        if "fecha_vencimiento" in data:
            fecha_vencimiento = data.get("fecha_vencimiento") or None
        else:
            fecha_vencimiento = actual["fecha_vencimiento"]

        cursor.execute(
            """
            UPDATE productos
            SET nombre = ?, stock_minimo = ?, unidad = ?, fecha_vencimiento = ?,
                alerta_dias = ?, precio = ?, vida_util_dias = ?, icono = ?,
                porcion_cantidad = ?, porcion_unidad = ?,
                stock_dependencia_tipo = ?, stock_dependencia_id = ?, stock_dependencia_cantidad = ?,
                categoria_tienda = ?, descripcion_tienda = ?, descuento_tienda_pct = ?,
                foto_fit_tienda = ?, foto_pos_tienda = ?, foto_pos_x_tienda = ?, foto_pos_y_tienda = ?, foto_zoom_tienda = ?,
                destacado_tienda = ?, orden_tienda = ?, activo_tienda = ?
            WHERE id = ?
            """,
            (
                nombre,
                stock_minimo,
                unidad,
                fecha_vencimiento,
                alerta_dias,
                precio,
                vida_util_dias,
                icono,
                porcion_cantidad,
                porcion_unidad,
                tipo_dep,
                (dep_id if dep_id > 0 else None),
                dep_cantidad,
                categoria_tienda,
                descripcion_tienda,
                descuento_tienda_pct,
                foto_fit_tienda,
                foto_pos_tienda,
                foto_pos_x_tienda,
                foto_pos_y_tienda,
                foto_zoom_tienda,
                destacado_tienda,
                orden_tienda,
                activo_tienda,
                producto_id,
            ),
        )

        if insumos_venta_normalizados is not None:
            cursor.execute("DELETE FROM producto_insumos_venta WHERE producto_id = ?", (producto_id,))
            for assoc in insumos_venta_normalizados:
                cursor.execute(
                    """
                    INSERT INTO producto_insumos_venta (producto_id, insumo_id, cantidad, unidad, actualizado)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (producto_id, assoc["insumo_id"], assoc["cantidad"], assoc["unidad"]),
                )

        if productos_venta_normalizados is not None:
            cursor.execute("DELETE FROM producto_productos_venta WHERE producto_id = ?", (producto_id,))
            for assoc in productos_venta_normalizados:
                cursor.execute(
                    """
                    INSERT INTO producto_productos_venta (producto_id, producto_asociado_id, cantidad, actualizado)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (producto_id, assoc["producto_asociado_id"], assoc["cantidad"]),
                )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    if actual is not None and "stock" in data:
        stock_objetivo = float(data.get("stock") or 0)
        stock_actual = float(actual["stock"] or 0)
        delta = stock_objetivo - stock_actual
        if abs(delta) > 0:
            actualizar_stock_producto(
                producto_id,
                delta,
                referencia_tipo="edicion_producto",
                detalle="Sincronización de stock desde edición de producto",
            )

    return True

def _descontar_lotes_fifo_cursor(cursor, producto_id, cantidad_a_descontar):
    cursor.execute(
        """
        SELECT id, cantidad, fecha_vencimiento
        FROM producto_lotes
        WHERE producto_id = ? AND cantidad > 0
        ORDER BY COALESCE(fecha_vencimiento, '9999-12-31') ASC, fecha_ingreso ASC, id ASC
        """,
        (producto_id,),
    )
    lotes = cursor.fetchall()

    if not lotes:
        return {"success": False, "error": "No hay lotes disponibles", "detalle": []}

    cantidad_restante = float(cantidad_a_descontar)
    detalle_lotes = []

    for lote in lotes:
        if cantidad_restante <= 0:
            break

        cantidad_lote = float(lote["cantidad"])
        cantidad_a_usar = min(cantidad_restante, cantidad_lote)
        nueva_cantidad = cantidad_lote - cantidad_a_usar

        cursor.execute(
            "UPDATE producto_lotes SET cantidad = ? WHERE id = ?",
            (nueva_cantidad, lote["id"]),
        )

        detalle_lotes.append(
            {
                "lote_id": lote["id"],
                "cantidad_usada": cantidad_a_usar,
                "fecha_vencimiento": lote["fecha_vencimiento"],
                "cantidad_restante_lote": nueva_cantidad,
            }
        )
        cantidad_restante -= cantidad_a_usar

    if cantidad_restante > 0:
        return {
            "success": False,
            "error": f"Stock insuficiente. Faltan {cantidad_restante} unidades",
            "detalle": detalle_lotes,
        }

    return {"success": True, "detalle": detalle_lotes}


def _recalcular_stock_y_vencimiento_producto(cursor, producto_id):
    cursor.execute(
        """
        SELECT COALESCE(SUM(cantidad), 0) AS stock_total,
               MIN(fecha_vencimiento) AS proximo_vencimiento
        FROM producto_lotes
        WHERE producto_id = ? AND cantidad > 0
        """,
        (producto_id,),
    )
    resumen = cursor.fetchone()
    stock_total = float(resumen["stock_total"] or 0)
    proximo_vencimiento = resumen["proximo_vencimiento"]

    cursor.execute(
        "UPDATE productos SET stock = ?, fecha_vencimiento = ? WHERE id = ?",
        (stock_total, proximo_vencimiento, producto_id),
    )
    return stock_total


def actualizar_stock_producto(
    id_producto,
    cantidad,
    referencia_tipo='ajuste_manual',
    referencia_id=None,
    detalle=None,
    fecha_vencimiento=None,
    codigo_operacion=None,
    origen_modulo=None,
    usuario=None,
):
    """Ajusta stock de producto manteniendo coherencia FIFO."""
    delta = float(cantidad or 0)
    if delta == 0:
        return {"success": True, "nuevo_stock": None, "detalle_fifo": []}

    conn = get_db()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT id, stock, fecha_vencimiento, unidad FROM productos WHERE id = ?", (id_producto,))
        producto = cursor.fetchone()
        if not producto:
            raise ValueError("Producto no encontrado")

        stock_anterior = float(producto["stock"] or 0)
        detalle_fifo = []

        if delta > 0:
            cursor.execute(
                """
                INSERT INTO producto_lotes (producto_id, cantidad, fecha_vencimiento)
                VALUES (?, ?, ?)
                """,
                (id_producto, delta, fecha_vencimiento if fecha_vencimiento is not None else producto["fecha_vencimiento"]),
            )
        else:
            consumo = _descontar_lotes_fifo_cursor(cursor, id_producto, abs(delta))
            if not consumo["success"]:
                raise ValueError(consumo["error"])
            detalle_fifo = consumo.get("detalle", [])

        nuevo_stock = _recalcular_stock_y_vencimiento_producto(cursor, id_producto)
        registrar_movimiento_stock(
            "producto",
            id_producto,
            "entrada_manual" if delta > 0 else "salida_manual",
            abs(delta),
            stock_anterior=stock_anterior,
            stock_nuevo=nuevo_stock,
            referencia_tipo=referencia_tipo,
            referencia_id=referencia_id,
            detalle=detalle,
            unidad=producto["unidad"] if producto and "unidad" in producto.keys() else None,
            origen_modulo=origen_modulo or "productos",
            codigo_operacion=codigo_operacion,
            usuario=usuario,
            conn=conn,
        )
        conn.commit()
        return {"success": True, "nuevo_stock": nuevo_stock, "detalle_fifo": detalle_fifo}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def actualizar_stock_insumo(
    codigo_barra,
    cantidad=1,
    lote_codigo=None,
    fecha_elaboracion=None,
    fecha_vencimiento=None,
    codigo_operacion=None,
):
    conn = get_db()
    cursor = conn.cursor()
    codigo_operacion = _normalizar_codigo_operacion(codigo_operacion, prefijo="OPI")
    codigo = str(codigo_barra or "").strip()

    try:
        insumo = _buscar_insumo_por_codigo_cursor(cursor, codigo)

        if insumo:
            insumo_id = int(insumo["id"])
            stock_anterior = float(insumo["stock"] or 0)
            cursor.execute(
                """
                UPDATE insumos
                SET stock = stock + ?,
                    codigo_barra = CASE
                        WHEN codigo_barra IS NULL OR TRIM(codigo_barra) = '' THEN ?
                        ELSE codigo_barra
                    END
                WHERE id = ?
                """,
                (cantidad, codigo or None, insumo_id),
            )
            if codigo:
                _asociar_codigo_insumo_cursor(cursor, insumo_id, codigo)

            cursor.execute("SELECT id, stock, nombre, unidad FROM insumos WHERE id = ?", (insumo_id,))
            actualizado = cursor.fetchone()
            nombre = actualizado["nombre"]

            if cantidad > 0:
                _crear_o_sumar_lote_insumo_cursor(
                    cursor,
                    insumo_id,
                    cantidad,
                    lote_codigo=lote_codigo,
                    fecha_elaboracion=fecha_elaboracion,
                    fecha_vencimiento=fecha_vencimiento,
                    merge=False,
                )
            elif cantidad < 0:
                _descontar_lotes_insumo_cursor(cursor, insumo_id, abs(cantidad))

            registrar_movimiento_stock(
                "insumo",
                insumo_id,
                "entrada_scanner" if float(cantidad or 0) >= 0 else "salida_scanner",
                cantidad,
                stock_anterior=stock_anterior,
                stock_nuevo=actualizado["stock"],
                referencia_tipo="scanner",
                detalle=f"Codigo {codigo or '-'}",
                origen_modulo="scanner",
                codigo_operacion=codigo_operacion,
                unidad=actualizado["unidad"],
                metadata={"codigo_barra": codigo, "lote_codigo": lote_codigo},
                conn=conn,
            )
        else:
            nombre = f"Insumo {codigo[-6:]}" if codigo else "Insumo nuevo"
            cursor.execute(
                "INSERT INTO insumos (codigo_barra, nombre, stock) VALUES (?, ?, ?)",
                (codigo or None, nombre, cantidad),
            )
            nuevo_id = cursor.lastrowid
            if codigo:
                _asociar_codigo_insumo_cursor(cursor, nuevo_id, codigo)
            if cantidad > 0:
                _crear_o_sumar_lote_insumo_cursor(
                    cursor,
                    nuevo_id,
                    cantidad,
                    lote_codigo=lote_codigo,
                    fecha_elaboracion=fecha_elaboracion,
                    fecha_vencimiento=fecha_vencimiento,
                    merge=False,
                )
            registrar_movimiento_stock(
                "insumo",
                nuevo_id,
                "alta_scanner",
                cantidad,
                stock_anterior=0,
                stock_nuevo=cantidad,
                referencia_tipo="scanner",
                detalle=f"Codigo {codigo or '-'}",
                origen_modulo="scanner",
                codigo_operacion=codigo_operacion,
                unidad="unidad",
                metadata={"codigo_barra": codigo, "lote_codigo": lote_codigo},
                conn=conn,
            )

        conn.commit()
        return nombre
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
def registrar_venta(
    productos_vendidos,
    detalle_fifo=None,
    codigo_pedido=None,
    codigo_operacion=None,
    canal_venta=None,
):
    """
    Registra venta con hora de Chile
    Ya NO descuenta stock - eso lo hace descontar_stock_fifo() antes
    """
    conn = get_db()
    cursor = conn.cursor()
    alertas = []
    
    try:
        # Registrar fecha/hora real de Chile, incluyendo cambios de horario.
        fecha_hora_str = obtener_hora_chile().strftime('%Y-%m-%d %H:%M:%S')
        
        total_items = sum(item['cantidad'] for item in productos_vendidos)
        codigo_operacion = _normalizar_codigo_operacion(codigo_operacion, prefijo="OPV")
        canal = _normalizar_canal_venta(canal_venta)
        
        codigo = str(codigo_pedido or "").strip()[:80] or None
        cursor.execute(
            """
            INSERT INTO ventas (fecha_hora, codigo_pedido, canal_venta, codigo_operacion, total_items, estado)
            VALUES (?, ?, ?, ?, ?, 'completada')
            """,
            (fecha_hora_str, codigo, canal, codigo_operacion, total_items)
        )
        venta_id = cursor.lastrowid
        
        # Insertar detalle de venta con info de lotes (FIFO)
        for item in productos_vendidos:
            producto_id = item['id']
            cantidad = item['cantidad']
            precio_unitario = item.get('precio', 0)
            subtotal = cantidad * precio_unitario
            
            # Obtener nombre del producto de la base de datos
            cursor.execute("SELECT nombre FROM productos WHERE id = ?", (producto_id,))
            prod = cursor.fetchone()
            nombre_producto = prod['nombre'] if prod else 'Producto desconocido'
            
            # Insertar en venta_detalles (nueva tabla para FIFO)
            cursor.execute("""
                INSERT INTO venta_detalles 
                (venta_id, producto_id, cantidad, precio_unitario, subtotal)
                VALUES (?, ?, ?, ?, ?)
            """, (venta_id, producto_id, cantidad, precio_unitario, subtotal))
            
            # Insertar también en venta_items (para compatibilidad con historial)
            cursor.execute("""
                INSERT INTO venta_items 
                (venta_id, producto_id, producto_nombre, cantidad)
                VALUES (?, ?, ?, ?)
            """, (venta_id, producto_id, nombre_producto, cantidad))
            
            # Si tenemos detalle FIFO, guardar qué lotes se usaron
            if detalle_fifo:
                lotes_usados = next((d['lotes_usados'] for d in detalle_fifo if d['producto_id'] == producto_id), [])
                for lote in lotes_usados:
                    cursor.execute("""
                        INSERT INTO venta_lotes (venta_id, producto_id, lote_id, cantidad_usada)
                        VALUES (?, ?, ?, ?)
                    """, (venta_id, producto_id, lote['lote_id'], lote['cantidad_usada']))
        
        conn.commit()
        return venta_id, alertas, codigo_operacion
        
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()


def _mapa_productos_asociados_venta(cursor):
    cursor.execute(
        """
        SELECT producto_id, producto_asociado_id, cantidad
        FROM producto_productos_venta
        WHERE cantidad > 0
        ORDER BY id ASC
        """
    )
    mapa = {}
    for row in cursor.fetchall():
        origen = int(row["producto_id"] or 0)
        destino = int(row["producto_asociado_id"] or 0)
        cantidad = float(row["cantidad"] or 0)
        if origen <= 0 or destino <= 0 or cantidad <= 0:
            continue
        mapa.setdefault(origen, []).append((destino, cantidad))
    return mapa


def _validar_ciclos_asociaciones_productos(mapa, origenes):
    visitado = set()
    en_pila = set()
    ruta = []

    def dfs(nodo):
        if nodo in en_pila:
            if nodo in ruta:
                idx = ruta.index(nodo)
                ciclo = ruta[idx:] + [nodo]
            else:
                ciclo = [nodo, nodo]
            ciclo_txt = " -> ".join(str(x) for x in ciclo)
            raise ValueError(f"Asociaciones de productos con ciclo detectado: {ciclo_txt}")
        if nodo in visitado:
            return
        en_pila.add(nodo)
        ruta.append(nodo)
        for destino, _cantidad in mapa.get(nodo, []):
            dfs(destino)
        ruta.pop()
        en_pila.remove(nodo)
        visitado.add(nodo)

    for origen in origenes:
        dfs(origen)


def _expandir_items_con_productos_asociados(cursor, items_directos):
    """
    Expande cantidades por asociaciones producto->producto.
    Usa propagación por deltas para contemplar cadenas de asociaciones.
    """
    mapa = _mapa_productos_asociados_venta(cursor)
    if not mapa:
        return dict(items_directos)

    _validar_ciclos_asociaciones_productos(mapa, list(items_directos.keys()))

    acumulado = {}
    cola = deque()
    for producto_id, cantidad in items_directos.items():
        cantidad_num = float(cantidad or 0)
        if cantidad_num <= 1e-9:
            continue
        cola.append((int(producto_id), cantidad_num))

    pasos = 0
    while cola:
        pasos += 1
        if pasos > 200000:
            raise ValueError("Demasiadas asociaciones de productos para procesar la venta")

        producto_id, delta_cantidad = cola.popleft()
        if delta_cantidad <= 1e-9:
            continue
        acumulado[producto_id] = acumulado.get(producto_id, 0) + delta_cantidad

        for producto_asociado_id, factor in mapa.get(producto_id, []):
            delta_asociado = float(delta_cantidad) * float(factor)
            if delta_asociado <= 1e-9:
                continue
            cola.append((producto_asociado_id, delta_asociado))

    return {
        int(pid): float(round(cantidad, 6))
        for pid, cantidad in acumulado.items()
        if float(cantidad) > 1e-9
    }


def procesar_venta_con_insumos(
    items,
    codigo_pedido=None,
    codigo_operacion=None,
    agenda_evento_id=None,
    fecha_venta=None,
    canal_venta=None,
):
    """
    Procesa venta en una única transacción:
    - descuenta stock de productos por porción (FIFO)
    - descuenta insumos asociados por producto vendido
    - descuenta dependencias de stock (producto/insumo) cuando aplique
    - registra venta + detalle FIFO + detalle de insumos consumidos
    """
    conn = get_db()
    cursor = conn.cursor()
    alertas = []

    try:
        if not isinstance(items, list) or not items:
            raise ValueError("Carrito vacío")

        items_directos = {}
        precio_unitario_por_producto = {}
        for raw in items:
            if not isinstance(raw, dict):
                raise ValueError("Formato de item inválido")

            try:
                producto_id = int(raw.get("id") or 0)
            except (TypeError, ValueError):
                raise ValueError("Producto inválido en carrito")

            try:
                cantidad_porciones = int(raw.get("cantidad") or 0)
            except (TypeError, ValueError):
                raise ValueError(f"Cantidad inválida para producto ID {producto_id}")

            if producto_id <= 0:
                raise ValueError("Producto inválido en carrito")
            if cantidad_porciones <= 0:
                continue

            precio_raw = raw.get("precio_unitario", raw.get("precio", None))
            if precio_raw not in (None, ""):
                try:
                    precio_unitario = float(precio_raw)
                except (TypeError, ValueError):
                    raise ValueError(f"Precio inválido para producto ID {producto_id}")
                if precio_unitario < 0:
                    raise ValueError(f"Precio inválido para producto ID {producto_id}")
                precio_unitario_por_producto[producto_id] = precio_unitario

            items_directos[producto_id] = items_directos.get(producto_id, 0) + cantidad_porciones

        if not items_directos:
            raise ValueError("Carrito vacío")

        items_agrupados = _expandir_items_con_productos_asociados(cursor, items_directos)
        if not items_agrupados:
            raise ValueError("Carrito vacío")

        ahora_chile = obtener_hora_chile()
        fecha_hora_str = ahora_chile.strftime('%Y-%m-%d %H:%M:%S')
        fecha_venta_iso = _normalizar_fecha_iso(fecha_venta, "fecha de venta")
        if fecha_venta_iso:
            fecha_hoy_iso = ahora_chile.strftime('%Y-%m-%d')
            if fecha_venta_iso > fecha_hoy_iso:
                raise ValueError("La fecha de venta no puede ser futura")
            hora_actual = ahora_chile.strftime('%H:%M:%S')
            fecha_hora_str = f"{fecha_venta_iso} {hora_actual}"
        agenda_evento_id_val = None
        if agenda_evento_id not in (None, "", 0, "0"):
            try:
                agenda_evento_id_val = int(agenda_evento_id)
            except (TypeError, ValueError):
                agenda_evento_id_val = None
        if agenda_evento_id_val:
            cursor.execute("SELECT codigo_operacion FROM agenda_eventos WHERE id = ?", (agenda_evento_id_val,))
            evento_row = cursor.fetchone()
            codigo_op_agenda = str(evento_row["codigo_operacion"] or "").strip() if evento_row else ""
            codigo_operacion = _normalizar_codigo_operacion(codigo_operacion or codigo_op_agenda, prefijo="OPV")
            if evento_row and not codigo_op_agenda:
                cursor.execute(
                    "UPDATE agenda_eventos SET codigo_operacion = COALESCE(NULLIF(codigo_operacion, ''), ?) WHERE id = ?",
                    (codigo_operacion, agenda_evento_id_val),
                )
        else:
            codigo_operacion = _normalizar_codigo_operacion(codigo_operacion, prefijo="OPV")
        codigo = str(codigo_pedido or "").strip()[:80] or None
        canal = _normalizar_canal_venta(canal_venta)
        total_items = int(round(sum(float(v or 0) for v in items_directos.values())))
        cursor.execute(
            """
            INSERT INTO ventas (fecha_hora, codigo_pedido, canal_venta, codigo_operacion, total_items, total_monto, estado)
            VALUES (?, ?, ?, ?, ?, 0, 'completada')
            """,
            (fecha_hora_str, codigo, canal, codigo_operacion, total_items),
        )
        venta_id = cursor.lastrowid

        detalle_fifo = []
        detalle_productos = []
        total_monto = 0.0
        detalle_insumos_venta = []
        detalle_insumo_lotes_venta = []
        productos_stock_modificados = set()
        insumos_stock_modificados = set()
        productos_directos_ids = {int(pid) for pid in items_directos.keys() if int(pid) > 0}

        cursor.execute(
            """
            SELECT producto_id, insumo_id
            FROM producto_insumos_venta
            WHERE cantidad > 0
            """
        )
        asociaciones_insumos_ids = set()
        for row in cursor.fetchall():
            producto_assoc_id = int(row["producto_id"] or 0)
            insumo_assoc_id = int(row["insumo_id"] or 0)
            if producto_assoc_id > 0 and insumo_assoc_id > 0:
                asociaciones_insumos_ids.add((producto_assoc_id, insumo_assoc_id))

        cursor.execute(
            """
            SELECT producto_id, producto_asociado_id
            FROM producto_productos_venta
            WHERE cantidad > 0
            """
        )
        asociaciones_productos_ids = set()
        for row in cursor.fetchall():
            producto_assoc_id = int(row["producto_id"] or 0)
            producto_dep_id = int(row["producto_asociado_id"] or 0)
            if producto_assoc_id > 0 and producto_dep_id > 0:
                asociaciones_productos_ids.add((producto_assoc_id, producto_dep_id))

        for producto_id, cantidad_porciones in items_agrupados.items():
            cursor.execute(
                """
                SELECT id, nombre, stock, stock_minimo, unidad, porcion_cantidad, porcion_unidad,
                       stock_dependencia_tipo, stock_dependencia_id, stock_dependencia_cantidad
                FROM productos
                WHERE id = ?
                """,
                (producto_id,),
            )
            producto = cursor.fetchone()
            if not producto:
                raise ValueError(f"Producto ID {producto_id}: producto no encontrado")

            producto_dict = dict(producto)
            info = _calcular_info_porciones_producto_db(producto_dict)
            if not info["success"]:
                raise ValueError(f"{producto['nombre']}: {info['error']}")

            if cantidad_porciones > info["porciones_disponibles"]:
                raise ValueError(
                    f"{producto['nombre']}: solo hay {info['porciones_disponibles']} porciones disponibles"
                )

            cantidad_stock = cantidad_porciones * float(info["porcion_stock_equivalente"] or 0)
            if cantidad_stock <= 0:
                raise ValueError(f"{producto['nombre']}: no se pudo calcular la porción a descontar")

            stock_anterior_producto = float(producto["stock"] or 0)
            consumo_fifo = _descontar_lotes_fifo_cursor(cursor, producto_id, cantidad_stock)
            if not consumo_fifo["success"]:
                raise ValueError(f"{producto['nombre']}: {consumo_fifo['error']}")

            nuevo_stock_producto = _recalcular_stock_y_vencimiento_producto(cursor, producto_id)
            registrar_movimiento_stock(
                "producto",
                producto_id,
                "salida_fifo",
                cantidad_stock,
                stock_anterior=stock_anterior_producto,
                stock_nuevo=nuevo_stock_producto,
                referencia_tipo="venta",
                referencia_id=venta_id,
                detalle=f"Venta de {cantidad_porciones} porciones de {producto['nombre']}",
                origen_modulo="ventas",
                codigo_operacion=codigo_operacion,
                unidad=info.get("unidad_stock"),
                metadata={
                    "venta_id": venta_id,
                    "producto_id": producto_id,
                    "cantidad_porciones": cantidad_porciones,
                    "agenda_evento_id": agenda_evento_id_val,
                },
                conn=conn,
            )
            productos_stock_modificados.add(int(producto_id))

            if int(producto_id) in productos_directos_ids:
                precio_unitario = precio_unitario_por_producto.get(int(producto_id))
                if precio_unitario is None:
                    try:
                        precio_unitario = float(producto.get("precio") or 0)
                    except (TypeError, ValueError):
                        precio_unitario = 0.0
                if precio_unitario < 0:
                    precio_unitario = 0.0
                subtotal = float(cantidad_porciones) * float(precio_unitario)
                total_monto += subtotal
                detalle_productos.append(
                    {
                        "producto_id": producto_id,
                        "nombre": producto["nombre"],
                        "cantidad": cantidad_porciones,
                        "precio_unitario": float(precio_unitario or 0),
                        "cantidad_stock": cantidad_stock,
                        "unidad_stock": info["unidad_stock"],
                    }
                )
            detalle_fifo.append(
                {
                    "producto_id": producto_id,
                    "lotes_usados": consumo_fifo.get("detalle", []),
                }
            )

            cursor.execute(
                """
                SELECT piv.insumo_id,
                       piv.cantidad AS cantidad_asociada,
                       piv.unidad AS unidad_asociada,
                       i.nombre AS insumo_nombre,
                       i.stock AS stock_actual,
                       COALESCE(NULLIF(TRIM(i.unidad), ''), 'unidad') AS unidad_stock
                FROM producto_insumos_venta piv
                JOIN insumos i ON i.id = piv.insumo_id
                WHERE piv.producto_id = ?
                ORDER BY piv.id ASC
                """,
                (producto_id,),
            )
            asociaciones_insumos = [dict(row) for row in cursor.fetchall()]

            tipo_dependencia = str(producto_dict.get("stock_dependencia_tipo") or "").strip().lower()
            dependencia_id = int(producto_dict.get("stock_dependencia_id") or 0)
            dependencia_cantidad = float(producto_dict.get("stock_dependencia_cantidad") or 1)
            if dependencia_cantidad <= 0:
                dependencia_cantidad = 1.0

            if (
                tipo_dependencia == "insumo"
                and dependencia_id > 0
                and (int(producto_id), dependencia_id) not in asociaciones_insumos_ids
            ):
                cursor.execute(
                    """
                    SELECT id AS insumo_id,
                           nombre AS insumo_nombre,
                           stock AS stock_actual,
                           COALESCE(NULLIF(TRIM(unidad), ''), 'unidad') AS unidad_stock
                    FROM insumos
                    WHERE id = ?
                    """,
                    (dependencia_id,),
                )
                insumo_dependiente = cursor.fetchone()
                if not insumo_dependiente:
                    raise ValueError(
                        f"{producto['nombre']}: insumo de dependencia no encontrado"
                    )
                asociaciones_insumos.append(
                    {
                        "insumo_id": int(insumo_dependiente["insumo_id"]),
                        "cantidad_asociada": float(dependencia_cantidad),
                        "unidad_asociada": _normalizar_unidad_producto(
                            insumo_dependiente["unidad_stock"] or "unidad"
                        ),
                        "insumo_nombre": insumo_dependiente["insumo_nombre"],
                        "stock_actual": float(insumo_dependiente["stock_actual"] or 0),
                        "unidad_stock": _normalizar_unidad_producto(
                            insumo_dependiente["unidad_stock"] or "unidad"
                        ),
                    }
                )

            insumos_requeridos = {}
            for assoc in asociaciones_insumos:
                insumo_id = int(assoc["insumo_id"])
                nombre_insumo = assoc["insumo_nombre"] or f"Insumo {insumo_id}"
                unidad_stock_insumo = _normalizar_unidad_producto(assoc["unidad_stock"] or "unidad")
                unidad_asociada = _normalizar_unidad_producto(assoc["unidad_asociada"] or unidad_stock_insumo)
                cantidad_asociada = float(assoc["cantidad_asociada"] or 0)
                if cantidad_asociada <= 0:
                    continue

                if not _unidades_compatibles_porcion(unidad_stock_insumo, unidad_asociada):
                    raise ValueError(
                        f"{nombre_insumo}: unidad incompatible ({unidad_asociada}) con el stock del insumo ({unidad_stock_insumo})"
                    )

                cantidad_total_asociada = cantidad_asociada * cantidad_porciones
                factor_stock = convertir_a_base(1, unidad_stock_insumo)
                if not factor_stock:
                    raise ValueError(f"{nombre_insumo}: unidad de stock inválida ({unidad_stock_insumo})")

                if unidad_stock_insumo == unidad_asociada:
                    cantidad_descontada_stock = cantidad_total_asociada
                else:
                    cantidad_total_base = convertir_a_base(cantidad_total_asociada, unidad_asociada)
                    cantidad_descontada_stock = cantidad_total_base / factor_stock

                if cantidad_descontada_stock <= 0:
                    continue

                acumulado = insumos_requeridos.setdefault(
                    insumo_id,
                    {
                        "insumo_id": insumo_id,
                        "insumo_nombre": nombre_insumo,
                        "unidad_stock": unidad_stock_insumo,
                        "cantidad_total_stock": 0.0,
                    },
                )
                acumulado["cantidad_total_stock"] += float(cantidad_descontada_stock)

            for insumo_id, req in insumos_requeridos.items():
                cursor.execute(
                    """
                    SELECT id, nombre, stock, COALESCE(NULLIF(TRIM(unidad), ''), 'unidad') AS unidad_stock
                    FROM insumos
                    WHERE id = ?
                    """,
                    (insumo_id,),
                )
                insumo_actual = cursor.fetchone()
                if not insumo_actual:
                    raise ValueError(f"Insumo {insumo_id}: no encontrado al procesar venta")

                nombre_insumo = str(insumo_actual["nombre"] or req["insumo_nombre"] or f"Insumo {insumo_id}")
                unidad_stock_insumo = _normalizar_unidad_producto(insumo_actual["unidad_stock"] or req["unidad_stock"] or "unidad")
                stock_actual_insumo = float(insumo_actual["stock"] or 0)
                cantidad_descontada_stock = float(req.get("cantidad_total_stock") or 0)
                if cantidad_descontada_stock <= 0:
                    continue

                nuevo_stock_insumo = stock_actual_insumo - cantidad_descontada_stock
                if nuevo_stock_insumo < -1e-9:
                    faltante = abs(nuevo_stock_insumo)
                    raise ValueError(
                        f"{nombre_insumo}: stock insuficiente. Faltan {round(faltante, 4)} {unidad_stock_insumo}"
                    )

                if abs(nuevo_stock_insumo) < 1e-9:
                    nuevo_stock_insumo = 0.0

                consumo_insumo_lotes = _descontar_lotes_insumo_cursor(
                    cursor,
                    insumo_id,
                    cantidad_descontada_stock,
                )
                lotes_consumidos = list(consumo_insumo_lotes.get("detalle") or [])
                restante_sin_lote = float(consumo_insumo_lotes.get("restante_sin_lote") or 0)
                if restante_sin_lote > 1e-9:
                    lotes_consumidos.append(
                        {
                            "insumo_lote_id": None,
                            "cantidad_usada": restante_sin_lote,
                            "lote_codigo": None,
                            "fecha_elaboracion": None,
                            "fecha_vencimiento": None,
                            "fecha_ingreso": None,
                        }
                    )

                cursor.execute(
                    "UPDATE insumos SET stock = ? WHERE id = ?",
                    (nuevo_stock_insumo, insumo_id),
                )

                registrar_movimiento_stock(
                    "insumo",
                    insumo_id,
                    "salida_venta",
                    cantidad_descontada_stock,
                    stock_anterior=stock_actual_insumo,
                    stock_nuevo=nuevo_stock_insumo,
                    referencia_tipo="venta",
                    referencia_id=venta_id,
                    detalle=f"Venta de {producto['nombre']} x{cantidad_porciones}",
                    origen_modulo="ventas",
                    codigo_operacion=codigo_operacion,
                    unidad=unidad_stock_insumo,
                    metadata={
                        "venta_id": venta_id,
                        "producto_id": producto_id,
                        "insumo_id": insumo_id,
                        "cantidad_porciones": cantidad_porciones,
                        "agenda_evento_id": agenda_evento_id_val,
                    },
                    conn=conn,
                )
                insumos_stock_modificados.add(insumo_id)

                cantidad_por_producto_stock = (
                    float(cantidad_descontada_stock) / float(cantidad_porciones)
                    if float(cantidad_porciones) > 0
                    else float(cantidad_descontada_stock)
                )
                detalle_insumos_venta.append(
                    {
                        "producto_id": producto_id,
                        "insumo_id": insumo_id,
                        "insumo_nombre": nombre_insumo,
                        "cantidad_por_producto": cantidad_por_producto_stock,
                        "unidad_asociada": unidad_stock_insumo,
                        "cantidad_total_asociada": cantidad_descontada_stock,
                        "cantidad_descontada_stock": cantidad_descontada_stock,
                        "unidad_stock": unidad_stock_insumo,
                        "lotes_usados": lotes_consumidos,
                    }
                )
                for lote in lotes_consumidos:
                    detalle_insumo_lotes_venta.append(
                        {
                            "producto_id": producto_id,
                            "insumo_id": insumo_id,
                            "insumo_lote_id": lote.get("insumo_lote_id"),
                            "cantidad_usada_stock": float(lote.get("cantidad_usada") or 0),
                            "unidad_stock": unidad_stock_insumo,
                            "lote_codigo": lote.get("lote_codigo"),
                            "fecha_elaboracion": lote.get("fecha_elaboracion"),
                            "fecha_vencimiento": lote.get("fecha_vencimiento"),
                            "fecha_ingreso": lote.get("fecha_ingreso"),
                        }
                    )

            if (
                tipo_dependencia == "producto"
                and dependencia_id > 0
                and (int(producto_id), dependencia_id) not in asociaciones_productos_ids
            ):
                if dependencia_id == int(producto_id):
                    raise ValueError(
                        f"{producto['nombre']}: dependencia de stock ciclica no permitida"
                    )
                cantidad_dependencia_porciones = float(cantidad_porciones) * float(dependencia_cantidad)
                if cantidad_dependencia_porciones > 1e-9:
                    cursor.execute(
                        """
                        SELECT id, nombre, stock, stock_minimo, unidad, porcion_cantidad, porcion_unidad
                        FROM productos
                        WHERE id = ?
                        """,
                        (dependencia_id,),
                    )
                    producto_dependiente = cursor.fetchone()
                    if not producto_dependiente:
                        raise ValueError(
                            f"{producto['nombre']}: producto de dependencia no encontrado"
                        )

                    producto_dependiente_dict = dict(producto_dependiente)
                    info_dependiente = _calcular_info_porciones_producto_db(producto_dependiente_dict)
                    if not info_dependiente["success"]:
                        raise ValueError(
                            f"{producto_dependiente['nombre']}: {info_dependiente['error']}"
                        )

                    cantidad_dependencia_stock = (
                        cantidad_dependencia_porciones
                        * float(info_dependiente["porcion_stock_equivalente"] or 0)
                    )
                    if cantidad_dependencia_stock <= 0:
                        raise ValueError(
                            f"{producto_dependiente['nombre']}: no se pudo calcular la porción a descontar por dependencia"
                        )

                    stock_anterior_dependencia = float(producto_dependiente["stock"] or 0)
                    if cantidad_dependencia_stock - stock_anterior_dependencia > 1e-9:
                        faltante_stock = max(0.0, cantidad_dependencia_stock - stock_anterior_dependencia)
                        factor_porcion = float(info_dependiente.get("porcion_stock_equivalente") or 1)
                        faltante_porciones = faltante_stock / factor_porcion if factor_porcion > 0 else faltante_stock
                        raise ValueError(
                            f"{producto['nombre']}: dependencia {producto_dependiente['nombre']} insuficiente. "
                            f"Faltan {round(faltante_porciones, 3)} porciones"
                        )

                    consumo_fifo_dependencia = _descontar_lotes_fifo_cursor(
                        cursor,
                        dependencia_id,
                        cantidad_dependencia_stock,
                    )
                    if not consumo_fifo_dependencia["success"]:
                        raise ValueError(
                            f"{producto['nombre']}: dependencia {producto_dependiente['nombre']}: "
                            f"{consumo_fifo_dependencia['error']}"
                        )

                    nuevo_stock_dependencia = _recalcular_stock_y_vencimiento_producto(cursor, dependencia_id)
                    registrar_movimiento_stock(
                        "producto",
                        dependencia_id,
                        "salida_dependencia_venta",
                        cantidad_dependencia_stock,
                        stock_anterior=stock_anterior_dependencia,
                        stock_nuevo=nuevo_stock_dependencia,
                        referencia_tipo="venta",
                        referencia_id=venta_id,
                        detalle=(
                            f"Venta de {producto['nombre']} x{cantidad_porciones}: "
                            f"consumo por dependencia de {producto_dependiente['nombre']}"
                        ),
                        origen_modulo="ventas",
                        codigo_operacion=codigo_operacion,
                        unidad=info_dependiente.get("unidad_stock"),
                        metadata={
                            "venta_id": venta_id,
                            "producto_origen_id": int(producto_id),
                            "producto_dependencia_id": dependencia_id,
                            "cantidad_porciones_origen": float(cantidad_porciones),
                            "cantidad_porciones_dependencia": float(cantidad_dependencia_porciones),
                            "agenda_evento_id": agenda_evento_id_val,
                        },
                        conn=conn,
                    )
                    productos_stock_modificados.add(int(dependencia_id))

        for prod in detalle_productos:
            precio_unitario = float(prod.get("precio_unitario") or 0)
            subtotal = prod["cantidad"] * precio_unitario

            cursor.execute(
                """
                INSERT INTO venta_detalles
                (venta_id, producto_id, cantidad, precio_unitario, subtotal)
                VALUES (?, ?, ?, ?, ?)
                """,
                (venta_id, prod["producto_id"], prod["cantidad"], precio_unitario, subtotal),
            )

            cursor.execute(
                """
                INSERT INTO venta_items
                (venta_id, producto_id, producto_nombre, cantidad)
                VALUES (?, ?, ?, ?)
                """,
                (venta_id, prod["producto_id"], prod["nombre"], prod["cantidad"]),
            )

        if total_monto < 0:
            total_monto = 0.0
        cursor.execute(
            "UPDATE ventas SET total_monto = ? WHERE id = ?",
            (float(total_monto), venta_id),
        )

        for det in detalle_fifo:
            for lote in det.get("lotes_usados", []):
                cursor.execute(
                    """
                    INSERT INTO venta_lotes (venta_id, producto_id, lote_id, cantidad_usada)
                    VALUES (?, ?, ?, ?)
                    """,
                    (venta_id, det["producto_id"], lote["lote_id"], lote["cantidad_usada"]),
                )

        for cons in detalle_insumos_venta:
            cursor.execute(
                """
                INSERT INTO venta_insumos (
                    venta_id, producto_id, insumo_id, cantidad_por_producto,
                    unidad_asociada, cantidad_total_asociada, cantidad_descontada_stock,
                    unidad_stock
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    venta_id,
                    cons["producto_id"],
                    cons["insumo_id"],
                    cons["cantidad_por_producto"],
                    cons["unidad_asociada"],
                    cons["cantidad_total_asociada"],
                    cons["cantidad_descontada_stock"],
                    cons["unidad_stock"],
                ),
            )

        for lote in detalle_insumo_lotes_venta:
            if float(lote.get("cantidad_usada_stock") or 0) <= 0:
                continue
            cursor.execute(
                """
                INSERT INTO venta_insumo_lotes (
                    venta_id, producto_id, insumo_id, insumo_lote_id,
                    cantidad_usada_stock, unidad_stock, lote_codigo,
                    fecha_elaboracion, fecha_vencimiento, fecha_ingreso
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    venta_id,
                    lote["producto_id"],
                    lote["insumo_id"],
                    lote.get("insumo_lote_id"),
                    lote["cantidad_usada_stock"],
                    lote["unidad_stock"],
                    lote.get("lote_codigo"),
                    lote.get("fecha_elaboracion"),
                    lote.get("fecha_vencimiento"),
                    lote.get("fecha_ingreso"),
                ),
            )

        productos_refresco_ids = {int(pid) for pid in items_agrupados.keys() if int(pid) > 0}
        productos_refresco_ids.update(int(pid) for pid in productos_stock_modificados if int(pid) > 0)

        if productos_stock_modificados:
            placeholders = ",".join(["?"] * len(productos_stock_modificados))
            cursor.execute(
                f"""
                SELECT id
                FROM productos
                WHERE stock_dependencia_tipo = 'producto'
                  AND stock_dependencia_id IN ({placeholders})
                """,
                tuple(int(pid) for pid in productos_stock_modificados),
            )
            for row in cursor.fetchall():
                productos_refresco_ids.add(int(row["id"]))

        if insumos_stock_modificados:
            placeholders = ",".join(["?"] * len(insumos_stock_modificados))
            cursor.execute(
                f"""
                SELECT id
                FROM productos
                WHERE stock_dependencia_tipo = 'insumo'
                  AND stock_dependencia_id IN ({placeholders})
                """,
                tuple(int(iid) for iid in insumos_stock_modificados),
            )
            for row in cursor.fetchall():
                productos_refresco_ids.add(int(row["id"]))

        productos_actualizados = []
        for producto_id in sorted(productos_refresco_ids):
            cursor.execute(
                """
                SELECT id, nombre, stock, stock_minimo, unidad, porcion_cantidad, porcion_unidad
                FROM productos
                WHERE id = ?
                """,
                (producto_id,),
            )
            row = cursor.fetchone()
            if not row:
                continue
            producto = dict(row)
            info = _calcular_info_porciones_producto_db(producto)
            productos_actualizados.append(
                {
                    "id": int(producto["id"]),
                    "nombre": producto["nombre"],
                    "stock": float(producto.get("stock") or 0),
                    "stock_minimo": float(producto.get("stock_minimo") or 0),
                    "unidad": info.get("unidad_stock") or _normalizar_unidad_producto(producto.get("unidad") or "unidad"),
                    "porcion_cantidad": float(info.get("porcion_cantidad") or 1),
                    "porcion_unidad": info.get("porcion_unidad") or _normalizar_unidad_producto(producto.get("porcion_unidad") or "unidad"),
                    "porcion_stock_equivalente": info.get("porcion_stock_equivalente"),
                    "porciones_disponibles": int(info.get("porciones_disponibles") or 0),
                    "sin_porcion_disponible": bool(info.get("sin_porcion_disponible")),
                    "baja_porcion": bool(info.get("baja_porcion")),
                    "bajo_minimo": bool(info.get("bajo_minimo")),
                    "cerca_minimo": bool(info.get("cerca_minimo")),
                    "porcion_error": info.get("error"),
                }
            )

        conn.commit()
        return {
            "success": True,
            "venta_id": venta_id,
            "codigo_operacion": codigo_operacion,
            "fecha_venta": str(fecha_hora_str).split(" ")[0],
            "agenda_evento_id": agenda_evento_id_val,
            "canal_venta": canal,
            "alertas": alertas,
            "productos_actualizados": productos_actualizados,
            "insumos_consumidos": detalle_insumos_venta,
            "total_monto": float(total_monto or 0),
        }
    except Exception as e:
        conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        conn.close()


def actualizar_stock_insumo_con_unidad(
    codigo_barra,
    cantidad,
    unidad='unidad',
    lote_codigo=None,
    fecha_elaboracion=None,
    fecha_vencimiento=None,
    codigo_operacion=None,
):
    conn = get_db()
    cursor = conn.cursor()
    codigo_operacion = _normalizar_codigo_operacion(codigo_operacion, prefijo="OPI")
    codigo = str(codigo_barra or "").strip()

    try:
        insumo = _buscar_insumo_por_codigo_cursor(cursor, codigo)

        if insumo:
            insumo_id = int(insumo["id"])
            stock_anterior = float(insumo["stock"] or 0)
            cursor.execute(
                """
                UPDATE insumos
                SET stock = stock + ?,
                    codigo_barra = CASE
                        WHEN codigo_barra IS NULL OR TRIM(codigo_barra) = '' THEN ?
                        ELSE codigo_barra
                    END
                WHERE id = ?
                """,
                (cantidad, codigo or None, insumo_id),
            )
            if codigo:
                _asociar_codigo_insumo_cursor(cursor, insumo_id, codigo)

            nombre = insumo['nombre']
            es_nuevo = False
            cursor.execute("SELECT id, stock, unidad FROM insumos WHERE id = ?", (insumo_id,))
            actualizado = cursor.fetchone()
            if cantidad > 0:
                _crear_o_sumar_lote_insumo_cursor(
                    cursor,
                    insumo_id,
                    cantidad,
                    lote_codigo=lote_codigo,
                    fecha_elaboracion=fecha_elaboracion,
                    fecha_vencimiento=fecha_vencimiento,
                    merge=False,
                )
            elif cantidad < 0:
                _descontar_lotes_insumo_cursor(cursor, insumo_id, abs(cantidad))
            registrar_movimiento_stock(
                "insumo",
                insumo_id,
                "entrada_scanner" if float(cantidad or 0) >= 0 else "salida_scanner",
                cantidad,
                stock_anterior=stock_anterior,
                stock_nuevo=actualizado["stock"],
                referencia_tipo="scanner_avanzado",
                detalle=f"{codigo or '-'} ({unidad})",
                origen_modulo="scanner_avanzado",
                codigo_operacion=codigo_operacion,
                unidad=actualizado["unidad"] if actualizado else unidad,
                metadata={"codigo_barra": codigo, "lote_codigo": lote_codigo},
                conn=conn,
            )
        else:
            nombre = f"Insumo {codigo[-6:]}" if codigo else "Insumo nuevo"
            cursor.execute(
                "INSERT INTO insumos (codigo_barra, nombre, stock, unidad) VALUES (?, ?, ?, ?)",
                (codigo or None, nombre, cantidad, unidad),
            )
            es_nuevo = True
            insumo_id = cursor.lastrowid
            if codigo:
                _asociar_codigo_insumo_cursor(cursor, insumo_id, codigo)
            if cantidad > 0:
                _crear_o_sumar_lote_insumo_cursor(
                    cursor,
                    insumo_id,
                    cantidad,
                    lote_codigo=lote_codigo,
                    fecha_elaboracion=fecha_elaboracion,
                    fecha_vencimiento=fecha_vencimiento,
                    merge=False,
                )
            registrar_movimiento_stock(
                "insumo",
                insumo_id,
                "alta_scanner",
                cantidad,
                stock_anterior=0,
                stock_nuevo=cantidad,
                referencia_tipo="scanner_avanzado",
                detalle=f"{codigo or '-'} ({unidad})",
                origen_modulo="scanner_avanzado",
                codigo_operacion=codigo_operacion,
                unidad=unidad,
                metadata={"codigo_barra": codigo, "lote_codigo": lote_codigo},
                conn=conn,
            )

        conn.commit()
        return {'nombre': nombre, 'es_nuevo': es_nuevo, 'insumo_id': int(insumo_id)}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def descartar_insumos_masivo(
    titulo,
    motivo,
    comentario,
    items,
    codigo_operacion=None,
):
    conn = get_db()
    cursor = conn.cursor()
    codigo_operacion = _normalizar_codigo_operacion(codigo_operacion, prefijo="OPD")

    try:
        titulo_norm = str(titulo or "").strip()[:120] or "Descarte de insumos"
        motivo_norm = str(motivo or "").strip()[:80] or "Descarte"
        comentario_norm = str(comentario or "").strip()[:500] or None

        if not isinstance(items, list) or not items:
            return {"success": False, "error": "Debes agregar al menos un insumo a la lista de descarte"}

        preparados = []
        for idx, raw in enumerate(items, start=1):
            if not isinstance(raw, dict):
                return {"success": False, "error": f"Item #{idx} inválido"}
            try:
                insumo_id = int(raw.get("insumo_id") or 0)
            except (TypeError, ValueError):
                insumo_id = 0
            if insumo_id <= 0:
                return {"success": False, "error": f"Item #{idx}: insumo inválido"}

            try:
                cantidad = float(raw.get("cantidad") or 0)
            except (TypeError, ValueError):
                cantidad = 0
            if cantidad <= 0:
                return {"success": False, "error": f"Item #{idx}: la cantidad debe ser mayor a 0"}

            cursor.execute(
                """
                SELECT id, nombre, stock, COALESCE(NULLIF(TRIM(unidad), ''), 'unidad') AS unidad
                FROM insumos
                WHERE id = ?
                """,
                (insumo_id,),
            )
            insumo = cursor.fetchone()
            if not insumo:
                return {"success": False, "error": f"Item #{idx}: insumo no encontrado"}

            unidad_stock = _normalizar_unidad_producto(insumo["unidad"] or "unidad")
            unidad_origen = _normalizar_unidad_producto(raw.get("unidad") or unidad_stock)
            conv = _convertir_cantidad_unidad_db(cantidad, unidad_origen, unidad_stock)
            if not conv.get("success"):
                return {
                    "success": False,
                    "error": f"{insumo['nombre']}: no se pudo convertir unidad ({conv.get('error')})",
                }

            cantidad_stock = float(conv.get("cantidad") or 0)
            if cantidad_stock <= 0:
                return {"success": False, "error": f"{insumo['nombre']}: cantidad inválida para descontar"}

            stock_anterior = float(insumo["stock"] or 0)
            if (stock_anterior + 1e-9) < cantidad_stock:
                faltante = round(cantidad_stock - stock_anterior, 4)
                return {
                    "success": False,
                    "error": f"{insumo['nombre']}: stock insuficiente (faltan {faltante} {unidad_stock})",
                }

            detalle_item = str(raw.get("detalle") or "").strip()[:200] or None
            stock_nuevo = stock_anterior - cantidad_stock
            if abs(stock_nuevo) < 1e-9:
                stock_nuevo = 0.0

            preparados.append(
                {
                    "insumo_id": insumo_id,
                    "nombre": str(insumo["nombre"] or "Insumo"),
                    "unidad_stock": unidad_stock,
                    "cantidad_input": float(cantidad),
                    "unidad_input": unidad_origen,
                    "cantidad_stock": cantidad_stock,
                    "stock_anterior": stock_anterior,
                    "stock_nuevo": stock_nuevo,
                    "detalle_item": detalle_item,
                }
            )

        procesados = []
        for item in preparados:
            cursor.execute(
                "UPDATE insumos SET stock = ? WHERE id = ?",
                (item["stock_nuevo"], item["insumo_id"]),
            )
            sincronizar_lotes_insumo_stock(item["insumo_id"], item["stock_nuevo"], conn=conn)

            detalle_bits = [
                f"Titulo: {titulo_norm}",
                f"Motivo: {motivo_norm}",
            ]
            if item["detalle_item"]:
                detalle_bits.append(f"Item: {item['detalle_item']}")
            if comentario_norm:
                detalle_bits.append(f"Comentario: {comentario_norm}")

            registrar_movimiento_stock(
                "insumo",
                item["insumo_id"],
                "salida_descarte",
                item["cantidad_stock"],
                stock_anterior=item["stock_anterior"],
                stock_nuevo=item["stock_nuevo"],
                referencia_tipo="descarte_masivo",
                detalle=" | ".join(detalle_bits),
                origen_modulo="insumos",
                codigo_operacion=codigo_operacion,
                unidad=item["unidad_stock"],
                metadata={
                    "titulo": titulo_norm,
                    "motivo": motivo_norm,
                    "comentario": comentario_norm,
                    "cantidad_input": item["cantidad_input"],
                    "unidad_input": item["unidad_input"],
                    "detalle_item": item["detalle_item"],
                },
                conn=conn,
            )

            procesados.append(
                {
                    "insumo_id": item["insumo_id"],
                    "nombre": item["nombre"],
                    "cantidad_descontada": item["cantidad_stock"],
                    "unidad": item["unidad_stock"],
                    "stock_nuevo": item["stock_nuevo"],
                }
            )

        conn.commit()
        return {
            "success": True,
            "codigo_operacion": codigo_operacion,
            "titulo": titulo_norm,
            "motivo": motivo_norm,
            "comentario": comentario_norm,
            "items_procesados": procesados,
            "total_items": len(procesados),
        }
    except Exception as e:
        conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        conn.close()


def obtener_insumo_detalle(insumo_id):
    """Retorna un insumo completo por ID para edición."""
    conn = get_db()
    cursor = conn.cursor()

    try:
        cursor.execute(
            '''
            SELECT id, codigo_barra, nombre, stock, stock_minimo, unidad,
                   precio_unitario, cantidad_comprada, unidad_compra, precio_incluye_iva,
                   cantidad_por_scan, unidad_por_scan,
                   nutricion_ref_cantidad, nutricion_ref_unidad,
                   nutricion_kcal, nutricion_proteinas_g, nutricion_carbohidratos_g,
                   nutricion_grasas_g, nutricion_azucares_g, nutricion_sodio_mg
            FROM insumos
            WHERE id = ?
            ''',
            (insumo_id,),
        )
        insumo = cursor.fetchone()
        if not insumo:
            return None

        data = dict(insumo)
        lote_referencia = _obtener_lote_referencia_insumo_cursor(cursor, insumo_id, incluir_cero=True)
        data["lote_codigo"] = lote_referencia.get("lote_codigo") if lote_referencia else None
        data["fecha_elaboracion"] = lote_referencia.get("fecha_elaboracion") if lote_referencia else None
        data["fecha_vencimiento_lote"] = lote_referencia.get("fecha_vencimiento") if lote_referencia else None

        cursor.execute(
            """
            SELECT id, lote_codigo, fecha_elaboracion, fecha_vencimiento, cantidad, fecha_ingreso
            FROM insumo_lotes
            WHERE insumo_id = ?
            ORDER BY
                CASE WHEN cantidad > 0 THEN 0 ELSE 1 END ASC,
                COALESCE(fecha_vencimiento, '9999-12-31') ASC,
                id DESC
            LIMIT 25
            """,
            (insumo_id,),
        )
        data["lotes"] = [
            {
                "id": int(row["id"]),
                "lote_codigo": row["lote_codigo"],
                "fecha_elaboracion": row["fecha_elaboracion"],
                "fecha_vencimiento": row["fecha_vencimiento"],
                "cantidad": float(row["cantidad"] or 0),
                "fecha_ingreso": row["fecha_ingreso"],
            }
            for row in cursor.fetchall()
        ]
        return data
    finally:
        conn.close()


def actualizar_insumo(insumo_id, data):
    """Actualiza los datos base de un insumo existente."""
    conn = get_db()
    cursor = conn.cursor()
    codigo_operacion = _normalizar_codigo_operacion(prefijo="OPI")

    try:
        cursor.execute("SELECT id FROM insumos WHERE id = ?", (insumo_id,))
        if not cursor.fetchone():
            raise ValueError("Insumo no encontrado")

        cursor.execute("SELECT stock FROM insumos WHERE id = ?", (insumo_id,))
        stock_anterior = float(cursor.fetchone()["stock"] or 0)

        nombre = str(data.get('nombre', '')).strip()
        if not nombre:
            raise ValueError("El nombre del insumo es obligatorio")

        codigo_barra = str(data.get('codigo_barra', '')).strip() or None
        unidad = str(data.get('unidad', 'unidad')).strip().lower() or 'unidad'
        unidad_compra = str(data.get('unidad_compra') or unidad).strip().lower() or unidad

        stock = float(data.get('stock', 0) or 0)
        stock_minimo = float(data.get('stock_minimo', 0) or 0)
        precio_unitario = float(data.get('precio_unitario', 0) or 0)
        cantidad_comprada = float(data.get('cantidad_comprada', 1) or 1)
        cantidad_por_scan = float(data.get('cantidad_por_scan', 1) or 1)
        unidad_por_scan = str(data.get('unidad_por_scan') or unidad).strip().lower() or unidad

        precio_incluye_iva_raw = data.get('precio_incluye_iva', True)
        precio_incluye_iva = 1 if precio_incluye_iva_raw in [True, 1, '1', 'true', 'True', 'on'] else 0

        def _opt_float(valor):
            if valor is None:
                return None
            if isinstance(valor, str):
                valor = valor.strip()
                if valor == '':
                    return None
            return float(valor)

        nutricion_ref_cantidad = _opt_float(data.get('nutricion_ref_cantidad'))
        if nutricion_ref_cantidad is None:
            nutricion_ref_cantidad = 100.0
        nutricion_ref_unidad_raw = data.get('nutricion_ref_unidad')
        nutricion_ref_unidad = (
            _normalizar_unidad_producto(nutricion_ref_unidad_raw)
            if str(nutricion_ref_unidad_raw or '').strip()
            else None
        )
        nutricion_kcal = _opt_float(data.get('nutricion_kcal'))
        nutricion_proteinas_g = _opt_float(data.get('nutricion_proteinas_g'))
        nutricion_carbohidratos_g = _opt_float(data.get('nutricion_carbohidratos_g'))
        nutricion_grasas_g = _opt_float(data.get('nutricion_grasas_g'))
        nutricion_azucares_g = _opt_float(data.get('nutricion_azucares_g'))
        nutricion_sodio_mg = _opt_float(data.get('nutricion_sodio_mg'))
        actualizar_metadata_lote = any(
            key in data for key in ("lote_codigo", "fecha_elaboracion", "fecha_vencimiento_lote", "fecha_vencimiento")
        )
        lote_codigo = data.get("lote_codigo") if actualizar_metadata_lote else None
        fecha_elaboracion = data.get("fecha_elaboracion") if actualizar_metadata_lote else None
        fecha_vencimiento_lote = (
            data.get("fecha_vencimiento_lote")
            if "fecha_vencimiento_lote" in data
            else data.get("fecha_vencimiento")
        ) if actualizar_metadata_lote else None

        if stock < 0:
            raise ValueError("El stock no puede ser negativo")
        if stock_minimo < 0:
            raise ValueError("El stock mínimo no puede ser negativo")
        if precio_unitario < 0:
            raise ValueError("El precio no puede ser negativo")
        if cantidad_comprada <= 0:
            raise ValueError("La cantidad comprada debe ser mayor a 0")
        if cantidad_por_scan <= 0:
            raise ValueError("La cantidad por escaneo debe ser mayor a 0")
        if nutricion_ref_cantidad <= 0:
            raise ValueError("La cantidad de referencia nutricional debe ser mayor a 0")
        if nutricion_ref_unidad and not _unidades_compatibles_porcion(unidad, nutricion_ref_unidad):
            raise ValueError(
                f"La unidad nutricional ({nutricion_ref_unidad}) no es compatible con la unidad del stock ({unidad})"
            )

        cursor.execute(
            '''
            UPDATE insumos
            SET codigo_barra = ?,
                nombre = ?,
                stock = ?,
                stock_minimo = ?,
                unidad = ?,
                precio_unitario = ?,
                cantidad_comprada = ?,
                unidad_compra = ?,
                precio_incluye_iva = ?,
                cantidad_por_scan = ?,
                unidad_por_scan = ?,
                nutricion_ref_cantidad = ?,
                nutricion_ref_unidad = ?,
                nutricion_kcal = ?,
                nutricion_proteinas_g = ?,
                nutricion_carbohidratos_g = ?,
                nutricion_grasas_g = ?,
                nutricion_azucares_g = ?,
                nutricion_sodio_mg = ?
            WHERE id = ?
            ''',
            (
                codigo_barra,
                nombre,
                stock,
                stock_minimo,
                unidad,
                precio_unitario,
                cantidad_comprada,
                unidad_compra,
                precio_incluye_iva,
                cantidad_por_scan,
                unidad_por_scan,
                nutricion_ref_cantidad,
                nutricion_ref_unidad,
                nutricion_kcal,
                nutricion_proteinas_g,
                nutricion_carbohidratos_g,
                nutricion_grasas_g,
                nutricion_azucares_g,
                nutricion_sodio_mg,
                insumo_id,
            ),
        )
        _sincronizar_lotes_insumo_a_stock_cursor(
            cursor,
            insumo_id,
            stock,
            lote_codigo=lote_codigo,
            fecha_elaboracion=fecha_elaboracion,
            fecha_vencimiento=fecha_vencimiento_lote,
        )
        if actualizar_metadata_lote:
            _actualizar_metadata_lote_referencia_cursor(
                cursor,
                insumo_id,
                stock,
                lote_codigo=lote_codigo,
                fecha_elaboracion=fecha_elaboracion,
                fecha_vencimiento=fecha_vencimiento_lote,
            )

        if abs(stock - stock_anterior) > 0:
            registrar_movimiento_stock(
                "insumo",
                insumo_id,
                "ajuste_edicion",
                abs(stock - stock_anterior),
                stock_anterior=stock_anterior,
                stock_nuevo=stock,
                referencia_tipo="edicion",
                detalle="Ajuste manual desde edición de insumo",
                origen_modulo="insumos",
                codigo_operacion=codigo_operacion,
                unidad=unidad,
                metadata={"insumo_id": insumo_id},
                conn=conn,
            )

        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def procesar_lote_rapido_insumos(items):
    """Aplica un lote de escaneo rápido editable en una sola transacción."""
    conn = get_db()
    cursor = conn.cursor()
    codigo_operacion = _normalizar_codigo_operacion(prefijo="OPL")
    procesados = []
    try:
        for raw in items or []:
            codigo = str(raw.get("codigo_barra") or "").strip()
            nombre = str(raw.get("nombre") or "").strip()
            cantidad = float(raw.get("cantidad") or 0)
            unidad = str(raw.get("unidad") or "unidad").strip().lower() or "unidad"
            precio_unitario = float(raw.get("precio_unitario") or 0)
            cantidad_comprada = float(raw.get("cantidad_comprada") or max(cantidad, 1))
            unidad_compra = str(raw.get("unidad_compra") or unidad).strip().lower() or unidad
            precio_incluye_iva = 1 if raw.get("precio_incluye_iva", True) else 0
            lote_codigo = _normalizar_lote_codigo(raw.get("lote_codigo"))
            fecha_elaboracion = _normalizar_fecha_iso(raw.get("fecha_elaboracion"), "Fecha de elaboracion")
            fecha_vencimiento = _normalizar_fecha_iso(raw.get("fecha_vencimiento"), "Fecha de vencimiento")
            _validar_fechas_lote(fecha_elaboracion, fecha_vencimiento)

            if cantidad <= 0:
                continue

            insumo = None
            if raw.get("insumo_id"):
                cursor.execute("SELECT * FROM insumos WHERE id = ?", (int(raw["insumo_id"]),))
                insumo = cursor.fetchone()
            if not insumo and codigo:
                insumo = _buscar_insumo_por_codigo_cursor(cursor, codigo)
            if not insumo and nombre:
                insumo = _buscar_insumo_por_nombre_cursor(cursor, nombre)

            if insumo:
                if not nombre:
                    nombre = str(insumo["nombre"] or "").strip()
                unidad_stock = _normalizar_unidad_producto(insumo["unidad"] or unidad)
                conversion = _convertir_cantidad_unidad_db(cantidad, unidad, unidad_stock)
                if not conversion["success"]:
                    referencia = nombre or codigo or f"ID {insumo['id']}"
                    raise ValueError(
                        f"No se pudo agregar '{referencia}': {conversion['error']}"
                    )
                cantidad_stock = float(conversion["cantidad"] or 0)
                if cantidad_stock <= 0:
                    continue

                stock_anterior = float(insumo["stock"] or 0)
                nuevo_stock = stock_anterior + cantidad_stock
                codigo_actual = str(insumo["codigo_barra"] or "").strip()
                if not codigo_actual and codigo:
                    codigo_actual = codigo
                cantidad_por_scan_actual = float(insumo["cantidad_por_scan"] or 0)
                if cantidad_por_scan_actual <= 0:
                    cantidad_por_scan_actual = max(cantidad_stock, 0.01)
                unidad_por_scan_actual = _normalizar_unidad_producto(insumo["unidad_por_scan"] or unidad_stock)
                cursor.execute(
                    """
                    UPDATE insumos
                    SET codigo_barra = ?,
                        stock = ?,
                        unidad = ?,
                        precio_unitario = ?,
                        cantidad_comprada = ?,
                        unidad_compra = ?,
                        precio_incluye_iva = ?,
                        cantidad_por_scan = ?,
                        unidad_por_scan = ?
                    WHERE id = ?
                    """,
                    (
                        codigo_actual or None,
                        nuevo_stock,
                        unidad_stock,
                        precio_unitario,
                        max(cantidad_comprada, 0.01),
                        unidad_compra or unidad_stock,
                        precio_incluye_iva,
                        cantidad_por_scan_actual,
                        unidad_por_scan_actual,
                        insumo["id"],
                    ),
                )
                insumo_id = insumo["id"]
                if codigo:
                    _asociar_codigo_insumo_cursor(cursor, insumo_id, codigo)
                _crear_o_sumar_lote_insumo_cursor(
                    cursor,
                    insumo_id,
                    cantidad_stock,
                    lote_codigo=lote_codigo,
                    fecha_elaboracion=fecha_elaboracion,
                    fecha_vencimiento=fecha_vencimiento,
                    merge=False,
                )
                registrar_movimiento_stock(
                    "insumo",
                    insumo_id,
                    "entrada_scanner",
                    cantidad_stock,
                    stock_anterior=stock_anterior,
                    stock_nuevo=nuevo_stock,
                    referencia_tipo="lote_rapido",
                    detalle=f"Código {codigo or '-'} | Nombre {nombre or '-'}",
                    origen_modulo="lote_rapido",
                    codigo_operacion=codigo_operacion,
                    unidad=unidad_stock,
                    metadata={
                        "insumo_id": insumo_id,
                        "codigo_barra": codigo,
                        "nombre": nombre,
                        "lote_codigo": lote_codigo,
                    },
                    conn=conn,
                )
            else:
                if not nombre:
                    nombre = f"Insumo {codigo[-6:]}" if codigo else "Insumo nuevo"
                cursor.execute(
                    """
                    INSERT INTO insumos
                    (codigo_barra, nombre, stock, unidad, precio_unitario, cantidad_comprada,
                     unidad_compra, precio_incluye_iva, cantidad_por_scan, unidad_por_scan)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        codigo or None,
                        nombre,
                        cantidad,
                        unidad,
                        precio_unitario,
                        max(cantidad_comprada, 0.01),
                        unidad_compra,
                        precio_incluye_iva,
                        max(cantidad, 0.01),
                        unidad,
                    ),
                )
                insumo_id = cursor.lastrowid
                if codigo:
                    _asociar_codigo_insumo_cursor(cursor, insumo_id, codigo)
                _crear_o_sumar_lote_insumo_cursor(
                    cursor,
                    insumo_id,
                    cantidad,
                    lote_codigo=lote_codigo,
                    fecha_elaboracion=fecha_elaboracion,
                    fecha_vencimiento=fecha_vencimiento,
                    merge=False,
                )
                registrar_movimiento_stock(
                    "insumo",
                    insumo_id,
                    "alta_scanner",
                    cantidad,
                    stock_anterior=0,
                    stock_nuevo=cantidad,
                    referencia_tipo="lote_rapido",
                    detalle=f"Código {codigo or '-'}",
                    origen_modulo="lote_rapido",
                    codigo_operacion=codigo_operacion,
                    unidad=unidad,
                    metadata={
                        "insumo_id": insumo_id,
                        "codigo_barra": codigo,
                        "nombre": nombre,
                        "lote_codigo": lote_codigo,
                    },
                    conn=conn,
                )

            procesados.append(
                {
                    "insumo_id": insumo_id,
                    "codigo_barra": codigo,
                    "nombre": nombre,
                    "cantidad": cantidad,
                    "unidad": unidad,
                    "lote_codigo": lote_codigo,
                    "fecha_elaboracion": fecha_elaboracion,
                    "fecha_vencimiento": fecha_vencimiento,
                }
            )

        conn.commit()
        return {"success": True, "procesados": procesados}
    except Exception as e:
        conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        conn.close()


def actualizar_preferencias_scan_insumo(insumo_id, cantidad_por_scan, unidad_por_scan):
    conn = get_db()
    cursor = conn.cursor()
    try:
        cantidad = float(cantidad_por_scan or 0)
        if cantidad <= 0:
            raise ValueError("La cantidad por escaneo debe ser mayor a 0")
        unidad = str(unidad_por_scan or "unidad").strip().lower() or "unidad"
        cursor.execute(
            """
            UPDATE insumos
            SET cantidad_por_scan = ?, unidad_por_scan = ?
            WHERE id = ?
            """,
            (cantidad, unidad, insumo_id),
        )
        if cursor.rowcount == 0:
            raise ValueError("Insumo no encontrado")
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def obtener_historial_ventas(fecha_desde=None, fecha_hasta=None):
    """Obtiene el historial de ventas con fechas formateadas"""
    conn = get_db()
    cursor = conn.cursor()
    
    query = """
        SELECT v.*, 
               GROUP_CONCAT(vi.producto_nombre || ' (x' || vi.cantidad || ')', ', ') as productos,
               COALESCE(v.total_monto, vt.total_monto, 0) AS total_monto_calc
        FROM ventas v
        LEFT JOIN venta_items vi ON v.id = vi.venta_id
        LEFT JOIN (
            SELECT venta_id, SUM(subtotal) AS total_monto
            FROM venta_detalles
            GROUP BY venta_id
        ) vt ON vt.venta_id = v.id
    """
    params = []
    
    if fecha_desde and fecha_hasta:
        query += " WHERE date(v.fecha_hora) BETWEEN ? AND ?"
        params = [fecha_desde, fecha_hasta]
    elif fecha_desde:
        query += " WHERE date(v.fecha_hora) >= ?"
        params = [fecha_desde]
    elif fecha_hasta:
        query += " WHERE date(v.fecha_hora) <= ?"
        params = [fecha_hasta]
    
    query += " GROUP BY v.id ORDER BY v.fecha_hora DESC"
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    
    # Formatear fechas
    resultado = []
    for row in rows:
        venta = dict(row)
        try:
            total_monto = float(venta.get("total_monto") or 0)
        except (TypeError, ValueError):
            total_monto = 0.0
        try:
            total_monto_calc = float(venta.get("total_monto_calc") or 0)
        except (TypeError, ValueError):
            total_monto_calc = 0.0
        if total_monto <= 0 and total_monto_calc > 0:
            venta["total_monto"] = total_monto_calc

        try:
            # Parsear fecha
            fecha_str = row['fecha_hora']
            if isinstance(fecha_str, str):
                from datetime import datetime
                # Formato: 2026-02-17 14:30:00
                fecha_obj = datetime.strptime(fecha_str, '%Y-%m-%d %H:%M:%S')
                # Formatear para mostrar: 17/02/2026 14:30
                venta['fecha_hora_formateada'] = fecha_obj.strftime('%d/%m/%Y %H:%M')
            else:
                venta['fecha_hora_formateada'] = str(fecha_str)
        except Exception as e:
            print(f"Error formateando fecha: {e}")
            venta['fecha_hora_formateada'] = str(row['fecha_hora'])
        
        resultado.append(venta)
    
    return resultado

def obtener_detalle_venta(venta_id):
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM ventas WHERE id = ?", (venta_id,))
    venta = cursor.fetchone()

    cursor.execute("""
        SELECT
            vi.*,
            vd.precio_unitario,
            vd.subtotal,
            p.stock as stock_actual
        FROM venta_items vi
        LEFT JOIN venta_detalles vd
            ON vd.venta_id = vi.venta_id
           AND vd.producto_id = vi.producto_id
        LEFT JOIN productos p ON vi.producto_id = p.id
        WHERE vi.venta_id = ?
    """, (venta_id,))
    items = [dict(r) for r in cursor.fetchall()]

    cursor.execute(
        "SELECT SUM(subtotal) AS total_monto FROM venta_detalles WHERE venta_id = ?",
        (venta_id,),
    )
    total_row = cursor.fetchone()
    total_monto_calc = float(total_row["total_monto"] or 0) if total_row else 0.0

    cursor.execute(
        """
        SELECT
            vl.producto_id,
            vl.lote_id,
            vl.cantidad_usada,
            pl.fecha_vencimiento,
            pl.fecha_ingreso
        FROM venta_lotes vl
        LEFT JOIN producto_lotes pl ON pl.id = vl.lote_id
        WHERE vl.venta_id = ?
        ORDER BY vl.producto_id ASC,
                 COALESCE(pl.fecha_vencimiento, '9999-12-31') ASC,
                 vl.id ASC
        """,
        (venta_id,),
    )
    lotes_por_producto = {}
    for row in cursor.fetchall():
        producto_id = int(row["producto_id"])
        lotes_por_producto.setdefault(producto_id, []).append(
            {
                "lote_id": row["lote_id"],
                "cantidad_usada": float(row["cantidad_usada"] or 0),
                "fecha_vencimiento": row["fecha_vencimiento"],
                "fecha_ingreso": row["fecha_ingreso"],
                "fecha_agregado": row["fecha_ingreso"],
                "fecha_elaboracion": row["fecha_ingreso"],
                "lote_activo": row["lote_id"] is not None and row["fecha_ingreso"] is not None,
            }
        )

    cursor.execute(
        """
        SELECT
            vi.producto_id,
            vi.insumo_id,
            i.nombre AS insumo_nombre,
            vi.cantidad_por_producto,
            vi.unidad_asociada,
            vi.cantidad_total_asociada,
            vi.cantidad_descontada_stock,
            vi.unidad_stock
        FROM venta_insumos vi
        LEFT JOIN insumos i ON i.id = vi.insumo_id
        WHERE vi.venta_id = ?
        ORDER BY vi.producto_id ASC, vi.id ASC
        """,
        (venta_id,),
    )
    filas_insumos = cursor.fetchall()

    cursor.execute(
        """
        SELECT
            vil.producto_id,
            vil.insumo_id,
            vil.insumo_lote_id,
            vil.cantidad_usada_stock,
            vil.unidad_stock,
            vil.lote_codigo,
            vil.fecha_elaboracion,
            vil.fecha_vencimiento,
            COALESCE(vil.fecha_ingreso, il.fecha_ingreso) AS fecha_ingreso
        FROM venta_insumo_lotes vil
        LEFT JOIN insumo_lotes il ON il.id = vil.insumo_lote_id
        WHERE vil.venta_id = ?
        ORDER BY vil.producto_id ASC, vil.insumo_id ASC, vil.id ASC
        """,
        (venta_id,),
    )
    lotes_insumo_map = {}
    for row in cursor.fetchall():
        key = (int(row["producto_id"]), int(row["insumo_id"]))
        lotes_insumo_map.setdefault(key, []).append(
            {
                "insumo_lote_id": row["insumo_lote_id"],
                "cantidad_usada_stock": float(row["cantidad_usada_stock"] or 0),
                "unidad_stock": row["unidad_stock"] or "unidad",
                "lote_codigo": row["lote_codigo"],
                "fecha_elaboracion": row["fecha_elaboracion"],
                "fecha_vencimiento": row["fecha_vencimiento"],
                "fecha_ingreso": row["fecha_ingreso"],
            }
        )

    insumos_por_producto = {}
    for row in filas_insumos:
        producto_id = int(row["producto_id"])
        insumo_id = int(row["insumo_id"])
        insumos_por_producto.setdefault(producto_id, []).append(
            {
                "insumo_id": insumo_id,
                "insumo_nombre": row["insumo_nombre"] or f"Insumo #{row['insumo_id']}",
                "cantidad_por_producto": float(row["cantidad_por_producto"] or 0),
                "unidad_asociada": row["unidad_asociada"] or "unidad",
                "cantidad_total_asociada": float(row["cantidad_total_asociada"] or 0),
                "cantidad_descontada_stock": float(row["cantidad_descontada_stock"] or 0),
                "unidad_stock": row["unidad_stock"] or "unidad",
                "lotes_usados": lotes_insumo_map.get((producto_id, insumo_id), []),
            }
        )

    for item in items:
        producto_id = int(item.get("producto_id") or 0)
        item["lotes_usados"] = lotes_por_producto.get(producto_id, [])
        item["insumos_asociados"] = insumos_por_producto.get(producto_id, [])

    if venta is not None:
        venta = dict(venta)
        try:
            total_actual = float(venta.get("total_monto") or 0)
        except (TypeError, ValueError):
            total_actual = 0.0
        if total_actual <= 0 and total_monto_calc > 0:
            venta["total_monto"] = float(total_monto_calc)

    conn.close()
    return venta, items


def obtener_codigo_operacion_venta(venta_id, conn=None):
    propia = conn is None
    if propia:
        conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT codigo_operacion FROM ventas WHERE id = ?", (venta_id,))
        row = cursor.fetchone()
        codigo = str(row["codigo_operacion"] or "").strip() if row else ""
        if codigo:
            return codigo
        cursor.execute(
            """
            SELECT codigo_operacion
            FROM stock_ledger
            WHERE referencia_tipo IN ('venta', 'venta_anulada')
              AND referencia_id = ?
            ORDER BY id ASC
            LIMIT 1
            """,
            (venta_id,),
        )
        row = cursor.fetchone()
        return str(row["codigo_operacion"] or "").strip() if row else None
    finally:
        if propia:
            conn.close()


def obtener_timeline_operacion(codigo_operacion, limit=600):
    codigo = str(codigo_operacion or "").strip()[:80]
    if not codigo:
        raise ValueError("Codigo de operación inválido")
    tope = max(10, min(int(limit or 600), 3000))

    conn = get_db()
    cursor = conn.cursor()
    try:
        eventos = []

        cursor.execute(
            """
            SELECT
                sl.id,
                sl.fecha,
                sl.tipo_operacion,
                sl.recurso_tipo,
                sl.recurso_id,
                COALESCE(i.nombre, p.nombre, sl.recurso_tipo || ' #' || sl.recurso_id) AS recurso_nombre,
                sl.cantidad_delta,
                sl.unidad,
                sl.lote_id,
                sl.origen_modulo,
                sl.referencia_tipo,
                sl.referencia_id,
                sl.detalle,
                sl.metadata_json
            FROM stock_ledger sl
            LEFT JOIN insumos i
                ON sl.recurso_tipo = 'insumo'
               AND i.id = sl.recurso_id
            LEFT JOIN productos p
                ON sl.recurso_tipo = 'producto'
               AND p.id = sl.recurso_id
            WHERE sl.codigo_operacion = ?
            ORDER BY datetime(sl.fecha) ASC, sl.id ASC
            LIMIT ?
            """,
            (codigo, tope),
        )
        for row in cursor.fetchall():
            eventos.append(
                {
                    "tipo": "stock",
                    "fecha": row["fecha"],
                    "orden": int(row["id"] or 0),
                    "origen_modulo": row["origen_modulo"] or row["referencia_tipo"] or "stock",
                    "titulo": f"{row['tipo_operacion']} | {row['recurso_tipo']} {row['recurso_nombre']}",
                    "detalle": row["detalle"] or "",
                    "payload": {
                        "movimiento_id": int(row["id"] or 0),
                        "tipo_operacion": row["tipo_operacion"],
                        "recurso_tipo": row["recurso_tipo"],
                        "recurso_id": int(row["recurso_id"] or 0),
                        "recurso_nombre": row["recurso_nombre"],
                        "cantidad_delta": float(row["cantidad_delta"] or 0),
                        "unidad": row["unidad"] or "unidad",
                        "lote_id": row["lote_id"],
                        "referencia_tipo": row["referencia_tipo"],
                        "referencia_id": row["referencia_id"],
                        "metadata_json": row["metadata_json"],
                    },
                }
            )

        cursor.execute(
            """
            SELECT id, fecha_hora, codigo_pedido, total_items, estado
            FROM ventas
            WHERE codigo_operacion = ?
            ORDER BY datetime(fecha_hora) ASC, id ASC
            LIMIT 50
            """,
            (codigo,),
        )
        for row in cursor.fetchall():
            eventos.append(
                {
                    "tipo": "venta",
                    "fecha": row["fecha_hora"],
                    "orden": int(row["id"] or 0),
                    "origen_modulo": "ventas",
                    "titulo": f"Venta #{row['id']}",
                    "detalle": f"Pedido: {row['codigo_pedido'] or '-'} | Items: {int(row['total_items'] or 0)}",
                    "payload": {
                        "venta_id": int(row["id"] or 0),
                        "codigo_pedido": row["codigo_pedido"],
                        "total_items": int(row["total_items"] or 0),
                        "estado": row["estado"] or "",
                    },
                }
            )

        cursor.execute(
            """
            SELECT p.id, p.fecha_hora, p.receta_id, p.cantidad, p.cantidad_resultado, r.nombre AS receta_nombre
            FROM producciones p
            LEFT JOIN recetas r ON r.id = p.receta_id
            WHERE p.codigo_operacion = ?
            ORDER BY datetime(p.fecha_hora) ASC, p.id ASC
            LIMIT 50
            """,
            (codigo,),
        )
        for row in cursor.fetchall():
            eventos.append(
                {
                    "tipo": "produccion",
                    "fecha": row["fecha_hora"],
                    "orden": int(row["id"] or 0),
                    "origen_modulo": "produccion",
                    "titulo": f"Produccion #{row['id']}",
                    "detalle": f"Receta: {row['receta_nombre'] or row['receta_id']} | Lotes: {int(row['cantidad'] or 0)} | Resultado: {float(row['cantidad_resultado'] or 0):.2f}",
                    "payload": {
                        "produccion_id": int(row["id"] or 0),
                        "receta_id": row["receta_id"],
                    },
                }
            )

        cursor.execute(
            """
            SELECT id, fecha, hora_inicio, titulo, cliente, estado
            FROM agenda_eventos
            WHERE codigo_operacion = ?
            ORDER BY date(fecha) ASC, COALESCE(hora_inicio, '00:00') ASC, id ASC
            LIMIT 50
            """,
            (codigo,),
        )
        for row in cursor.fetchall():
            eventos.append(
                {
                    "tipo": "agenda",
                    "fecha": f"{row['fecha']} {row['hora_inicio'] or '00:00'}:00",
                    "orden": int(row["id"] or 0),
                    "origen_modulo": "agenda",
                    "titulo": f"Agenda #{row['id']}",
                    "detalle": f"{row['titulo'] or ''} | Cliente: {row['cliente'] or '-'} | Estado: {row['estado'] or '-'}",
                    "payload": {"evento_id": int(row["id"] or 0)},
                }
            )

        cursor.execute(
            """
            SELECT pm.id, pm.creado, pm.producto_id, p.nombre AS producto_nombre, pm.cantidad, pm.motivo, pm.estado
            FROM producto_mermas pm
            LEFT JOIN productos p ON p.id = pm.producto_id
            WHERE pm.codigo_operacion = ?
            ORDER BY datetime(pm.creado) ASC, pm.id ASC
            LIMIT 50
            """,
            (codigo,),
        )
        for row in cursor.fetchall():
            eventos.append(
                {
                    "tipo": "merma",
                    "fecha": row["creado"],
                    "orden": int(row["id"] or 0),
                    "origen_modulo": "mermas",
                    "titulo": f"Merma #{row['id']}",
                    "detalle": f"{row['producto_nombre'] or ('Producto #' + str(row['producto_id']))} | {float(row['cantidad'] or 0):.2f} | {row['motivo'] or '-'} | Estado: {row['estado'] or '-'}",
                    "payload": {"merma_id": int(row["id"] or 0)},
                }
            )

        eventos.sort(key=lambda e: (str(e.get("fecha") or ""), int(e.get("orden") or 0)))
        return {
            "codigo_operacion": codigo,
            "total_eventos": len(eventos),
            "timeline": eventos[:tope],
        }
    finally:
        conn.close()

def eliminar_venta(venta_id):
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT id, codigo_operacion FROM ventas WHERE id = ?", (venta_id,))
        venta_row = cursor.fetchone()
        if not venta_row:
            raise ValueError("Venta no encontrada")
        codigo_operacion = _normalizar_codigo_operacion(venta_row["codigo_operacion"], prefijo="OPV")

        cursor.execute(
            """
            SELECT producto_id, lote_id, cantidad_usada
            FROM venta_lotes
            WHERE venta_id = ?
            ORDER BY id
            """,
            (venta_id,),
        )
        detalle_lotes = cursor.fetchall()

        restaurados_por_producto = {}
        for row in detalle_lotes:
            producto_id = int(row["producto_id"])
            lote_id = row["lote_id"]
            cantidad = float(row["cantidad_usada"] or 0)
            if cantidad <= 0:
                continue

            if lote_id is not None:
                cursor.execute(
                    "SELECT id FROM producto_lotes WHERE id = ? AND producto_id = ?",
                    (lote_id, producto_id),
                )
                lote = cursor.fetchone()
                if lote:
                    cursor.execute(
                        "UPDATE producto_lotes SET cantidad = COALESCE(cantidad, 0) + ? WHERE id = ?",
                        (cantidad, lote_id),
                    )
                else:
                    cursor.execute(
                        "INSERT INTO producto_lotes (producto_id, cantidad, fecha_vencimiento) VALUES (?, ?, ?)",
                        (producto_id, cantidad, None),
                    )
            else:
                cursor.execute(
                    "INSERT INTO producto_lotes (producto_id, cantidad, fecha_vencimiento) VALUES (?, ?, ?)",
                    (producto_id, cantidad, None),
                )
            restaurados_por_producto[producto_id] = restaurados_por_producto.get(producto_id, 0.0) + cantidad

        if not restaurados_por_producto:
            cursor.execute("SELECT producto_id, cantidad FROM venta_items WHERE venta_id = ?", (venta_id,))
            for item in cursor.fetchall():
                producto_id = int(item["producto_id"])
                cantidad = float(item["cantidad"] or 0)
                if cantidad <= 0:
                    continue
                cursor.execute(
                    "UPDATE productos SET stock = COALESCE(stock, 0) + ? WHERE id = ?",
                    (cantidad, producto_id),
                )
                restaurados_por_producto[producto_id] = restaurados_por_producto.get(producto_id, 0.0) + cantidad

        for producto_id in restaurados_por_producto.keys():
            try:
                nuevo_stock_producto = _recalcular_stock_y_vencimiento_producto(cursor, producto_id)
            except Exception:
                cursor.execute(
                    """
                    UPDATE productos
                    SET stock = (
                        SELECT COALESCE(SUM(cantidad), 0)
                        FROM producto_lotes
                        WHERE producto_id = ? AND cantidad > 0
                    )
                    WHERE id = ?
                    """,
                    (producto_id, producto_id),
                )
                cursor.execute("SELECT stock FROM productos WHERE id = ?", (producto_id,))
                row_prod = cursor.fetchone()
                nuevo_stock_producto = float(row_prod["stock"] or 0) if row_prod else 0

            cantidad_restaurada_producto = float(restaurados_por_producto.get(producto_id) or 0)
            if cantidad_restaurada_producto <= 0:
                continue
            stock_anterior_producto = nuevo_stock_producto - cantidad_restaurada_producto
            cursor.execute("SELECT unidad FROM productos WHERE id = ?", (producto_id,))
            row_unidad = cursor.fetchone()
            registrar_movimiento_stock(
                "producto",
                producto_id,
                "reversion_venta",
                cantidad_restaurada_producto,
                stock_anterior=stock_anterior_producto,
                stock_nuevo=nuevo_stock_producto,
                referencia_tipo="venta_anulada",
                referencia_id=venta_id,
                detalle="Reposición de producto por anulación de venta",
                origen_modulo="ventas",
                codigo_operacion=codigo_operacion,
                unidad=row_unidad["unidad"] if row_unidad else "unidad",
                metadata={"venta_id": venta_id, "contrapartida": "anulacion_venta"},
                conn=conn,
            )

        cursor.execute(
            """
            SELECT insumo_id, insumo_lote_id, cantidad_usada_stock, lote_codigo, fecha_elaboracion, fecha_vencimiento
            FROM venta_insumo_lotes
            WHERE venta_id = ?
            ORDER BY id ASC
            """,
            (venta_id,),
        )
        detalle_lotes_insumo = cursor.fetchall()
        restaurado_lotes_por_insumo = {}
        for row in detalle_lotes_insumo:
            insumo_id = int(row["insumo_id"])
            insumo_lote_id = row["insumo_lote_id"]
            cantidad_lote = float(row["cantidad_usada_stock"] or 0)
            if cantidad_lote <= 0:
                continue

            if insumo_lote_id is not None:
                cursor.execute(
                    "SELECT id FROM insumo_lotes WHERE id = ? AND insumo_id = ?",
                    (insumo_lote_id, insumo_id),
                )
                lote_existente = cursor.fetchone()
            else:
                lote_existente = None

            if lote_existente:
                cursor.execute(
                    """
                    UPDATE insumo_lotes
                    SET cantidad = COALESCE(cantidad, 0) + ?,
                        actualizado = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (cantidad_lote, insumo_lote_id),
                )
            else:
                _crear_o_sumar_lote_insumo_cursor(
                    cursor,
                    insumo_id,
                    cantidad_lote,
                    lote_codigo=row["lote_codigo"],
                    fecha_elaboracion=row["fecha_elaboracion"],
                    fecha_vencimiento=row["fecha_vencimiento"],
                    merge=True,
                )
            restaurado_lotes_por_insumo[insumo_id] = restaurado_lotes_por_insumo.get(insumo_id, 0.0) + cantidad_lote

        cursor.execute(
            """
            SELECT insumo_id, cantidad_descontada_stock, unidad_stock
            FROM venta_insumos
            WHERE venta_id = ?
            ORDER BY id DESC
            """,
            (venta_id,),
        )
        consumos_insumos = cursor.fetchall()
        for row in consumos_insumos:
            insumo_id = int(row["insumo_id"])
            cantidad_reponer = float(row["cantidad_descontada_stock"] or 0)
            if cantidad_reponer <= 0:
                continue

            cursor.execute("SELECT stock FROM insumos WHERE id = ?", (insumo_id,))
            insumo = cursor.fetchone()
            if not insumo:
                continue

            stock_anterior_insumo = float(insumo["stock"] or 0)
            stock_nuevo_insumo = stock_anterior_insumo + cantidad_reponer
            cursor.execute(
                "UPDATE insumos SET stock = ? WHERE id = ?",
                (stock_nuevo_insumo, insumo_id),
            )
            registrar_movimiento_stock(
                "insumo",
                insumo_id,
                "reversion_venta",
                cantidad_reponer,
                stock_anterior=stock_anterior_insumo,
                stock_nuevo=stock_nuevo_insumo,
                referencia_tipo="venta_anulada",
                referencia_id=venta_id,
                detalle="Reposición de insumo por anulación de venta",
                origen_modulo="ventas",
                codigo_operacion=codigo_operacion,
                unidad=row["unidad_stock"],
                metadata={"venta_id": venta_id, "contrapartida": "anulacion_venta"},
                conn=conn,
            )
            restaurado_lotes = float(restaurado_lotes_por_insumo.get(insumo_id, 0.0) or 0.0)
            faltante_lotes = cantidad_reponer - restaurado_lotes
            if faltante_lotes > 1e-9:
                _crear_o_sumar_lote_insumo_cursor(
                    cursor,
                    insumo_id,
                    faltante_lotes,
                    merge=True,
                )
                restaurado_lotes_por_insumo[insumo_id] = restaurado_lotes + faltante_lotes

        cursor.execute("DELETE FROM venta_insumo_lotes WHERE venta_id = ?", (venta_id,))
        cursor.execute("DELETE FROM venta_insumos WHERE venta_id = ?", (venta_id,))
        cursor.execute("DELETE FROM venta_lotes WHERE venta_id = ?", (venta_id,))
        cursor.execute("DELETE FROM venta_detalles WHERE venta_id = ?", (venta_id,))
        cursor.execute("DELETE FROM venta_items WHERE venta_id = ?", (venta_id,))
        cursor.execute("DELETE FROM ventas WHERE id = ?", (venta_id,))
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def obtener_reporte_ventas(tipo='diario'):
    conn = get_db()
    cursor = conn.cursor()
    
    if tipo == 'diario':
        query = """
            SELECT date(fecha_hora) as periodo, COUNT(*) as cantidad_ventas, SUM(total_items) as total_items
            FROM ventas
            GROUP BY date(fecha_hora)
            ORDER BY periodo DESC
            LIMIT 30
        """
    elif tipo == 'mensual':
        query = """
            SELECT strftime('%Y-%m', fecha_hora) as periodo, COUNT(*) as cantidad_ventas, SUM(total_items) as total_items
            FROM ventas
            GROUP BY strftime('%Y-%m', fecha_hora)
            ORDER BY periodo DESC
            LIMIT 12
        """
    else:
        query = """
            SELECT strftime('%Y', fecha_hora) as periodo, COUNT(*) as cantidad_ventas, SUM(total_items) as total_items
            FROM ventas
            GROUP BY strftime('%Y', fecha_hora)
            ORDER BY periodo DESC
        """
    
    cursor.execute(query)
    resumen = cursor.fetchall()
    
    cursor.execute("""
        SELECT producto_nombre, SUM(cantidad) as total_vendido
        FROM venta_items
        GROUP BY producto_id
        ORDER BY total_vendido DESC
        LIMIT 10
    """)
    productos_top = cursor.fetchall()
    
    conn.close()
    return resumen, productos_top

def obtener_recetas():
    """Obtiene recetas y componentes mixtos con costo actualizado."""
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT r.id, r.nombre, r.producto_id, r.rendimiento, p.nombre AS producto_nombre
            FROM recetas r
            LEFT JOIN productos p ON r.producto_id = p.id
            ORDER BY r.nombre
            """
        )
        recetas = cursor.fetchall()
        resultado = []

        for receta in recetas:
            cursor.execute(
                """
                SELECT ri.id, ri.tipo, ri.insumo_id, ri.producto_id, ri.cantidad, ri.unidad,
                       i.nombre AS insumo_nombre,
                       pr.nombre AS producto_componente_nombre
                FROM receta_items ri
                LEFT JOIN insumos i ON i.id = ri.insumo_id
                LEFT JOIN productos pr ON pr.id = ri.producto_id
                WHERE ri.receta_id = ?
                ORDER BY ri.id
                """,
                (receta["id"],),
            )
            rows = cursor.fetchall()

            componentes = []
            insumos_legacy = []
            for row in rows:
                tipo = (row["tipo"] or "insumo").lower()
                if tipo == "producto":
                    nombre = row["producto_componente_nombre"] or "Producto"
                    recurso_id = row["producto_id"]
                else:
                    nombre = row["insumo_nombre"] or "Insumo"
                    recurso_id = row["insumo_id"]
                    insumos_legacy.append(
                        {
                            "id": row["id"],
                            "insumo_id": row["insumo_id"],
                            "insumo_nombre": nombre,
                            "cantidad": row["cantidad"],
                            "unidad": row["unidad"] or "unidad",
                        }
                    )
                componentes.append(
                    {
                        "id": row["id"],
                        "tipo": tipo,
                        "recurso_id": recurso_id,
                        "nombre": nombre,
                        "cantidad": row["cantidad"],
                        "unidad": row["unidad"] or "unidad",
                    }
                )

            costo_info = calcular_costo_receta(receta["id"])
            resultado.append(
                {
                    "id": receta["id"],
                    "nombre": receta["nombre"],
                    "producto_id": receta["producto_id"],
                    "producto_nombre": receta["producto_nombre"] or "Sin producto asociado",
                    "rendimiento": float(receta["rendimiento"] or 1),
                    "componentes": componentes,
                    "insumos": insumos_legacy,
                    "costo_total": costo_info["costo_total"],
                    "costo_detalle": costo_info["detalle"],
                }
            )

        return resultado
    finally:
        conn.close()
    
def guardar_receta(nombre, producto_id, items, rendimiento=1):
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        nombre = (nombre or "").strip()
        if not nombre:
            raise ValueError("El nombre de la receta es obligatorio")
        if not items:
            raise ValueError("La receta debe tener al menos un componente")
        rendimiento = float(rendimiento or 1)
        if rendimiento <= 0:
            raise ValueError("El rendimiento debe ser mayor a 0")

        cursor.execute(
            "INSERT INTO recetas (nombre, producto_id, rendimiento) VALUES (?, ?, ?)",
            (nombre, producto_id, rendimiento)
        )
        receta_id = cursor.lastrowid
        
        for item in items:
            tipo = str(item.get("tipo") or "insumo").strip().lower()
            cantidad = float(item["cantidad"])
            unidad = (item.get("unidad") or "unidad").strip().lower()
            if cantidad <= 0:
                raise ValueError("La cantidad de cada componente debe ser mayor a 0")
            if tipo not in {"insumo", "producto"}:
                raise ValueError("Tipo de componente inválido")
            recurso_id = item.get("id")
            if recurso_id is None:
                recurso_id = item.get("insumo_id") if tipo == "insumo" else item.get("producto_id")
            if recurso_id is None:
                raise ValueError("Falta el componente de la receta")
            cursor.execute(
                """
                INSERT INTO receta_items (receta_id, tipo, insumo_id, producto_id, cantidad, unidad)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    receta_id,
                    tipo,
                    int(recurso_id) if tipo == "insumo" else None,
                    int(recurso_id) if tipo == "producto" else None,
                    cantidad,
                    unidad,
                )
            )
        
        conn.commit()
        return receta_id
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()


def obtener_receta_detalle(receta_id):
    """Retorna una receta con sus componentes para edición."""
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT r.id, r.nombre, r.producto_id, r.rendimiento, p.nombre AS producto_nombre
            FROM recetas r
            LEFT JOIN productos p ON p.id = r.producto_id
            WHERE r.id = ?
            """,
            (receta_id,),
        )
        receta = cursor.fetchone()
        if not receta:
            return None

        cursor.execute(
            """
            SELECT ri.id, ri.tipo, ri.insumo_id, ri.producto_id, ri.cantidad, ri.unidad,
                   i.nombre AS insumo_nombre, pr.nombre AS producto_nombre_item
            FROM receta_items ri
            LEFT JOIN insumos i ON i.id = ri.insumo_id
            LEFT JOIN productos pr ON pr.id = ri.producto_id
            WHERE ri.receta_id = ?
            ORDER BY ri.id
            """,
            (receta_id,),
        )
        rows = cursor.fetchall()
        items = []
        for row in rows:
            tipo = (row["tipo"] or "insumo").lower()
            items.append(
                {
                    "id": row["id"],
                    "tipo": tipo,
                    "recurso_id": row["producto_id"] if tipo == "producto" else row["insumo_id"],
                    "nombre": row["producto_nombre_item"] if tipo == "producto" else row["insumo_nombre"],
                    "cantidad": row["cantidad"],
                    "unidad": row["unidad"] or "unidad",
                    "insumo_id": row["insumo_id"],
                    "producto_id": row["producto_id"],
                }
            )

        nutricion = calcular_nutricion_receta(receta_id, conn=conn)

        return {
            "id": receta["id"],
            "nombre": receta["nombre"],
            "producto_id": receta["producto_id"],
            "producto_nombre": receta["producto_nombre"] or "Sin producto asociado",
            "rendimiento": float(receta["rendimiento"] or 1),
            "items": items,
            "nutricion": nutricion,
        }
    finally:
        conn.close()


def actualizar_receta(receta_id, nombre, producto_id, items, rendimiento=1):
    """Actualiza nombre, producto y componentes de una receta existente."""
    conn = get_db()
    cursor = conn.cursor()

    try:
        nombre = (nombre or "").strip()
        if not nombre:
            raise ValueError("El nombre de la receta es obligatorio")
        if not items:
            raise ValueError("La receta debe tener al menos un componente")

        rendimiento = float(rendimiento or 1)
        if rendimiento <= 0:
            raise ValueError("El rendimiento debe ser mayor a 0")

        cursor.execute("SELECT id FROM recetas WHERE id = ?", (receta_id,))
        if not cursor.fetchone():
            raise ValueError("La receta no existe")

        cursor.execute(
            "UPDATE recetas SET nombre = ?, producto_id = ?, rendimiento = ? WHERE id = ?",
            (nombre, producto_id, rendimiento, receta_id),
        )

        cursor.execute("DELETE FROM receta_items WHERE receta_id = ?", (receta_id,))

        for item in items:
            tipo = str(item.get("tipo") or "insumo").strip().lower()
            cantidad = float(item.get("cantidad") or 0)
            unidad = (item.get("unidad") or "unidad").strip().lower()
            if tipo not in {"insumo", "producto"}:
                raise ValueError("Tipo de componente inválido")
            if cantidad <= 0:
                raise ValueError("La cantidad de cada componente debe ser mayor a 0")

            recurso_id = item.get("id")
            if recurso_id is None:
                recurso_id = item.get("insumo_id") if tipo == "insumo" else item.get("producto_id")
            if recurso_id is None:
                raise ValueError("Falta un componente en la receta")

            cursor.execute(
                """
                INSERT INTO receta_items (receta_id, tipo, insumo_id, producto_id, cantidad, unidad)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    receta_id,
                    tipo,
                    int(recurso_id) if tipo == "insumo" else None,
                    int(recurso_id) if tipo == "producto" else None,
                    cantidad,
                    unidad,
                ),
            )

        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _descontar_stock_con_conversion_cursor(cursor, insumo_id, cantidad_necesaria, unidad_necesaria):
    cursor.execute("SELECT stock, unidad FROM insumos WHERE id = ?", (insumo_id,))
    insumo = cursor.fetchone()
    if not insumo:
        raise ValueError("Insumo no encontrado")

    stock_actual = float(insumo["stock"] or 0)
    unidad_stock = (insumo["unidad"] or "unidad").lower().strip()
    unidad_necesaria = (unidad_necesaria or "unidad").lower().strip()

    if unidad_stock == unidad_necesaria:
        nuevo_stock = stock_actual - cantidad_necesaria
        cantidad_stock = float(cantidad_necesaria)
        cursor.execute("UPDATE insumos SET stock = ? WHERE id = ?", (nuevo_stock, insumo_id))
        lotes_descontados = _descontar_lotes_insumo_cursor(cursor, insumo_id, cantidad_stock)
        return {
            "stock_anterior": stock_actual,
            "stock_nuevo": nuevo_stock,
            "cantidad_stock": cantidad_stock,
            "unidad_stock": unidad_stock,
            "lotes_descontados": lotes_descontados.get("detalle", []),
        }

    stock_en_base = convertir_a_base(stock_actual, unidad_stock)
    necesario_en_base = convertir_a_base(cantidad_necesaria, unidad_necesaria)
    factor_stock = FACTORES_CONVERSION.get(unidad_stock, 1)
    nuevo_stock = (stock_en_base - necesario_en_base) / factor_stock
    cantidad_stock = necesario_en_base / factor_stock

    cursor.execute("UPDATE insumos SET stock = ? WHERE id = ?", (nuevo_stock, insumo_id))
    lotes_descontados = _descontar_lotes_insumo_cursor(cursor, insumo_id, cantidad_stock)
    return {
        "stock_anterior": stock_actual,
        "stock_nuevo": nuevo_stock,
        "cantidad_stock": float(cantidad_stock),
        "unidad_stock": unidad_stock,
        "lotes_descontados": lotes_descontados.get("detalle", []),
    }


def _sumar_stock_con_conversion_cursor(cursor, insumo_id, cantidad_sumar, unidad_sumar):
    cursor.execute("SELECT stock, unidad FROM insumos WHERE id = ?", (insumo_id,))
    insumo = cursor.fetchone()
    if not insumo:
        raise ValueError("Insumo no encontrado")

    stock_actual = float(insumo["stock"] or 0)
    unidad_stock = (insumo["unidad"] or "unidad").lower().strip()
    unidad_sumar = (unidad_sumar or "unidad").lower().strip()

    if unidad_stock == unidad_sumar:
        nuevo_stock = stock_actual + cantidad_sumar
        cantidad_stock = float(cantidad_sumar)
        cursor.execute("UPDATE insumos SET stock = ? WHERE id = ?", (nuevo_stock, insumo_id))
        lote_id = None
        if cantidad_stock > 0:
            lote_id = _crear_o_sumar_lote_insumo_cursor(cursor, insumo_id, cantidad_stock, merge=True)
        return {
            "stock_anterior": stock_actual,
            "stock_nuevo": nuevo_stock,
            "cantidad_stock": cantidad_stock,
            "unidad_stock": unidad_stock,
            "lote_id": lote_id,
        }

    stock_en_base = convertir_a_base(stock_actual, unidad_stock)
    sumar_en_base = convertir_a_base(cantidad_sumar, unidad_sumar)
    factor_stock = FACTORES_CONVERSION.get(unidad_stock, 1)
    nuevo_stock = (stock_en_base + sumar_en_base) / factor_stock
    cantidad_stock = sumar_en_base / factor_stock

    cursor.execute("UPDATE insumos SET stock = ? WHERE id = ?", (nuevo_stock, insumo_id))
    lote_id = None
    if cantidad_stock > 0:
        lote_id = _crear_o_sumar_lote_insumo_cursor(cursor, insumo_id, cantidad_stock, merge=True)
    return {
        "stock_anterior": stock_actual,
        "stock_nuevo": nuevo_stock,
        "cantidad_stock": float(cantidad_stock),
        "unidad_stock": unidad_stock,
        "lote_id": lote_id,
    }


def producir_receta(receta_id, cantidad, cantidad_resultado=None, fecha_vencimiento=None):
    """Produce según receta con trazabilidad para reversión total."""
    conn = get_db()
    cursor = conn.cursor()
    codigo_operacion = _normalizar_codigo_operacion(prefijo="OPP")

    try:
        cantidad = int(cantidad or 0)
        if cantidad <= 0:
            return {"success": False, "error": "La cantidad debe ser mayor a 0"}

        cursor.execute(
            """
            SELECT r.id, r.nombre, r.producto_id, r.rendimiento,
                   p.nombre AS producto_nombre, p.vida_util_dias, p.fecha_vencimiento
            FROM recetas r
            LEFT JOIN productos p ON p.id = r.producto_id
            WHERE r.id = ?
            """,
            (receta_id,),
        )
        receta = cursor.fetchone()
        if not receta:
            return {"success": False, "error": "Receta no encontrada"}

        cursor.execute(
            """
            SELECT ri.id, ri.tipo, ri.insumo_id, ri.producto_id, ri.cantidad, ri.unidad,
                   i.nombre AS insumo_nombre, pr.nombre AS producto_nombre
            FROM receta_items ri
            LEFT JOIN insumos i ON i.id = ri.insumo_id
            LEFT JOIN productos pr ON pr.id = ri.producto_id
            WHERE ri.receta_id = ?
            ORDER BY ri.id
            """,
            (receta_id,),
        )
        items = cursor.fetchall()
        if not items:
            return {"success": False, "error": "La receta no tiene componentes"}

        for item in items:
            total_item = float(item["cantidad"] or 0) * cantidad
            tipo = (item["tipo"] or "insumo").lower()
            if tipo == "producto":
                cursor.execute(
                    "SELECT COALESCE(SUM(cantidad), 0) AS stock_lotes FROM producto_lotes WHERE producto_id = ? AND cantidad > 0",
                    (item["producto_id"],),
                )
                stock_disponible = float(cursor.fetchone()["stock_lotes"] or 0)
                if stock_disponible < total_item:
                    return {
                        "success": False,
                        "error": f"Stock insuficiente de producto '{item['producto_nombre']}'. Necesita {total_item}, disponible {stock_disponible}",
                    }
            else:
                resultado = verificar_stock_con_conversion(item["insumo_id"], total_item, item["unidad"] or "unidad")
                if not resultado["suficiente"]:
                    return {"success": False, "error": resultado["error"]}

        rendimiento = float(receta["rendimiento"] or 1)
        cantidad_resultado_final = float(cantidad_resultado) if cantidad_resultado is not None else (cantidad * rendimiento)
        if cantidad_resultado_final <= 0:
            return {"success": False, "error": "La cantidad resultante debe ser mayor a 0"}

        cursor.execute(
            """
            INSERT INTO producciones (receta_id, cantidad, cantidad_resultado, fecha_hora, codigo_operacion, metadata_json)
            VALUES (?, ?, ?, datetime('now'), ?, ?)
            """,
            (
                receta_id,
                cantidad,
                cantidad_resultado_final,
                codigo_operacion,
                json.dumps({"rendimiento": rendimiento, "codigo_operacion": codigo_operacion}, ensure_ascii=True),
            ),
        )
        produccion_id = cursor.lastrowid

        insumos_descontados = 0
        productos_descontados = 0
        descuentos_resumen = {}
        trazabilidad_insumo_lotes = []

        for item in items:
            total_item = float(item["cantidad"] or 0) * cantidad
            tipo = (item["tipo"] or "insumo").lower()

            if tipo == "producto":
                cursor.execute("SELECT stock FROM productos WHERE id = ?", (item["producto_id"],))
                stock_before_row = cursor.fetchone()
                stock_anterior = float(stock_before_row["stock"] or 0) if stock_before_row else 0

                descuento = _descontar_lotes_fifo_cursor(cursor, item["producto_id"], total_item)
                if not descuento["success"]:
                    raise ValueError(descuento["error"])

                for lote in descuento.get("detalle", []):
                    cursor.execute(
                        """
                        INSERT INTO produccion_movimientos
                        (produccion_id, tipo_recurso, recurso_id, lote_id, accion, cantidad, unidad, metadata_json)
                        VALUES (?, 'producto_lote_consumido', ?, ?, 'descuento', ?, 'unidad', ?)
                        """,
                        (
                            produccion_id,
                            item["producto_id"],
                            lote["lote_id"],
                            lote["cantidad_usada"],
                            json.dumps({"receta_item_id": item["id"]}, ensure_ascii=True),
                        ),
                    )

                nuevo_stock_prod = _recalcular_stock_y_vencimiento_producto(cursor, item["producto_id"])
                registrar_movimiento_stock(
                    "producto",
                    item["producto_id"],
                    "consumo_receta",
                    total_item,
                    stock_anterior=stock_anterior,
                    stock_nuevo=nuevo_stock_prod,
                    referencia_tipo="produccion",
                    referencia_id=produccion_id,
                    detalle=f"Receta {receta['nombre']}",
                    origen_modulo="produccion",
                    codigo_operacion=codigo_operacion,
                    unidad="unidad",
                    metadata={
                        "produccion_id": produccion_id,
                        "receta_id": receta_id,
                        "receta_item_id": item["id"],
                    },
                    conn=conn,
                )
                productos_descontados += 1
                clave_desc = ("producto", int(item["producto_id"]), "unidad")
                if clave_desc not in descuentos_resumen:
                    descuentos_resumen[clave_desc] = {
                        "tipo": "producto",
                        "recurso_id": int(item["producto_id"]),
                        "nombre": item["producto_nombre"] or f"Producto #{item['producto_id']}",
                        "cantidad": 0.0,
                        "unidad": "unidad",
                    }
                descuentos_resumen[clave_desc]["cantidad"] += float(total_item or 0)
            else:
                ajuste = _descontar_stock_con_conversion_cursor(
                    cursor,
                    item["insumo_id"],
                    total_item,
                    item["unidad"] or "unidad",
                )
                cursor.execute(
                    """
                    INSERT INTO produccion_movimientos
                    (produccion_id, tipo_recurso, recurso_id, accion, cantidad, unidad, metadata_json)
                    VALUES (?, 'insumo', ?, 'descuento', ?, ?, ?)
                    """,
                    (
                        produccion_id,
                        item["insumo_id"],
                        ajuste["cantidad_stock"],
                        ajuste["unidad_stock"],
                        json.dumps({"unidad_receta": item["unidad"], "cantidad_receta": total_item}, ensure_ascii=True),
                    ),
                )
                registrar_movimiento_stock(
                    "insumo",
                    item["insumo_id"],
                    "consumo_receta",
                    ajuste["cantidad_stock"],
                    stock_anterior=ajuste["stock_anterior"],
                    stock_nuevo=ajuste["stock_nuevo"],
                    referencia_tipo="produccion",
                    referencia_id=produccion_id,
                    detalle=f"Receta {receta['nombre']}",
                    origen_modulo="produccion",
                    codigo_operacion=codigo_operacion,
                    unidad=ajuste["unidad_stock"],
                    metadata={
                        "produccion_id": produccion_id,
                        "receta_id": receta_id,
                        "receta_item_id": item["id"],
                    },
                    conn=conn,
                )
                insumos_descontados += 1
                unidad_desc = str(ajuste.get("unidad_stock") or (item["unidad"] or "unidad")).strip() or "unidad"
                clave_desc = ("insumo", int(item["insumo_id"]), unidad_desc.lower())
                if clave_desc not in descuentos_resumen:
                    descuentos_resumen[clave_desc] = {
                        "tipo": "insumo",
                        "recurso_id": int(item["insumo_id"]),
                        "nombre": item["insumo_nombre"] or f"Insumo #{item['insumo_id']}",
                        "cantidad": 0.0,
                        "unidad": unidad_desc,
                    }
                descuentos_resumen[clave_desc]["cantidad"] += float(ajuste.get("cantidad_stock") or 0)

                lotes_insumo = list(ajuste.get("lotes_descontados") or [])
                if not lotes_insumo and float(ajuste.get("cantidad_stock") or 0) > 0:
                    lotes_insumo = [
                        {
                            "insumo_lote_id": None,
                            "cantidad_usada": float(ajuste.get("cantidad_stock") or 0),
                            "lote_codigo": None,
                            "fecha_elaboracion": None,
                            "fecha_vencimiento": None,
                            "fecha_ingreso": None,
                        }
                    ]

                for lote in lotes_insumo:
                    cantidad_lote = float(lote.get("cantidad_usada") or 0)
                    if cantidad_lote <= 0:
                        continue
                    trazabilidad_insumo_lotes.append(
                        {
                            "insumo_id": int(item["insumo_id"]),
                            "insumo_nombre": item["insumo_nombre"] or f"Insumo #{item['insumo_id']}",
                            "insumo_lote_id": lote.get("insumo_lote_id"),
                            "insumo_lote_codigo": lote.get("lote_codigo"),
                            "insumo_fecha_elaboracion": lote.get("fecha_elaboracion"),
                            "insumo_fecha_vencimiento": lote.get("fecha_vencimiento"),
                            "insumo_fecha_ingreso": lote.get("fecha_ingreso"),
                            "cantidad_insumo_usada": cantidad_lote,
                            "unidad_insumo": ajuste.get("unidad_stock") or (item["unidad"] or "unidad"),
                        }
                    )

        lote_creado_id = None
        lote_producto_info = None
        if receta["producto_id"]:
            fecha_venc = fecha_vencimiento
            if not fecha_venc:
                vida_util = int(receta["vida_util_dias"] or 0)
                if vida_util > 0:
                    fecha_venc = (datetime.now() + timedelta(days=vida_util)).strftime("%Y-%m-%d")
                else:
                    fecha_venc = receta["fecha_vencimiento"]

            cursor.execute("SELECT stock FROM productos WHERE id = ?", (receta["producto_id"],))
            stock_row = cursor.fetchone()
            stock_anterior_producto = float(stock_row["stock"] or 0) if stock_row else 0

            cursor.execute(
                "INSERT INTO producto_lotes (producto_id, cantidad, fecha_vencimiento) VALUES (?, ?, ?)",
                (receta["producto_id"], cantidad_resultado_final, fecha_venc),
            )
            lote_creado_id = cursor.lastrowid
            cursor.execute(
                """
                SELECT id, fecha_ingreso, fecha_vencimiento, cantidad
                FROM producto_lotes
                WHERE id = ?
                """,
                (lote_creado_id,),
            )
            lote_producto_info = cursor.fetchone()
            nuevo_stock_producto = _recalcular_stock_y_vencimiento_producto(cursor, receta["producto_id"])

            cursor.execute(
                """
                INSERT INTO produccion_movimientos
                (produccion_id, tipo_recurso, recurso_id, lote_id, accion, cantidad, unidad, metadata_json)
                VALUES (?, 'producto_lote_producido', ?, ?, 'alta', ?, 'unidad', ?)
                """,
                (
                    produccion_id,
                    receta["producto_id"],
                    lote_creado_id,
                    cantidad_resultado_final,
                    json.dumps({"fecha_vencimiento": fecha_venc}, ensure_ascii=True),
                ),
            )

            registrar_movimiento_stock(
                "producto",
                receta["producto_id"],
                "produccion",
                cantidad_resultado_final,
                stock_anterior=stock_anterior_producto,
                stock_nuevo=nuevo_stock_producto,
                referencia_tipo="produccion",
                referencia_id=produccion_id,
                detalle=f"Lote producido desde receta {receta['nombre']}",
                origen_modulo="produccion",
                codigo_operacion=codigo_operacion,
                unidad="unidad",
                lote_id=lote_creado_id,
                metadata={
                    "produccion_id": produccion_id,
                    "receta_id": receta_id,
                    "lote_id": lote_creado_id,
                },
                conn=conn,
            )

            if trazabilidad_insumo_lotes:
                cursor.execute("SELECT fecha_hora FROM producciones WHERE id = ?", (produccion_id,))
                produccion_row = cursor.fetchone()
                producido_en = produccion_row["fecha_hora"] if produccion_row else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                mes_clave = str(producido_en or "")[:7] if producido_en else datetime.now().strftime("%Y-%m")
                producto_fecha_elaboracion = lote_producto_info["fecha_ingreso"] if lote_producto_info else producido_en
                producto_fecha_vencimiento = lote_producto_info["fecha_vencimiento"] if lote_producto_info else fecha_venc
                cantidad_lote_producto = float(lote_producto_info["cantidad"] or cantidad_resultado_final) if lote_producto_info else float(cantidad_resultado_final)

                for traza in trazabilidad_insumo_lotes:
                    cursor.execute(
                        """
                        INSERT INTO haccp_trazabilidad_insumos (
                            produccion_id, receta_id, producto_id, producto_nombre, producto_lote_id,
                            producto_fecha_elaboracion, producto_fecha_vencimiento, cantidad_producto_lote,
                            insumo_id, insumo_nombre, insumo_lote_id, insumo_lote_codigo,
                            insumo_fecha_elaboracion, insumo_fecha_vencimiento, insumo_fecha_ingreso,
                            cantidad_insumo_usada, unidad_insumo, producido_en, mes_clave
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            produccion_id,
                            receta_id,
                            receta["producto_id"],
                            receta["producto_nombre"] if "producto_nombre" in receta.keys() else None,
                            lote_creado_id,
                            producto_fecha_elaboracion,
                            producto_fecha_vencimiento,
                            cantidad_lote_producto,
                            traza["insumo_id"],
                            traza["insumo_nombre"],
                            traza.get("insumo_lote_id"),
                            traza.get("insumo_lote_codigo"),
                            traza.get("insumo_fecha_elaboracion"),
                            traza.get("insumo_fecha_vencimiento"),
                            traza.get("insumo_fecha_ingreso"),
                            traza["cantidad_insumo_usada"],
                            traza["unidad_insumo"],
                            producido_en,
                            mes_clave,
                        ),
                    )

        descuentos_detalle = sorted(
            [
                {
                    "tipo": d["tipo"],
                    "recurso_id": d["recurso_id"],
                    "nombre": d["nombre"],
                    "cantidad": round(float(d["cantidad"] or 0), 4),
                    "unidad": d["unidad"],
                }
                for d in descuentos_resumen.values()
                if float(d.get("cantidad") or 0) > 0
            ],
            key=lambda x: (x.get("tipo") != "insumo", str(x.get("nombre") or "").lower()),
        )

        conn.commit()
        return {
            "success": True,
            "produccion_id": produccion_id,
            "codigo_operacion": codigo_operacion,
            "insumos_descontados": insumos_descontados,
            "productos_descontados": productos_descontados,
            "cantidad_resultado": cantidad_resultado_final,
            "lote_creado_id": lote_creado_id,
            "descuentos_detalle": descuentos_detalle,
        }
    except Exception as e:
        conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        conn.close()


def limpiar_producciones_antiguas(meses=6):
    """Elimina registros de producción anteriores al límite configurado."""
    conn = get_db()
    cursor = conn.cursor()

    try:
        meses = max(int(meses or 6), 1)
        cursor.execute(
            "DELETE FROM producciones WHERE datetime(fecha_hora) < datetime('now', ?)",
            (f"-{meses} months",),
        )
        eliminadas = cursor.rowcount if cursor.rowcount is not None else 0
        conn.commit()
        return eliminadas
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def obtener_historial_produccion_semanal(page=1, weeks_per_page=6, meses=6):
    """
    Retorna producciones de los últimos meses separadas en:
    - esta semana
    - semanas anteriores (agrupadas y paginadas)
    """
    page = max(int(page or 1), 1)
    weeks_per_page = min(max(int(weeks_per_page or 6), 1), 20)
    meses = max(int(meses or 6), 1)

    # Mantener solo historial útil y evitar crecimiento indefinido.
    limpiar_producciones_antiguas(meses=meses)

    conn = get_db()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT p.id, p.cantidad, p.cantidad_resultado, p.fecha_hora, r.nombre AS receta_nombre
            FROM producciones p
            JOIN recetas r ON p.receta_id = r.id
            WHERE datetime(p.fecha_hora) >= datetime('now', ?)
            ORDER BY p.fecha_hora DESC
            """,
            (f"-{meses} months",),
        )
        rows = cursor.fetchall()

        now_chile = obtener_hora_chile()
        inicio_semana = (now_chile - timedelta(days=now_chile.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        fin_semana = inicio_semana + timedelta(days=7)

        meses_es = [
            "ene", "feb", "mar", "abr", "may", "jun",
            "jul", "ago", "sep", "oct", "nov", "dic",
        ]

        def parse_fecha_utc(fecha_str):
            if not fecha_str:
                return None
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
                try:
                    return datetime.strptime(fecha_str, fmt).replace(tzinfo=pytz.utc)
                except ValueError:
                    continue
            return None

        def fmt_dia(fecha_obj):
            return f"{fecha_obj.day:02d} {meses_es[fecha_obj.month - 1]}"

        this_week = []
        groups = {}

        for row in rows:
            fecha_utc = parse_fecha_utc(row["fecha_hora"])
            if not fecha_utc:
                continue

            fecha_chile = fecha_utc.astimezone(ZONA_HORARIA_CHILE)
            item = {
                "id": row["id"],
                "cantidad": float(row["cantidad_resultado"] or row["cantidad"] or 0),
                "cantidad_lotes": row["cantidad"],
                "receta_nombre": row["receta_nombre"],
                "fecha_hora": formatear_fecha_chile(row["fecha_hora"]),
                "fecha_hora_iso": fecha_chile.strftime("%Y-%m-%d %H:%M:%S"),
            }

            if inicio_semana <= fecha_chile < fin_semana:
                this_week.append(item)
                continue

            week_start = (fecha_chile - timedelta(days=fecha_chile.weekday())).date()
            week_end = week_start + timedelta(days=6)
            iso_year, iso_week, _ = fecha_chile.isocalendar()
            week_key = week_start.isoformat()

            if week_key not in groups:
                groups[week_key] = {
                    "week_key": week_key,
                    "year": iso_year,
                    "week_number": iso_week,
                    "semana_label": f"Semana {iso_week} ({fmt_dia(week_start)} - {fmt_dia(week_end)})",
                    "rango_label": f"{fmt_dia(week_start)} al {fmt_dia(week_end)}",
                    "total_registros": 0,
                    "total_cantidad": 0,
                    "registros": [],
                }

            groups[week_key]["registros"].append(item)
            groups[week_key]["total_registros"] += 1
            groups[week_key]["total_cantidad"] += float(row["cantidad_resultado"] or row["cantidad"] or 0)

        this_week.sort(key=lambda x: x["fecha_hora_iso"], reverse=True)

        previous_weeks = sorted(
            groups.values(),
            key=lambda w: w["week_key"],
            reverse=True,
        )

        start = (page - 1) * weeks_per_page
        end = start + weeks_per_page
        paged_weeks = previous_weeks[start:end]

        return {
            "this_week": this_week,
            "this_week_total_registros": len(this_week),
            "this_week_total_cantidad": sum(item["cantidad"] for item in this_week),
            "previous_weeks": paged_weeks,
            "previous_total_weeks": len(previous_weeks),
            "page": page,
            "weeks_per_page": weeks_per_page,
            "has_more": end < len(previous_weeks),
            "months_kept": meses,
        }
    finally:
        conn.close()


def _obtener_agenda_produccion_rango_cursor(cursor, fecha_desde, fecha_hasta):
    try:
        desde_iso = _normalizar_fecha_iso(fecha_desde, "fecha desde")
        hasta_iso = _normalizar_fecha_iso(fecha_hasta, "fecha hasta")
    except ValueError:
        return []

    if not desde_iso or not hasta_iso:
        return []

    try:
        desde_dt = datetime.strptime(desde_iso, "%Y-%m-%d").date()
        hasta_dt = datetime.strptime(hasta_iso, "%Y-%m-%d").date()
    except ValueError:
        return []

    if desde_dt > hasta_dt:
        desde_dt, hasta_dt = hasta_dt, desde_dt

    cursor.execute(
        """
        SELECT
            pa.id,
            pa.fecha,
            pa.receta_id,
            pa.receta_nombre AS receta_nombre_guardada,
            pa.producto_id,
            pa.producto_nombre AS producto_nombre_guardado,
            pa.cantidad,
            pa.nota,
            pa.estado,
            COALESCE(r.nombre, pa.receta_nombre, 'Receta') AS receta_nombre,
            COALESCE(r.producto_id, pa.producto_id) AS producto_id_resuelto,
            COALESCE(p.nombre, pa.producto_nombre, 'Sin producto asociado') AS producto_nombre
        FROM produccion_agendada pa
        LEFT JOIN recetas r ON r.id = pa.receta_id
        LEFT JOIN productos p ON p.id = COALESCE(r.producto_id, pa.producto_id)
        WHERE LOWER(TRIM(COALESCE(pa.estado, 'pendiente'))) = 'pendiente'
        ORDER BY pa.id ASC
        """
    )

    rows = cursor.fetchall()
    filtradas = []
    for row in rows:
        raw_fecha = str(row["fecha"] or "").strip()
        if not raw_fecha:
            continue
        try:
            fecha_iso = _normalizar_fecha_iso(raw_fecha, "fecha")
            fecha_dt = datetime.strptime(fecha_iso, "%Y-%m-%d").date()
        except ValueError:
            # Ignorar filas antiguas con formato irreconocible sin romper el panel.
            continue

        if desde_dt <= fecha_dt <= hasta_dt:
            item = dict(row)
            item["fecha"] = fecha_iso
            filtradas.append(item)

    filtradas.sort(key=lambda x: (str(x.get("fecha") or ""), int(x.get("id") or 0)))
    return filtradas


def agendar_produccion_manual(receta_id, fecha, cantidad=1, nota=None):
    conn = get_db()
    cursor = conn.cursor()
    try:
        receta_id_int = int(receta_id or 0)
        if receta_id_int <= 0:
            return {"success": False, "error": "Receta invalida"}

        fecha_iso = _normalizar_fecha_iso(fecha, "fecha")
        if not fecha_iso:
            return {"success": False, "error": "La fecha es obligatoria"}

        cantidad_num = float(cantidad or 0)
        if cantidad_num <= 0:
            return {"success": False, "error": "La cantidad debe ser mayor a 0"}

        nota_txt = str(nota or "").strip()
        if len(nota_txt) > 400:
            nota_txt = nota_txt[:400]

        cursor.execute(
            """
            SELECT
                r.id,
                r.nombre,
                r.producto_id,
                p.nombre AS producto_nombre
            FROM recetas r
            LEFT JOIN productos p ON p.id = r.producto_id
            WHERE r.id = ?
            """,
            (receta_id_int,),
        )
        receta = cursor.fetchone()
        if not receta:
            return {"success": False, "error": "Receta no encontrada"}

        receta_nombre = str(receta["nombre"] or "Receta").strip() or "Receta"
        producto_id = int(receta["producto_id"]) if receta["producto_id"] else None
        producto_nombre = str(receta["producto_nombre"] or "Sin producto asociado").strip() or "Sin producto asociado"

        cursor.execute(
            """
            INSERT INTO produccion_agendada
                (fecha, receta_id, receta_nombre, producto_id, producto_nombre, cantidad, nota, estado, actualizado)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'pendiente', CURRENT_TIMESTAMP)
            """,
            (
                fecha_iso,
                receta_id_int,
                receta_nombre,
                producto_id,
                producto_nombre,
                cantidad_num,
                nota_txt if nota_txt else None,
            ),
        )
        agendado_id = int(cursor.lastrowid or 0)
        conn.commit()

        return {
            "success": True,
            "agendado_id": agendado_id,
            "agendado": {
                "id": agendado_id,
                "fecha": fecha_iso,
                "receta_id": receta_id_int,
                "receta_nombre": receta_nombre,
                "producto_id": producto_id,
                "producto_nombre": producto_nombre,
                "cantidad": cantidad_num,
                "nota": nota_txt,
            },
        }
    except ValueError as exc:
        return {"success": False, "error": str(exc)}
    except Exception as exc:
        conn.rollback()
        return {"success": False, "error": str(exc)}
    finally:
        conn.close()


def eliminar_produccion_agendada(agendado_id):
    conn = get_db()
    cursor = conn.cursor()
    try:
        agendado_id_int = int(agendado_id or 0)
        if agendado_id_int <= 0:
            return {"success": False, "error": "Registro invalido"}

        cursor.execute("SELECT id FROM produccion_agendada WHERE id = ?", (agendado_id_int,))
        existe = cursor.fetchone()
        if not existe:
            return {"success": False, "error": "Registro no encontrado"}

        cursor.execute("DELETE FROM produccion_agendada WHERE id = ?", (agendado_id_int,))
        conn.commit()
        return {"success": True, "eliminado_id": agendado_id_int}
    except Exception as exc:
        conn.rollback()
        return {"success": False, "error": str(exc)}
    finally:
        conn.close()


def obtener_agenda_produccion_semanal(dias=7, fecha_base=None):
    """
    Lista la produccion agendada manual para una ventana semanal simple,
    separada del plan sugerido para evitar mezclar logicas.
    """
    dias_int = max(1, min(int(dias or 7), 60))

    if fecha_base:
        fecha_base_iso = _normalizar_fecha_iso(fecha_base, "fecha base")
        fecha_base_dt = datetime.strptime(fecha_base_iso, "%Y-%m-%d").date()
    else:
        fecha_base_dt = obtener_hora_chile().date()

    fecha_desde = fecha_base_dt
    fecha_hasta = fecha_base_dt + timedelta(days=dias_int - 1)
    dias_semana_es = ["Lun", "Mar", "Mie", "Jue", "Vie", "Sab", "Dom"]

    conn = get_db()
    cursor = conn.cursor()
    try:
        agenda_rows = _obtener_agenda_produccion_rango_cursor(
            cursor,
            fecha_desde.isoformat(),
            fecha_hasta.isoformat(),
        )

        agenda_por_fecha = {}
        total_lotes = 0.0
        for row in agenda_rows:
            fecha_item = str(row["fecha"] or "").strip()
            if not fecha_item:
                continue

            cantidad = float(row["cantidad"] or 0)
            total_lotes += max(0.0, cantidad)
            item = {
                "id": int(row["id"] or 0),
                "fecha": fecha_item,
                "receta_id": int(row["receta_id"] or 0) if row["receta_id"] else None,
                "receta_nombre": row["receta_nombre"] or row["receta_nombre_guardada"] or "Receta",
                "producto_id": int(row["producto_id_resuelto"] or 0) if row["producto_id_resuelto"] else None,
                "producto_nombre": row["producto_nombre"] or row["producto_nombre_guardado"] or "Sin producto asociado",
                "cantidad": cantidad,
                "nota": row["nota"] or "",
            }
            agenda_por_fecha.setdefault(fecha_item, []).append(item)

        days = []
        for offset in range(dias_int):
            fecha_obj = fecha_desde + timedelta(days=offset)
            fecha_iso = fecha_obj.isoformat()
            items = list(agenda_por_fecha.get(fecha_iso, []))
            items.sort(key=lambda x: (str(x.get("receta_nombre") or ""), int(x.get("id") or 0)))

            days.append(
                {
                    "fecha": fecha_iso,
                    "fecha_label": f"{dias_semana_es[fecha_obj.weekday()]} {fecha_obj.strftime('%d/%m')}",
                    "items": items,
                    "total_items": len(items),
                    "total_lotes": round(sum(float(i.get("cantidad") or 0) for i in items), 2),
                }
            )

        return {
            "fecha_desde": fecha_desde.isoformat(),
            "fecha_hasta": fecha_hasta.isoformat(),
            "days": days,
            "resumen": {
                "total_items": len(agenda_rows),
                "total_lotes": round(total_lotes, 2),
                "dias_con_items": sum(1 for d in days if d["total_items"] > 0),
            },
        }
    finally:
        conn.close()


def obtener_plan_produccion_semanal(dias_historial=28, dias_proyeccion=7):
    """
    Genera un plan sugerido por 7 dias (o el rango indicado) basado en:
    - ventas historicas por producto
    - estacionalidad semanal (dia de la semana)
    - eventos de agenda pendientes en los proximos dias
    - porciones disponibles actuales en stock
    - agenda manual de produccion para la semana
    """
    dias_historial = max(7, int(dias_historial or 28))
    dias_proyeccion = max(3, min(int(dias_proyeccion or 7), 21))

    conn = get_db()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT id, nombre, stock, stock_minimo, unidad, porcion_cantidad, porcion_unidad
            FROM productos
            ORDER BY nombre ASC
            """
        )
        productos = [dict(r) for r in cursor.fetchall()]

        cursor.execute(
            """
            SELECT
                vi.producto_id AS producto_id,
                CAST(strftime('%w', v.fecha_hora) AS INTEGER) AS weekday_sql,
                SUM(COALESCE(vi.cantidad, 0)) AS total
            FROM venta_items vi
            JOIN ventas v ON v.id = vi.venta_id
            WHERE datetime(v.fecha_hora) >= datetime('now', ?)
            GROUP BY vi.producto_id, weekday_sql
            """,
            (f"-{dias_historial} days",),
        )
        ventas_rows = cursor.fetchall()
        dias_historial_efectivo = dias_historial
        if not ventas_rows:
            # Fallback: si no hubo ventas recientes, usar una ventana mayor
            # para mantener sugerencias automáticas útiles.
            dias_historial_efectivo = max(dias_historial, 180)
            cursor.execute(
                """
                SELECT
                    vi.producto_id AS producto_id,
                    CAST(strftime('%w', v.fecha_hora) AS INTEGER) AS weekday_sql,
                    SUM(COALESCE(vi.cantidad, 0)) AS total
                FROM venta_items vi
                JOIN ventas v ON v.id = vi.venta_id
                WHERE datetime(v.fecha_hora) >= datetime('now', ?)
                GROUP BY vi.producto_id, weekday_sql
                """,
                (f"-{dias_historial_efectivo} days",),
            )
            ventas_rows = cursor.fetchall()

        demanda_total_por_producto = {}
        demanda_semanal_por_producto = {}
        for row in ventas_rows:
            producto_id = int(row["producto_id"])
            total = float(row["total"] or 0)
            weekday_sql = int(row["weekday_sql"] or 0)
            weekday_py = (weekday_sql + 6) % 7  # sqlite: dom=0 .. sab=6 -> python: lun=0 .. dom=6

            demanda_total_por_producto[producto_id] = demanda_total_por_producto.get(producto_id, 0.0) + total
            if producto_id not in demanda_semanal_por_producto:
                demanda_semanal_por_producto[producto_id] = {}
            demanda_semanal_por_producto[producto_id][weekday_py] = (
                demanda_semanal_por_producto[producto_id].get(weekday_py, 0.0) + total
            )

        cursor.execute(
            """
            SELECT fecha, COUNT(*) AS total
            FROM agenda_eventos
            WHERE estado = 'pendiente'
              AND tipo <> 'bloqueo'
              AND date(fecha) BETWEEN date('now') AND date('now', ?)
            GROUP BY fecha
            """,
            (f"+{dias_proyeccion - 1} days",),
        )
        agenda_por_fecha = {str(r["fecha"]): int(r["total"] or 0) for r in cursor.fetchall()}

        semanas_observadas = max(1, int(math.ceil(dias_historial_efectivo / 7)))
        now_date = obtener_hora_chile().date()
        fecha_hasta_date = now_date + timedelta(days=dias_proyeccion - 1)
        dias_semana_es = ["Lun", "Mar", "Mie", "Jue", "Vie", "Sab", "Dom"]

        agenda_manual_rows = _obtener_agenda_produccion_rango_cursor(
            cursor,
            now_date.isoformat(),
            fecha_hasta_date.isoformat(),
        )
        agenda_manual_por_fecha = {}
        for row in agenda_manual_rows:
            fecha_manual = str(row["fecha"] or "").strip()
            if not fecha_manual:
                continue
            item_manual = {
                "id": int(row["id"] or 0),
                "fecha": fecha_manual,
                "receta_id": int(row["receta_id"] or 0) if row["receta_id"] else None,
                "receta_nombre": row["receta_nombre"] or row["receta_nombre_guardada"] or "Receta",
                "producto_id": int(row["producto_id_resuelto"] or 0) if row["producto_id_resuelto"] else None,
                "producto_nombre": row["producto_nombre"] or row["producto_nombre_guardado"] or "Sin producto asociado",
                "cantidad": float(row["cantidad"] or 0),
                "nota": row["nota"] or "",
            }
            agenda_manual_por_fecha.setdefault(fecha_manual, []).append(item_manual)

        # Estado base para simular cobertura dia a dia.
        productos_meta = {}
        stock_simulado = {}
        for producto in productos:
            producto_id = int(producto["id"])
            info = _calcular_info_porciones_producto_db(producto)
            stock_porciones = int(info.get("porciones_disponibles") or 0) if info.get("success") else 0
            demanda_total = float(demanda_total_por_producto.get(producto_id, 0.0))
            demanda_base = demanda_total / float(dias_historial_efectivo)
            porcion_equivalente = float(info.get("porcion_stock_equivalente") or 0) if info.get("success") else 0.0
            stock_minimo = float(info.get("stock_minimo") or 0)
            if porcion_equivalente > 0 and stock_minimo > 0:
                minimo_porciones = int(math.ceil(stock_minimo / porcion_equivalente))
            else:
                minimo_porciones = 0

            productos_meta[producto_id] = {
                "id": producto_id,
                "nombre": producto.get("nombre") or f"Producto #{producto_id}",
                "demanda_base": demanda_base,
                "demanda_por_dia": demanda_semanal_por_producto.get(producto_id, {}),
                "stock_inicial_porciones": stock_porciones,
                "minimo_porciones": max(0, minimo_porciones),
                "error_porcion": info.get("error") if not info.get("success") else None,
            }
            stock_simulado[producto_id] = stock_porciones

        resultado_dias = []
        total_porciones_sugeridas = 0
        productos_con_sugerencia = set()
        dias_con_movimiento = 0
        total_programado_manual = 0.0
        recetas_programadas = set()

        for offset in range(dias_proyeccion):
            fecha_obj = now_date + timedelta(days=offset)
            fecha_iso = fecha_obj.isoformat()
            weekday_py = fecha_obj.weekday()
            eventos_dia = int(agenda_por_fecha.get(fecha_iso, 0))
            factor_evento = min(1.8, 1 + (0.18 * eventos_dia)) if eventos_dia > 0 else 1.0
            items_dia = []
            agenda_manual_dia = list(agenda_manual_por_fecha.get(fecha_iso, []))

            manual_por_producto = {}
            for manual in agenda_manual_dia:
                cantidad_manual = max(0.0, float(manual.get("cantidad") or 0))
                total_programado_manual += cantidad_manual
                receta_id_manual = int(manual.get("receta_id") or 0)
                if receta_id_manual > 0:
                    recetas_programadas.add(receta_id_manual)
                producto_manual_id = int(manual.get("producto_id") or 0)
                if producto_manual_id > 0 and cantidad_manual > 0:
                    manual_por_producto[producto_manual_id] = manual_por_producto.get(producto_manual_id, 0.0) + cantidad_manual

            for producto_id, meta in productos_meta.items():
                demanda_base = float(meta["demanda_base"] or 0)
                demanda_estacional = float(meta["demanda_por_dia"].get(weekday_py, 0.0)) / float(semanas_observadas)
                demanda_estim = demanda_estacional if demanda_estacional > 0 else demanda_base
                demanda_estim *= factor_evento
                minimo_porciones = int(meta.get("minimo_porciones") or 0)

                demanda_entera = int(math.ceil(max(0.0, demanda_estim)))
                manual_programado = float(manual_por_producto.get(producto_id, 0.0))
                if demanda_entera <= 0 and demanda_base <= 0 and minimo_porciones <= 0 and manual_programado <= 0:
                    continue

                stock_inicio = int(stock_simulado.get(producto_id, 0))
                seguridad = max(1, int(math.ceil(demanda_base))) if demanda_base > 0 else 0
                objetivo_dia = max(minimo_porciones, demanda_entera + seguridad)
                sugerido_base = max(0.0, float(objetivo_dia - stock_inicio))
                sugerido = int(math.ceil(max(0.0, sugerido_base - manual_programado)))
                stock_fin = max(0, int(math.floor(stock_inicio + manual_programado + sugerido - demanda_entera)))
                stock_simulado[producto_id] = stock_fin

                if demanda_entera <= 0 and sugerido <= 0 and manual_programado <= 0:
                    continue

                if sugerido > 0:
                    total_porciones_sugeridas += sugerido
                    productos_con_sugerencia.add(producto_id)

                items_dia.append(
                    {
                        "producto_id": producto_id,
                        "producto_nombre": meta["nombre"],
                        "demanda_estimada": demanda_entera,
                        "sugerido_producir": sugerido,
                        "programado_manual": manual_programado,
                        "stock_inicio_porciones": stock_inicio,
                        "stock_fin_porciones": stock_fin,
                        "error_porcion": meta["error_porcion"],
                    }
                )

            items_dia.sort(
                key=lambda x: (
                    -int(x.get("sugerido_producir") or 0),
                    -int(math.ceil(float(x.get("programado_manual") or 0))),
                    -int(x.get("demanda_estimada") or 0),
                    str(x.get("producto_nombre") or ""),
                )
            )

            if items_dia or agenda_manual_dia:
                dias_con_movimiento += 1

            resultado_dias.append(
                {
                    "fecha": fecha_iso,
                    "fecha_label": f"{dias_semana_es[weekday_py]} {fecha_obj.strftime('%d/%m')}",
                    "eventos_agenda": eventos_dia,
                    "items": items_dia,
                    "manual_items": agenda_manual_dia,
                }
            )

        return {
            "dias_historial": dias_historial,
            "dias_historial_efectivo": dias_historial_efectivo,
            "dias_proyeccion": dias_proyeccion,
            "fecha_generacion": obtener_hora_chile().strftime("%Y-%m-%d %H:%M:%S"),
            "days": resultado_dias,
            "resumen": {
                "productos_considerados": len(productos_meta),
                "productos_con_sugerencia": len(productos_con_sugerencia),
                "total_porciones_sugeridas": int(total_porciones_sugeridas),
                "dias_con_movimiento": dias_con_movimiento,
                "total_programado_manual": round(total_programado_manual, 2),
                "recetas_programadas": len(recetas_programadas),
            },
        }
    finally:
        conn.close()


def eliminar_receta(receta_id):
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        try:
            receta_id = int(receta_id)
        except (TypeError, ValueError):
            return {"success": False, "error": "Receta inválida"}
        if receta_id <= 0:
            return {"success": False, "error": "Receta inválida"}

        cursor.execute("SELECT id, nombre FROM recetas WHERE id = ?", (receta_id,))
        receta = cursor.fetchone()
        if not receta:
            return {"success": False, "error": "Receta no encontrada"}

        cursor.execute("SELECT COUNT(*) AS total FROM producciones WHERE receta_id = ?", (receta_id,))
        en_producciones = int(cursor.fetchone()["total"] or 0)
        if en_producciones > 0:
            return {
                "success": False,
                "error": f"No se puede eliminar la receta: tiene {en_producciones} producción(es) registrada(s).",
            }

        cursor.execute("SELECT COUNT(*) AS total FROM haccp_trazabilidad_insumos WHERE receta_id = ?", (receta_id,))
        en_haccp = int(cursor.fetchone()["total"] or 0)
        if en_haccp > 0:
            return {
                "success": False,
                "error": f"No se puede eliminar la receta: tiene {en_haccp} registro(s) HACCP asociados.",
            }

        cursor.execute("DELETE FROM receta_items WHERE receta_id = ?", (receta_id,))
        cursor.execute("DELETE FROM recetas WHERE id = ?", (receta_id,))
        eliminado = (cursor.rowcount or 0) > 0
        if not eliminado:
            conn.rollback()
            return {"success": False, "error": "Receta no encontrada"}
        conn.commit()
        return {"success": True, "receta_id": receta_id, "receta_nombre": receta["nombre"]}
    except Exception as e:
        conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        conn.close()

def revertir_produccion(produccion_id):
    """Revierte una producción y deshace todos los movimientos asociados."""
    conn = get_db()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT p.id, p.receta_id, p.cantidad, p.cantidad_resultado, p.codigo_operacion, p.metadata_json,
                   r.rendimiento, r.producto_id
            FROM producciones p
            LEFT JOIN recetas r ON r.id = p.receta_id
            WHERE p.id = ?
            """,
            (produccion_id,),
        )
        produccion = cursor.fetchone()
        if not produccion:
            return {"success": False, "error": "Producción no encontrada"}
        codigo_operacion = _normalizar_codigo_operacion(produccion["codigo_operacion"], prefijo="OPP")

        cursor.execute(
            """
            SELECT id, tipo_recurso, recurso_id, lote_id, accion, cantidad, unidad, metadata_json
            FROM produccion_movimientos
            WHERE produccion_id = ?
            ORDER BY id DESC
            """,
            (produccion_id,),
        )
        movimientos = cursor.fetchall()

        # La trazabilidad HACCP guarda referencia al lote/producto generado.
        # Si se revierte una producción, esas filas deben eliminarse en la misma transacción
        # antes de modificar/eliminar lotes para evitar bloqueo por FK.
        cursor.execute(
            "DELETE FROM haccp_trazabilidad_insumos WHERE produccion_id = ?",
            (produccion_id,),
        )

        if not movimientos:
            # Compatibilidad con producciones antiguas (sin trazabilidad en produccion_movimientos).
            cantidad_lotes = int(produccion["cantidad"] or 0)
            cantidad_resultado = float(produccion["cantidad_resultado"] or 0)
            rendimiento = float(produccion["rendimiento"] or 1)
            if cantidad_resultado <= 0 and cantidad_lotes > 0:
                cantidad_resultado = float(cantidad_lotes) * max(rendimiento, 1)

            cursor.execute(
                """
                SELECT tipo, insumo_id, producto_id, cantidad, unidad
                FROM receta_items
                WHERE receta_id = ?
                ORDER BY id DESC
                """,
                (produccion["receta_id"],),
            )
            items = cursor.fetchall()
            if not items:
                # Si no hay receta/items (cambiaron o se eliminaron), al menos limpiar el registro para no bloquear UI.
                cursor.execute("DELETE FROM producciones WHERE id = ?", (produccion_id,))
                conn.commit()
                return {
                    "success": True,
                    "insumos_devueltos": 0,
                    "productos_revertidos": 0,
                    "legacy_sin_trazabilidad": True,
                }

            insumos_devueltos = 0
            productos_revertidos = 0

            # 1) Reponer insumos/productos consumidos por la receta.
            for item in items:
                tipo = (item["tipo"] or "insumo").lower()
                cantidad_item = float(item["cantidad"] or 0) * max(cantidad_lotes, 0)
                unidad_item = item["unidad"] or "unidad"
                if cantidad_item <= 0:
                    continue

                if tipo == "producto":
                    recurso_id = item["producto_id"]
                    if not recurso_id:
                        continue
                    cursor.execute("SELECT fecha_vencimiento, stock FROM productos WHERE id = ?", (recurso_id,))
                    prod_row = cursor.fetchone()
                    if not prod_row:
                        continue
                    stock_anterior = float(prod_row["stock"] or 0)
                    cursor.execute(
                        "INSERT INTO producto_lotes (producto_id, cantidad, fecha_vencimiento) VALUES (?, ?, ?)",
                        (recurso_id, cantidad_item, prod_row["fecha_vencimiento"]),
                    )
                    nuevo_stock = _recalcular_stock_y_vencimiento_producto(cursor, recurso_id)
                    registrar_movimiento_stock(
                        "producto",
                        recurso_id,
                        "reversion_consumo_receta_legacy",
                        cantidad_item,
                        stock_anterior=stock_anterior,
                        stock_nuevo=nuevo_stock,
                        referencia_tipo="produccion_revertida_legacy",
                        referencia_id=produccion_id,
                        origen_modulo="produccion",
                        codigo_operacion=codigo_operacion,
                        unidad="unidad",
                        metadata={"produccion_id": produccion_id, "legacy": True},
                        conn=conn,
                    )
                    productos_revertidos += 1
                else:
                    recurso_id = item["insumo_id"]
                    if not recurso_id:
                        continue
                    ajuste = _sumar_stock_con_conversion_cursor(
                        cursor,
                        recurso_id,
                        cantidad_item,
                        unidad_item,
                    )
                    registrar_movimiento_stock(
                        "insumo",
                        recurso_id,
                        "reversion_produccion_legacy",
                        ajuste["cantidad_stock"],
                        stock_anterior=ajuste["stock_anterior"],
                        stock_nuevo=ajuste["stock_nuevo"],
                        referencia_tipo="produccion_revertida_legacy",
                        referencia_id=produccion_id,
                        origen_modulo="produccion",
                        codigo_operacion=codigo_operacion,
                        unidad=ajuste.get("unidad_stock"),
                        metadata={"produccion_id": produccion_id, "legacy": True},
                        conn=conn,
                    )
                    insumos_devueltos += 1

            # 2) Deshacer producto generado por la producción (si la receta produce producto).
            producto_resultado_id = produccion["producto_id"]
            if producto_resultado_id and cantidad_resultado > 0:
                cursor.execute(
                    "SELECT COALESCE(SUM(cantidad), 0) AS stock_lotes FROM producto_lotes WHERE producto_id = ? AND cantidad > 0",
                    (producto_resultado_id,),
                )
                stock_lotes = float(cursor.fetchone()["stock_lotes"] or 0)
                if stock_lotes < cantidad_resultado:
                    conn.rollback()
                    return {
                        "success": False,
                        "error": "No se puede revertir completamente: parte del producto producido ya no está en stock.",
                    }

                cursor.execute("SELECT stock FROM productos WHERE id = ?", (producto_resultado_id,))
                prod_stock_row = cursor.fetchone()
                stock_anterior_res = float(prod_stock_row["stock"] or 0) if prod_stock_row else 0

                restante = float(cantidad_resultado)
                cursor.execute(
                    """
                    SELECT id, cantidad
                    FROM producto_lotes
                    WHERE producto_id = ? AND cantidad > 0
                    ORDER BY datetime(fecha_ingreso) DESC, id DESC
                    """,
                    (producto_resultado_id,),
                )
                lotes = cursor.fetchall()
                for lote in lotes:
                    if restante <= 0:
                        break
                    disponible = float(lote["cantidad"] or 0)
                    usar = min(disponible, restante)
                    nuevo = disponible - usar
                    if nuevo <= 0:
                        cursor.execute("DELETE FROM producto_lotes WHERE id = ?", (lote["id"],))
                    else:
                        cursor.execute("UPDATE producto_lotes SET cantidad = ? WHERE id = ?", (nuevo, lote["id"]))
                    restante -= usar

                nuevo_stock_res = _recalcular_stock_y_vencimiento_producto(cursor, producto_resultado_id)
                registrar_movimiento_stock(
                    "producto",
                    producto_resultado_id,
                    "reversion_produccion_legacy",
                    cantidad_resultado,
                    stock_anterior=stock_anterior_res,
                    stock_nuevo=nuevo_stock_res,
                    referencia_tipo="produccion_revertida_legacy",
                    referencia_id=produccion_id,
                    origen_modulo="produccion",
                    codigo_operacion=codigo_operacion,
                    unidad="unidad",
                    metadata={"produccion_id": produccion_id, "legacy": True},
                    conn=conn,
                )
                productos_revertidos += 1

            cursor.execute("DELETE FROM producciones WHERE id = ?", (produccion_id,))
            conn.commit()
            return {
                "success": True,
                "insumos_devueltos": insumos_devueltos,
                "productos_revertidos": productos_revertidos,
                "legacy_sin_trazabilidad": True,
            }

        insumos_devueltos = 0
        productos_revertidos = 0

        for mov in movimientos:
            tipo = mov["tipo_recurso"]
            cantidad = float(mov["cantidad"] or 0)

            if tipo == "insumo":
                cursor.execute("SELECT stock FROM insumos WHERE id = ?", (mov["recurso_id"],))
                row = cursor.fetchone()
                if not row:
                    continue
                stock_anterior = float(row["stock"] or 0)
                stock_nuevo = stock_anterior + cantidad
                cursor.execute("UPDATE insumos SET stock = ? WHERE id = ?", (stock_nuevo, mov["recurso_id"]))
                registrar_movimiento_stock(
                    "insumo",
                    mov["recurso_id"],
                    "reversion_produccion",
                    cantidad,
                    stock_anterior=stock_anterior,
                    stock_nuevo=stock_nuevo,
                    referencia_tipo="produccion_revertida",
                    referencia_id=produccion_id,
                    origen_modulo="produccion",
                    codigo_operacion=codigo_operacion,
                    unidad=mov["unidad"] if "unidad" in mov.keys() else None,
                    lote_id=mov["lote_id"] if "lote_id" in mov.keys() else None,
                    metadata={"produccion_id": produccion_id},
                    conn=conn,
                )
                insumos_devueltos += 1
                continue

            if tipo == "producto_lote_consumido":
                if mov["lote_id"]:
                    cursor.execute(
                        "UPDATE producto_lotes SET cantidad = cantidad + ? WHERE id = ?",
                        (cantidad, mov["lote_id"]),
                    )
                nuevo_stock = _recalcular_stock_y_vencimiento_producto(cursor, mov["recurso_id"])
                registrar_movimiento_stock(
                    "producto",
                    mov["recurso_id"],
                    "reversion_consumo_receta",
                    cantidad,
                    stock_nuevo=nuevo_stock,
                    referencia_tipo="produccion_revertida",
                    referencia_id=produccion_id,
                    origen_modulo="produccion",
                    codigo_operacion=codigo_operacion,
                    unidad=(mov["unidad"] if "unidad" in mov.keys() else None) or "unidad",
                    lote_id=mov["lote_id"] if "lote_id" in mov.keys() else None,
                    metadata={"produccion_id": produccion_id},
                    conn=conn,
                )
                productos_revertidos += 1
                continue

            if tipo == "producto_lote_producido":
                if mov["lote_id"]:
                    cursor.execute("SELECT cantidad FROM producto_lotes WHERE id = ?", (mov["lote_id"],))
                    lote = cursor.fetchone()
                    if lote:
                        restante = float(lote["cantidad"] or 0) - cantidad
                        if restante <= 0:
                            cursor.execute("DELETE FROM producto_lotes WHERE id = ?", (mov["lote_id"],))
                        else:
                            cursor.execute("UPDATE producto_lotes SET cantidad = ? WHERE id = ?", (restante, mov["lote_id"]))

                nuevo_stock = _recalcular_stock_y_vencimiento_producto(cursor, mov["recurso_id"])
                registrar_movimiento_stock(
                    "producto",
                    mov["recurso_id"],
                    "reversion_produccion",
                    cantidad,
                    stock_nuevo=nuevo_stock,
                    referencia_tipo="produccion_revertida",
                    referencia_id=produccion_id,
                    origen_modulo="produccion",
                    codigo_operacion=codigo_operacion,
                    unidad=(mov["unidad"] if "unidad" in mov.keys() else None) or "unidad",
                    lote_id=mov["lote_id"] if "lote_id" in mov.keys() else None,
                    metadata={"produccion_id": produccion_id},
                    conn=conn,
                )
                productos_revertidos += 1

        cursor.execute("DELETE FROM produccion_movimientos WHERE produccion_id = ?", (produccion_id,))
        cursor.execute("DELETE FROM producciones WHERE id = ?", (produccion_id,))

        conn.commit()
        return {
            "success": True,
            "insumos_devueltos": insumos_devueltos,
            "productos_revertidos": productos_revertidos,
        }
    except Exception as e:
        conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        conn.close()
        
# Factores de conversión a gramos (para sólidos) o ml (para líquidos)
FACTORES_CONVERSION = {
    # Sólidos - todo se convierte a gramos
    'mg': 0.001,      # miligramos a gramos
    'g': 1,           # gramos
    'gr': 1,          # gramos (alternativo)
    'kg': 1000,       # kilogramos a gramos
    'oz': 28.35,      # onzas a gramos
    'lb': 453.59,     # libras a gramos
    
    # Líquidos - todo se convierte a ml
    'ml': 1,          # mililitros
    'cc': 1,          # centímetros cúbicos = ml
    'lt': 1000,       # litros a ml
    'l': 1000,        # litros (alternativo)
    'taza': 240,      # taza estándar a ml
    'cda': 15,        # cucharada a ml
    'cdt': 5,         # cucharadita a ml
    
    # Unidades sin conversión
    'unidad': 1,
    'pieza': 1,
    'porcion': 1,
    'docena': 12,
}

def convertir_a_base(cantidad, unidad_origen):
    """Convierte cualquier cantidad a su unidad base (g o ml)"""
    if not unidad_origen:
        return cantidad
    
    unidad = unidad_origen.lower().strip()
    factor = FACTORES_CONVERSION.get(unidad, 1)
    return cantidad * factor

def son_unidades_compatibles(unidad1, unidad2):
    """Verifica si dos unidades son del mismo tipo (sólido o líquido)"""
    if not unidad1 or not unidad2:
        return True
    
    solidos = {'mg', 'g', 'gr', 'kg', 'oz', 'lb'}
    liquidos = {'ml', 'cc', 'lt', 'l', 'taza', 'cda', 'cdt'}
    genericos = {'unidad', 'pieza', 'docena', 'porcion'}
    
    u1 = unidad1.lower().strip()
    u2 = unidad2.lower().strip()
    
    # Si alguno es genérico, son compatibles
    if u1 in genericos or u2 in genericos:
        return True
    
    # Ambos sólidos o ambos líquidos
    es_solido1 = u1 in solidos
    es_solido2 = u2 in solidos
    es_liquido1 = u1 in liquidos
    es_liquido2 = u2 in liquidos
    
    return (es_solido1 and es_solido2) or (es_liquido1 and es_liquido2)

def verificar_stock_con_conversion(insumo_id, cantidad_necesaria, unidad_necesaria):
    """Verifica si hay suficiente stock considerando conversiones de unidad"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("SELECT stock, unidad, nombre FROM insumos WHERE id = ?", (insumo_id,))
    insumo = cursor.fetchone()
    conn.close()
    
    if not insumo:
        return {'suficiente': False, 'error': 'Insumo no encontrado'}
    
    stock_actual = insumo['stock']
    unidad_stock = insumo['unidad'] or 'unidad'
    nombre = insumo['nombre']
    
    # Si son la misma unidad, comparación directa
    if unidad_stock.lower().strip() == unidad_necesaria.lower().strip():
        if stock_actual < cantidad_necesaria:
            return {
                'suficiente': False, 
                'error': f"{nombre}: necesita {cantidad_necesaria} {unidad_necesaria}, tiene {stock_actual} {unidad_stock}"
            }
        return {'suficiente': True, 'stock_disponible': stock_actual}
    
    # Verificar si son compatibles
    if not son_unidades_compatibles(unidad_stock, unidad_necesaria):
        return {
            'suficiente': False,
            'error': f"{nombre}: unidades incompatibles ({unidad_stock} vs {unidad_necesaria})"
        }
    
    # Convertir ambos a unidad base
    stock_en_base = convertir_a_base(stock_actual, unidad_stock)
    necesario_en_base = convertir_a_base(cantidad_necesaria, unidad_necesaria)
    
    if stock_en_base < necesario_en_base:
        # Calcular cuánto falta en la unidad original
        falta_en_original = (necesario_en_base - stock_en_base) / FACTORES_CONVERSION.get(unidad_stock.lower().strip(), 1)
        return {
            'suficiente': False,
            'error': f"{nombre}: necesita {cantidad_necesaria} {unidad_necesaria} (~{necesario_en_base:.0f}g/ml), tiene {stock_actual} {unidad_stock} (~{stock_en_base:.0f}g/ml). Falta {falta_en_original:.1f} {unidad_stock}"
        }
    
    return {'suficiente': True, 'stock_disponible': stock_actual}

def descontar_stock_con_conversion(insumo_id, cantidad_necesaria, unidad_necesaria):
    """Descuenta stock considerando conversiones"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("SELECT stock, unidad FROM insumos WHERE id = ?", (insumo_id,))
    insumo = cursor.fetchone()
    
    stock_actual = insumo['stock']
    unidad_stock = insumo['unidad'] or 'unidad'
    
    # Si son la misma unidad, descuento directo
    if unidad_stock.lower().strip() == unidad_necesaria.lower().strip():
        nuevo_stock = stock_actual - cantidad_necesaria
        cursor.execute("UPDATE insumos SET stock = ? WHERE id = ?", (nuevo_stock, insumo_id))
        conn.commit()
        conn.close()
        return {'success': True, 'nuevo_stock': nuevo_stock, 'unidad': unidad_stock}
    
    # Convertir y descontar
    stock_en_base = convertir_a_base(stock_actual, unidad_stock)
    necesario_en_base = convertir_a_base(cantidad_necesaria, unidad_necesaria)
    nuevo_stock_en_base = stock_en_base - necesario_en_base
    
    # Convertir de vuelta a la unidad original del insumo
    nuevo_stock = nuevo_stock_en_base / FACTORES_CONVERSION.get(unidad_stock.lower().strip(), 1)
    
    cursor.execute("UPDATE insumos SET stock = ? WHERE id = ?", (nuevo_stock, insumo_id))
    conn.commit()
    conn.close()
    
    return {'success': True, 'nuevo_stock': nuevo_stock, 'unidad': unidad_stock}
def agregar_lote_producto(producto_id, cantidad, fecha_vencimiento):
    """Agrega un nuevo lote y sincroniza stock/vencimiento del producto."""
    conn = get_db()
    cursor = conn.cursor()
    codigo_operacion = _normalizar_codigo_operacion(prefijo="OPL")

    try:
        cursor.execute("SELECT stock, unidad FROM productos WHERE id = ?", (producto_id,))
        producto = cursor.fetchone()
        if not producto:
            return {'success': False, 'error': 'Producto no encontrado'}

        stock_anterior = float(producto["stock"] or 0)
        cursor.execute(
            """
            INSERT INTO producto_lotes (producto_id, cantidad, fecha_vencimiento)
            VALUES (?, ?, ?)
            """,
            (producto_id, cantidad, fecha_vencimiento),
        )
        lote_id = cursor.lastrowid

        nuevo_stock = _recalcular_stock_y_vencimiento_producto(cursor, producto_id)
        registrar_movimiento_stock(
            "producto",
            producto_id,
            "alta_lote",
            cantidad,
            stock_anterior=stock_anterior,
            stock_nuevo=nuevo_stock,
            referencia_tipo="lote_manual",
            detalle="Alta manual de lote",
            origen_modulo="productos",
            codigo_operacion=codigo_operacion,
            unidad=producto["unidad"] if producto else None,
            lote_id=lote_id,
            metadata={
                "producto_id": producto_id,
                "lote_id": lote_id,
                "fecha_vencimiento": fecha_vencimiento,
            },
            conn=conn,
        )

        conn.commit()
        return {'success': True, 'lote_id': lote_id, "codigo_operacion": codigo_operacion}
    except Exception as e:
        conn.rollback()
        return {'success': False, 'error': str(e)}
    finally:
        conn.close()


def obtener_lotes_por_producto(producto_id):
    """Obtiene lotes ordenados por FEFO (vence primero, luego fecha de ingreso)."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT id, cantidad, fecha_vencimiento, fecha_ingreso 
        FROM producto_lotes
        WHERE producto_id = ? AND cantidad > 0
        ORDER BY COALESCE(fecha_vencimiento, '9999-12-31') ASC, fecha_ingreso ASC, id ASC
    ''', (producto_id,))
    lotes = cursor.fetchall()
    conn.close()
    return lotes

def descontar_stock_fifo(producto_id, cantidad_a_descontar):
    """
    Descuenta stock usando FIFO.
    Retorna: {'success': True, 'detalle': [...]} o {'success': False, 'error': '...'}
    """
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT stock FROM productos WHERE id = ?", (producto_id,))
        producto = cursor.fetchone()
        if not producto:
            return {'success': False, 'error': 'Producto no encontrado'}

        stock_anterior = float(producto['stock'] or 0)
        resultado = _descontar_lotes_fifo_cursor(cursor, producto_id, cantidad_a_descontar)
        if not resultado['success']:
            conn.rollback()
            return {'success': False, 'error': resultado['error']}

        nuevo_stock = _recalcular_stock_y_vencimiento_producto(cursor, producto_id)
        registrar_movimiento_stock(
            "producto",
            producto_id,
            "salida_fifo",
            cantidad_a_descontar,
            stock_anterior=stock_anterior,
            stock_nuevo=nuevo_stock,
            referencia_tipo="fifo",
            conn=conn,
        )

        conn.commit()
        return {'success': True, 'detalle': resultado.get('detalle', [])}

    except Exception as e:
        conn.rollback()
        return {'success': False, 'error': str(e)}
    finally:
        conn.close()


def _cargar_lotes_merma(raw_json):
    if not raw_json:
        return []
    try:
        data = json.loads(raw_json)
    except Exception:
        return []
    return data if isinstance(data, list) else []


def registrar_merma_producto(producto_id, cantidad, motivo, detalle=None):
    conn = get_db()
    cursor = conn.cursor()
    codigo_operacion = _normalizar_codigo_operacion(prefijo="OPM")
    try:
        producto_id = int(producto_id)
        cantidad = float(cantidad or 0)
        if cantidad <= 0:
            return {"success": False, "error": "La cantidad debe ser mayor a 0"}

        motivo = _normalizar_motivo_merma(motivo)
        detalle = (detalle or "").strip() or None

        cursor.execute("SELECT id, nombre, stock FROM productos WHERE id = ?", (producto_id,))
        producto = cursor.fetchone()
        if not producto:
            return {"success": False, "error": "Producto no encontrado"}

        stock_anterior = float(producto["stock"] or 0)
        consumo = _descontar_lotes_fifo_cursor(cursor, producto_id, cantidad)
        if not consumo["success"]:
            conn.rollback()
            return {"success": False, "error": consumo["error"]}

        nuevo_stock = _recalcular_stock_y_vencimiento_producto(cursor, producto_id)
        lotes_json = json.dumps(consumo.get("detalle", []), ensure_ascii=False)

        cursor.execute(
            """
            INSERT INTO producto_mermas (
                producto_id, cantidad, motivo, detalle, lotes_json, stock_anterior, stock_nuevo, codigo_operacion
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                producto_id,
                cantidad,
                motivo,
                detalle,
                lotes_json,
                stock_anterior,
                nuevo_stock,
                codigo_operacion,
            ),
        )
        merma_id = cursor.lastrowid

        movimiento_detalle = motivo if not detalle else f"{motivo}: {detalle}"
        registrar_movimiento_stock(
            "producto",
            producto_id,
            "merma",
            cantidad,
            stock_anterior=stock_anterior,
            stock_nuevo=nuevo_stock,
            referencia_tipo="merma_producto",
            referencia_id=merma_id,
            detalle=movimiento_detalle,
            origen_modulo="mermas",
            codigo_operacion=codigo_operacion,
            unidad="unidad",
            metadata={"merma_id": merma_id, "motivo": motivo},
            conn=conn,
        )

        conn.commit()
        return {
            "success": True,
            "id": merma_id,
            "producto_id": producto_id,
            "producto_nombre": producto["nombre"],
            "cantidad": cantidad,
            "motivo": motivo,
            "stock_nuevo": nuevo_stock,
            "codigo_operacion": codigo_operacion,
        }
    except ValueError as e:
        conn.rollback()
        return {"success": False, "error": str(e)}
    except Exception as e:
        conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        conn.close()


def revertir_merma_producto(merma_id):
    conn = get_db()
    cursor = conn.cursor()
    codigo_operacion_rev = _normalizar_codigo_operacion(prefijo="OPM")
    try:
        cursor.execute(
            """
            SELECT pm.*, p.nombre AS producto_nombre
            FROM producto_mermas pm
            LEFT JOIN productos p ON p.id = pm.producto_id
            WHERE pm.id = ?
            """,
            (merma_id,),
        )
        merma = cursor.fetchone()
        if not merma:
            return {"success": False, "error": "Merma no encontrada"}
        if (merma["estado"] or "activa") != "activa":
            return {"success": False, "error": "Esta merma ya fue revertida"}

        producto_id = int(merma["producto_id"])
        cantidad_original = float(merma["cantidad"] or 0)
        if cantidad_original <= 0:
            return {"success": False, "error": "Cantidad de merma inválida"}

        cursor.execute("SELECT stock, fecha_vencimiento FROM productos WHERE id = ?", (producto_id,))
        producto = cursor.fetchone()
        if not producto:
            return {"success": False, "error": "Producto no encontrado"}

        stock_anterior = float(producto["stock"] or 0)
        lotes = _cargar_lotes_merma(merma["lotes_json"])
        restaurado = 0.0

        for item in lotes:
            cantidad_usada = float(item.get("cantidad_usada") or 0)
            if cantidad_usada <= 0:
                continue

            lote_id = item.get("lote_id")
            fecha_vencimiento = item.get("fecha_vencimiento")

            if lote_id is not None:
                cursor.execute(
                    "SELECT id FROM producto_lotes WHERE id = ? AND producto_id = ?",
                    (lote_id, producto_id),
                )
                lote_actual = cursor.fetchone()
                if lote_actual:
                    cursor.execute(
                        "UPDATE producto_lotes SET cantidad = COALESCE(cantidad, 0) + ? WHERE id = ?",
                        (cantidad_usada, lote_id),
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO producto_lotes (producto_id, cantidad, fecha_vencimiento)
                        VALUES (?, ?, ?)
                        """,
                        (producto_id, cantidad_usada, fecha_vencimiento),
                    )
            else:
                cursor.execute(
                    """
                    INSERT INTO producto_lotes (producto_id, cantidad, fecha_vencimiento)
                    VALUES (?, ?, ?)
                    """,
                    (producto_id, cantidad_usada, fecha_vencimiento),
                )
            restaurado += cantidad_usada

        if restaurado <= 0:
            restaurado = cantidad_original
            cursor.execute(
                """
                INSERT INTO producto_lotes (producto_id, cantidad, fecha_vencimiento)
                VALUES (?, ?, ?)
                """,
                (producto_id, restaurado, producto["fecha_vencimiento"]),
            )

        nuevo_stock = _recalcular_stock_y_vencimiento_producto(cursor, producto_id)
        cursor.execute(
            """
            UPDATE producto_mermas
            SET estado = 'revertida', revertida_en = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (merma_id,),
        )

        registrar_movimiento_stock(
            "producto",
            producto_id,
            "reversion_merma",
            restaurado,
            stock_anterior=stock_anterior,
            stock_nuevo=nuevo_stock,
            referencia_tipo="merma_producto",
            referencia_id=merma_id,
            detalle=f"Reversa merma ({merma['motivo']})",
            origen_modulo="mermas",
            codigo_operacion=codigo_operacion_rev,
            unidad="unidad",
            metadata={
                "merma_id": merma_id,
                "contrapartida_codigo_operacion": (
                    merma["codigo_operacion"] if "codigo_operacion" in merma.keys() else None
                ),
            },
            conn=conn,
        )

        conn.commit()
        return {
            "success": True,
            "id": int(merma_id),
            "producto_id": producto_id,
            "producto_nombre": merma["producto_nombre"] or f"Producto #{producto_id}",
            "cantidad_restaurada": restaurado,
            "stock_nuevo": nuevo_stock,
            "codigo_operacion": codigo_operacion_rev,
            "contrapartida_codigo_operacion": (
                merma["codigo_operacion"] if "codigo_operacion" in merma.keys() else None
            ),
        }
    except Exception as e:
        conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        conn.close()


def eliminar_lote(lote_id):
    """Elimina un lote específico y actualiza el stock del producto."""
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT producto_id, cantidad FROM producto_lotes WHERE id = ?", (lote_id,))
        lote = cursor.fetchone()

        if not lote:
            return {"success": False, "error": "Lote no encontrado"}

        producto_id = int(lote["producto_id"])
        cantidad_lote = float(lote["cantidad"] or 0)
        cursor.execute("SELECT stock FROM productos WHERE id = ?", (producto_id,))
        prod = cursor.fetchone()
        stock_anterior = float(prod["stock"] or 0) if prod else 0

        cursor.execute("DELETE FROM producto_lotes WHERE id = ?", (lote_id,))
        nuevo_stock = _recalcular_stock_y_vencimiento_producto(cursor, producto_id)
        registrar_movimiento_stock(
            "producto",
            producto_id,
            "eliminar_lote",
            cantidad_lote,
            stock_anterior=stock_anterior,
            stock_nuevo=nuevo_stock,
            referencia_tipo="lote_manual",
            detalle=f"Eliminación de lote {lote_id}",
            conn=conn,
        )

        conn.commit()
        return {"success": True}
    except Exception as e:
        conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        conn.close()

def calcular_dias_restantes(fecha_vencimiento_str):
    """Calcula días restantes hasta el vencimiento"""
    from datetime import datetime
    try:
        fecha_venc = datetime.strptime(fecha_vencimiento_str, '%Y-%m-%d')
        hoy = datetime.now()
        dias = (fecha_venc - hoy).days
        return dias
    except Exception:
        return None

def obtener_estado_lote(dias_restantes):
    """Retorna el estado y color según los días restantes."""
    if dias_restantes is None:
        return {"estado": "Desconocido", "color": "gray", "emoji": "o"}
    elif dias_restantes < 0:
        return {"estado": "Vencido", "color": "black", "emoji": "x"}
    elif dias_restantes <= 2:
        return {"estado": "Urgente", "color": "red", "emoji": "!"}
    elif dias_restantes <= 5:
        return {"estado": "Proximo", "color": "yellow", "emoji": "~"}
    else:
        return {"estado": "OK", "color": "green", "emoji": "+"}

def calcular_precio_unitario_base(insumo_id):
    """
    Calcula el precio por unidad base (gramo, ml, unidad) incluyendo IVA
    Retorna: {'precio_base': float, 'unidad_base': str} o None si no tiene precio
    """
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            SELECT precio_unitario, cantidad_comprada, unidad_compra, 
                   unidad, precio_incluye_iva
            FROM insumos WHERE id = ?
        ''', (insumo_id,))
        insumo = cursor.fetchone()
        
        if not insumo or not insumo['precio_unitario'] or insumo['cantidad_comprada'] <= 0:
            return None
        
        # Calcular precio con IVA
        precio = insumo['precio_unitario']
        if not insumo['precio_incluye_iva']:
            precio = precio * 1.19  # Agregar IVA 19%
        
        # Determinar unidad base
        unidad_compra = (insumo['unidad_compra'] or 'unidad').lower().strip()
        unidad_stock = (insumo['unidad'] or 'unidad').lower().strip()
        
        # Convertir a unidad base
        FACTORES = {
            'mg': 0.001, 'g': 1, 'gr': 1, 'kg': 1000,
            'ml': 1, 'cc': 1, 'lt': 1000, 'l': 1000,
            'unidad': 1, 'pieza': 1, 'caja': None  # caja se maneja especial
        }
        
        factor_compra = FACTORES.get(unidad_compra, 1)
        
        # Si es caja, usamos la cantidad_comprada como unidades totales
        if unidad_compra == 'caja':
            total_unidades = insumo['cantidad_comprada']
        else:
            total_unidades = insumo['cantidad_comprada'] * factor_compra
        
        precio_base = precio / total_unidades if total_unidades > 0 else 0
        
        # Determinar unidad base de retorno
        if unidad_compra in ['mg', 'g', 'gr', 'kg']:
            unidad_base = 'g'
        elif unidad_compra in ['ml', 'cc', 'lt', 'l']:
            unidad_base = 'ml'
        else:
            unidad_base = 'unidad'
        
        return {
            'precio_base': round(precio_base, 4),
            'unidad_base': unidad_base,
            'precio_total_con_iva': round(precio, 2)
        }
        
    except Exception as e:
        print(f"Error calculando precio base: {e}")
        return None
       
def migrar_db():
    """Aplica migraciones idempotentes para mantener compatibilidad de bases existentes."""
    conn = get_db()
    try:
        conn.execute("PRAGMA foreign_keys=OFF")

        # Columnas en insumos
        _ensure_column(conn, "insumos", "precio_unitario", "REAL DEFAULT 0")
        _ensure_column(conn, "insumos", "cantidad_comprada", "REAL DEFAULT 1")
        _ensure_column(conn, "insumos", "unidad_compra", "TEXT DEFAULT 'unidad'")
        _ensure_column(conn, "insumos", "precio_incluye_iva", "INTEGER DEFAULT 1")
        _ensure_column(conn, "insumos", "cantidad_por_scan", "REAL DEFAULT 1")
        _ensure_column(conn, "insumos", "unidad_por_scan", "TEXT DEFAULT NULL")
        _ensure_column(conn, "insumos", "nutricion_ref_cantidad", "REAL DEFAULT 100")
        _ensure_column(conn, "insumos", "nutricion_ref_unidad", "TEXT DEFAULT NULL")
        _ensure_column(conn, "insumos", "nutricion_kcal", "REAL DEFAULT NULL")
        _ensure_column(conn, "insumos", "nutricion_proteinas_g", "REAL DEFAULT NULL")
        _ensure_column(conn, "insumos", "nutricion_carbohidratos_g", "REAL DEFAULT NULL")
        _ensure_column(conn, "insumos", "nutricion_grasas_g", "REAL DEFAULT NULL")
        _ensure_column(conn, "insumos", "nutricion_azucares_g", "REAL DEFAULT NULL")
        _ensure_column(conn, "insumos", "nutricion_sodio_mg", "REAL DEFAULT NULL")
        conn.execute(
            """
            UPDATE insumos
            SET nutricion_ref_cantidad = CASE
                WHEN nutricion_ref_cantidad IS NULL OR nutricion_ref_cantidad <= 0 THEN 100
                ELSE nutricion_ref_cantidad
            END
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS insumo_codigos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                insumo_id INTEGER NOT NULL,
                codigo_barra TEXT NOT NULL UNIQUE,
                creado TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (insumo_id) REFERENCES insumos(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO insumo_codigos (insumo_id, codigo_barra)
            SELECT z.id, z.codigo
            FROM (
                SELECT MIN(id) AS id, TRIM(codigo_barra) AS codigo
                FROM insumos
                WHERE codigo_barra IS NOT NULL
                  AND TRIM(codigo_barra) <> ''
                GROUP BY TRIM(codigo_barra)
            ) z
            WHERE z.codigo IS NOT NULL AND z.codigo <> ''
            """
        )

        # Columnas en productos/recetas/producciones
        _ensure_column(conn, "productos", "foto", "TEXT")
        _ensure_column(conn, "productos", "precio", "REAL DEFAULT 0")
        _ensure_column(conn, "productos", "vida_util_dias", "INTEGER DEFAULT 0")
        _ensure_column(conn, "productos", "icono", "TEXT DEFAULT 'cupcake'")
        _ensure_column(conn, "productos", "porcion_cantidad", "REAL DEFAULT 1")
        _ensure_column(conn, "productos", "porcion_unidad", "TEXT DEFAULT 'unidad'")
        _ensure_column(conn, "productos", "stock_dependencia_tipo", "TEXT")
        _ensure_column(conn, "productos", "stock_dependencia_id", "INTEGER")
        _ensure_column(conn, "productos", "stock_dependencia_cantidad", "REAL DEFAULT 1")
        _ensure_column(conn, "productos", "categoria_tienda", "TEXT DEFAULT 'General'")
        _ensure_column(conn, "productos", "descripcion_tienda", "TEXT DEFAULT ''")
        _ensure_column(conn, "productos", "descuento_tienda_pct", "REAL DEFAULT 0")
        _ensure_column(conn, "productos", "foto_fit_tienda", "TEXT DEFAULT 'cover'")
        _ensure_column(conn, "productos", "foto_pos_tienda", "TEXT DEFAULT 'center'")
        _ensure_column(conn, "productos", "foto_pos_x_tienda", "REAL DEFAULT 50")
        _ensure_column(conn, "productos", "foto_pos_y_tienda", "REAL DEFAULT 50")
        _ensure_column(conn, "productos", "foto_zoom_tienda", "REAL DEFAULT 100")
        _ensure_column(conn, "productos", "destacado_tienda", "INTEGER DEFAULT 0")
        _ensure_column(conn, "productos", "orden_tienda", "INTEGER DEFAULT 0")
        _ensure_column(conn, "productos", "activo_tienda", "INTEGER DEFAULT 1")
        _ensure_column(conn, "agenda_eventos", "codigo_operacion", "TEXT")
        _ensure_column(conn, "ventas", "codigo_pedido", "TEXT")
        _ensure_column(conn, "ventas", "canal_venta", "TEXT DEFAULT 'presencial'")
        _ensure_column(conn, "ventas", "codigo_operacion", "TEXT")
        _ensure_column(conn, "ventas", "total_monto", "REAL DEFAULT 0")
        _ensure_column(conn, "ventas", "cliente_nombre", "TEXT")
        _ensure_column(conn, "ventas", "cliente_email", "TEXT")
        _ensure_column(conn, "ventas", "cliente_telefono", "TEXT")
        _ensure_column(conn, "ventas", "descuento_codigo", "TEXT")
        _ensure_column(conn, "ventas", "descuento_monto", "REAL DEFAULT 0")
        _ensure_column(conn, "recetas", "rendimiento", "REAL DEFAULT 1")
        _ensure_column(conn, "producciones", "cantidad_resultado", "REAL DEFAULT 0")
        _ensure_column(conn, "producciones", "metadata_json", "TEXT")
        _ensure_column(conn, "producciones", "codigo_operacion", "TEXT")
        conn.execute("UPDATE agenda_eventos SET codigo_operacion = COALESCE(NULLIF(codigo_operacion, ''), 'OPA-LEGACY-' || id)")
        conn.execute("UPDATE ventas SET canal_venta = COALESCE(NULLIF(TRIM(canal_venta), ''), 'presencial')")
        conn.execute("UPDATE ventas SET codigo_operacion = COALESCE(NULLIF(codigo_operacion, ''), 'OPV-LEGACY-' || id)")
        conn.execute("UPDATE producciones SET codigo_operacion = COALESCE(NULLIF(codigo_operacion, ''), 'OPP-LEGACY-' || id)")
        conn.execute(
            """
            UPDATE productos
            SET porcion_cantidad = CASE
                WHEN porcion_cantidad IS NULL OR porcion_cantidad <= 0 THEN 1
                ELSE porcion_cantidad
            END
            """
        )
        conn.execute(
            """
            UPDATE productos
            SET porcion_unidad = COALESCE(NULLIF(TRIM(porcion_unidad), ''), COALESCE(NULLIF(TRIM(unidad), ''), 'unidad'))
            """
        )
        conn.execute(
            """
            UPDATE productos
            SET icono = COALESCE(NULLIF(TRIM(icono), ''), 'cupcake')
            """
        )
        conn.execute(
            """
            UPDATE productos
            SET stock_dependencia_tipo = NULL
            WHERE stock_dependencia_tipo IS NOT NULL
              AND TRIM(stock_dependencia_tipo) <> ''
              AND LOWER(TRIM(stock_dependencia_tipo)) NOT IN ('producto', 'insumo')
            """
        )
        conn.execute(
            """
            UPDATE productos
            SET stock_dependencia_tipo = LOWER(TRIM(stock_dependencia_tipo))
            WHERE stock_dependencia_tipo IS NOT NULL
              AND TRIM(stock_dependencia_tipo) <> ''
            """
        )
        conn.execute(
            """
            UPDATE productos
            SET stock_dependencia_cantidad = 1
            WHERE stock_dependencia_cantidad IS NULL OR stock_dependencia_cantidad <= 0
            """
        )
        conn.execute(
            """
            UPDATE productos
            SET stock_dependencia_tipo = NULL,
                stock_dependencia_id = NULL,
                stock_dependencia_cantidad = 1
            WHERE stock_dependencia_tipo IS NULL
            """
        )
        conn.execute(
            """
            UPDATE productos
            SET categoria_tienda = COALESCE(NULLIF(TRIM(categoria_tienda), ''), 'General')
            """
        )
        conn.execute(
            """
            UPDATE productos
            SET descripcion_tienda = COALESCE(descripcion_tienda, '')
            """
        )
        conn.execute(
            """
            UPDATE productos
            SET descuento_tienda_pct = 0
            WHERE descuento_tienda_pct IS NULL OR descuento_tienda_pct < 0 OR descuento_tienda_pct > 100
            """
        )
        conn.execute(
            """
            UPDATE productos
            SET foto_fit_tienda = CASE
                WHEN LOWER(TRIM(COALESCE(foto_fit_tienda, ''))) IN ('cover', 'contain')
                THEN LOWER(TRIM(foto_fit_tienda))
                ELSE 'cover'
            END
            """
        )
        conn.execute(
            """
            UPDATE productos
            SET foto_pos_tienda = CASE
                WHEN LOWER(TRIM(COALESCE(foto_pos_tienda, ''))) IN ('center', 'top', 'bottom')
                THEN LOWER(TRIM(foto_pos_tienda))
                ELSE 'center'
            END
            """
        )
        conn.execute(
            """
            UPDATE productos
            SET foto_pos_x_tienda = 50
            WHERE foto_pos_x_tienda IS NULL OR foto_pos_x_tienda < 0 OR foto_pos_x_tienda > 100
            """
        )
        conn.execute(
            """
            UPDATE productos
            SET foto_pos_y_tienda = 50
            WHERE foto_pos_y_tienda IS NULL OR foto_pos_y_tienda < 0 OR foto_pos_y_tienda > 100
            """
        )
        conn.execute(
            """
            UPDATE productos
            SET foto_zoom_tienda = 100
            WHERE foto_zoom_tienda IS NULL OR foto_zoom_tienda < 50 OR foto_zoom_tienda > 220
            """
        )
        conn.execute(
            """
            UPDATE productos
            SET destacado_tienda = CASE
                WHEN destacado_tienda IS NULL OR destacado_tienda = 0 THEN 0
                ELSE 1
            END
            """
        )
        conn.execute(
            """
            UPDATE productos
            SET orden_tienda = CASE
                WHEN orden_tienda IS NULL OR orden_tienda < 0 THEN 0
                ELSE orden_tienda
            END
            """
        )
        conn.execute(
            """
            UPDATE productos
            SET activo_tienda = CASE
                WHEN activo_tienda IS NULL OR activo_tienda = 0 THEN 0
                ELSE 1
            END
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tienda_categorias (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT NOT NULL UNIQUE,
                activo INTEGER NOT NULL DEFAULT 1,
                orden INTEGER NOT NULL DEFAULT 0,
                descuento_pct REAL NOT NULL DEFAULT 0,
                horario_habilitado INTEGER NOT NULL DEFAULT 0,
                dias_semana TEXT DEFAULT '',
                hora_inicio TEXT,
                hora_fin TEXT,
                creado_en TEXT DEFAULT CURRENT_TIMESTAMP,
                actualizado_en TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO tienda_categorias (nombre, activo, orden, descuento_pct, horario_habilitado, dias_semana, hora_inicio, hora_fin)
            VALUES ('General', 1, 0, 0, 0, '', NULL, NULL)
            """
        )
        conn.execute(
            """
            UPDATE tienda_categorias
            SET activo = CASE WHEN activo IS NULL OR activo = 0 THEN 0 ELSE 1 END,
                orden = CASE WHEN orden IS NULL OR orden < 0 THEN 0 ELSE orden END,
                descuento_pct = CASE
                    WHEN descuento_pct IS NULL OR descuento_pct < 0 THEN 0
                    WHEN descuento_pct > 100 THEN 100
                    ELSE descuento_pct
                END,
                horario_habilitado = CASE WHEN horario_habilitado IS NULL OR horario_habilitado = 0 THEN 0 ELSE 1 END,
                dias_semana = COALESCE(TRIM(dias_semana), '')
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tienda_cupones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                codigo TEXT NOT NULL UNIQUE,
                nombre TEXT NOT NULL DEFAULT '',
                tipo_descuento TEXT NOT NULL DEFAULT 'porcentaje',
                valor_descuento REAL NOT NULL DEFAULT 0,
                activo INTEGER NOT NULL DEFAULT 1,
                fecha_inicio TEXT,
                fecha_fin TEXT,
                hora_inicio TEXT,
                hora_fin TEXT,
                usos_max_total INTEGER,
                usos_max_por_cliente INTEGER,
                monto_minimo REAL NOT NULL DEFAULT 0,
                solo_sin_oferta INTEGER NOT NULL DEFAULT 0,
                creado_en TEXT DEFAULT CURRENT_TIMESTAMP,
                actualizado_en TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tienda_cupon_usos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cupon_id INTEGER NOT NULL,
                venta_id INTEGER NOT NULL,
                cliente_ref TEXT,
                descuento_aplicado REAL NOT NULL DEFAULT 0,
                creado_en TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(cupon_id) REFERENCES tienda_cupones(id),
                FOREIGN KEY(venta_id) REFERENCES ventas(id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tienda_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                modo_manual TEXT NOT NULL DEFAULT 'auto',
                horario_habilitado INTEGER NOT NULL DEFAULT 0,
                hora_apertura TEXT NOT NULL DEFAULT '09:00',
                hora_cierre TEXT NOT NULL DEFAULT '19:00',
                actualizado_en TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO tienda_config (id, modo_manual, horario_habilitado, hora_apertura, hora_cierre)
            VALUES (1, 'auto', 0, '09:00', '19:00')
            """
        )
        conn.execute(
            """
            UPDATE tienda_config
            SET modo_manual = CASE
                WHEN LOWER(TRIM(COALESCE(modo_manual, ''))) IN ('auto', 'abierta', 'cerrada')
                THEN LOWER(TRIM(modo_manual))
                ELSE 'auto'
            END
            WHERE id = 1
            """
        )
        conn.execute(
            """
            UPDATE tienda_config
            SET horario_habilitado = CASE
                WHEN horario_habilitado IS NULL OR horario_habilitado = 0 THEN 0
                ELSE 1
            END
            WHERE id = 1
            """
        )
        conn.execute(
            """
            UPDATE tienda_config
            SET hora_apertura = COALESCE(NULLIF(TRIM(hora_apertura), ''), '09:00'),
                hora_cierre = COALESCE(NULLIF(TRIM(hora_cierre), ''), '19:00')
            WHERE id = 1
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tienda_visitas (
                session_id TEXT PRIMARY KEY,
                primera_visita TEXT DEFAULT CURRENT_TIMESTAMP,
                ultima_actividad TEXT DEFAULT CURRENT_TIMESTAMP,
                pagina TEXT DEFAULT '/tienda',
                carrito_items INTEGER NOT NULL DEFAULT 0,
                carrito_total REAL NOT NULL DEFAULT 0,
                checkouts INTEGER NOT NULL DEFAULT 0,
                ultimo_checkout TEXT,
                user_agent TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tienda_visitas_actividad
            ON tienda_visitas(ultima_actividad)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tienda_visitas_carrito
            ON tienda_visitas(carrito_items, ultima_actividad)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tienda_cupon_usos_cupon_cliente
            ON tienda_cupon_usos(cupon_id, cliente_ref)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_ventas_canal_id
            ON ventas(canal_venta, id)
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS producto_insumos_venta (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                producto_id INTEGER NOT NULL,
                insumo_id INTEGER NOT NULL,
                cantidad REAL NOT NULL,
                unidad TEXT DEFAULT 'unidad',
                creado TEXT DEFAULT CURRENT_TIMESTAMP,
                actualizado TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(producto_id, insumo_id),
                FOREIGN KEY (producto_id) REFERENCES productos(id) ON DELETE CASCADE,
                FOREIGN KEY (insumo_id) REFERENCES insumos(id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS producto_productos_venta (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                producto_id INTEGER NOT NULL,
                producto_asociado_id INTEGER NOT NULL,
                cantidad REAL NOT NULL DEFAULT 1,
                creado TEXT DEFAULT CURRENT_TIMESTAMP,
                actualizado TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(producto_id, producto_asociado_id),
                FOREIGN KEY (producto_id) REFERENCES productos(id) ON DELETE CASCADE,
                FOREIGN KEY (producto_asociado_id) REFERENCES productos(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS producto_desactivaciones_manuales (
                producto_id INTEGER PRIMARY KEY,
                confirmado_en TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (producto_id) REFERENCES productos(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS venta_insumos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                venta_id INTEGER NOT NULL,
                producto_id INTEGER NOT NULL,
                insumo_id INTEGER NOT NULL,
                cantidad_por_producto REAL NOT NULL,
                unidad_asociada TEXT DEFAULT 'unidad',
                cantidad_total_asociada REAL NOT NULL,
                cantidad_descontada_stock REAL NOT NULL,
                unidad_stock TEXT DEFAULT 'unidad',
                FOREIGN KEY (venta_id) REFERENCES ventas(id) ON DELETE CASCADE,
                FOREIGN KEY (producto_id) REFERENCES productos(id),
                FOREIGN KEY (insumo_id) REFERENCES insumos(id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS insumo_lotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                insumo_id INTEGER NOT NULL,
                lote_codigo TEXT,
                fecha_elaboracion TEXT,
                fecha_vencimiento TEXT,
                cantidad REAL NOT NULL,
                fecha_ingreso TEXT DEFAULT CURRENT_TIMESTAMP,
                actualizado TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (insumo_id) REFERENCES insumos(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS venta_insumo_lotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                venta_id INTEGER NOT NULL,
                producto_id INTEGER NOT NULL,
                insumo_id INTEGER NOT NULL,
                insumo_lote_id INTEGER,
                cantidad_usada_stock REAL NOT NULL,
                unidad_stock TEXT DEFAULT 'unidad',
                lote_codigo TEXT,
                fecha_elaboracion TEXT,
                fecha_vencimiento TEXT,
                fecha_ingreso TEXT,
                creado TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (venta_id) REFERENCES ventas(id) ON DELETE CASCADE,
                FOREIGN KEY (producto_id) REFERENCES productos(id),
                FOREIGN KEY (insumo_id) REFERENCES insumos(id),
                FOREIGN KEY (insumo_lote_id) REFERENCES insumo_lotes(id)
            )
            """
        )
        _ensure_column(conn, "venta_insumo_lotes", "fecha_ingreso", "TEXT")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS produccion_agendada (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha TEXT NOT NULL,
                receta_id INTEGER,
                receta_nombre TEXT,
                producto_id INTEGER,
                producto_nombre TEXT,
                cantidad REAL NOT NULL DEFAULT 1,
                nota TEXT,
                estado TEXT DEFAULT 'pendiente',
                creado TEXT DEFAULT CURRENT_TIMESTAMP,
                actualizado TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (receta_id) REFERENCES recetas(id) ON DELETE SET NULL,
                FOREIGN KEY (producto_id) REFERENCES productos(id) ON DELETE SET NULL
            )
            """
        )
        _ensure_column(conn, "produccion_agendada", "receta_nombre", "TEXT")
        _ensure_column(conn, "produccion_agendada", "producto_id", "INTEGER")
        _ensure_column(conn, "produccion_agendada", "producto_nombre", "TEXT")
        _ensure_column(conn, "produccion_agendada", "cantidad", "REAL DEFAULT 1")
        _ensure_column(conn, "produccion_agendada", "nota", "TEXT")
        _ensure_column(conn, "produccion_agendada", "estado", "TEXT DEFAULT 'pendiente'")
        _ensure_column(conn, "produccion_agendada", "actualizado", "TEXT DEFAULT CURRENT_TIMESTAMP")
        conn.execute(
            """
            UPDATE produccion_agendada
            SET estado = CASE
                WHEN estado IS NULL OR TRIM(estado) = '' THEN 'pendiente'
                ELSE LOWER(TRIM(estado))
            END
            """
        )
        conn.execute(
            """
            UPDATE produccion_agendada
            SET cantidad = 1
            WHERE cantidad IS NULL OR cantidad <= 0
            """
        )
        conn.execute(
            """
            INSERT INTO insumo_lotes (insumo_id, cantidad)
            SELECT i.id, i.stock
            FROM insumos i
            WHERE COALESCE(i.stock, 0) > 0
              AND NOT EXISTS (
                  SELECT 1
                  FROM insumo_lotes il
                  WHERE il.insumo_id = i.id
              )
            """
        )

        # Migración estructural receta_items (insumo/producto)
        conn.execute("CREATE TABLE IF NOT EXISTS _migracion_flags (clave TEXT PRIMARY KEY, valor TEXT)")
        cur = conn.execute("SELECT valor FROM _migracion_flags WHERE clave = 'receta_items_v2'")
        flag = cur.fetchone()

        if not flag:
            cols = _table_columns(conn, "receta_items") if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='receta_items'").fetchone() else set()
            necesita_recrear = True
            if cols and {"tipo", "producto_id", "insumo_id", "cantidad", "unidad", "receta_id"}.issubset(cols):
                necesita_recrear = False

            if necesita_recrear:
                conn.execute("ALTER TABLE receta_items RENAME TO receta_items_old")
                conn.execute(
                    """
                    CREATE TABLE receta_items (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        receta_id INTEGER NOT NULL,
                        tipo TEXT DEFAULT 'insumo',
                        insumo_id INTEGER,
                        producto_id INTEGER,
                        cantidad REAL NOT NULL,
                        unidad TEXT DEFAULT 'unidad',
                        FOREIGN KEY (receta_id) REFERENCES recetas(id) ON DELETE CASCADE,
                        FOREIGN KEY (insumo_id) REFERENCES insumos(id),
                        FOREIGN KEY (producto_id) REFERENCES productos(id)
                    )
                    """
                )

                old_cols = _table_columns(conn, "receta_items_old")
                if "tipo" in old_cols and "producto_id" in old_cols:
                    conn.execute(
                        """
                        INSERT INTO receta_items (id, receta_id, tipo, insumo_id, producto_id, cantidad, unidad)
                        SELECT id, receta_id, COALESCE(tipo, 'insumo'), insumo_id, producto_id, cantidad, COALESCE(unidad, 'unidad')
                        FROM receta_items_old
                        """
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO receta_items (id, receta_id, tipo, insumo_id, producto_id, cantidad, unidad)
                        SELECT id, receta_id, 'insumo', insumo_id, NULL, cantidad, COALESCE(unidad, 'unidad')
                        FROM receta_items_old
                        """
                    )
                conn.execute("DROP TABLE receta_items_old")

            conn.execute("INSERT OR REPLACE INTO _migracion_flags (clave, valor) VALUES ('receta_items_v2', 'ok')")

        # Tablas auxiliares
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS produccion_movimientos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                produccion_id INTEGER NOT NULL,
                tipo_recurso TEXT NOT NULL,
                recurso_id INTEGER,
                lote_id INTEGER,
                accion TEXT NOT NULL,
                cantidad REAL NOT NULL,
                unidad TEXT,
                metadata_json TEXT,
                creado TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (produccion_id) REFERENCES producciones(id) ON DELETE CASCADE
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_movimientos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tipo_recurso TEXT NOT NULL,
                recurso_id INTEGER NOT NULL,
                accion TEXT NOT NULL,
                cantidad REAL NOT NULL,
                stock_anterior REAL,
                stock_nuevo REAL,
                referencia_tipo TEXT,
                referencia_id INTEGER,
                detalle TEXT,
                creado TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha TEXT DEFAULT CURRENT_TIMESTAMP,
                tipo_operacion TEXT NOT NULL,
                recurso_tipo TEXT NOT NULL,
                recurso_id INTEGER NOT NULL,
                cantidad_delta REAL NOT NULL,
                stock_anterior REAL,
                stock_nuevo REAL,
                unidad TEXT,
                lote_id INTEGER,
                origen_modulo TEXT,
                codigo_operacion TEXT NOT NULL,
                usuario TEXT,
                metadata_json TEXT,
                referencia_tipo TEXT,
                referencia_id INTEGER,
                detalle TEXT,
                legacy_movimiento_id INTEGER UNIQUE,
                creado TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS historial_cambios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recurso_tipo TEXT NOT NULL,
                recurso_id INTEGER,
                recurso_nombre TEXT NOT NULL,
                accion TEXT NOT NULL,
                detalle TEXT,
                origen_modulo TEXT,
                metadata_json TEXT,
                fecha_hora TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        _ensure_column(conn, "stock_movimientos", "tipo_operacion", "TEXT")
        _ensure_column(conn, "stock_movimientos", "recurso_tipo", "TEXT")
        _ensure_column(conn, "stock_movimientos", "cantidad_delta", "REAL")
        _ensure_column(conn, "stock_movimientos", "unidad", "TEXT")
        _ensure_column(conn, "stock_movimientos", "lote_id", "INTEGER")
        _ensure_column(conn, "stock_movimientos", "origen_modulo", "TEXT")
        _ensure_column(conn, "stock_movimientos", "codigo_operacion", "TEXT")
        _ensure_column(conn, "stock_movimientos", "usuario", "TEXT")
        _ensure_column(conn, "stock_movimientos", "metadata_json", "TEXT")
        _ensure_column(conn, "stock_movimientos", "fecha", "TEXT")
        _ensure_column(conn, "stock_ledger", "stock_anterior", "REAL")
        _ensure_column(conn, "stock_ledger", "stock_nuevo", "REAL")
        _ensure_column(conn, "stock_ledger", "legacy_movimiento_id", "INTEGER")
        _ensure_column(conn, "productos", "eliminado", "INTEGER DEFAULT 0")
        conn.execute("UPDATE productos SET eliminado = COALESCE(eliminado, 0)")
        conn.execute("UPDATE stock_movimientos SET tipo_operacion = COALESCE(NULLIF(tipo_operacion, ''), accion)")
        conn.execute("UPDATE stock_movimientos SET recurso_tipo = COALESCE(NULLIF(recurso_tipo, ''), tipo_recurso)")
        conn.execute("UPDATE stock_movimientos SET cantidad_delta = COALESCE(cantidad_delta, cantidad)")
        conn.execute("UPDATE stock_movimientos SET fecha = COALESCE(NULLIF(fecha, ''), creado)")
        conn.execute("UPDATE stock_movimientos SET origen_modulo = COALESCE(NULLIF(origen_modulo, ''), referencia_tipo, 'legacy')")
        conn.execute("UPDATE stock_movimientos SET codigo_operacion = COALESCE(NULLIF(codigo_operacion, ''), 'LEGACY-' || id)")
        conn.execute(
            """
            INSERT INTO stock_ledger (
                fecha, tipo_operacion, recurso_tipo, recurso_id, cantidad_delta,
                stock_anterior, stock_nuevo, unidad, lote_id, origen_modulo, codigo_operacion, usuario,
                metadata_json, referencia_tipo, referencia_id, detalle,
                legacy_movimiento_id, creado
            )
            SELECT
                COALESCE(NULLIF(sm.fecha, ''), sm.creado),
                COALESCE(NULLIF(sm.tipo_operacion, ''), sm.accion),
                COALESCE(NULLIF(sm.recurso_tipo, ''), sm.tipo_recurso),
                sm.recurso_id,
                COALESCE(sm.cantidad_delta, sm.cantidad),
                sm.stock_anterior,
                sm.stock_nuevo,
                sm.unidad,
                sm.lote_id,
                COALESCE(NULLIF(sm.origen_modulo, ''), sm.referencia_tipo, 'legacy'),
                COALESCE(NULLIF(sm.codigo_operacion, ''), 'LEGACY-' || sm.id),
                sm.usuario,
                sm.metadata_json,
                sm.referencia_tipo,
                sm.referencia_id,
                sm.detalle,
                sm.id,
                sm.creado
            FROM stock_movimientos sm
            LEFT JOIN stock_ledger sl
                ON sl.legacy_movimiento_id = sm.id
            WHERE sl.id IS NULL
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alertas_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                notificaciones_activas INTEGER DEFAULT 1,
                repetir_minutos INTEGER DEFAULT 15,
                dias_anticipacion INTEGER DEFAULT 2,
                incluir_stock_bajo INTEGER DEFAULT 1,
                incluir_vencimientos INTEGER DEFAULT 1,
                incluir_agenda INTEGER DEFAULT 1,
                inicio_windows INTEGER DEFAULT 1,
                actualizado TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute("INSERT OR IGNORE INTO alertas_config (id) VALUES (1)")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS camaras_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                plataforma TEXT DEFAULT 'XVRview',
                modo TEXT DEFAULT 'local',
                device_id TEXT DEFAULT '',
                user_id TEXT DEFAULT '',
                servidor_1 TEXT DEFAULT '',
                servidor_2 TEXT DEFAULT '',
                actualizado TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sidebar_clima_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                habilitado INTEGER DEFAULT 1,
                ubicacion TEXT DEFAULT 'Santiago, Chile',
                latitud REAL,
                longitud REAL,
                nombre_mostrado TEXT DEFAULT '',
                timezone TEXT DEFAULT '',
                actualizado TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS updater_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                habilitado INTEGER DEFAULT 0,
                github_repo TEXT DEFAULT '',
                release_asset TEXT DEFAULT 'GestionStockPro.exe',
                permitir_prerelease INTEGER DEFAULT 0,
                github_token TEXT DEFAULT '',
                actualizado TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO sidebar_clima_config (id, habilitado, ubicacion)
            VALUES (1, 1, 'Santiago, Chile')
            """
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO updater_config (
                id, habilitado, github_repo, release_asset, permitir_prerelease, github_token
            )
            VALUES (1, 0, '', 'GestionStockPro.exe', 0, '')
            """
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO camaras_config
                (id, plataforma, modo, device_id, user_id, servidor_1, servidor_2)
            VALUES
                (1, 'XVRview', 'local', 'rjphdn5bniqq', 'admin', '108.181.68.141', '177.54.156.56')
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS camaras_panel (
                id INTEGER PRIMARY KEY,
                nombre TEXT NOT NULL,
                abrir_url TEXT,
                embed_url TEXT,
                rtsp_url TEXT,
                activa INTEGER DEFAULT 1,
                orden INTEGER DEFAULT 0,
                actualizado TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO camaras_panel (id, nombre, abrir_url, embed_url, activa, orden)
            VALUES
                (1, 'Camara 1', 'http://108.181.68.141', '', 1, 1),
                (2, 'Camara 2', 'http://108.181.68.141', '', 1, 2),
                (3, 'Camara 3', 'http://177.54.156.56', '', 1, 3),
                (4, 'Camara 4', 'http://177.54.156.56', '', 1, 4)
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agenda_recordatorios_descartados (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                evento_id INTEGER NOT NULL,
                ventana_clave TEXT NOT NULL,
                descartado_en TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(evento_id, ventana_clave)
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS compras_pendientes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                insumo_id INTEGER,
                nombre TEXT NOT NULL,
                cantidad REAL DEFAULT 0,
                unidad TEXT DEFAULT 'unidad',
                precio_unitario REAL DEFAULT 0,
                precio_incluye_iva INTEGER DEFAULT 1,
                estado TEXT DEFAULT 'pendiente',
                nota TEXT,
                creado TEXT DEFAULT CURRENT_TIMESTAMP,
                actualizado TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (insumo_id) REFERENCES insumos(id)
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agenda_notas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                titulo TEXT NOT NULL,
                contenido TEXT,
                tipo TEXT DEFAULT 'texto',
                checklist_json TEXT,
                color TEXT DEFAULT 'amarilla',
                fijada INTEGER DEFAULT 0,
                recordatorio TEXT,
                estado TEXT DEFAULT 'activa',
                creada TEXT DEFAULT CURRENT_TIMESTAMP,
                actualizada TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        _ensure_column(conn, "agenda_notas", "tipo", "TEXT DEFAULT 'texto'")
        _ensure_column(conn, "agenda_notas", "checklist_json", "TEXT")
        conn.execute(
            """
            UPDATE agenda_notas
            SET tipo = CASE
                WHEN tipo IS NULL OR TRIM(tipo) = '' THEN 'texto'
                WHEN LOWER(TRIM(tipo)) IN ('texto', 'checklist') THEN LOWER(TRIM(tipo))
                ELSE 'texto'
            END
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS producto_mermas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                producto_id INTEGER NOT NULL,
                cantidad REAL NOT NULL,
                motivo TEXT NOT NULL,
                detalle TEXT,
                lotes_json TEXT,
                stock_anterior REAL,
                stock_nuevo REAL,
                estado TEXT DEFAULT 'activa',
                creado TEXT DEFAULT CURRENT_TIMESTAMP,
                revertida_en TEXT,
                FOREIGN KEY (producto_id) REFERENCES productos(id)
            )
            """
        )
        _ensure_column(conn, "producto_mermas", "codigo_operacion", "TEXT")
        conn.execute("UPDATE producto_mermas SET codigo_operacion = COALESCE(NULLIF(codigo_operacion, ''), 'OPM-LEGACY-' || id)")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS facturas_archivo (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                proveedor TEXT NOT NULL,
                fecha_factura TEXT NOT NULL,
                mes_clave TEXT NOT NULL,
                numero_factura TEXT,
                monto_total REAL DEFAULT 0,
                observacion TEXT,
                archivo_nombre_original TEXT NOT NULL,
                archivo_nombre_guardado TEXT NOT NULL,
                archivo_ruta_relativa TEXT NOT NULL,
                archivo_extension TEXT,
                archivo_mime TEXT,
                archivo_bytes INTEGER DEFAULT 0,
                eliminado INTEGER DEFAULT 0,
                eliminado_en TEXT,
                eliminado_motivo TEXT,
                creado TEXT DEFAULT CURRENT_TIMESTAMP,
                actualizado TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS facturas_auditoria (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                factura_id INTEGER,
                accion TEXT NOT NULL,
                snapshot_antes TEXT,
                snapshot_despues TEXT,
                metadata TEXT,
                creado TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (factura_id) REFERENCES facturas_archivo(id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS facturas_sii_ajustes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                anio INTEGER NOT NULL,
                mes_clave TEXT NOT NULL,
                valores_json TEXT NOT NULL,
                creado TEXT DEFAULT CURRENT_TIMESTAMP,
                actualizado TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(anio, mes_clave)
            )
            """
        )
        _ensure_column(conn, "facturas_archivo", "eliminado", "INTEGER DEFAULT 0")
        _ensure_column(conn, "facturas_archivo", "eliminado_en", "TEXT")
        _ensure_column(conn, "facturas_archivo", "eliminado_motivo", "TEXT")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ventas_semanales (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                semana_inicio TEXT NOT NULL,
                semana_fin TEXT NOT NULL,
                ventas_local REAL DEFAULT 0,
                ventas_uber REAL DEFAULT 0,
                ventas_pedidosya REAL DEFAULT 0,
                ventas_monto REAL DEFAULT 0,
                marketing_monto REAL DEFAULT 0,
                otros_descuentos_monto REAL DEFAULT 0,
                tasa_servicio_pct REAL DEFAULT 30,
                impuesto_tasa_servicio_pct REAL DEFAULT 19,
                notas TEXT,
                creado TEXT DEFAULT CURRENT_TIMESTAMP,
                actualizado TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(semana_inicio)
            )
            """
        )
        _ensure_column(conn, "ventas_semanales", "semana_fin", "TEXT")
        _ensure_column(conn, "ventas_semanales", "ventas_local", "REAL DEFAULT 0")
        _ensure_column(conn, "ventas_semanales", "ventas_uber", "REAL DEFAULT 0")
        _ensure_column(conn, "ventas_semanales", "ventas_pedidosya", "REAL DEFAULT 0")
        _ensure_column(conn, "ventas_semanales", "ventas_monto", "REAL DEFAULT 0")
        _ensure_column(conn, "ventas_semanales", "marketing_monto", "REAL DEFAULT 0")
        _ensure_column(conn, "ventas_semanales", "otros_descuentos_monto", "REAL DEFAULT 0")
        _ensure_column(conn, "ventas_semanales", "tasa_servicio_pct", "REAL DEFAULT 30")
        _ensure_column(conn, "ventas_semanales", "impuesto_tasa_servicio_pct", "REAL DEFAULT 19")
        _ensure_column(conn, "ventas_semanales", "notas", "TEXT")
        _ensure_column(conn, "ventas_semanales", "actualizado", "TEXT DEFAULT CURRENT_TIMESTAMP")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS resumen_mensual (
                anio INTEGER NOT NULL,
                mes INTEGER NOT NULL,
                mes_clave TEXT NOT NULL,
                ventas_local REAL DEFAULT 0,
                ventas_uber REAL DEFAULT 0,
                ventas_pedidosya REAL DEFAULT 0,
                compras_con_iva REAL DEFAULT 0,
                documentos_compra INTEGER DEFAULT 0,
                semanas_consideradas INTEGER DEFAULT 0,
                dirty INTEGER DEFAULT 1,
                actualizado TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (anio, mes)
            )
            """
        )
        _ensure_column(conn, "resumen_mensual", "dirty", "INTEGER DEFAULT 1")
        _ensure_column(conn, "resumen_mensual", "actualizado", "TEXT DEFAULT CURRENT_TIMESTAMP")
        conn.execute(
            """
            UPDATE ventas_semanales
            SET semana_fin = date(semana_inicio, '+6 day')
            WHERE semana_fin IS NULL OR trim(semana_fin) = ''
            """
        )
        conn.execute(
            """
            UPDATE ventas_semanales
            SET ventas_monto = COALESCE(ventas_local, 0) + COALESCE(ventas_uber, 0) + COALESCE(ventas_pedidosya, 0)
            WHERE COALESCE(ventas_monto, 0) = 0
              AND (COALESCE(ventas_local, 0) <> 0 OR COALESCE(ventas_uber, 0) <> 0 OR COALESCE(ventas_pedidosya, 0) <> 0)
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS haccp_puntos_control (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT NOT NULL,
                categoria TEXT DEFAULT 'General',
                tipo_control TEXT DEFAULT 'rango',
                frecuencia_horas INTEGER DEFAULT 4,
                limite_min REAL,
                limite_max REAL,
                unidad TEXT DEFAULT '',
                activo INTEGER DEFAULT 1,
                orden INTEGER DEFAULT 100,
                creado TEXT DEFAULT CURRENT_TIMESTAMP,
                actualizado TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS haccp_registros (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                punto_id INTEGER NOT NULL,
                valor REAL,
                cumple INTEGER NOT NULL,
                observacion TEXT,
                accion_correctiva TEXT,
                responsable TEXT,
                registrado_en TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (punto_id) REFERENCES haccp_puntos_control(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS haccp_trazabilidad_insumos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                produccion_id INTEGER,
                receta_id INTEGER,
                producto_id INTEGER NOT NULL,
                producto_nombre TEXT,
                producto_lote_id INTEGER,
                producto_fecha_elaboracion TEXT,
                producto_fecha_vencimiento TEXT,
                cantidad_producto_lote REAL DEFAULT 0,
                insumo_id INTEGER NOT NULL,
                insumo_nombre TEXT,
                insumo_lote_id INTEGER,
                insumo_lote_codigo TEXT,
                insumo_fecha_elaboracion TEXT,
                insumo_fecha_vencimiento TEXT,
                insumo_fecha_ingreso TEXT,
                cantidad_insumo_usada REAL NOT NULL,
                unidad_insumo TEXT DEFAULT 'unidad',
                producido_en TEXT DEFAULT CURRENT_TIMESTAMP,
                mes_clave TEXT,
                creado TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (produccion_id) REFERENCES producciones(id) ON DELETE CASCADE,
                FOREIGN KEY (producto_id) REFERENCES productos(id),
                FOREIGN KEY (producto_lote_id) REFERENCES producto_lotes(id),
                FOREIGN KEY (insumo_id) REFERENCES insumos(id),
                FOREIGN KEY (insumo_lote_id) REFERENCES insumo_lotes(id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS haccp_tuya_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                habilitado INTEGER DEFAULT 0,
                user_code TEXT DEFAULT '',
                endpoint TEXT DEFAULT '',
                terminal_id TEXT DEFAULT '',
                device_id TEXT DEFAULT '',
                device_name TEXT DEFAULT '',
                device_bindings_json TEXT DEFAULT '',
                auto_interval_min INTEGER DEFAULT 15,
                token_info_json TEXT DEFAULT '',
                ultimo_temp REAL,
                ultima_humedad REAL,
                ultima_lectura_en TEXT,
                actualizado TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS haccp_tuya_lecturas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                device_name TEXT DEFAULT '',
                punto_id INTEGER,
                temperatura REAL,
                humedad REAL,
                origen TEXT DEFAULT 'manual',
                leida_en TEXT DEFAULT CURRENT_TIMESTAMP,
                creado TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        _ensure_column(conn, "haccp_puntos_control", "tipo_control", "TEXT DEFAULT 'rango'")
        _ensure_column(conn, "haccp_puntos_control", "frecuencia_horas", "INTEGER DEFAULT 4")
        _ensure_column(conn, "haccp_puntos_control", "orden", "INTEGER DEFAULT 100")
        _ensure_column(conn, "haccp_registros", "accion_correctiva", "TEXT")
        _ensure_column(conn, "haccp_registros", "responsable", "TEXT")
        _ensure_column(conn, "haccp_trazabilidad_insumos", "producto_nombre", "TEXT")
        _ensure_column(conn, "haccp_trazabilidad_insumos", "producto_lote_id", "INTEGER")
        _ensure_column(conn, "haccp_trazabilidad_insumos", "producto_fecha_elaboracion", "TEXT")
        _ensure_column(conn, "haccp_trazabilidad_insumos", "producto_fecha_vencimiento", "TEXT")
        _ensure_column(conn, "haccp_trazabilidad_insumos", "cantidad_producto_lote", "REAL DEFAULT 0")
        _ensure_column(conn, "haccp_trazabilidad_insumos", "insumo_nombre", "TEXT")
        _ensure_column(conn, "haccp_trazabilidad_insumos", "insumo_lote_id", "INTEGER")
        _ensure_column(conn, "haccp_trazabilidad_insumos", "insumo_lote_codigo", "TEXT")
        _ensure_column(conn, "haccp_trazabilidad_insumos", "insumo_fecha_elaboracion", "TEXT")
        _ensure_column(conn, "haccp_trazabilidad_insumos", "insumo_fecha_vencimiento", "TEXT")
        _ensure_column(conn, "haccp_trazabilidad_insumos", "insumo_fecha_ingreso", "TEXT")
        _ensure_column(conn, "haccp_trazabilidad_insumos", "cantidad_insumo_usada", "REAL DEFAULT 0")
        _ensure_column(conn, "haccp_trazabilidad_insumos", "unidad_insumo", "TEXT DEFAULT 'unidad'")
        _ensure_column(conn, "haccp_trazabilidad_insumos", "producido_en", "TEXT DEFAULT CURRENT_TIMESTAMP")
        _ensure_column(conn, "haccp_trazabilidad_insumos", "mes_clave", "TEXT")
        _ensure_column(conn, "haccp_tuya_config", "habilitado", "INTEGER DEFAULT 0")
        _ensure_column(conn, "haccp_tuya_config", "user_code", "TEXT DEFAULT ''")
        _ensure_column(conn, "haccp_tuya_config", "endpoint", "TEXT DEFAULT ''")
        _ensure_column(conn, "haccp_tuya_config", "terminal_id", "TEXT DEFAULT ''")
        _ensure_column(conn, "haccp_tuya_config", "device_id", "TEXT DEFAULT ''")
        _ensure_column(conn, "haccp_tuya_config", "device_name", "TEXT DEFAULT ''")
        _ensure_column(conn, "haccp_tuya_config", "device_bindings_json", "TEXT DEFAULT ''")
        _ensure_column(conn, "haccp_tuya_config", "auto_interval_min", "INTEGER DEFAULT 15")
        _ensure_column(conn, "haccp_tuya_config", "token_info_json", "TEXT DEFAULT ''")
        _ensure_column(conn, "haccp_tuya_config", "ultimo_temp", "REAL")
        _ensure_column(conn, "haccp_tuya_config", "ultima_humedad", "REAL")
        _ensure_column(conn, "haccp_tuya_config", "ultima_lectura_en", "TEXT")
        _ensure_column(conn, "haccp_tuya_config", "actualizado", "TEXT DEFAULT CURRENT_TIMESTAMP")
        _ensure_column(conn, "sidebar_clima_config", "habilitado", "INTEGER DEFAULT 1")
        _ensure_column(conn, "sidebar_clima_config", "ubicacion", "TEXT DEFAULT 'Santiago, Chile'")
        _ensure_column(conn, "sidebar_clima_config", "latitud", "REAL")
        _ensure_column(conn, "sidebar_clima_config", "longitud", "REAL")
        _ensure_column(conn, "sidebar_clima_config", "nombre_mostrado", "TEXT DEFAULT ''")
        _ensure_column(conn, "sidebar_clima_config", "timezone", "TEXT DEFAULT ''")
        _ensure_column(conn, "sidebar_clima_config", "actualizado", "TEXT DEFAULT CURRENT_TIMESTAMP")
        _ensure_column(conn, "camaras_panel", "rtsp_url", "TEXT DEFAULT ''")
        conn.execute("INSERT OR IGNORE INTO haccp_tuya_config (id, habilitado) VALUES (1, 0)")
        conn.execute(
            """
            INSERT OR IGNORE INTO sidebar_clima_config (id, habilitado, ubicacion)
            VALUES (1, 1, 'Santiago, Chile')
            """
        )
        conn.execute(
            """
            UPDATE haccp_trazabilidad_insumos
            SET mes_clave = substr(COALESCE(NULLIF(producido_en, ''), datetime('now')), 1, 7)
            WHERE mes_clave IS NULL OR TRIM(mes_clave) = ''
            """
        )
        flag_haccp_tz = conn.execute(
            "SELECT valor FROM _migracion_flags WHERE clave = 'haccp_registros_localtime_v1'"
        ).fetchone()
        if not flag_haccp_tz:
            conn.execute(
                """
                UPDATE haccp_registros
                SET registrado_en = datetime(registrado_en, 'localtime')
                WHERE registrado_en IS NOT NULL
                  AND TRIM(registrado_en) <> ''
                  AND datetime(registrado_en) IS NOT NULL
                """
            )
            conn.execute(
                "INSERT OR REPLACE INTO _migracion_flags (clave, valor) VALUES ('haccp_registros_localtime_v1', 'ok')"
            )

        _sembrar_haccp_base(conn)

        conn.execute("CREATE INDEX IF NOT EXISTS idx_stock_movimientos_recurso ON stock_movimientos(tipo_recurso, recurso_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_stock_movimientos_creado ON stock_movimientos(creado)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_stock_movimientos_codigo_op ON stock_movimientos(codigo_operacion)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_stock_movimientos_origen ON stock_movimientos(origen_modulo)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_stock_ledger_recurso_fecha ON stock_ledger(recurso_tipo, recurso_id, fecha)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_stock_ledger_tipo_fecha ON stock_ledger(tipo_operacion, fecha)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_stock_ledger_codigo_op ON stock_ledger(codigo_operacion)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_stock_ledger_origen ON stock_ledger(origen_modulo)")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_ledger_legacy ON stock_ledger(legacy_movimiento_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_produccion_movimientos_prod ON produccion_movimientos(produccion_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_compras_pendientes_estado ON compras_pendientes(estado)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_compras_pendientes_nombre ON compras_pendientes(nombre)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_agenda_notas_estado ON agenda_notas(estado)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_agenda_notas_recordatorio ON agenda_notas(recordatorio)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_producto_mermas_creado ON producto_mermas(creado)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_producto_mermas_producto ON producto_mermas(producto_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_producto_mermas_estado ON producto_mermas(estado)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_facturas_archivo_mes ON facturas_archivo(mes_clave)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_facturas_archivo_proveedor ON facturas_archivo(proveedor)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_facturas_archivo_fecha ON facturas_archivo(fecha_factura)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_facturas_archivo_eliminado ON facturas_archivo(eliminado)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_facturas_auditoria_factura ON facturas_auditoria(factura_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_facturas_auditoria_creado ON facturas_auditoria(creado)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_facturas_sii_ajustes_anio ON facturas_sii_ajustes(anio)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_facturas_sii_ajustes_mes ON facturas_sii_ajustes(mes_clave)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ventas_semanales_inicio ON ventas_semanales(semana_inicio)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ventas_semanales_fin ON ventas_semanales(semana_fin)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_resumen_mensual_clave ON resumen_mensual(anio, mes_clave)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_resumen_mensual_dirty ON resumen_mensual(dirty)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_producto_insumos_venta_producto ON producto_insumos_venta(producto_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_producto_insumos_venta_insumo ON producto_insumos_venta(insumo_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_producto_productos_venta_producto ON producto_productos_venta(producto_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_producto_productos_venta_asociado ON producto_productos_venta(producto_asociado_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_producto_desactivaciones_manuales_producto ON producto_desactivaciones_manuales(producto_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_venta_insumos_venta ON venta_insumos(venta_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_venta_insumos_insumo ON venta_insumos(insumo_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_insumo_lotes_insumo ON insumo_lotes(insumo_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_insumo_lotes_vencimiento ON insumo_lotes(fecha_vencimiento)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_insumo_codigos_insumo ON insumo_codigos(insumo_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_venta_insumo_lotes_venta ON venta_insumo_lotes(venta_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_venta_insumo_lotes_insumo ON venta_insumo_lotes(insumo_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ventas_fecha_hora ON ventas(fecha_hora)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_venta_items_venta ON venta_items(venta_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_venta_items_producto ON venta_items(producto_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_venta_detalles_venta ON venta_detalles(venta_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_venta_lotes_venta ON venta_lotes(venta_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_producto_lotes_producto ON producto_lotes(producto_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_recetas_producto ON recetas(producto_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_receta_items_receta ON receta_items(receta_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_producciones_fecha_hora ON producciones(fecha_hora)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_produccion_agendada_fecha ON produccion_agendada(fecha)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_produccion_agendada_estado ON produccion_agendada(estado)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_insumos_codigo_barra ON insumos(codigo_barra)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_stock_movimientos_tipo_fecha ON stock_movimientos(tipo_recurso, creado)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_stock_movimientos_accion_fecha ON stock_movimientos(accion, creado)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_historial_cambios_fecha ON historial_cambios(fecha_hora)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_historial_cambios_recurso ON historial_cambios(recurso_tipo, recurso_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_haccp_puntos_activo_orden ON haccp_puntos_control(activo, orden, id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_haccp_registros_punto_fecha ON haccp_registros(punto_id, registrado_en DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_haccp_registros_fecha ON haccp_registros(registrado_en DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_haccp_traza_mes ON haccp_trazabilidad_insumos(mes_clave)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_haccp_traza_insumo_lote ON haccp_trazabilidad_insumos(insumo_id, insumo_lote_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_haccp_traza_producto_lote ON haccp_trazabilidad_insumos(producto_lote_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_haccp_traza_produccion ON haccp_trazabilidad_insumos(produccion_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_haccp_tuya_lecturas_device_fecha ON haccp_tuya_lecturas(device_id, leida_en DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_haccp_tuya_lecturas_fecha ON haccp_tuya_lecturas(leida_en DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_camaras_panel_orden ON camaras_panel(orden, id)")

        _limpiar_huerfanos_fk(conn)

        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"[WARN] Error en migración: {e}")
        return False
    finally:
        try:
            conn.execute("PRAGMA foreign_keys=ON")
        except Exception:
            pass
        conn.close()


def _factor_unidad_costo(unidad):
    if not unidad:
        return 1, "unidad"

    u = str(unidad).strip().lower().split("(")[0].strip()
    if u in ["mg", "miligramo", "miligramos"]:
        return 0.001, "g"
    if u in ["g", "gr", "gramo", "gramos"]:
        return 1, "g"
    if u in ["kg", "kilo", "kilos", "kilogramo", "kilogramos"]:
        return 1000, "g"
    if u in ["ml", "mililitro", "mililitros", "cc", "cm3"]:
        return 1, "ml"
    if u in ["l", "lt", "litro", "litros"]:
        return 1000, "ml"
    if u in ["docena", "docenas"]:
        return 12, "unidad"
    return 1, "unidad"


def calcular_costo_receta(receta_id, _visitados=None):
    """Calcula costo considerando componentes de insumos y productos."""
    if _visitados is None:
        _visitados = set()
    if receta_id in _visitados:
        return {"costo_total": 0, "detalle": [{"error": "Referencia circular de receta"}]}

    _visitados = set(_visitados)
    _visitados.add(receta_id)

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id, rendimiento FROM recetas WHERE id = ?", (receta_id,))
        receta = cursor.fetchone()
        if not receta:
            return {"costo_total": 0, "detalle": [{"error": "Receta no encontrada"}]}

        cursor.execute(
            """
            SELECT ri.tipo, ri.insumo_id, ri.producto_id, ri.cantidad, ri.unidad,
                   i.nombre AS insumo_nombre, i.unidad AS unidad_insumo,
                   i.precio_unitario, i.cantidad_comprada, i.unidad_compra, i.precio_incluye_iva,
                   p.nombre AS producto_nombre, p.precio AS producto_precio
            FROM receta_items ri
            LEFT JOIN insumos i ON ri.insumo_id = i.id
            LEFT JOIN productos p ON ri.producto_id = p.id
            WHERE ri.receta_id = ?
            ORDER BY ri.id
            """,
            (receta_id,),
        )
        items = cursor.fetchall()

        costo_total = 0
        detalle = []

        for item in items:
            tipo = (item["tipo"] or "insumo").lower()
            cantidad_receta = float(item["cantidad"] or 0)
            unidad_receta = item["unidad"] or "unidad"

            if tipo == "producto":
                nombre_producto = item["producto_nombre"] or "Producto"
                cursor.execute("SELECT id, rendimiento FROM recetas WHERE producto_id = ? ORDER BY id LIMIT 1", (item["producto_id"],))
                receta_asociada = cursor.fetchone()

                costo_unitario = 0
                origen = "precio_producto"

                if receta_asociada and receta_asociada["id"] not in _visitados:
                    sub = calcular_costo_receta(receta_asociada["id"], _visitados=_visitados)
                    rendimiento_sub = float(receta_asociada["rendimiento"] or 1)
                    costo_unitario = float(sub.get("costo_total", 0)) / max(rendimiento_sub, 1)
                    origen = "receta_asociada"
                elif float(item["producto_precio"] or 0) > 0:
                    costo_unitario = float(item["producto_precio"] or 0)
                else:
                    detalle.append(
                        {
                            "tipo": "producto",
                            "insumo": nombre_producto,
                            "cantidad": cantidad_receta,
                            "unidad": unidad_receta,
                            "costo": 0,
                            "error": "Producto sin receta asociada ni precio",
                        }
                    )
                    continue

                costo_item = cantidad_receta * costo_unitario
                costo_total += costo_item
                detalle.append(
                    {
                        "tipo": "producto",
                        "insumo": nombre_producto,
                        "cantidad": cantidad_receta,
                        "unidad": unidad_receta,
                        "precio_por_unidad": round(costo_unitario, 4),
                        "costo": round(costo_item, 2),
                        "origen": origen,
                    }
                )
                continue

            # costo de insumos
            precio_compra = float(item["precio_unitario"] or 0)
            if precio_compra <= 0:
                detalle.append(
                    {
                        "tipo": "insumo",
                        "insumo": item["insumo_nombre"],
                        "cantidad": cantidad_receta,
                        "unidad": unidad_receta,
                        "costo": 0,
                        "error": "Sin precio registrado",
                    }
                )
                continue

            if not item["precio_incluye_iva"]:
                precio_compra *= 1.19

            cantidad_comprada = float(item["cantidad_comprada"] or 1)
            unidad_compra = item["unidad_compra"] or item["unidad_insumo"] or "unidad"

            factor_compra, tipo_base = _factor_unidad_costo(unidad_compra)
            factor_receta, _ = _factor_unidad_costo(unidad_receta)

            cantidad_comprada_base = cantidad_comprada * factor_compra
            cantidad_receta_base = cantidad_receta * factor_receta
            precio_por_unidad_base = (precio_compra / cantidad_comprada_base) if cantidad_comprada_base > 0 else 0

            costo_item = cantidad_receta_base * precio_por_unidad_base
            costo_total += costo_item
            detalle.append(
                {
                    "tipo": "insumo",
                    "insumo": item["insumo_nombre"],
                    "cantidad": cantidad_receta,
                    "unidad": unidad_receta,
                    "cantidad_base": round(cantidad_receta_base, 2),
                    "unidad_base": tipo_base,
                    "precio_por_unidad": round(precio_por_unidad_base, 4),
                    "costo": round(costo_item, 2),
                }
            )

        return {"costo_total": round(costo_total, 2), "detalle": detalle}
    except Exception as e:
        return {"costo_total": 0, "detalle": [], "error": str(e)}
    finally:
        conn.close()


_NUTRICION_COLUMNAS = {
    "kcal": "nutricion_kcal",
    "proteinas_g": "nutricion_proteinas_g",
    "carbohidratos_g": "nutricion_carbohidratos_g",
    "grasas_g": "nutricion_grasas_g",
    "azucares_g": "nutricion_azucares_g",
    "sodio_mg": "nutricion_sodio_mg",
}


def _nutricion_vacia():
    return {
        "kcal": 0.0,
        "proteinas_g": 0.0,
        "carbohidratos_g": 0.0,
        "grasas_g": 0.0,
        "azucares_g": 0.0,
        "sodio_mg": 0.0,
    }


def _nutricion_float(valor):
    try:
        return float(valor or 0)
    except (TypeError, ValueError):
        return 0.0


def _sumar_nutricion(destino, origen):
    for clave in destino.keys():
        destino[clave] = float(destino.get(clave, 0) or 0) + float(origen.get(clave, 0) or 0)


def _escalar_nutricion(origen, factor):
    factor_num = float(factor or 0)
    return {clave: float(origen.get(clave, 0) or 0) * factor_num for clave in origen.keys()}


def _redondear_nutricion(valores, decimales=2):
    return {clave: round(float(valores.get(clave, 0) or 0), decimales) for clave in valores.keys()}


def _unidad_base_nutricional(unidad_stock):
    tipo = _tipo_unidad(unidad_stock or "unidad")
    if tipo == "solido":
        return "gr"
    if tipo == "liquido":
        return "ml"
    return "unidad"


def _normalizar_referencia_nutricional(unidad_stock, ref_cantidad, ref_unidad):
    unidad_base = _unidad_base_nutricional(unidad_stock)
    unidad_referencia = (
        _normalizar_unidad_producto(ref_unidad)
        if str(ref_unidad or "").strip()
        else unidad_base
    )
    if not _unidades_compatibles_porcion(unidad_stock, unidad_referencia):
        unidad_referencia = unidad_base

    try:
        cantidad_referencia = float(ref_cantidad)
    except (TypeError, ValueError):
        cantidad_referencia = 100.0 if unidad_referencia in {"gr", "ml"} else 1.0
    if cantidad_referencia <= 0:
        cantidad_referencia = 100.0 if unidad_referencia in {"gr", "ml"} else 1.0
    return cantidad_referencia, unidad_referencia


def calcular_nutricion_receta(receta_id, _visitados=None, conn=None):
    """Calcula nutrición total de receta y nutrición por porción (según rendimiento)."""
    visitados = set(_visitados or set())
    if receta_id in visitados:
        return {
            "success": False,
            "error": "Referencia circular de receta",
            "rendimiento": 1,
            "por_lote": _nutricion_vacia(),
            "por_porcion": _nutricion_vacia(),
            "detalle": [],
            "faltantes": [],
        }
    visitados.add(receta_id)

    own_conn = conn is None
    if own_conn:
        conn = get_db()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT id, nombre, rendimiento FROM recetas WHERE id = ?", (receta_id,))
        receta = cursor.fetchone()
        if not receta:
            return {
                "success": False,
                "error": "Receta no encontrada",
                "rendimiento": 1,
                "por_lote": _nutricion_vacia(),
                "por_porcion": _nutricion_vacia(),
                "detalle": [],
                "faltantes": [],
            }

        cursor.execute(
            """
            SELECT ri.id, ri.tipo, ri.insumo_id, ri.producto_id, ri.cantidad, ri.unidad,
                   i.nombre AS insumo_nombre, i.unidad AS unidad_insumo,
                   i.nutricion_ref_cantidad, i.nutricion_ref_unidad,
                   i.nutricion_kcal, i.nutricion_proteinas_g, i.nutricion_carbohidratos_g,
                   i.nutricion_grasas_g, i.nutricion_azucares_g, i.nutricion_sodio_mg,
                   p.nombre AS producto_nombre
            FROM receta_items ri
            LEFT JOIN insumos i ON ri.insumo_id = i.id
            LEFT JOIN productos p ON ri.producto_id = p.id
            WHERE ri.receta_id = ?
            ORDER BY ri.id
            """,
            (receta_id,),
        )
        items = cursor.fetchall()

        total_lote = _nutricion_vacia()
        detalle = []
        faltantes = []

        for item in items:
            tipo = (item["tipo"] or "insumo").lower()
            cantidad_receta = float(item["cantidad"] or 0)
            unidad_receta = _normalizar_unidad_producto(item["unidad"] or "unidad")
            nombre = (
                item["producto_nombre"]
                if tipo == "producto"
                else item["insumo_nombre"]
            ) or ("Producto" if tipo == "producto" else "Insumo")

            if cantidad_receta <= 0:
                faltantes.append(
                    {
                        "tipo": tipo,
                        "nombre": nombre,
                        "motivo": "Cantidad de receta inválida",
                    }
                )
                continue

            if tipo == "producto":
                cursor.execute(
                    "SELECT id, rendimiento FROM recetas WHERE producto_id = ? ORDER BY id LIMIT 1",
                    (item["producto_id"],),
                )
                receta_asociada = cursor.fetchone()
                if not receta_asociada:
                    faltantes.append(
                        {
                            "tipo": "producto",
                            "nombre": nombre,
                            "motivo": "Producto sin receta asociada para calcular nutrición",
                        }
                    )
                    continue
                if receta_asociada["id"] in visitados:
                    faltantes.append(
                        {
                            "tipo": "producto",
                            "nombre": nombre,
                            "motivo": "Referencia circular de receta",
                        }
                    )
                    continue

                conversion = _convertir_cantidad_unidad_db(cantidad_receta, unidad_receta, "unidad")
                if not conversion.get("success"):
                    faltantes.append(
                        {
                            "tipo": "producto",
                            "nombre": nombre,
                            "motivo": conversion.get("error") or "No se pudo convertir cantidad a unidad",
                        }
                    )
                    continue

                cantidad_unidades = float(conversion.get("cantidad") or 0)
                rendimiento_sub = float(receta_asociada["rendimiento"] or 1)
                rendimiento_sub = rendimiento_sub if rendimiento_sub > 0 else 1.0
                lotes_equivalentes = cantidad_unidades / rendimiento_sub

                sub_nutri = calcular_nutricion_receta(
                    receta_asociada["id"],
                    _visitados=visitados,
                    conn=conn,
                )
                if not sub_nutri.get("success"):
                    faltantes.append(
                        {
                            "tipo": "producto",
                            "nombre": nombre,
                            "motivo": sub_nutri.get("error") or "No se pudo calcular nutrición de la receta asociada",
                        }
                    )
                    continue

                contribucion = _escalar_nutricion(sub_nutri.get("por_lote", _nutricion_vacia()), lotes_equivalentes)
                _sumar_nutricion(total_lote, contribucion)
                detalle.append(
                    {
                        "tipo": "producto",
                        "nombre": nombre,
                        "cantidad": cantidad_receta,
                        "unidad": unidad_receta,
                        "origen": "receta_asociada",
                        "contribucion": _redondear_nutricion(contribucion, decimales=2),
                    }
                )
                continue

            unidad_insumo = _normalizar_unidad_producto(item["unidad_insumo"] or "unidad")
            ref_cantidad, ref_unidad = _normalizar_referencia_nutricional(
                unidad_insumo,
                item["nutricion_ref_cantidad"],
                item["nutricion_ref_unidad"],
            )
            conversion = _convertir_cantidad_unidad_db(cantidad_receta, unidad_receta, ref_unidad)
            if not conversion.get("success"):
                faltantes.append(
                    {
                        "tipo": "insumo",
                        "nombre": nombre,
                        "motivo": conversion.get("error") or "No se pudo convertir unidad para nutrición",
                    }
                )
                continue

            cantidad_en_ref = float(conversion.get("cantidad") or 0)
            factor_ref = cantidad_en_ref / ref_cantidad if ref_cantidad > 0 else 0
            por_referencia = {
                clave: _nutricion_float(item[columna]) for clave, columna in _NUTRICION_COLUMNAS.items()
            }
            if not any(abs(v) > 0 for v in por_referencia.values()):
                faltantes.append(
                    {
                        "tipo": "insumo",
                        "nombre": nombre,
                        "motivo": "Insumo sin datos nutricionales cargados",
                    }
                )

            contribucion = _escalar_nutricion(por_referencia, factor_ref)
            _sumar_nutricion(total_lote, contribucion)
            detalle.append(
                {
                    "tipo": "insumo",
                    "nombre": nombre,
                    "cantidad": cantidad_receta,
                    "unidad": unidad_receta,
                    "referencia": {
                        "cantidad": ref_cantidad,
                        "unidad": ref_unidad,
                    },
                    "contribucion": _redondear_nutricion(contribucion, decimales=2),
                }
            )

        rendimiento = float(receta["rendimiento"] or 1)
        if rendimiento <= 0:
            rendimiento = 1.0
        por_porcion = _escalar_nutricion(total_lote, 1.0 / rendimiento)

        return {
            "success": True,
            "receta_id": int(receta["id"]),
            "receta_nombre": receta["nombre"] or "Receta",
            "rendimiento": rendimiento,
            "por_lote": _redondear_nutricion(total_lote, decimales=2),
            "por_porcion": _redondear_nutricion(por_porcion, decimales=2),
            "detalle": detalle,
            "faltantes": faltantes,
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "rendimiento": 1,
            "por_lote": _nutricion_vacia(),
            "por_porcion": _nutricion_vacia(),
            "detalle": [],
            "faltantes": [],
        }
    finally:
        if own_conn and conn:
            conn.close()


def obtener_config_alertas():
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM alertas_config WHERE id = 1")
        row = cursor.fetchone()
        if not row:
            return {
                "notificaciones_activas": 1,
                "repetir_minutos": 15,
                "dias_anticipacion": 2,
                "incluir_stock_bajo": 1,
                "incluir_vencimientos": 1,
                "incluir_agenda": 1,
                "inicio_windows": 1,
            }
        return dict(row)
    finally:
        conn.close()


def guardar_config_alertas(data):
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            UPDATE alertas_config
            SET notificaciones_activas = ?,
                repetir_minutos = ?,
                dias_anticipacion = ?,
                incluir_stock_bajo = ?,
                incluir_vencimientos = ?,
                incluir_agenda = ?,
                inicio_windows = ?,
                actualizado = CURRENT_TIMESTAMP
            WHERE id = 1
            """,
            (
                1 if data.get("notificaciones_activas", True) else 0,
                max(1, int(data.get("repetir_minutos", 15) or 15)),
                max(0, int(data.get("dias_anticipacion", 2) or 2)),
                1 if data.get("incluir_stock_bajo", True) else 0,
                1 if data.get("incluir_vencimientos", True) else 0,
                1 if data.get("incluir_agenda", True) else 0,
                1 if data.get("inicio_windows", True) else 0,
            ),
        )
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _float_or_none(valor):
    if valor in (None, "", "null"):
        return None
    try:
        num = float(valor)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(num):
        return None
    return num


def obtener_config_clima_sidebar():
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM sidebar_clima_config WHERE id = 1")
        row = cursor.fetchone()
        if not row:
            return {
                "habilitado": 1,
                "ubicacion": "Santiago, Chile",
                "latitud": None,
                "longitud": None,
                "nombre_mostrado": "",
                "timezone": "",
            }
        item = dict(row)
        item["habilitado"] = int(item.get("habilitado") or 0)
        item["ubicacion"] = _normalizar_texto_simple(item.get("ubicacion"), 120) or "Santiago, Chile"
        item["latitud"] = _float_or_none(item.get("latitud"))
        item["longitud"] = _float_or_none(item.get("longitud"))
        item["nombre_mostrado"] = _normalizar_texto_simple(item.get("nombre_mostrado"), 180)
        item["timezone"] = _normalizar_texto_simple(item.get("timezone"), 80)
        return item
    finally:
        conn.close()


def guardar_config_clima_sidebar(data):
    payload = data if isinstance(data, dict) else {}
    habilitado = 1 if bool(payload.get("habilitado", True)) else 0
    ubicacion = _normalizar_texto_simple(payload.get("ubicacion"), 120) or "Santiago, Chile"
    latitud = _float_or_none(payload.get("latitud"))
    longitud = _float_or_none(payload.get("longitud"))
    if latitud is not None:
        latitud = max(-90.0, min(90.0, latitud))
    if longitud is not None:
        longitud = max(-180.0, min(180.0, longitud))
    nombre_mostrado = _normalizar_texto_simple(payload.get("nombre_mostrado"), 180)
    timezone = _normalizar_texto_simple(payload.get("timezone"), 80)

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO sidebar_clima_config (
                id, habilitado, ubicacion, latitud, longitud, nombre_mostrado, timezone, actualizado
            )
            VALUES (1, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(id) DO UPDATE SET
                habilitado = excluded.habilitado,
                ubicacion = excluded.ubicacion,
                latitud = excluded.latitud,
                longitud = excluded.longitud,
                nombre_mostrado = excluded.nombre_mostrado,
                timezone = excluded.timezone,
                actualizado = CURRENT_TIMESTAMP
            """,
            (
                habilitado,
                ubicacion,
                latitud,
                longitud,
                nombre_mostrado,
                timezone,
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return obtener_config_clima_sidebar()


def obtener_config_updater():
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM updater_config WHERE id = 1")
        row = cursor.fetchone()
        item = dict(row) if row else {}
        if not item:
            item = {
                "id": 1,
                "habilitado": 0,
                "github_repo": "",
                "release_asset": "GestionStockPro.exe",
                "permitir_prerelease": 0,
                "github_token": "",
            }
        item["habilitado"] = int(item.get("habilitado") or 0)
        item["permitir_prerelease"] = int(item.get("permitir_prerelease") or 0)
        item["github_repo"] = _normalizar_texto_simple(item.get("github_repo"), 160)
        item["release_asset"] = _normalizar_texto_simple(item.get("release_asset"), 180) or "GestionStockPro.exe"
        item["github_token"] = _normalizar_texto_simple(item.get("github_token"), 300)
        return item
    finally:
        conn.close()


def guardar_config_updater(data):
    payload = data if isinstance(data, dict) else {}
    habilitado = 1 if bool(payload.get("habilitado", False)) else 0
    github_repo = _normalizar_texto_simple(payload.get("github_repo"), 160)
    release_asset = _normalizar_texto_simple(payload.get("release_asset"), 180) or "GestionStockPro.exe"
    permitir_prerelease = 1 if bool(payload.get("permitir_prerelease", False)) else 0
    github_token = _normalizar_texto_simple(payload.get("github_token"), 300)

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO updater_config (
                id, habilitado, github_repo, release_asset, permitir_prerelease, github_token, actualizado
            )
            VALUES (1, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(id) DO UPDATE SET
                habilitado = excluded.habilitado,
                github_repo = excluded.github_repo,
                release_asset = excluded.release_asset,
                permitir_prerelease = excluded.permitir_prerelease,
                github_token = excluded.github_token,
                actualizado = CURRENT_TIMESTAMP
            """,
            (
                habilitado,
                github_repo,
                release_asset,
                permitir_prerelease,
                github_token,
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return obtener_config_updater()


def _normalizar_texto_simple(valor, max_len=200):
    texto = str(valor or "").strip()
    if not texto:
        return ""
    return texto[:max_len]


def _normalizar_url_camara(valor, allow_relative=False, allow_rtsp=False):
    url = _normalizar_texto_simple(valor, max_len=500)
    if not url:
        return ""
    lower = url.lower()
    if allow_relative and url.startswith("/") and not url.startswith("//"):
        return url
    if lower.startswith("http://") or lower.startswith("https://"):
        return url
    if allow_rtsp and (lower.startswith("rtsp://") or lower.startswith("rtsps://")):
        return url
    return ""


def obtener_config_camaras():
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM camaras_config WHERE id = 1")
        row = cursor.fetchone()
        config = dict(row) if row else {}
        if not config:
            config = {
                "id": 1,
                "plataforma": "XVRview",
                "modo": "local",
                "device_id": "rjphdn5bniqq",
                "user_id": "admin",
                "servidor_1": "108.181.68.141",
                "servidor_2": "177.54.156.56",
            }

        cursor.execute(
            """
            SELECT id, nombre, abrir_url, embed_url, rtsp_url, activa, orden
            FROM camaras_panel
            ORDER BY orden ASC, id ASC
            """
        )
        paneles = [dict(r) for r in cursor.fetchall()]
        if not paneles:
            paneles = [
                {"id": 1, "nombre": "Camara 1", "abrir_url": "http://108.181.68.141", "embed_url": "", "rtsp_url": "", "activa": 1, "orden": 1},
                {"id": 2, "nombre": "Camara 2", "abrir_url": "http://108.181.68.141", "embed_url": "", "rtsp_url": "", "activa": 1, "orden": 2},
                {"id": 3, "nombre": "Camara 3", "abrir_url": "http://177.54.156.56", "embed_url": "", "rtsp_url": "", "activa": 1, "orden": 3},
                {"id": 4, "nombre": "Camara 4", "abrir_url": "http://177.54.156.56", "embed_url": "", "rtsp_url": "", "activa": 1, "orden": 4},
            ]

        return {
            "success": True,
            "config": config,
            "paneles": paneles,
        }
    finally:
        conn.close()


def guardar_config_camaras(data):
    payload = data if isinstance(data, dict) else {}
    paneles = payload.get("paneles")
    if not isinstance(paneles, list):
        paneles = []

    config = {
        "plataforma": _normalizar_texto_simple(payload.get("plataforma"), 80) or "XVRview",
        "modo": _normalizar_texto_simple(payload.get("modo"), 30) or "local",
        "device_id": _normalizar_texto_simple(payload.get("device_id"), 120),
        "user_id": _normalizar_texto_simple(payload.get("user_id"), 120),
        "servidor_1": _normalizar_texto_simple(payload.get("servidor_1"), 120),
        "servidor_2": _normalizar_texto_simple(payload.get("servidor_2"), 120),
    }

    normalizados = []
    for idx, item in enumerate(paneles, start=1):
        if not isinstance(item, dict):
            continue
        try:
            panel_id = int(item.get("id") or idx)
        except (TypeError, ValueError):
            panel_id = idx
        panel_id = max(1, min(4, panel_id))
        normalizados.append(
            {
                "id": panel_id,
                "nombre": _normalizar_texto_simple(item.get("nombre"), 80) or f"Camara {panel_id}",
                "abrir_url": _normalizar_url_camara(item.get("abrir_url")),
                "embed_url": _normalizar_url_camara(item.get("embed_url"), allow_relative=True),
                "rtsp_url": _normalizar_url_camara(item.get("rtsp_url"), allow_rtsp=True),
                "activa": 1 if bool(item.get("activa", True)) else 0,
                "orden": max(1, min(4, int(item.get("orden") or panel_id))),
            }
        )

    by_id = {item["id"]: item for item in normalizados}
    defaults = {
        1: {"nombre": "Camara 1", "abrir_url": "http://108.181.68.141", "embed_url": "", "rtsp_url": "", "activa": 1, "orden": 1},
        2: {"nombre": "Camara 2", "abrir_url": "http://108.181.68.141", "embed_url": "", "rtsp_url": "", "activa": 1, "orden": 2},
        3: {"nombre": "Camara 3", "abrir_url": "http://177.54.156.56", "embed_url": "", "rtsp_url": "", "activa": 1, "orden": 3},
        4: {"nombre": "Camara 4", "abrir_url": "http://177.54.156.56", "embed_url": "", "rtsp_url": "", "activa": 1, "orden": 4},
    }
    for panel_id in range(1, 5):
        if panel_id not in by_id:
            by_id[panel_id] = {"id": panel_id, **defaults[panel_id]}

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO camaras_config (id, plataforma, modo, device_id, user_id, servidor_1, servidor_2, actualizado)
            VALUES (1, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(id) DO UPDATE SET
                plataforma = excluded.plataforma,
                modo = excluded.modo,
                device_id = excluded.device_id,
                user_id = excluded.user_id,
                servidor_1 = excluded.servidor_1,
                servidor_2 = excluded.servidor_2,
                actualizado = CURRENT_TIMESTAMP
            """,
            (
                config["plataforma"],
                config["modo"],
                config["device_id"],
                config["user_id"],
                config["servidor_1"],
                config["servidor_2"],
            ),
        )

        for panel_id in range(1, 5):
            item = by_id[panel_id]
            cursor.execute(
                """
                INSERT INTO camaras_panel (id, nombre, abrir_url, embed_url, rtsp_url, activa, orden, actualizado)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(id) DO UPDATE SET
                    nombre = excluded.nombre,
                    abrir_url = excluded.abrir_url,
                    embed_url = excluded.embed_url,
                    rtsp_url = excluded.rtsp_url,
                    activa = excluded.activa,
                    orden = excluded.orden,
                    actualizado = CURRENT_TIMESTAMP
                """,
                (
                    panel_id,
                    item["nombre"],
                    item["abrir_url"],
                    item["embed_url"],
                    item["rtsp_url"],
                    int(item["activa"]),
                    int(item["orden"]),
                ),
            )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return obtener_config_camaras()


def _cargar_json_dict_seguro(raw):
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _serializar_json_seguro(data):
    if not isinstance(data, dict):
        data = {}
    try:
        return json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return "{}"


def _normalizar_tuya_intervalo_min(valor, default=15):
    try:
        raw = int(float(valor))
    except Exception:
        raw = int(default or 15)
    return max(1, min(720, raw))


def _normalizar_tuya_alerta_temp(valor):
    if valor in (None, "", "null"):
        return None
    try:
        num = float(valor)
    except Exception:
        return None
    if not math.isfinite(num):
        return None
    return round(max(-100.0, min(300.0, num)), 2)


def _normalizar_tuya_binding_item(item, default_intervalo=15):
    payload = item if isinstance(item, dict) else {}
    device_id = _normalizar_texto_simple(payload.get("device_id"), 120)
    if not device_id:
        return None

    punto_id_raw = payload.get("punto_id")
    punto_id = None
    if punto_id_raw not in (None, "", 0, "0"):
        try:
            punto_id_int = int(punto_id_raw)
            punto_id = punto_id_int if punto_id_int > 0 else None
        except Exception:
            punto_id = None

    alerta_min_temp = _normalizar_tuya_alerta_temp(payload.get("alerta_min_temp"))
    alerta_max_temp = _normalizar_tuya_alerta_temp(payload.get("alerta_max_temp"))
    if alerta_min_temp is not None and alerta_max_temp is not None and alerta_min_temp > alerta_max_temp:
        alerta_min_temp, alerta_max_temp = alerta_max_temp, alerta_min_temp

    return {
        "device_id": device_id,
        "device_name": _normalizar_texto_simple(payload.get("device_name"), 200),
        "punto_id": punto_id,
        "activo": 1 if bool(payload.get("activo", True)) else 0,
        "intervalo_min": _normalizar_tuya_intervalo_min(
            payload.get("intervalo_min"), default=default_intervalo
        ),
        "alerta_min_temp": alerta_min_temp,
        "alerta_max_temp": alerta_max_temp,
    }


def _normalizar_tuya_bindings_lista(bindings, default_intervalo=15):
    if not isinstance(bindings, list):
        return []
    normalizados = []
    usados = set()
    for item in bindings:
        binding = _normalizar_tuya_binding_item(item, default_intervalo=default_intervalo)
        if not binding:
            continue
        device_id = binding["device_id"]
        if device_id in usados:
            continue
        usados.add(device_id)
        normalizados.append(binding)
        if len(normalizados) >= 120:
            break
    return normalizados


def obtener_config_tuya_haccp():
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM haccp_tuya_config WHERE id = 1")
        row = cursor.fetchone()
        if not row:
            config = {
                "id": 1,
                "habilitado": 0,
                "user_code": "",
                "endpoint": "",
                "terminal_id": "",
                "device_id": "",
                "device_name": "",
                "device_bindings_json": "",
                "auto_interval_min": 15,
                "token_info_json": "",
                "ultimo_temp": None,
                "ultima_humedad": None,
                "ultima_lectura_en": None,
                "actualizado": None,
            }
        else:
            config = dict(row)
        token_info = _cargar_json_dict_seguro(config.get("token_info_json"))
        config["token_info"] = token_info
        config["auto_interval_min"] = _normalizar_tuya_intervalo_min(
            config.get("auto_interval_min"), default=15
        )
        bindings_raw = config.get("device_bindings_json")
        bindings = []
        if bindings_raw:
            try:
                parsed = json.loads(bindings_raw)
            except Exception:
                parsed = []
            bindings = _normalizar_tuya_bindings_lista(
                parsed, default_intervalo=config["auto_interval_min"]
            )
        config["device_bindings"] = bindings
        config["token_disponible"] = bool(
            token_info.get("access_token") and token_info.get("refresh_token")
        )
        return config
    finally:
        conn.close()


def guardar_config_tuya_haccp(data):
    payload = data if isinstance(data, dict) else {}
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT OR IGNORE INTO haccp_tuya_config (id, habilitado) VALUES (1, 0)"
        )

        updates = []
        params = []

        if "habilitado" in payload:
            updates.append("habilitado = ?")
            params.append(1 if bool(payload.get("habilitado")) else 0)
        if "user_code" in payload:
            updates.append("user_code = ?")
            params.append(_normalizar_texto_simple(payload.get("user_code"), 80))
        if "endpoint" in payload:
            updates.append("endpoint = ?")
            params.append(_normalizar_texto_simple(payload.get("endpoint"), 200))
        if "terminal_id" in payload:
            updates.append("terminal_id = ?")
            params.append(_normalizar_texto_simple(payload.get("terminal_id"), 120))
        if "device_id" in payload:
            updates.append("device_id = ?")
            params.append(_normalizar_texto_simple(payload.get("device_id"), 120))
        if "device_name" in payload:
            updates.append("device_name = ?")
            params.append(_normalizar_texto_simple(payload.get("device_name"), 200))
        if "auto_interval_min" in payload:
            updates.append("auto_interval_min = ?")
            params.append(
                _normalizar_tuya_intervalo_min(payload.get("auto_interval_min"), default=15)
            )
        if "device_bindings" in payload:
            normalizados = _normalizar_tuya_bindings_lista(
                payload.get("device_bindings"),
                default_intervalo=_normalizar_tuya_intervalo_min(
                    payload.get("auto_interval_min"), default=15
                ),
            )
            updates.append("device_bindings_json = ?")
            params.append(
                json.dumps(normalizados, ensure_ascii=False, separators=(",", ":"))
            )
        if "token_info" in payload:
            updates.append("token_info_json = ?")
            params.append(_serializar_json_seguro(payload.get("token_info")))

        if updates:
            updates.append("actualizado = CURRENT_TIMESTAMP")
            cursor.execute(
                f"UPDATE haccp_tuya_config SET {', '.join(updates)} WHERE id = 1",
                tuple(params),
            )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return obtener_config_tuya_haccp()


def guardar_auth_tuya_haccp(user_code, endpoint, terminal_id, token_info):
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT OR IGNORE INTO haccp_tuya_config (id, habilitado) VALUES (1, 0)"
        )
        cursor.execute(
            """
            UPDATE haccp_tuya_config
            SET habilitado = 1,
                user_code = ?,
                endpoint = ?,
                terminal_id = ?,
                token_info_json = ?,
                actualizado = CURRENT_TIMESTAMP
            WHERE id = 1
            """,
            (
                _normalizar_texto_simple(user_code, 80),
                _normalizar_texto_simple(endpoint, 200),
                _normalizar_texto_simple(terminal_id, 120),
                _serializar_json_seguro(token_info),
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return obtener_config_tuya_haccp()


def guardar_lectura_tuya_haccp(temp=None, humedad=None, device_id=None, device_name=None, leida_en=None):
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT OR IGNORE INTO haccp_tuya_config (id, habilitado) VALUES (1, 0)"
        )

        marca_tiempo = (
            _normalizar_texto_simple(leida_en, 30)
            if leida_en
            else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        updates = []
        params = []
        temp_num = None
        hum_num = None
        if temp is not None:
            temp_num = float(temp)
            updates.append("ultimo_temp = ?")
            params.append(temp_num)
        if humedad is not None:
            hum_num = float(humedad)
            updates.append("ultima_humedad = ?")
            params.append(hum_num)
        if device_id is not None:
            updates.append("device_id = ?")
            params.append(_normalizar_texto_simple(device_id, 120))
        if device_name is not None:
            updates.append("device_name = ?")
            params.append(_normalizar_texto_simple(device_name, 200))

        updates.append("ultima_lectura_en = ?")
        params.append(marca_tiempo)
        updates.append("actualizado = CURRENT_TIMESTAMP")

        cursor.execute(
            f"UPDATE haccp_tuya_config SET {', '.join(updates)} WHERE id = 1",
            tuple(params),
        )
        if device_id:
            cursor.execute(
                """
                INSERT INTO haccp_tuya_lecturas (
                    device_id, device_name, temperatura, humedad, origen, leida_en
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    _normalizar_texto_simple(device_id, 120),
                    _normalizar_texto_simple(device_name, 200),
                    temp_num,
                    hum_num,
                    "manual",
                    marca_tiempo,
                ),
            )
            cursor.execute(
                """
                DELETE FROM haccp_tuya_lecturas
                WHERE datetime(leida_en) < datetime('now', 'localtime', '-7 day')
                """
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return obtener_config_tuya_haccp()


def guardar_vinculaciones_tuya_haccp(bindings, auto_interval_min=None):
    payload = {"device_bindings": bindings}
    if auto_interval_min is not None:
        payload["auto_interval_min"] = auto_interval_min
    return guardar_config_tuya_haccp(payload)


def obtener_vinculaciones_tuya_haccp():
    cfg = obtener_config_tuya_haccp()
    return cfg.get("device_bindings") if isinstance(cfg.get("device_bindings"), list) else []


def registrar_lectura_tuya_haccp(
    *,
    device_id,
    device_name="",
    temperatura=None,
    humedad=None,
    punto_id=None,
    origen="auto",
    leida_en=None,
):
    if not device_id:
        raise ValueError("device_id es obligatorio para registrar lectura Tuya")

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT OR IGNORE INTO haccp_tuya_config (id, habilitado) VALUES (1, 0)"
        )

        marca_tiempo = (
            _normalizar_texto_simple(leida_en, 30)
            if leida_en
            else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        temp_num = None if temperatura is None else float(temperatura)
        hum_num = None if humedad is None else float(humedad)
        punto_val = None
        if punto_id not in (None, "", 0, "0"):
            try:
                punto_int = int(punto_id)
                punto_val = punto_int if punto_int > 0 else None
            except Exception:
                punto_val = None

        cursor.execute(
            """
            INSERT INTO haccp_tuya_lecturas (
                device_id, device_name, punto_id, temperatura, humedad, origen, leida_en
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _normalizar_texto_simple(device_id, 120),
                _normalizar_texto_simple(device_name, 200),
                punto_val,
                temp_num,
                hum_num,
                _normalizar_texto_simple(origen, 30) or "auto",
                marca_tiempo,
            ),
        )
        cursor.execute(
            """
            DELETE FROM haccp_tuya_lecturas
            WHERE datetime(leida_en) < datetime('now', 'localtime', '-7 day')
            """
        )
        cursor.execute(
            """
            UPDATE haccp_tuya_config
            SET device_id = ?,
                device_name = ?,
                ultimo_temp = ?,
                ultima_humedad = ?,
                ultima_lectura_en = ?,
                actualizado = CURRENT_TIMESTAMP
            WHERE id = 1
            """,
            (
                _normalizar_texto_simple(device_id, 120),
                _normalizar_texto_simple(device_name, 200),
                temp_num,
                hum_num,
                marca_tiempo,
            ),
        )
        conn.commit()
        return {
            "device_id": _normalizar_texto_simple(device_id, 120),
            "device_name": _normalizar_texto_simple(device_name, 200),
            "punto_id": punto_val,
            "temperatura": temp_num,
            "humedad": hum_num,
            "origen": _normalizar_texto_simple(origen, 30) or "auto",
            "leida_en": marca_tiempo,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def obtener_historial_tuya_haccp(device_id=None, dias=7, agrupado_por_hora=True, limit=5000):
    dias_int = max(1, min(30, int(dias or 7)))
    limit_int = max(1, min(20000, int(limit or 5000)))
    device = _normalizar_texto_simple(device_id, 120)

    conn = get_db()
    cursor = conn.cursor()
    try:
        params = [f"-{dias_int} day"]
        where = ["datetime(leida_en) >= datetime('now', 'localtime', ?)"]
        if device:
            where.append("device_id = ?")
            params.append(device)
        where_sql = " AND ".join(where)

        if agrupado_por_hora:
            cursor.execute(
                f"""
                SELECT
                    strftime('%Y-%m-%d %H:00:00', leida_en) AS hora,
                    AVG(temperatura) AS temperatura,
                    AVG(humedad) AS humedad,
                    COUNT(*) AS muestras
                FROM haccp_tuya_lecturas
                WHERE {where_sql}
                GROUP BY strftime('%Y-%m-%d %H:00:00', leida_en)
                ORDER BY hora ASC
                """,
                tuple(params),
            )
            return [dict(r) for r in cursor.fetchall()]

        params.append(limit_int)
        cursor.execute(
            f"""
            SELECT
                device_id,
                device_name,
                punto_id,
                temperatura,
                humedad,
                origen,
                leida_en
            FROM haccp_tuya_lecturas
            WHERE {where_sql}
            ORDER BY leida_en ASC
            LIMIT ?
            """,
            tuple(params),
        )
        return [dict(r) for r in cursor.fetchall()]
    finally:
        conn.close()


def _ventana_recordatorio(evento):
    fecha = evento.get("fecha")
    hora = evento.get("hora_inicio") or "00:00"
    return f"{fecha}T{hora}"


def limpiar_recordatorios_descartados(meses=6):
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "DELETE FROM agenda_recordatorios_descartados WHERE datetime(descartado_en) < datetime('now', ?)",
            (f"-{max(1, int(meses or 6))} months",),
        )
        conn.commit()
    finally:
        conn.close()


def descartar_recordatorio_agenda(evento_id, ventana_clave):
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT OR REPLACE INTO agenda_recordatorios_descartados (evento_id, ventana_clave, descartado_en)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            """,
            (evento_id, ventana_clave),
        )
        conn.commit()
        return True
    finally:
        conn.close()


def obtener_recordatorios_agenda_pendientes():
    conn = get_db()
    cursor = conn.cursor()
    try:
        cfg = obtener_config_alertas()
        if not cfg.get("incluir_agenda", 1):
            return []

        cursor.execute(
            """
            SELECT * FROM agenda_eventos
            WHERE estado = 'pendiente'
              AND tipo <> 'bloqueo'
              AND datetime(fecha || ' ' || COALESCE(hora_inicio, '00:00')) >= datetime('now', '-1 day')
              AND datetime(fecha || ' ' || COALESCE(hora_inicio, '00:00')) <= datetime('now', '+' || ? || ' days')
            ORDER BY fecha, hora_inicio
            """,
            (max(1, int(cfg.get("dias_anticipacion", 2) or 2)),),
        )
        eventos = [dict(r) for r in cursor.fetchall()]

        pendientes = []
        for evento in eventos:
            ventana = _ventana_recordatorio(evento)
            cursor.execute(
                "SELECT 1 FROM agenda_recordatorios_descartados WHERE evento_id = ? AND ventana_clave = ?",
                (evento["id"], ventana),
            )
            if cursor.fetchone():
                continue
            evento["ventana_clave"] = ventana
            pendientes.append(evento)

        return pendientes
    finally:
        conn.close()


def _sembrar_haccp_base(conn):
    if not _table_exists(conn, "haccp_puntos_control"):
        return
    row = conn.execute("SELECT COUNT(*) AS total FROM haccp_puntos_control").fetchone()
    if row is None:
        total = 0
    elif isinstance(row, sqlite3.Row):
        total = int(row["total"] or 0)
    else:
        total = int(row[0] or 0)
    if total > 0:
        return

    base = [
        {
            "nombre": "Vitrina refrigerada",
            "categoria": "Cadena de frio",
            "tipo_control": "rango",
            "frecuencia_horas": 2,
            "limite_min": 0,
            "limite_max": 5,
            "unidad": "C",
        },
        {
            "nombre": "Congelador",
            "categoria": "Cadena de frio",
            "tipo_control": "rango",
            "frecuencia_horas": 4,
            "limite_min": -25,
            "limite_max": -15,
            "unidad": "C",
        },
        {
            "nombre": "Rellenos y cremas en uso",
            "categoria": "Produccion",
            "tipo_control": "rango",
            "frecuencia_horas": 2,
            "limite_min": 0,
            "limite_max": 8,
            "unidad": "C",
        },
        {
            "nombre": "Limpieza de superficies",
            "categoria": "Higiene",
            "tipo_control": "check",
            "frecuencia_horas": 4,
            "limite_min": None,
            "limite_max": None,
            "unidad": "",
        },
        {
            "nombre": "Higiene de manos y utensilios",
            "categoria": "Higiene",
            "tipo_control": "check",
            "frecuencia_horas": 3,
            "limite_min": None,
            "limite_max": None,
            "unidad": "",
        },
        {
            "nombre": "Recepcion de insumos refrigerados",
            "categoria": "Recepcion",
            "tipo_control": "rango",
            "frecuencia_horas": 8,
            "limite_min": 0,
            "limite_max": 7,
            "unidad": "C",
        },
    ]
    for idx, punto in enumerate(base):
        conn.execute(
            """
            INSERT INTO haccp_puntos_control (
                nombre, categoria, tipo_control, frecuencia_horas,
                limite_min, limite_max, unidad, activo, orden
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)
            """,
            (
                punto["nombre"],
                punto["categoria"],
                punto["tipo_control"],
                int(punto["frecuencia_horas"]),
                punto["limite_min"],
                punto["limite_max"],
                punto["unidad"],
                (idx + 1) * 10,
            ),
        )


def _haccp_to_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    txt = str(value).strip().lower()
    return txt in {"1", "true", "si", "yes", "ok", "cumple"}


def _haccp_int(value, default=0, min_value=None, max_value=None):
    try:
        num = int(value)
    except (TypeError, ValueError):
        num = int(default)
    if min_value is not None:
        num = max(min_value, num)
    if max_value is not None:
        num = min(max_value, num)
    return num


def _haccp_float_or_none(value):
    if value is None:
        return None
    txt = str(value).strip().replace(",", ".")
    if txt == "":
        return None
    return float(txt)


def _haccp_tipo_control(value):
    tipo = str(value or "").strip().lower()
    if tipo in {"check", "checklist", "booleano", "bool", "si_no"}:
        return "check"
    return "rango"


def _haccp_normalizar_punto_payload(data, partial=False):
    payload = data or {}
    nombre = str(payload.get("nombre") or "").strip()
    if not partial and not nombre:
        raise ValueError("Nombre del punto HACCP es obligatorio")
    if nombre and len(nombre) > 120:
        raise ValueError("Nombre del punto HACCP demasiado largo")

    categoria = str(payload.get("categoria") or "General").strip() or "General"
    tipo_control = _haccp_tipo_control(payload.get("tipo_control"))
    frecuencia_horas = _haccp_int(payload.get("frecuencia_horas", 4), default=4, min_value=1, max_value=168)
    limite_min = _haccp_float_or_none(payload.get("limite_min"))
    limite_max = _haccp_float_or_none(payload.get("limite_max"))
    unidad = str(payload.get("unidad") or "").strip()[:20]
    activo = 1 if _haccp_to_bool(payload.get("activo", True)) else 0
    orden = _haccp_int(payload.get("orden", 100), default=100, min_value=0, max_value=9999)

    if tipo_control == "rango":
        if not partial and limite_min is None and limite_max is None:
            raise ValueError("Define un limite minimo o maximo para control por rango")
        if limite_min is not None and limite_max is not None and limite_min > limite_max:
            raise ValueError("Limite minimo no puede ser mayor que limite maximo")
    else:
        limite_min = None
        limite_max = None
        unidad = ""

    resultado = {
        "nombre": nombre,
        "categoria": categoria[:60],
        "tipo_control": tipo_control,
        "frecuencia_horas": frecuencia_horas,
        "limite_min": limite_min,
        "limite_max": limite_max,
        "unidad": unidad,
        "activo": activo,
        "orden": orden,
    }
    if partial:
        return {k: v for k, v in resultado.items() if k != "nombre" or v}
    return resultado


def _haccp_parse_datetime(valor):
    txt = str(valor or "").strip()
    if not txt:
        return None
    formatos = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S")
    for fmt in formatos:
        try:
            return datetime.strptime(txt, fmt)
        except ValueError:
            continue
    return None


def _haccp_estado_punto(row):
    tipo_control = _haccp_tipo_control(row.get("tipo_control"))
    ultimo_registro = row.get("ultimo_registrado_en")
    ultimo_cumple = row.get("ultimo_cumple")
    vencido = bool(row.get("vencido"))
    if not ultimo_registro:
        return "sin_registro"
    if vencido:
        return "vencido"
    if tipo_control == "check":
        return "ok" if int(ultimo_cumple or 0) == 1 else "desvio"
    return "ok" if int(ultimo_cumple or 0) == 1 else "desvio"


def listar_haccp_puntos(incluir_inactivos=False):
    conn = get_db()
    cursor = conn.cursor()
    try:
        where = "1=1" if incluir_inactivos else "p.activo = 1"
        cursor.execute(
            f"""
            SELECT
                p.*,
                u.id AS ultimo_registro_id,
                u.valor AS ultimo_valor,
                u.cumple AS ultimo_cumple,
                u.observacion AS ultimo_observacion,
                u.accion_correctiva AS ultima_accion_correctiva,
                u.responsable AS ultimo_responsable,
                u.registrado_en AS ultimo_registrado_en,
                CASE
                    WHEN u.registrado_en IS NULL THEN 1
                    WHEN datetime(u.registrado_en, '+' || p.frecuencia_horas || ' hours') <= datetime('now', 'localtime') THEN 1
                    ELSE 0
                END AS vencido,
                datetime(
                    COALESCE(u.registrado_en, datetime('now', 'localtime', '-' || p.frecuencia_horas || ' hours')),
                    '+' || p.frecuencia_horas || ' hours'
                ) AS proximo_control_en
            FROM haccp_puntos_control p
            LEFT JOIN (
                SELECT r1.*
                FROM haccp_registros r1
                JOIN (
                    SELECT punto_id, MAX(id) AS max_id
                    FROM haccp_registros
                    GROUP BY punto_id
                ) x ON x.max_id = r1.id
            ) u ON u.punto_id = p.id
            WHERE {where}
            ORDER BY p.activo DESC, p.orden ASC, p.id ASC
            """
        )
        filas = []
        ahora = datetime.now()
        for row in cursor.fetchall():
            item = dict(row)
            item["activo"] = int(item.get("activo") or 0)
            item["frecuencia_horas"] = int(item.get("frecuencia_horas") or 1)
            item["tipo_control"] = _haccp_tipo_control(item.get("tipo_control"))
            item["vencido"] = bool(item.get("vencido"))
            item["estado"] = _haccp_estado_punto(item)
            item["ultimo_cumple"] = int(item.get("ultimo_cumple") or 0) if item.get("ultimo_registro_id") else None
            ultima_fecha = _haccp_parse_datetime(item.get("ultimo_registrado_en"))
            if ultima_fecha:
                delta_horas = (ahora - ultima_fecha).total_seconds() / 3600.0
                item["horas_desde_ultimo"] = round(max(0.0, delta_horas), 2)
            else:
                item["horas_desde_ultimo"] = None
            filas.append(item)
        return filas
    finally:
        conn.close()


def obtener_haccp_registros(limit=80, punto_id=None):
    conn = get_db()
    cursor = conn.cursor()
    try:
        limit = _haccp_int(limit, default=80, min_value=1, max_value=500)
        sql = """
            SELECT
                r.*,
                p.nombre AS punto_nombre,
                p.categoria AS punto_categoria,
                p.tipo_control,
                p.unidad
            FROM haccp_registros r
            JOIN haccp_puntos_control p ON p.id = r.punto_id
        """
        params = []
        if punto_id:
            sql += " WHERE r.punto_id = ?"
            params.append(int(punto_id))
        sql += " ORDER BY r.registrado_en DESC, r.id DESC LIMIT ?"
        params.append(limit)
        cursor.execute(sql, tuple(params))
        filas = []
        for row in cursor.fetchall():
            item = dict(row)
            item["cumple"] = int(item.get("cumple") or 0)
            item["tipo_control"] = _haccp_tipo_control(item.get("tipo_control"))
            filas.append(item)
        return filas
    finally:
        conn.close()


def _haccp_normalizar_mes(valor):
    raw = str(valor or "").strip()
    if not raw:
        return None
    candidato = raw[:7]
    try:
        return datetime.strptime(candidato, "%Y-%m").strftime("%Y-%m")
    except ValueError:
        raise ValueError("Mes invalido. Usa formato AAAA-MM.")


def obtener_haccp_trazabilidad_insumos(limit=250, mes=None, fecha_desde=None, fecha_hasta=None, busqueda=None):
    conn = get_db()
    cursor = conn.cursor()
    try:
        limit = _haccp_int(limit, default=250, min_value=1, max_value=2000)
        mes_val = _haccp_normalizar_mes(mes)
        desde_val = _normalizar_fecha_iso(fecha_desde, "Fecha desde") if str(fecha_desde or "").strip() else None
        hasta_val = _normalizar_fecha_iso(fecha_hasta, "Fecha hasta") if str(fecha_hasta or "").strip() else None
        if desde_val and hasta_val and hasta_val < desde_val:
            raise ValueError("La fecha hasta no puede ser menor que la fecha desde")

        where = []
        params = []
        if mes_val:
            where.append("t.mes_clave = ?")
            params.append(mes_val)
        if desde_val:
            where.append("date(t.producido_en) >= date(?)")
            params.append(desde_val)
        if hasta_val:
            where.append("date(t.producido_en) <= date(?)")
            params.append(hasta_val)

        buscar = str(busqueda or "").strip().lower()
        if buscar:
            like = f"%{buscar}%"
            where.append(
                """
                (
                    LOWER(COALESCE(t.insumo_nombre, '')) LIKE ? OR
                    LOWER(COALESCE(t.insumo_lote_codigo, '')) LIKE ? OR
                    LOWER(COALESCE(il.lote_codigo, '')) LIKE ? OR
                    LOWER(COALESCE(t.producto_nombre, '')) LIKE ? OR
                    LOWER(COALESCE(i.nombre, '')) LIKE ? OR
                    LOWER(COALESCE(p.nombre, '')) LIKE ?
                )
                """
            )
            params.extend([like, like, like, like, like, like])

        where_sql = ""
        if where:
            where_sql = "WHERE " + " AND ".join(where)

        cursor.execute(
            f"""
            SELECT
                t.*,
                COALESCE(NULLIF(TRIM(t.insumo_lote_codigo), ''), NULLIF(TRIM(il.lote_codigo), '')) AS insumo_lote_codigo_resuelto,
                COALESCE(t.insumo_fecha_elaboracion, il.fecha_elaboracion) AS insumo_fecha_elaboracion_resuelta,
                COALESCE(t.insumo_fecha_vencimiento, il.fecha_vencimiento) AS insumo_fecha_vencimiento_resuelta,
                COALESCE(t.insumo_fecha_ingreso, il.fecha_ingreso) AS insumo_fecha_ingreso_resuelta,
                COALESCE(NULLIF(TRIM(i.nombre), ''), t.insumo_nombre, 'Insumo #' || t.insumo_id) AS insumo_nombre_resuelto,
                COALESCE(NULLIF(TRIM(p.nombre), ''), t.producto_nombre, 'Producto #' || t.producto_id) AS producto_nombre_resuelto
            FROM haccp_trazabilidad_insumos t
            LEFT JOIN insumos i ON i.id = t.insumo_id
            LEFT JOIN productos p ON p.id = t.producto_id
            LEFT JOIN insumo_lotes il ON il.id = t.insumo_lote_id
            {where_sql}
            ORDER BY datetime(t.producido_en) DESC, t.id DESC
            LIMIT ?
            """,
            tuple(params + [limit]),
        )
        filas = [dict(r) for r in cursor.fetchall()]

        cursor.execute(
            """
            SELECT DISTINCT mes_clave
            FROM haccp_trazabilidad_insumos
            WHERE mes_clave IS NOT NULL AND TRIM(mes_clave) <> ''
            ORDER BY mes_clave DESC
            LIMIT 36
            """
        )
        meses_disponibles = [str(r["mes_clave"]) for r in cursor.fetchall() if r["mes_clave"]]

        if not filas:
            return {"lotes": [], "meses_disponibles": meses_disponibles}

        producto_lote_ids = sorted(
            {
                int(r["producto_lote_id"])
                for r in filas
                if r.get("producto_lote_id") not in (None, "")
            }
        )
        producto_ids = sorted(
            {
                int(r["producto_id"])
                for r in filas
                if r.get("producto_id") not in (None, "")
            }
        )

        ventas_por_lote = {}
        if producto_lote_ids:
            placeholders = ",".join(["?"] * len(producto_lote_ids))
            cursor.execute(
                f"""
                SELECT
                    vl.lote_id,
                    vl.cantidad_usada,
                    v.id AS venta_id,
                    v.fecha_hora,
                    v.codigo_pedido
                FROM venta_lotes vl
                JOIN ventas v ON v.id = vl.venta_id
                WHERE vl.lote_id IN ({placeholders})
                ORDER BY datetime(v.fecha_hora) DESC, v.id DESC
                """,
                tuple(producto_lote_ids),
            )
            for row in cursor.fetchall():
                lote_id = int(row["lote_id"])
                ventas_por_lote.setdefault(lote_id, []).append(
                    {
                        "venta_id": int(row["venta_id"]),
                        "fecha_hora": row["fecha_hora"],
                        "codigo_pedido": row["codigo_pedido"],
                        "cantidad": float(row["cantidad_usada"] or 0),
                    }
                )

        mermas_por_lote = {}
        if producto_ids and producto_lote_ids:
            placeholders = ",".join(["?"] * len(producto_ids))
            cursor.execute(
                f"""
                SELECT id, producto_id, cantidad, motivo, detalle, lotes_json, creado, estado
                FROM producto_mermas
                WHERE producto_id IN ({placeholders})
                  AND COALESCE(estado, 'activa') = 'activa'
                ORDER BY datetime(creado) DESC, id DESC
                """,
                tuple(producto_ids),
            )
            ids_lote_set = set(producto_lote_ids)
            for row in cursor.fetchall():
                lotes = _cargar_lotes_merma(row["lotes_json"])
                for item in lotes:
                    lote_id_raw = item.get("lote_id")
                    if lote_id_raw in (None, ""):
                        continue
                    try:
                        lote_id = int(lote_id_raw)
                    except (TypeError, ValueError):
                        continue
                    if lote_id not in ids_lote_set:
                        continue
                    mermas_por_lote.setdefault(lote_id, []).append(
                        {
                            "merma_id": int(row["id"]),
                            "fecha_hora": row["creado"],
                            "motivo": row["motivo"],
                            "detalle": row["detalle"],
                            "cantidad": float(item.get("cantidad_usada") or 0),
                        }
                    )

        grupos = {}
        for row in filas:
            insumo_id = int(row.get("insumo_id") or 0)
            insumo_lote_id = row.get("insumo_lote_id")
            insumo_lote_id_val = int(insumo_lote_id) if insumo_lote_id not in (None, "") else None
            insumo_lote_codigo = (row.get("insumo_lote_codigo_resuelto") or "").strip() or None
            insumo_fecha_ingreso = row.get("insumo_fecha_ingreso_resuelta") or row.get("insumo_fecha_ingreso")
            insumo_fecha_elaboracion = row.get("insumo_fecha_elaboracion_resuelta") or row.get("insumo_fecha_elaboracion")
            insumo_fecha_vencimiento = row.get("insumo_fecha_vencimiento_resuelta") or row.get("insumo_fecha_vencimiento")
            if insumo_lote_id_val is not None:
                key = ("lote_id", insumo_id, insumo_lote_id_val)
            else:
                key = (
                    "sin_id",
                    insumo_id,
                    insumo_lote_codigo,
                    insumo_fecha_ingreso,
                    insumo_fecha_elaboracion,
                    insumo_fecha_vencimiento,
                )

            grupo = grupos.get(key)
            if not grupo:
                grupo = {
                    "insumo_id": insumo_id,
                    "insumo_nombre": row.get("insumo_nombre_resuelto") or row.get("insumo_nombre") or f"Insumo #{insumo_id}",
                    "insumo_lote_id": insumo_lote_id_val,
                    "insumo_lote_codigo": insumo_lote_codigo,
                    "insumo_fecha_ingreso": insumo_fecha_ingreso,
                    "insumo_fecha_elaboracion": insumo_fecha_elaboracion,
                    "insumo_fecha_vencimiento": insumo_fecha_vencimiento,
                    "cantidad_total_usada": 0.0,
                    "unidad_referencia": row.get("unidad_insumo") or "unidad",
                    "productos": [],
                    "meses": set(),
                    "_productos_map": {},
                    "_fecha_max": row.get("producido_en"),
                }
                grupos[key] = grupo
            else:
                if not grupo.get("insumo_lote_codigo") and insumo_lote_codigo:
                    grupo["insumo_lote_codigo"] = insumo_lote_codigo
                if not grupo.get("insumo_fecha_ingreso") and insumo_fecha_ingreso:
                    grupo["insumo_fecha_ingreso"] = insumo_fecha_ingreso
                if not grupo.get("insumo_fecha_elaboracion") and insumo_fecha_elaboracion:
                    grupo["insumo_fecha_elaboracion"] = insumo_fecha_elaboracion
                if not grupo.get("insumo_fecha_vencimiento") and insumo_fecha_vencimiento:
                    grupo["insumo_fecha_vencimiento"] = insumo_fecha_vencimiento

            cantidad_usada = float(row.get("cantidad_insumo_usada") or 0)
            grupo["cantidad_total_usada"] += cantidad_usada
            if row.get("mes_clave"):
                grupo["meses"].add(str(row.get("mes_clave")))

            if row.get("producido_en") and (
                not grupo["_fecha_max"] or str(row.get("producido_en")) > str(grupo["_fecha_max"])
            ):
                grupo["_fecha_max"] = row.get("producido_en")

            producto_lote_id = row.get("producto_lote_id")
            producto_lote_id_val = int(producto_lote_id) if producto_lote_id not in (None, "") else None
            producto_key = (
                int(row.get("produccion_id") or 0),
                int(row.get("producto_id") or 0),
                producto_lote_id_val,
            )
            producto_item = grupo["_productos_map"].get(producto_key)
            if not producto_item:
                ventas = ventas_por_lote.get(producto_lote_id_val, []) if producto_lote_id_val else []
                mermas = mermas_por_lote.get(producto_lote_id_val, []) if producto_lote_id_val else []
                producto_item = {
                    "produccion_id": int(row.get("produccion_id") or 0),
                    "receta_id": int(row.get("receta_id") or 0) if row.get("receta_id") not in (None, "") else None,
                    "producto_id": int(row.get("producto_id") or 0),
                    "producto_nombre": row.get("producto_nombre_resuelto") or row.get("producto_nombre") or f"Producto #{row.get('producto_id')}",
                    "producto_lote_id": producto_lote_id_val,
                    "producto_fecha_elaboracion": row.get("producto_fecha_elaboracion"),
                    "producto_fecha_vencimiento": row.get("producto_fecha_vencimiento"),
                    "producido_en": row.get("producido_en"),
                    "mes_clave": row.get("mes_clave"),
                    "cantidad_lote_producto": float(row.get("cantidad_producto_lote") or 0),
                    "cantidad_insumo_usada": 0.0,
                    "unidad_insumo": row.get("unidad_insumo") or "unidad",
                    "ventas": ventas[:12],
                    "mermas": mermas[:12],
                    "ultima_venta": ventas[0]["fecha_hora"] if ventas else None,
                    "ultimo_descarte": mermas[0]["fecha_hora"] if mermas else None,
                }
                grupo["_productos_map"][producto_key] = producto_item
                grupo["productos"].append(producto_item)

            producto_item["cantidad_insumo_usada"] += cantidad_usada

        lotes = []
        for grupo in grupos.values():
            grupo["cantidad_total_usada"] = round(float(grupo["cantidad_total_usada"] or 0), 4)
            for prod in grupo["productos"]:
                prod["cantidad_insumo_usada"] = round(float(prod.get("cantidad_insumo_usada") or 0), 4)
            grupo["productos"].sort(
                key=lambda p: (str(p.get("producido_en") or ""), int(p.get("producto_lote_id") or 0)),
                reverse=True,
            )
            grupo["productos_total"] = len(grupo["productos"])
            grupo["meses"] = sorted(list(grupo["meses"]), reverse=True)
            grupo["fecha_referencia"] = grupo["_fecha_max"]
            grupo.pop("_productos_map", None)
            grupo.pop("_fecha_max", None)
            lotes.append(grupo)

        lotes.sort(key=lambda g: str(g.get("fecha_referencia") or ""), reverse=True)
        return {"lotes": lotes, "meses_disponibles": meses_disponibles}
    finally:
        conn.close()


def contar_haccp_vencidos(conn=None):
    own_conn = False
    if conn is None:
        conn = get_db()
        own_conn = True
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COUNT(*) AS total
            FROM haccp_puntos_control p
            LEFT JOIN (
                SELECT punto_id, MAX(registrado_en) AS ultimo_registrado_en
                FROM haccp_registros
                GROUP BY punto_id
            ) u ON u.punto_id = p.id
            WHERE p.activo = 1
              AND (
                  u.ultimo_registrado_en IS NULL OR
                  datetime(u.ultimo_registrado_en, '+' || p.frecuencia_horas || ' hours') <= datetime('now', 'localtime')
              )
            """
        )
        return int(cursor.fetchone()["total"] or 0)
    finally:
        if own_conn:
            conn.close()


def obtener_haccp_puntos_vencidos(limit=20, conn=None):
    own_conn = False
    if conn is None:
        conn = get_db()
        own_conn = True
    try:
        limit = _haccp_int(limit, default=20, min_value=1, max_value=200)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                p.id,
                p.nombre,
                p.categoria,
                p.frecuencia_horas,
                p.tipo_control,
                p.unidad,
                u.ultimo_registrado_en,
                u.ultimo_cumple,
                datetime(
                    COALESCE(u.ultimo_registrado_en, datetime('now', 'localtime', '-' || p.frecuencia_horas || ' hours')),
                    '+' || p.frecuencia_horas || ' hours'
                ) AS proximo_control_en
            FROM haccp_puntos_control p
            LEFT JOIN (
                SELECT r1.punto_id, r1.registrado_en AS ultimo_registrado_en, r1.cumple AS ultimo_cumple
                FROM haccp_registros r1
                JOIN (
                    SELECT punto_id, MAX(id) AS max_id
                    FROM haccp_registros
                    GROUP BY punto_id
                ) x ON x.max_id = r1.id
            ) u ON u.punto_id = p.id
            WHERE p.activo = 1
              AND (
                  u.ultimo_registrado_en IS NULL OR
                  datetime(u.ultimo_registrado_en, '+' || p.frecuencia_horas || ' hours') <= datetime('now', 'localtime')
              )
            ORDER BY p.categoria ASC, p.orden ASC, p.id ASC
            LIMIT ?
            """,
            (limit,),
        )
        filas = [dict(r) for r in cursor.fetchall()]
        for item in filas:
            item["tipo_control"] = _haccp_tipo_control(item.get("tipo_control"))
            item["ultimo_cumple"] = int(item.get("ultimo_cumple") or 0) if item.get("ultimo_registrado_en") else None
        return filas
    finally:
        if own_conn:
            conn.close()


def obtener_resumen_haccp(conn=None):
    own_conn = False
    if conn is None:
        conn = get_db()
        own_conn = True
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) AS total FROM haccp_puntos_control WHERE activo = 1")
        total_puntos = int(cursor.fetchone()["total"] or 0)

        cursor.execute("SELECT COUNT(*) AS total FROM haccp_registros WHERE date(registrado_en) = date('now', 'localtime')")
        controles_hoy = int(cursor.fetchone()["total"] or 0)

        cursor.execute(
            """
            SELECT COUNT(*) AS total
            FROM haccp_registros
            WHERE date(registrado_en) = date('now', 'localtime')
              AND cumple = 0
            """
        )
        desvios_hoy = int(cursor.fetchone()["total"] or 0)

        vencidos = contar_haccp_vencidos(conn=conn)
        puntos_al_dia = max(0, total_puntos - vencidos)
        if controles_hoy > 0:
            cumplimiento_hoy = round(max(0.0, ((controles_hoy - desvios_hoy) / controles_hoy) * 100.0), 1)
        else:
            cumplimiento_hoy = 100.0

        return {
            "total_puntos": total_puntos,
            "puntos_al_dia": puntos_al_dia,
            "puntos_vencidos": vencidos,
            "controles_hoy": controles_hoy,
            "desvios_hoy": desvios_hoy,
            "cumplimiento_hoy": cumplimiento_hoy,
        }
    finally:
        if own_conn:
            conn.close()


def crear_haccp_punto(data):
    payload = _haccp_normalizar_punto_payload(data, partial=False)
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO haccp_puntos_control (
                nombre, categoria, tipo_control, frecuencia_horas,
                limite_min, limite_max, unidad, activo, orden, actualizado
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                payload["nombre"],
                payload["categoria"],
                payload["tipo_control"],
                payload["frecuencia_horas"],
                payload["limite_min"],
                payload["limite_max"],
                payload["unidad"],
                payload["activo"],
                payload["orden"],
            ),
        )
        punto_id = int(cursor.lastrowid)
        conn.commit()
        return {"success": True, "id": punto_id}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def actualizar_haccp_punto(punto_id, data):
    punto_id = int(punto_id)
    payload = _haccp_normalizar_punto_payload(data, partial=False)
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id FROM haccp_puntos_control WHERE id = ?", (punto_id,))
        if not cursor.fetchone():
            raise ValueError("Punto HACCP no encontrado")

        cursor.execute(
            """
            UPDATE haccp_puntos_control
            SET nombre = ?, categoria = ?, tipo_control = ?, frecuencia_horas = ?,
                limite_min = ?, limite_max = ?, unidad = ?, activo = ?, orden = ?,
                actualizado = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                payload["nombre"],
                payload["categoria"],
                payload["tipo_control"],
                payload["frecuencia_horas"],
                payload["limite_min"],
                payload["limite_max"],
                payload["unidad"],
                payload["activo"],
                payload["orden"],
                punto_id,
            ),
        )
        conn.commit()
        return {"success": True, "id": punto_id}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def cambiar_estado_haccp_punto(punto_id, activo):
    punto_id = int(punto_id)
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            UPDATE haccp_puntos_control
            SET activo = ?, actualizado = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (1 if _haccp_to_bool(activo) else 0, punto_id),
        )
        if cursor.rowcount <= 0:
            raise ValueError("Punto HACCP no encontrado")
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def registrar_haccp_control(data):
    payload = data or {}
    punto_id = _haccp_int(payload.get("punto_id"), default=0, min_value=0)
    if punto_id <= 0:
        raise ValueError("Selecciona un punto HACCP valido")

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM haccp_puntos_control WHERE id = ?", (punto_id,))
        punto_row = cursor.fetchone()
        if not punto_row:
            raise ValueError("Punto HACCP no encontrado")
        punto = dict(punto_row)

        tipo = _haccp_tipo_control(punto.get("tipo_control"))
        valor = _haccp_float_or_none(payload.get("valor"))
        cumple_forzado_raw = payload.get("cumple")
        cumple_forzado = None if cumple_forzado_raw is None else _haccp_to_bool(cumple_forzado_raw)

        if tipo == "rango":
            if valor is None:
                raise ValueError("Debes ingresar un valor medido")
            limite_min = _haccp_float_or_none(punto.get("limite_min"))
            limite_max = _haccp_float_or_none(punto.get("limite_max"))
            cumple = True
            if limite_min is not None and valor < limite_min:
                cumple = False
            if limite_max is not None and valor > limite_max:
                cumple = False
        else:
            if cumple_forzado is None:
                raise ValueError("Indica si el control cumple o no")
            cumple = bool(cumple_forzado)

        observacion = str(payload.get("observacion") or "").strip()[:300]
        accion_correctiva = str(payload.get("accion_correctiva") or "").strip()[:300]
        responsable = str(payload.get("responsable") or "").strip()[:80]

        if not cumple and not accion_correctiva:
            raise ValueError("Debe registrar accion correctiva cuando hay desvio")

        registrado_en = obtener_hora_chile().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute(
            """
            INSERT INTO haccp_registros (
                punto_id, valor, cumple, observacion, accion_correctiva, responsable, registrado_en
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                punto_id,
                valor,
                1 if cumple else 0,
                observacion,
                accion_correctiva,
                responsable,
                registrado_en,
            ),
        )
        registro_id = int(cursor.lastrowid)
        conn.commit()
        return {
            "success": True,
            "registro_id": registro_id,
            "punto_id": punto_id,
            "cumple": bool(cumple),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _reporte_filtro_fechas(base_sql, fecha_desde=None, fecha_hasta=None):
    where = []
    params = []
    if fecha_desde:
        where.append("date(creado) >= date(?)")
        params.append(fecha_desde)
    if fecha_hasta:
        where.append("date(creado) <= date(?)")
        params.append(fecha_hasta)

    if where:
        base_sql += " WHERE " + " AND ".join(where)
    return base_sql, params


def obtener_reporte_produccion(fecha_desde=None, fecha_hasta=None):
    conn = get_db()
    cursor = conn.cursor()
    try:
        sql = """
            SELECT p.id, p.fecha_hora AS creado, r.nombre AS receta_nombre,
                   p.cantidad, p.cantidad_resultado
            FROM producciones p
            JOIN recetas r ON r.id = p.receta_id
        """
        where = []
        params = []
        if fecha_desde:
            where.append("date(p.fecha_hora) >= date(?)")
            params.append(fecha_desde)
        if fecha_hasta:
            where.append("date(p.fecha_hora) <= date(?)")
            params.append(fecha_hasta)
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY p.fecha_hora DESC"

        cursor.execute(sql, params)
        return [dict(r) for r in cursor.fetchall()]
    finally:
        conn.close()


def obtener_reporte_insumos_agregados(fecha_desde=None, fecha_hasta=None):
    conn = get_db()
    cursor = conn.cursor()
    try:
        sql = """
            SELECT
                sl.id,
                sl.fecha AS creado,
                sl.recurso_tipo AS tipo_recurso,
                sl.recurso_id,
                sl.tipo_operacion AS accion,
                ABS(sl.cantidad_delta) AS cantidad,
                sl.stock_anterior,
                sl.stock_nuevo,
                sl.unidad,
                sl.lote_id,
                sl.origen_modulo,
                sl.codigo_operacion,
                sl.usuario,
                sl.referencia_tipo,
                sl.referencia_id,
                sl.detalle,
                sl.metadata_json,
                i.nombre AS nombre_recurso
            FROM stock_ledger sl
            LEFT JOIN insumos i ON i.id = sl.recurso_id
            WHERE sl.recurso_tipo = 'insumo'
              AND sl.tipo_operacion IN ('alta_scanner', 'entrada_scanner', 'ajuste_edicion', 'entrada_manual')
              AND sl.cantidad_delta > 0
        """
        params = []
        if fecha_desde:
            sql += " AND date(sl.fecha) >= date(?)"
            params.append(fecha_desde)
        if fecha_hasta:
            sql += " AND date(sl.fecha) <= date(?)"
            params.append(fecha_hasta)
        sql += " ORDER BY sl.fecha DESC, sl.id DESC"
        cursor.execute(sql, params)
        return [dict(r) for r in cursor.fetchall()]
    finally:
        conn.close()


def obtener_reporte_productos_agregados(fecha_desde=None, fecha_hasta=None):
    conn = get_db()
    cursor = conn.cursor()
    try:
        sql = """
            SELECT
                sl.id,
                sl.fecha AS creado,
                sl.recurso_tipo AS tipo_recurso,
                sl.recurso_id,
                sl.tipo_operacion AS accion,
                ABS(sl.cantidad_delta) AS cantidad,
                sl.stock_anterior,
                sl.stock_nuevo,
                sl.unidad,
                sl.lote_id,
                sl.origen_modulo,
                sl.codigo_operacion,
                sl.usuario,
                sl.referencia_tipo,
                sl.referencia_id,
                sl.detalle,
                sl.metadata_json,
                p.nombre AS nombre_recurso
            FROM stock_ledger sl
            LEFT JOIN productos p ON p.id = sl.recurso_id
            WHERE sl.recurso_tipo = 'producto'
              AND sl.tipo_operacion IN ('alta_lote', 'entrada_manual', 'produccion', 'ajuste_edicion')
              AND sl.cantidad_delta > 0
        """
        params = []
        if fecha_desde:
            sql += " AND date(sl.fecha) >= date(?)"
            params.append(fecha_desde)
        if fecha_hasta:
            sql += " AND date(sl.fecha) <= date(?)"
            params.append(fecha_hasta)
        sql += " ORDER BY sl.fecha DESC, sl.id DESC"
        cursor.execute(sql, params)
        return [dict(r) for r in cursor.fetchall()]
    finally:
        conn.close()


def obtener_reporte_mermas_productos(fecha_desde=None, fecha_hasta=None):
    conn = get_db()
    cursor = conn.cursor()
    try:
        sql = """
            SELECT pm.*, p.nombre AS producto_nombre
            FROM producto_mermas pm
            LEFT JOIN productos p ON p.id = pm.producto_id
            WHERE 1=1
        """
        params = []
        if fecha_desde:
            sql += " AND date(pm.creado) >= date(?)"
            params.append(fecha_desde)
        if fecha_hasta:
            sql += " AND date(pm.creado) <= date(?)"
            params.append(fecha_hasta)
        sql += " ORDER BY pm.creado DESC, pm.id DESC"

        cursor.execute(sql, params)
        rows = []
        for row in cursor.fetchall():
            item = dict(row)
            item["lotes_afectados"] = len(_cargar_lotes_merma(item.get("lotes_json")))
            rows.append(item)
        return rows
    finally:
        conn.close()


def obtener_resumen_mermas_por_fecha(fecha_desde=None, fecha_hasta=None):
    conn = get_db()
    cursor = conn.cursor()
    try:
        sql = """
            SELECT
                date(creado) AS fecha,
                COUNT(*) AS registros,
                SUM(cantidad) AS cantidad_bruta,
                SUM(CASE WHEN estado = 'revertida' THEN cantidad ELSE 0 END) AS cantidad_revertida,
                SUM(CASE WHEN estado = 'activa' THEN cantidad ELSE 0 END) AS cantidad_neta
            FROM producto_mermas
            WHERE 1=1
        """
        params = []
        if fecha_desde:
            sql += " AND date(creado) >= date(?)"
            params.append(fecha_desde)
        if fecha_hasta:
            sql += " AND date(creado) <= date(?)"
            params.append(fecha_hasta)
        sql += " GROUP BY date(creado) ORDER BY date(creado) ASC"

        cursor.execute(sql, params)
        return [dict(r) for r in cursor.fetchall()]
    finally:
        conn.close()


def obtener_kardex_movimientos(fecha_desde=None, fecha_hasta=None, tipo_recurso=None, recurso_id=None, limit=300):
    conn = get_db()
    cursor = conn.cursor()
    try:
        tope = max(1, min(int(limit or 300), 2000))
        sql = """
            SELECT
                sl.id,
                sl.fecha AS creado,
                sl.recurso_tipo AS tipo_recurso,
                sl.recurso_id,
                COALESCE(i.nombre, p.nombre, sl.recurso_tipo || ' #' || sl.recurso_id) AS nombre_recurso,
                sl.tipo_operacion AS accion,
                sl.cantidad_delta AS cantidad,
                sl.stock_anterior AS stock_anterior,
                sl.stock_nuevo AS stock_nuevo,
                sl.referencia_tipo,
                sl.referencia_id,
                sl.detalle,
                sl.unidad,
                sl.lote_id,
                sl.origen_modulo,
                sl.codigo_operacion,
                sl.usuario,
                sl.metadata_json
            FROM stock_ledger sl
            LEFT JOIN insumos i
                ON sl.recurso_tipo = 'insumo'
               AND i.id = sl.recurso_id
            LEFT JOIN productos p
                ON sl.recurso_tipo = 'producto'
               AND p.id = sl.recurso_id
            WHERE 1=1
        """
        params = []
        if fecha_desde:
            sql += " AND date(sl.fecha) >= date(?)"
            params.append(fecha_desde)
        if fecha_hasta:
            sql += " AND date(sl.fecha) <= date(?)"
            params.append(fecha_hasta)
        if tipo_recurso in {"insumo", "producto"}:
            sql += " AND sl.recurso_tipo = ?"
            params.append(tipo_recurso)
        if recurso_id:
            sql += " AND sl.recurso_id = ?"
            params.append(int(recurso_id))
        sql += " ORDER BY sl.fecha DESC, sl.id DESC LIMIT ?"
        params.append(tope)

        cursor.execute(sql, params)
        return [dict(r) for r in cursor.fetchall()]
    finally:
        conn.close()


def obtener_sugerencias_compra_insumos(dias_historico=30, dias_cobertura=14, limite=20):
    conn = get_db()
    cursor = conn.cursor()
    try:
        dias_historico = max(7, int(dias_historico or 30))
        dias_cobertura = max(1, int(dias_cobertura or 14))
        limite = max(1, min(int(limite or 20), 200))

        cursor.execute(
            """
            SELECT
                i.id,
                i.nombre,
                COALESCE(i.unidad, 'unidad') AS unidad,
                COALESCE(i.stock, 0) AS stock_actual,
                COALESCE(i.stock_minimo, 0) AS stock_minimo,
                COALESCE(c.consumo_historico, 0) AS consumo_historico
            FROM insumos i
            LEFT JOIN (
                SELECT
                    recurso_id,
                    SUM(ABS(cantidad_delta)) AS consumo_historico
                FROM stock_ledger
                WHERE recurso_tipo = 'insumo'
                  AND tipo_operacion IN ('salida_venta', 'consumo_receta', 'merma')
                  AND datetime(fecha) >= datetime('now', ?)
                GROUP BY recurso_id
            ) c ON c.recurso_id = i.id
            ORDER BY i.nombre ASC
            """,
            (f"-{dias_historico} days",),
        )

        rows = cursor.fetchall()
        sugerencias = []
        for row in rows:
            stock_actual = float(row["stock_actual"] or 0)
            stock_minimo = float(row["stock_minimo"] or 0)
            consumo_historico = float(row["consumo_historico"] or 0)
            consumo_diario = (consumo_historico / dias_historico) if consumo_historico > 0 else 0.0
            stock_objetivo = max(stock_minimo, consumo_diario * dias_cobertura)
            cantidad_sugerida = max(0.0, stock_objetivo - stock_actual)

            # Si no hay mínimo definido, solo sugerir cuando exista déficit por consumo objetivo.
            if stock_minimo <= 0 and cantidad_sugerida <= 0:
                continue

            # Solo incluir si hay déficit objetivo o está bajo mínimo configurado.
            if cantidad_sugerida <= 0 and stock_minimo > 0 and stock_actual > stock_minimo:
                continue

            dias_cobertura_estimados = None
            if consumo_diario > 0:
                dias_cobertura_estimados = stock_actual / consumo_diario

            if (stock_minimo > 0 and stock_actual <= stock_minimo) or (dias_cobertura_estimados is not None and dias_cobertura_estimados < 3):
                prioridad = "alta"
            elif dias_cobertura_estimados is not None and dias_cobertura_estimados < dias_cobertura:
                prioridad = "media"
            else:
                prioridad = "baja"

            sugerencias.append(
                {
                    "id": int(row["id"]),
                    "nombre": row["nombre"],
                    "unidad": row["unidad"],
                    "stock_actual": round(stock_actual, 4),
                    "stock_minimo": round(stock_minimo, 4),
                    "consumo_historico": round(consumo_historico, 4),
                    "consumo_diario": round(consumo_diario, 4),
                    "stock_objetivo": round(stock_objetivo, 4),
                    "cantidad_sugerida": round(cantidad_sugerida, 4),
                    "dias_cobertura_estimados": None if dias_cobertura_estimados is None else round(dias_cobertura_estimados, 2),
                    "prioridad": prioridad,
                }
            )

        prioridad_orden = {"alta": 0, "media": 1, "baja": 2}
        sugerencias.sort(
            key=lambda x: (
                prioridad_orden.get(x["prioridad"], 9),
                -float(x["cantidad_sugerida"] or 0),
                x["nombre"],
            )
        )
        return sugerencias[:limite]
    finally:
        conn.close()


def obtener_resumen_margen_ventas(fecha_desde=None, fecha_hasta=None):
    """
    Estima margen bruto:
    - ingresos desde venta_detalles (subtotal/precio_unitario * cantidad)
    - costo producto vendido desde costo de receta por unidad
    - costo de insumos asociados a venta desde venta_insumos + costo base del insumo
    """
    conn = get_db()
    cursor = conn.cursor()
    try:
        where = []
        params = []
        if fecha_desde:
            where.append("date(v.fecha_hora) >= date(?)")
            params.append(fecha_desde)
        if fecha_hasta:
            where.append("date(v.fecha_hora) <= date(?)")
            params.append(fecha_hasta)
        where_sql = (" WHERE " + " AND ".join(where)) if where else ""

        cursor.execute(
            f"""
            SELECT
                vd.venta_id,
                vd.producto_id,
                COALESCE(vd.cantidad, 0) AS cantidad,
                COALESCE(vd.precio_unitario, 0) AS precio_unitario,
                COALESCE(vd.subtotal, 0) AS subtotal
            FROM venta_detalles vd
            JOIN ventas v ON v.id = vd.venta_id
            {where_sql}
            """,
            params,
        )
        detalles = cursor.fetchall()

        ingresos_totales = 0.0
        unidades_vendidas = 0.0
        ventas_ids = set()
        costo_productos = 0.0
        costo_unitario_cache = {}
        lineas_sin_precio = 0
        lineas_sin_costo = 0

        for row in detalles:
            venta_id = int(row["venta_id"])
            producto_id = int(row["producto_id"])
            cantidad = float(row["cantidad"] or 0)
            precio_u = float(row["precio_unitario"] or 0)
            subtotal = float(row["subtotal"] or 0)

            ventas_ids.add(venta_id)
            ingreso_linea = subtotal if subtotal > 0 else max(0.0, cantidad * precio_u)
            if ingreso_linea <= 0 and cantidad > 0:
                lineas_sin_precio += 1
            ingresos_totales += max(0.0, ingreso_linea)
            unidades_vendidas += max(0.0, cantidad)

            if producto_id not in costo_unitario_cache:
                cursor.execute(
                    "SELECT id, rendimiento FROM recetas WHERE producto_id = ? ORDER BY id DESC LIMIT 1",
                    (producto_id,),
                )
                receta = cursor.fetchone()
                costo_u = 0.0
                if receta:
                    costo = calcular_costo_receta(int(receta["id"]))
                    rendimiento = float(receta["rendimiento"] or 1)
                    costo_u = float(costo.get("costo_total") or 0) / max(1.0, rendimiento)
                else:
                    costo_u = 0.0
                costo_unitario_cache[producto_id] = max(0.0, costo_u)
                if costo_unitario_cache[producto_id] <= 0:
                    lineas_sin_costo += 1

            costo_productos += max(0.0, cantidad) * float(costo_unitario_cache[producto_id])

        cursor.execute(
            f"""
            SELECT
                vi.insumo_id,
                COALESCE(vi.cantidad_descontada_stock, 0) AS cantidad_descontada_stock,
                COALESCE(vi.unidad_stock, 'unidad') AS unidad_stock
            FROM venta_insumos vi
            JOIN ventas v ON v.id = vi.venta_id
            {where_sql}
            """,
            params,
        )
        consumo_insumos = cursor.fetchall()

        costo_insumos_asociados = 0.0
        costo_base_cache = {}
        for row in consumo_insumos:
            insumo_id = int(row["insumo_id"])
            cantidad_stock = float(row["cantidad_descontada_stock"] or 0)
            unidad_stock = _normalizar_unidad_producto(row["unidad_stock"] or "unidad")

            if insumo_id not in costo_base_cache:
                costo_base_cache[insumo_id] = calcular_precio_unitario_base(insumo_id)

            costo_base = costo_base_cache[insumo_id]
            if not costo_base:
                continue

            precio_base = float(costo_base.get("precio_base") or 0)
            cantidad_base = float(convertir_a_base(cantidad_stock, unidad_stock) or 0)
            costo_insumos_asociados += max(0.0, cantidad_base * precio_base)

        costo_total = costo_productos + costo_insumos_asociados
        margen_bruto = ingresos_totales - costo_total
        margen_pct = (margen_bruto / ingresos_totales * 100.0) if ingresos_totales > 0 else 0.0
        ticket_promedio = (ingresos_totales / len(ventas_ids)) if ventas_ids else 0.0

        return {
            "ventas": len(ventas_ids),
            "unidades_vendidas": round(unidades_vendidas, 2),
            "ingresos_totales": round(ingresos_totales, 2),
            "costo_productos": round(costo_productos, 2),
            "costo_insumos_asociados": round(costo_insumos_asociados, 2),
            "costo_total": round(costo_total, 2),
            "margen_bruto": round(margen_bruto, 2),
            "margen_pct": round(margen_pct, 2),
            "ticket_promedio": round(ticket_promedio, 2),
            "lineas_sin_precio": int(lineas_sin_precio),
            "lineas_sin_costo": int(lineas_sin_costo),
        }
    finally:
        conn.close()


def obtener_eventos_agenda(fecha_desde=None, fecha_hasta=None):
    """Obtiene todos los eventos de la agenda"""
    conn = get_db()
    cursor = conn.cursor()
    
    query = "SELECT * FROM agenda_eventos WHERE 1=1"
    params = []
    
    if fecha_desde:
        query += " AND fecha >= ?"
        params.append(fecha_desde)
    if fecha_hasta:
        query += " AND fecha <= ?"
        params.append(fecha_hasta)
    
    query += " ORDER BY fecha, hora_inicio"
    
    cursor.execute(query, params)
    eventos = cursor.fetchall()
    conn.close()
    
    resultado = []
    for e in eventos:
        evento = dict(e)
        # Convertir es_envio de int a bool
        evento['es_envio'] = bool(evento['es_envio'])
        resultado.append(evento)
    
    return resultado


def obtener_evento_agenda_por_id(evento_id):
    """Obtiene un evento puntual de agenda por ID."""
    try:
        evento_id = int(evento_id)
    except (TypeError, ValueError):
        return None
    if evento_id <= 0:
        return None

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM agenda_eventos WHERE id = ?", (evento_id,))
        row = cursor.fetchone()
        if not row:
            return None
        evento = dict(row)
        evento["es_envio"] = bool(evento.get("es_envio"))
        return evento
    finally:
        conn.close()


def actualizar_estado_evento_agenda(evento_id, estado):
    """Actualiza el estado de un evento de agenda."""
    try:
        evento_id = int(evento_id)
    except (TypeError, ValueError):
        return {"success": False, "error": "Evento inválido"}
    if evento_id <= 0:
        return {"success": False, "error": "Evento inválido"}

    estado_normalizado = str(estado or "").strip().lower()
    if estado_normalizado not in {"pendiente", "completado", "cancelado"}:
        return {"success": False, "error": "Estado inválido"}

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id, estado FROM agenda_eventos WHERE id = ?", (evento_id,))
        actual = cursor.fetchone()
        if not actual:
            return {"success": False, "error": "Evento no encontrado"}

        cursor.execute(
            "UPDATE agenda_eventos SET estado = ? WHERE id = ?",
            (estado_normalizado, evento_id),
        )
        conn.commit()
        return {"success": True, "id": evento_id, "estado": estado_normalizado}
    except Exception as e:
        conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        conn.close()

def guardar_evento_agenda(evento):
    """Guarda un evento nuevo o actualiza existente"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        tipo = evento.get("tipo") or "torta"
        codigo_operacion_evento = str(evento.get("codigo_operacion") or "").strip()[:80] or None
        hora_inicio = evento.get("hora_inicio") or None
        hora_fin = evento.get("hora_fin") or None
        if tipo == "bloqueo" and not hora_inicio and not hora_fin:
            # bloqueo de día completo
            hora_inicio = None
            hora_fin = None

        if evento.get('id'):
            cursor.execute("SELECT codigo_operacion FROM agenda_eventos WHERE id = ?", (evento["id"],))
            row_actual = cursor.fetchone()
            codigo_actual = str(row_actual["codigo_operacion"] or "").strip() if row_actual else ""
            if not codigo_operacion_evento:
                codigo_operacion_evento = codigo_actual or generar_codigo_operacion("OPA")
            # Actualizar existente
            cursor.execute('''
                UPDATE agenda_eventos SET
                    tipo = ?, titulo = ?, fecha = ?, hora_inicio = ?,
                    hora_fin = ?, hora_entrega = ?, cliente = ?, telefono = ?,
                    es_envio = ?, direccion = ?, ingredientes = ?, total = ?,
                    abono = ?, motivo = ?, alerta_minutos = ?, codigo_operacion = ?
                WHERE id = ?
            ''', (
                tipo, evento['titulo'], evento['fecha'], 
                hora_inicio, hora_fin, 
                evento.get('hora_entrega'), evento.get('cliente'), 
                evento.get('telefono'), 1 if evento.get('es_envio') else 0,
                evento.get('direccion'), evento.get('ingredientes'),
                evento.get('total', 0), evento.get('abono', 0),
                evento.get('motivo'), evento.get('alerta_minutos', 1440), codigo_operacion_evento,
                evento['id']
            ))
        else:
            if not codigo_operacion_evento:
                codigo_operacion_evento = generar_codigo_operacion("OPA")
            # Insertar nuevo
            cursor.execute('''
                INSERT INTO agenda_eventos (
                    tipo, titulo, fecha, hora_inicio, hora_fin, hora_entrega,
                    cliente, telefono, es_envio, direccion, ingredientes,
                    total, abono, motivo, alerta_minutos, estado, codigo_operacion
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                tipo, evento['titulo'], evento['fecha'],
                hora_inicio, hora_fin,
                evento.get('hora_entrega'), evento.get('cliente'),
                evento.get('telefono'), 1 if evento.get('es_envio') else 0,
                evento.get('direccion'), evento.get('ingredientes'),
                evento.get('total', 0), evento.get('abono', 0),
                evento.get('motivo'), evento.get('alerta_minutos', 1440),
                'pendiente', codigo_operacion_evento
            ))
            evento['id'] = cursor.lastrowid

        # Al editar/crear se reinician descartes para recalcular nuevos avisos.
        cursor.execute("DELETE FROM agenda_recordatorios_descartados WHERE evento_id = ?", (evento["id"],))
        
        conn.commit()
        return {'success': True, 'id': evento['id']}
    except Exception as e:
        conn.rollback()
        return {'success': False, 'error': str(e)}
    finally:
        conn.close()

def eliminar_evento_agenda(evento_id):
    """Elimina un evento de la agenda"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        try:
            evento_id = int(evento_id)
        except (TypeError, ValueError):
            return {'success': False, 'error': 'Evento inválido'}
        if evento_id <= 0:
            return {'success': False, 'error': 'Evento inválido'}

        cursor.execute("DELETE FROM agenda_eventos WHERE id = ?", (evento_id,))
        eliminado = (cursor.rowcount or 0) > 0
        cursor.execute("DELETE FROM agenda_recordatorios_descartados WHERE evento_id = ?", (evento_id,))
        if not eliminado:
            conn.rollback()
            return {'success': False, 'error': 'Evento no encontrado'}
        conn.commit()
        return {'success': True}
    except Exception as e:
        conn.rollback()
        return {'success': False, 'error': str(e)}
    finally:
        conn.close()

def obtener_eventos_proximos_agenda(dias=30):
    """Obtiene eventos próximos para notificaciones"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT * FROM agenda_eventos 
        WHERE fecha >= date('now') 
        AND fecha <= date('now', '+' || ? || ' days')
        AND estado = 'pendiente'
        ORDER BY fecha, hora_inicio
    ''', (dias,))
    
    eventos = cursor.fetchall()
    conn.close()
    return [dict(e) for e in eventos]


def _parse_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _to_db_datetime(value):
    if value is None:
        return None
    txt = str(value).strip()
    if not txt:
        return None
    return txt.replace("T", " ")


def _normalizar_color_nota(color):
    validos = {"amarilla", "naranja", "verde", "azul", "rosa", "lila"}
    color_normalizado = (color or "amarilla").strip().lower()
    return color_normalizado if color_normalizado in validos else "amarilla"


def _normalizar_estado_compra(estado):
    return "comprado" if str(estado or "").strip().lower() == "comprado" else "pendiente"


def _normalizar_estado_nota(estado):
    return "completada" if str(estado or "").strip().lower() == "completada" else "activa"


def _normalizar_tipo_nota(tipo):
    return "checklist" if str(tipo or "").strip().lower() == "checklist" else "texto"


def _normalizar_checklist_nota(checklist):
    items = checklist
    if items is None:
        return []
    if isinstance(items, str):
        raw = items.strip()
        if not raw:
            return []
        try:
            items = json.loads(raw)
        except Exception:
            return []
    if not isinstance(items, list):
        return []

    normalizados = []
    for item in items[:120]:
        if isinstance(item, dict):
            texto_raw = item.get("texto")
            if texto_raw is None:
                texto_raw = item.get("titulo")
            completada_raw = item.get("completada")
            if isinstance(completada_raw, str):
                completada = completada_raw.strip().lower() in {"1", "true", "si", "sí", "on", "x", "checked"}
            else:
                completada = bool(completada_raw)
        else:
            texto_raw = item
            completada = False

        texto = str(texto_raw or "").strip()
        if not texto:
            continue
        normalizados.append(
            {
                "texto": texto[:180],
                "completada": completada,
            }
        )
    return normalizados


def _normalizar_motivo_merma(motivo):
    valor = str(motivo or "").strip()
    if not valor:
        raise ValueError("El motivo es obligatorio")
    return valor[:120]


def _purgar_notas_completadas_antiguas(cursor):
    cursor.execute(
        """
        DELETE FROM agenda_notas
        WHERE estado = 'completada'
          AND datetime(COALESCE(actualizada, creada)) < datetime('now', '-7 days')
        """
    )


def _totales_compra(cantidad, precio_unitario, precio_incluye_iva):
    cantidad = max(0.0, _parse_float(cantidad, 0))
    precio_unitario = max(0.0, _parse_float(precio_unitario, 0))
    incluye_iva = bool(precio_incluye_iva)

    bruto = cantidad * precio_unitario
    if incluye_iva:
        total_con_iva = bruto
        total_sin_iva = bruto / 1.19 if bruto > 0 else 0
    else:
        total_sin_iva = bruto
        total_con_iva = bruto * 1.19

    return total_sin_iva, total_con_iva


def obtener_compras_pendientes(incluir_comprados=True):
    conn = get_db()
    cursor = conn.cursor()
    try:
        if incluir_comprados:
            cursor.execute(
                """
                SELECT *
                FROM compras_pendientes
                ORDER BY CASE WHEN estado = 'pendiente' THEN 0 ELSE 1 END, actualizado DESC, id DESC
                """
            )
        else:
            cursor.execute(
                """
                SELECT *
                FROM compras_pendientes
                WHERE estado = 'pendiente'
                ORDER BY actualizado DESC, id DESC
                """
            )

        rows = cursor.fetchall()
        items = []
        resumen = {
            "total_items": 0,
            "pendientes": 0,
            "comprados": 0,
            "total_sin_iva": 0.0,
            "total_con_iva": 0.0,
            "pendiente_sin_iva": 0.0,
            "pendiente_con_iva": 0.0,
        }

        for row in rows:
            item = dict(row)
            item["cantidad"] = _parse_float(item.get("cantidad"), 0)
            item["precio_unitario"] = _parse_float(item.get("precio_unitario"), 0)
            item["precio_incluye_iva"] = bool(item.get("precio_incluye_iva"))
            item["estado"] = _normalizar_estado_compra(item.get("estado"))
            item["unidad"] = (item.get("unidad") or "unidad").strip() or "unidad"
            item["nombre"] = (item.get("nombre") or "").strip()
            item["nota"] = (item.get("nota") or "").strip()

            total_sin_iva, total_con_iva = _totales_compra(
                item["cantidad"], item["precio_unitario"], item["precio_incluye_iva"]
            )
            item["total_sin_iva"] = round(total_sin_iva, 2)
            item["total_con_iva"] = round(total_con_iva, 2)

            resumen["total_items"] += 1
            resumen["total_sin_iva"] += total_sin_iva
            resumen["total_con_iva"] += total_con_iva
            if item["estado"] == "comprado":
                resumen["comprados"] += 1
            else:
                resumen["pendientes"] += 1
                resumen["pendiente_sin_iva"] += total_sin_iva
                resumen["pendiente_con_iva"] += total_con_iva

            items.append(item)

        for clave in ("total_sin_iva", "total_con_iva", "pendiente_sin_iva", "pendiente_con_iva"):
            resumen[clave] = round(resumen[clave], 2)

        return {"items": items, "resumen": resumen}
    finally:
        conn.close()


def agregar_compra_pendiente(data):
    payload = data or {}
    nombre = (payload.get("nombre") or "").strip()
    if not nombre:
        return {"success": False, "error": "El nombre es obligatorio"}

    cantidad = max(0.0, _parse_float(payload.get("cantidad"), 1))
    if cantidad <= 0:
        return {"success": False, "error": "La cantidad debe ser mayor a 0"}

    unidad = (payload.get("unidad") or "unidad").strip() or "unidad"
    precio_unitario = max(0.0, _parse_float(payload.get("precio_unitario"), 0))
    precio_incluye_iva = 1 if payload.get("precio_incluye_iva", True) else 0
    estado = _normalizar_estado_compra(payload.get("estado"))
    nota = (payload.get("nota") or "").strip() or None

    insumo_id = payload.get("insumo_id")
    try:
        insumo_id = int(insumo_id) if insumo_id not in (None, "", 0, "0") else None
    except (TypeError, ValueError):
        insumo_id = None

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO compras_pendientes (
                insumo_id, nombre, cantidad, unidad, precio_unitario, precio_incluye_iva, estado, nota, actualizado
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                insumo_id,
                nombre,
                cantidad,
                unidad,
                precio_unitario,
                precio_incluye_iva,
                estado,
                nota,
            ),
        )
        conn.commit()
        return {"success": True, "id": cursor.lastrowid}
    except Exception as e:
        conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        conn.close()


def agregar_lote_compras_pendientes(items, combinar=True):
    if not isinstance(items, list) or len(items) == 0:
        return {"success": False, "error": "No hay elementos para agregar"}

    conn = get_db()
    cursor = conn.cursor()
    procesados = 0
    insertados = 0
    actualizados = 0
    try:
        for raw in items:
            if not isinstance(raw, dict):
                continue

            nombre = (raw.get("nombre") or "").strip()
            if not nombre:
                continue

            cantidad = max(0.0, _parse_float(raw.get("cantidad"), 1))
            if cantidad <= 0:
                continue

            unidad = (raw.get("unidad") or "unidad").strip() or "unidad"
            precio_unitario = max(0.0, _parse_float(raw.get("precio_unitario"), 0))
            precio_incluye_iva = 1 if raw.get("precio_incluye_iva", True) else 0
            nota = (raw.get("nota") or "").strip() or None

            insumo_id = raw.get("insumo_id")
            try:
                insumo_id = int(insumo_id) if insumo_id not in (None, "", 0, "0") else None
            except (TypeError, ValueError):
                insumo_id = None

            merged = False
            if combinar:
                if insumo_id:
                    cursor.execute(
                        """
                        SELECT * FROM compras_pendientes
                        WHERE estado = 'pendiente' AND insumo_id = ?
                        ORDER BY id DESC
                        LIMIT 1
                        """,
                        (insumo_id,),
                    )
                else:
                    cursor.execute(
                        """
                        SELECT * FROM compras_pendientes
                        WHERE estado = 'pendiente'
                          AND lower(nombre) = lower(?)
                          AND unidad = ?
                        ORDER BY id DESC
                        LIMIT 1
                        """,
                        (nombre, unidad),
                    )

                existente = cursor.fetchone()
                if existente:
                    nueva_cantidad = _parse_float(existente["cantidad"], 0) + cantidad
                    nuevo_precio = precio_unitario if precio_unitario > 0 else _parse_float(existente["precio_unitario"], 0)
                    nuevo_iva = precio_incluye_iva if precio_unitario > 0 else int(existente["precio_incluye_iva"] or 0)
                    nueva_nota = nota if nota else existente["nota"]

                    cursor.execute(
                        """
                        UPDATE compras_pendientes
                        SET nombre = ?, cantidad = ?, unidad = ?, precio_unitario = ?,
                            precio_incluye_iva = ?, nota = ?, actualizado = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (
                            nombre,
                            nueva_cantidad,
                            unidad,
                            nuevo_precio,
                            nuevo_iva,
                            nueva_nota,
                            existente["id"],
                        ),
                    )
                    actualizados += 1
                    merged = True

            if not merged:
                cursor.execute(
                    """
                    INSERT INTO compras_pendientes (
                        insumo_id, nombre, cantidad, unidad, precio_unitario, precio_incluye_iva, estado, nota, actualizado
                    )
                    VALUES (?, ?, ?, ?, ?, ?, 'pendiente', ?, CURRENT_TIMESTAMP)
                    """,
                    (
                        insumo_id,
                        nombre,
                        cantidad,
                        unidad,
                        precio_unitario,
                        precio_incluye_iva,
                        nota,
                    ),
                )
                insertados += 1

            procesados += 1

        conn.commit()
        return {
            "success": True,
            "procesados": procesados,
            "insertados": insertados,
            "actualizados": actualizados,
        }
    except Exception as e:
        conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        conn.close()


def actualizar_compra_pendiente(item_id, data):
    payload = data or {}
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM compras_pendientes WHERE id = ?", (item_id,))
        existente = cursor.fetchone()
        if not existente:
            return {"success": False, "error": "Ítem no encontrado"}

        nombre = (payload.get("nombre", existente["nombre"]) or "").strip()
        if not nombre:
            return {"success": False, "error": "El nombre es obligatorio"}

        cantidad = max(0.0, _parse_float(payload.get("cantidad", existente["cantidad"]), 0))
        unidad = (payload.get("unidad", existente["unidad"]) or "unidad").strip() or "unidad"
        precio_unitario = max(0.0, _parse_float(payload.get("precio_unitario", existente["precio_unitario"]), 0))
        precio_incluye_iva = 1 if payload.get("precio_incluye_iva", bool(existente["precio_incluye_iva"])) else 0
        estado = _normalizar_estado_compra(payload.get("estado", existente["estado"]))
        nota = (payload.get("nota", existente["nota"]) or "").strip() or None

        insumo_id = payload.get("insumo_id", existente["insumo_id"])
        try:
            insumo_id = int(insumo_id) if insumo_id not in (None, "", 0, "0") else None
        except (TypeError, ValueError):
            insumo_id = None

        cursor.execute(
            """
            UPDATE compras_pendientes
            SET insumo_id = ?, nombre = ?, cantidad = ?, unidad = ?, precio_unitario = ?,
                precio_incluye_iva = ?, estado = ?, nota = ?, actualizado = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                insumo_id,
                nombre,
                cantidad,
                unidad,
                precio_unitario,
                precio_incluye_iva,
                estado,
                nota,
                item_id,
            ),
        )
        conn.commit()
        return {"success": True}
    except Exception as e:
        conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        conn.close()


def eliminar_compra_pendiente(item_id):
    conn = get_db()
    cursor = conn.cursor()
    try:
        try:
            item_id = int(item_id)
        except (TypeError, ValueError):
            return {"success": False, "error": "Ítem inválido"}
        if item_id <= 0:
            return {"success": False, "error": "Ítem inválido"}

        cursor.execute("DELETE FROM compras_pendientes WHERE id = ?", (item_id,))
        eliminado = (cursor.rowcount or 0) > 0
        if not eliminado:
            conn.rollback()
            return {"success": False, "error": "Ítem no encontrado"}
        conn.commit()
        return {"success": True}
    except Exception as e:
        conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        conn.close()


def limpiar_compras_pendientes(solo_comprados=False):
    conn = get_db()
    cursor = conn.cursor()
    try:
        if solo_comprados:
            cursor.execute("DELETE FROM compras_pendientes WHERE estado = 'comprado'")
        else:
            cursor.execute("DELETE FROM compras_pendientes")
        afectados = cursor.rowcount or 0
        conn.commit()
        return {"success": True, "eliminados": afectados}
    except Exception as e:
        conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        conn.close()


def marcar_compras_pendientes_completadas():
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM compras_pendientes ORDER BY id ASC")
        pendientes = cursor.fetchall()
        codigo_operacion = _normalizar_codigo_operacion(prefijo="OPC")
        for item in pendientes:
            try:
                insumo_id = int(item["insumo_id"] or 0)
            except (TypeError, ValueError):
                insumo_id = 0
            if insumo_id <= 0:
                continue
            cursor.execute("SELECT stock, unidad FROM insumos WHERE id = ?", (insumo_id,))
            row_insumo = cursor.fetchone()
            if not row_insumo:
                continue
            stock_actual = float(row_insumo["stock"] or 0)
            registrar_movimiento_stock(
                "insumo",
                insumo_id,
                "compra_pendiente_finalizada",
                0,
                stock_anterior=stock_actual,
                stock_nuevo=stock_actual,
                referencia_tipo="compras_pendientes",
                referencia_id=item["id"],
                detalle=f"Compra pendiente finalizada: {item['nombre']}",
                origen_modulo="compras_pendientes",
                codigo_operacion=codigo_operacion,
                unidad=row_insumo["unidad"] if row_insumo else "unidad",
                metadata={
                    "compra_pendiente_id": item["id"],
                    "cantidad": float(item["cantidad"] or 0),
                    "unidad": item["unidad"],
                },
                conn=conn,
            )

        cursor.execute(
            """
            DELETE FROM compras_pendientes
            """
        )
        eliminados = cursor.rowcount or 0
        conn.commit()
        return {"success": True, "eliminados": eliminados, "codigo_operacion": codigo_operacion}
    except Exception as e:
        conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        conn.close()


def obtener_notas_agenda(incluir_completadas=False):
    conn = get_db()
    cursor = conn.cursor()
    try:
        _purgar_notas_completadas_antiguas(cursor)
        if incluir_completadas:
            where_estado = ""
        else:
            where_estado = "WHERE estado = 'activa'"
        cursor.execute(
            """
            SELECT *
            FROM agenda_notas
            {where_estado}
            ORDER BY CASE WHEN estado = 'activa' THEN 0 ELSE 1 END,
                     CASE WHEN fijada = 1 THEN 0 ELSE 1 END,
                     actualizada DESC,
                     id DESC
            """
            .format(where_estado=where_estado)
        )
        rows = cursor.fetchall()
        notas = []
        for row in rows:
            nota = dict(row)
            nota["fijada"] = bool(nota.get("fijada"))
            nota["color"] = _normalizar_color_nota(nota.get("color"))
            nota["estado"] = _normalizar_estado_nota(nota.get("estado"))
            nota["recordatorio"] = nota.get("recordatorio") or None
            tipo = _normalizar_tipo_nota(nota.get("tipo"))
            checklist = _normalizar_checklist_nota(nota.get("checklist_json"))
            if tipo != "checklist":
                checklist = []
            nota["tipo"] = tipo
            nota["checklist"] = checklist
            nota["checklist_total"] = len(checklist)
            nota["checklist_completadas"] = sum(1 for item in checklist if item.get("completada"))
            notas.append(nota)
        return notas
    finally:
        conn.close()


def guardar_nota_agenda(nota):
    payload = nota or {}
    titulo = (payload.get("titulo") or "").strip() or "Nota"
    tipo = _normalizar_tipo_nota(payload.get("tipo"))
    contenido = (payload.get("contenido") or "").strip()
    checklist = _normalizar_checklist_nota(payload.get("checklist"))
    if tipo == "checklist":
        contenido = ""
    checklist_json = json.dumps(checklist, ensure_ascii=False) if tipo == "checklist" and checklist else None
    color = _normalizar_color_nota(payload.get("color"))
    fijada = 1 if payload.get("fijada", False) else 0
    estado = _normalizar_estado_nota(payload.get("estado"))
    recordatorio = _to_db_datetime(payload.get("recordatorio"))

    conn = get_db()
    cursor = conn.cursor()
    try:
        if payload.get("id"):
            cursor.execute(
                """
                UPDATE agenda_notas
                SET titulo = ?, contenido = ?, tipo = ?, checklist_json = ?, color = ?, fijada = ?, recordatorio = ?,
                    estado = ?, actualizada = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    titulo,
                    contenido,
                    tipo,
                    checklist_json,
                    color,
                    fijada,
                    recordatorio,
                    estado,
                    payload["id"],
                ),
            )
            nota_id = payload["id"]
        else:
            cursor.execute(
                """
                INSERT INTO agenda_notas (
                    titulo, contenido, tipo, checklist_json, color, fijada, recordatorio, estado, actualizada
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    titulo,
                    contenido,
                    tipo,
                    checklist_json,
                    color,
                    fijada,
                    recordatorio,
                    estado,
                ),
            )
            nota_id = cursor.lastrowid

        conn.commit()
        return {"success": True, "id": nota_id}
    except Exception as e:
        conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        conn.close()


def eliminar_nota_agenda(nota_id):
    conn = get_db()
    cursor = conn.cursor()
    try:
        try:
            nota_id = int(nota_id)
        except (TypeError, ValueError):
            return {"success": False, "error": "Nota inválida"}
        if nota_id <= 0:
            return {"success": False, "error": "Nota inválida"}

        cursor.execute("DELETE FROM agenda_notas WHERE id = ?", (nota_id,))
        eliminado = (cursor.rowcount or 0) > 0
        if not eliminado:
            conn.rollback()
            return {"success": False, "error": "Nota no encontrada"}
        conn.commit()
        return {"success": True}
    except Exception as e:
        conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        conn.close()


def _normalizar_fecha_factura(valor):
    raw = str(valor or "").strip()
    if not raw:
        raise ValueError("La fecha de factura es obligatoria")

    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError("Fecha de factura inválida")


def _normalizar_mes_factura(mes, fecha_factura):
    raw = str(mes or "").strip()
    if raw:
        for fmt in ("%Y-%m", "%m/%Y", "%m-%Y"):
            try:
                dt = datetime.strptime(raw, fmt)
                return dt.strftime("%Y-%m")
            except ValueError:
                continue
    return str(fecha_factura)[:7]


def _json_dumps_safe(value):
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return None


def _registrar_auditoria_factura(cursor, factura_id, accion, snapshot_antes=None, snapshot_despues=None, metadata=None):
    cursor.execute(
        """
        INSERT INTO facturas_auditoria (
            factura_id, accion, snapshot_antes, snapshot_despues, metadata
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            factura_id,
            str(accion or "").strip()[:60] or "accion",
            _json_dumps_safe(snapshot_antes),
            _json_dumps_safe(snapshot_despues),
            _json_dumps_safe(metadata),
        ),
    )


def guardar_factura_archivo(data):
    payload = data or {}

    proveedor = (payload.get("proveedor") or "").strip()
    if not proveedor:
        return {"success": False, "error": "El proveedor es obligatorio"}
    proveedor = proveedor[:120]

    try:
        fecha_factura = _normalizar_fecha_factura(payload.get("fecha_factura"))
    except ValueError as e:
        return {"success": False, "error": str(e)}

    mes_clave = _normalizar_mes_factura(payload.get("mes_clave"), fecha_factura)
    numero_factura = (payload.get("numero_factura") or "").strip()[:80] or None
    observacion = (payload.get("observacion") or "").strip()[:500] or None
    monto_total = max(0.0, _parse_float(payload.get("monto_total"), 0))

    archivo_nombre_original = (payload.get("archivo_nombre_original") or "").strip()
    archivo_nombre_guardado = (payload.get("archivo_nombre_guardado") or "").strip()
    archivo_ruta_relativa = (payload.get("archivo_ruta_relativa") or "").strip()
    archivo_extension = (payload.get("archivo_extension") or "").strip().lower()[:12] or None
    archivo_mime = (payload.get("archivo_mime") or "").strip()[:120] or None
    archivo_bytes = int(max(0, _parse_float(payload.get("archivo_bytes"), 0)))

    if not archivo_nombre_original or not archivo_nombre_guardado or not archivo_ruta_relativa:
        return {"success": False, "error": "Archivo inválido para registrar"}

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO facturas_archivo (
                proveedor, fecha_factura, mes_clave, numero_factura, monto_total, observacion,
                archivo_nombre_original, archivo_nombre_guardado, archivo_ruta_relativa,
                archivo_extension, archivo_mime, archivo_bytes, actualizado
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                proveedor,
                fecha_factura,
                mes_clave,
                numero_factura,
                monto_total,
                observacion,
                archivo_nombre_original,
                archivo_nombre_guardado,
                archivo_ruta_relativa,
                archivo_extension,
                archivo_mime,
                archivo_bytes,
            ),
        )
        factura_id = cursor.lastrowid
        _registrar_auditoria_factura(
            cursor,
            factura_id,
            "crear",
            snapshot_antes=None,
            snapshot_despues={
                "id": factura_id,
                "proveedor": proveedor,
                "fecha_factura": fecha_factura,
                "numero_factura": numero_factura,
                "monto_total": monto_total,
                "archivo": archivo_nombre_original,
            },
            metadata={"origen": "subida"},
        )
        _marcar_resumen_mensual_dirty_cursor(cursor, mes_clave)
        conn.commit()
        return {"success": True, "id": factura_id}
    except Exception as e:
        conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        conn.close()


def obtener_facturas_archivadas(proveedor=None, mes=None, busqueda=None):
    conn = get_db()
    cursor = conn.cursor()
    try:
        where = ["COALESCE(eliminado, 0) = 0"]
        params = []

        proveedor = (proveedor or "").strip()
        if proveedor:
            where.append("lower(proveedor) = lower(?)")
            params.append(proveedor)

        mes = (mes or "").strip()
        if mes:
            where.append("mes_clave = ?")
            params.append(mes[:7])

        busqueda = (busqueda or "").strip()
        if busqueda:
            where.append(
                "(lower(proveedor) LIKE lower(?) OR lower(COALESCE(numero_factura,'')) LIKE lower(?) OR "
                "lower(COALESCE(observacion,'')) LIKE lower(?) OR lower(archivo_nombre_original) LIKE lower(?))"
            )
            like = f"%{busqueda}%"
            params.extend([like, like, like, like])

        sql = "SELECT * FROM facturas_archivo"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY fecha_factura DESC, id DESC"

        cursor.execute(sql, params)
        return [dict(r) for r in cursor.fetchall()]
    finally:
        conn.close()


def obtener_factura_archivo(factura_id, incluir_eliminadas=False):
    conn = get_db()
    cursor = conn.cursor()
    try:
        if incluir_eliminadas:
            cursor.execute("SELECT * FROM facturas_archivo WHERE id = ?", (factura_id,))
        else:
            cursor.execute(
                "SELECT * FROM facturas_archivo WHERE id = ? AND COALESCE(eliminado, 0) = 0",
                (factura_id,),
            )
        row = cursor.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def eliminar_factura_archivo(factura_id):
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT * FROM facturas_archivo WHERE id = ? AND COALESCE(eliminado, 0) = 0",
            (factura_id,),
        )
        row = cursor.fetchone()
        if not row:
            return {"success": False, "error": "Factura no encontrada"}

        before = dict(row)
        cursor.execute(
            """
            UPDATE facturas_archivo
            SET eliminado = 1,
                eliminado_en = CURRENT_TIMESTAMP,
                actualizado = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (factura_id,),
        )
        cursor.execute("SELECT * FROM facturas_archivo WHERE id = ?", (factura_id,))
        after = cursor.fetchone()
        _registrar_auditoria_factura(
            cursor,
            factura_id,
            "eliminar",
            snapshot_antes=before,
            snapshot_despues=dict(after) if after else None,
            metadata={"modo": "soft_delete"},
        )
        _marcar_resumen_mensual_dirty_cursor(cursor, before.get("mes_clave"))
        conn.commit()
        return {"success": True, "factura": before}
    except Exception as e:
        conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        conn.close()


def actualizar_factura_archivo(factura_id, data):
    payload = data or {}

    try:
        factura_id = int(factura_id)
    except (TypeError, ValueError):
        return {"success": False, "error": "Factura inválida"}
    if factura_id <= 0:
        return {"success": False, "error": "Factura inválida"}

    proveedor = (payload.get("proveedor") or "").strip()
    if not proveedor:
        return {"success": False, "error": "El proveedor es obligatorio"}
    proveedor = proveedor[:120]

    try:
        fecha_factura = _normalizar_fecha_factura(payload.get("fecha_factura"))
    except ValueError as e:
        return {"success": False, "error": str(e)}

    mes_clave = _normalizar_mes_factura(payload.get("mes_clave"), fecha_factura)
    numero_factura = (payload.get("numero_factura") or "").strip()[:80] or None
    observacion = (payload.get("observacion") or "").strip()[:500] or None
    monto_total = max(0.0, _parse_float(payload.get("monto_total"), 0))

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT * FROM facturas_archivo WHERE id = ? AND COALESCE(eliminado, 0) = 0",
            (factura_id,),
        )
        existente = cursor.fetchone()
        if not existente:
            return {"success": False, "error": "Factura no encontrada"}
        before = dict(existente)

        cursor.execute(
            """
            UPDATE facturas_archivo
            SET
                proveedor = ?,
                fecha_factura = ?,
                mes_clave = ?,
                numero_factura = ?,
                monto_total = ?,
                observacion = ?,
                actualizado = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                proveedor,
                fecha_factura,
                mes_clave,
                numero_factura,
                monto_total,
                observacion,
                factura_id,
            ),
        )
        cursor.execute("SELECT * FROM facturas_archivo WHERE id = ?", (factura_id,))
        after = cursor.fetchone()
        _registrar_auditoria_factura(
            cursor,
            factura_id,
            "actualizar",
            snapshot_antes=before,
            snapshot_despues=dict(after) if after else None,
            metadata={"origen": "edicion"},
        )
        _marcar_resumen_mensual_dirty_cursor(cursor, before.get("mes_clave"))
        _marcar_resumen_mensual_dirty_cursor(cursor, mes_clave)
        conn.commit()

        return {"success": True, "factura": dict(after) if after else None}
    except Exception as e:
        conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        conn.close()


def obtener_auditoria_factura(factura_id=None, limite=150):
    conn = get_db()
    cursor = conn.cursor()
    try:
        tope = max(1, min(int(limite or 150), 1000))
        params = []
        sql = """
            SELECT
                a.id,
                a.factura_id,
                a.accion,
                a.snapshot_antes,
                a.snapshot_despues,
                a.metadata,
                a.creado
            FROM facturas_auditoria a
            LEFT JOIN facturas_archivo f ON f.id = a.factura_id
            WHERE 1=1
        """
        if factura_id is not None:
            sql += " AND a.factura_id = ?"
            params.append(int(factura_id))
        sql += " ORDER BY a.creado DESC, a.id DESC LIMIT ?"
        params.append(tope)
        cursor.execute(sql, params)
        rows = []
        for row in cursor.fetchall():
            item = dict(row)
            for campo in ("snapshot_antes", "snapshot_despues", "metadata"):
                raw = item.get(campo)
                if raw:
                    try:
                        item[campo] = json.loads(raw)
                    except Exception:
                        item[campo] = None
                else:
                    item[campo] = None
            rows.append(item)
        return rows
    finally:
        conn.close()


def obtener_filtros_facturas():
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT DISTINCT proveedor
            FROM facturas_archivo
            WHERE COALESCE(eliminado, 0) = 0
              AND proveedor IS NOT NULL
              AND trim(proveedor) <> ''
            ORDER BY proveedor COLLATE NOCASE ASC
            """
        )
        proveedores = [r["proveedor"] for r in cursor.fetchall()]

        cursor.execute(
            """
            SELECT DISTINCT mes_clave
            FROM facturas_archivo
            WHERE COALESCE(eliminado, 0) = 0
              AND mes_clave IS NOT NULL
              AND trim(mes_clave) <> ''
            ORDER BY mes_clave DESC
            """
        )
        meses = [r["mes_clave"] for r in cursor.fetchall()]

        return {"proveedores": proveedores, "meses": meses}
    finally:
        conn.close()


def _normalizar_tasa_decimal(valor, default=0.0, campo="tasa"):
    if valor in (None, ""):
        return max(0.0, float(default or 0))
    try:
        tasa = float(valor)
    except (TypeError, ValueError):
        raise ValueError(f"{campo} invalida")
    if tasa < 0:
        raise ValueError(f"{campo} no puede ser negativa")
    if tasa > 1:
        tasa = tasa / 100.0
    return max(0.0, tasa)


SII_AJUSTE_EDITABLE_FIELDS = (
    "ventas_brutas",
    "ventas_netas_sin_iva",
    "compras_sin_iva",
    "iva_debito_estimado",
    "iva_credito_estimado",
    "ppm_estimado",
)


def _normalizar_anio_tributario_sii(anio):
    if anio in (None, ""):
        return int(datetime.now().year)
    try:
        anio_int = int(anio)
    except (TypeError, ValueError):
        raise ValueError("A\u00f1o tributario inv\u00e1lido")
    if anio_int < 2000 or anio_int > 2100:
        raise ValueError("A\u00f1o tributario fuera de rango")
    return anio_int


def _normalizar_mes_clave_sii(mes_clave, anio_int):
    raw = str(mes_clave or "").strip()
    if len(raw) != 7 or raw[4] != "-":
        raise ValueError("Mes invalido para ajustes SII")
    year_txt = raw[:4]
    month_txt = raw[5:7]
    if not (year_txt.isdigit() and month_txt.isdigit()):
        raise ValueError("Mes invalido para ajustes SII")
    year_int = int(year_txt)
    month_int = int(month_txt)
    if year_int != int(anio_int):
        raise ValueError("El mes no corresponde al a\u00f1o seleccionado")
    if month_int < 1 or month_int > 12:
        raise ValueError("Mes fuera de rango para ajustes SII")
    return f"{year_int}-{month_int:02d}"


def _limpiar_valores_ajuste_sii(item):
    valores = {}
    if not isinstance(item, dict):
        return valores
    for field in SII_AJUSTE_EDITABLE_FIELDS:
        if field not in item:
            continue
        raw = item.get(field)
        if raw in (None, ""):
            continue
        try:
            num = float(raw)
        except (TypeError, ValueError):
            raise ValueError(f"Valor invalido en {field}")
        if not math.isfinite(num):
            raise ValueError(f"Valor invalido en {field}")
        if num < 0:
            raise ValueError(f"{field} no puede ser negativo")
        valores[field] = round(num, 2)
    return valores


def obtener_ajustes_sii_facturas(anio=None):
    anio_int = _normalizar_anio_tributario_sii(anio)
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT mes_clave, valores_json
            FROM facturas_sii_ajustes
            WHERE anio = ?
            ORDER BY mes_clave ASC
            """,
            (anio_int,),
        )
        salida = {}
        for row in cursor.fetchall():
            mes_clave = str(row["mes_clave"] or "").strip()
            if not mes_clave:
                continue
            try:
                payload = json.loads(row["valores_json"] or "{}")
            except Exception:
                payload = {}
            try:
                salida[mes_clave] = _limpiar_valores_ajuste_sii(payload)
            except ValueError:
                salida[mes_clave] = {}
        return salida
    finally:
        conn.close()


def guardar_ajustes_sii_facturas(anio, ajustes):
    anio_int = _normalizar_anio_tributario_sii(anio)
    rows = ajustes if isinstance(ajustes, list) else []

    normalizados = []
    vistos = set()
    for item in rows:
        if not isinstance(item, dict):
            raise ValueError("Formato de ajustes SII invalido")
        mes_clave = _normalizar_mes_clave_sii(item.get("mes_clave"), anio_int)
        if mes_clave in vistos:
            raise ValueError("Mes duplicado en ajustes SII")
        vistos.add(mes_clave)
        valores = _limpiar_valores_ajuste_sii(item)
        if not valores:
            continue
        normalizados.append((mes_clave, valores))

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM facturas_sii_ajustes WHERE anio = ?", (anio_int,))
        for mes_clave, valores in normalizados:
            cursor.execute(
                """
                INSERT INTO facturas_sii_ajustes (anio, mes_clave, valores_json, actualizado)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (anio_int, mes_clave, json.dumps(valores, ensure_ascii=False)),
            )
        conn.commit()
        return {
            "success": True,
            "anio": anio_int,
            "ajustes_guardados": len(normalizados),
        }
    finally:
        conn.close()


def limpiar_ajustes_sii_facturas(anio):
    anio_int = _normalizar_anio_tributario_sii(anio)
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM facturas_sii_ajustes WHERE anio = ?", (anio_int,))
        conn.commit()
        return {"success": True, "anio": anio_int}
    finally:
        conn.close()


def _normalizar_mes_clave_resumen(valor):
    raw = str(valor or "").strip()
    if len(raw) >= 7 and raw[4] == "-":
        anio_txt = raw[:4]
        mes_txt = raw[5:7]
        if anio_txt.isdigit() and mes_txt.isdigit():
            anio = int(anio_txt)
            mes = int(mes_txt)
            if 2000 <= anio <= 2100 and 1 <= mes <= 12:
                return f"{anio}-{mes:02d}"
    return None


def _mes_clave_desde_fecha(valor):
    raw = str(valor or "").strip()
    if not raw:
        return None
    try:
        if len(raw) >= 10:
            dt = datetime.strptime(raw[:10], "%Y-%m-%d")
        else:
            dt = datetime.strptime(raw, "%Y-%m-%d")
    except Exception:
        return _normalizar_mes_clave_resumen(raw)
    return dt.strftime("%Y-%m")


def _marcar_resumen_mensual_dirty_cursor(cursor, mes_clave):
    mes_norm = _normalizar_mes_clave_resumen(mes_clave)
    if not mes_norm:
        return
    anio = int(mes_norm[:4])
    mes = int(mes_norm[5:7])
    cursor.execute(
        """
        INSERT INTO resumen_mensual (anio, mes, mes_clave, dirty, actualizado)
        VALUES (?, ?, ?, 1, CURRENT_TIMESTAMP)
        ON CONFLICT(anio, mes) DO UPDATE SET
            mes_clave = excluded.mes_clave,
            dirty = 1,
            actualizado = CURRENT_TIMESTAMP
        """,
        (anio, mes, mes_norm),
    )


def _marcar_resumen_mensual_dirty_por_fecha_cursor(cursor, fecha_iso):
    mes_clave = _mes_clave_desde_fecha(fecha_iso)
    if mes_clave:
        _marcar_resumen_mensual_dirty_cursor(cursor, mes_clave)


def _marcar_resumen_mensual_dirty_por_rango_cursor(cursor, fecha_desde, fecha_hasta):
    try:
        inicio = datetime.strptime(str(fecha_desde)[:10], "%Y-%m-%d")
        fin = datetime.strptime(str(fecha_hasta)[:10], "%Y-%m-%d")
    except Exception:
        _marcar_resumen_mensual_dirty_por_fecha_cursor(cursor, fecha_desde)
        _marcar_resumen_mensual_dirty_por_fecha_cursor(cursor, fecha_hasta)
        return
    if fin < inicio:
        inicio, fin = fin, inicio
    cursor_dt = inicio.replace(day=1)
    fin_mes = fin.replace(day=1)
    while cursor_dt <= fin_mes:
        _marcar_resumen_mensual_dirty_cursor(cursor, cursor_dt.strftime("%Y-%m"))
        if cursor_dt.month == 12:
            cursor_dt = cursor_dt.replace(year=cursor_dt.year + 1, month=1, day=1)
        else:
            cursor_dt = cursor_dt.replace(month=cursor_dt.month + 1, day=1)


def _recalcular_resumen_mensual_anio_cursor(cursor, anio_int, meses_objetivo=None):
    anio_int = int(anio_int)
    inicio_anio = datetime.strptime(f"{anio_int}-01-01", "%Y-%m-%d")
    fin_anio = datetime.strptime(f"{anio_int}-12-31", "%Y-%m-%d")
    if meses_objetivo:
        meses_filtrados = sorted({int(m) for m in meses_objetivo if 1 <= int(m) <= 12})
    else:
        meses_filtrados = list(range(1, 13))
    month_keys = [f"{anio_int}-{mes:02d}" for mes in meses_filtrados]

    map_ventas = {
        key: {"ventas_local": 0.0, "ventas_uber": 0.0, "ventas_pedidosya": 0.0}
        for key in month_keys
    }
    semanas_por_mes = {key: set() for key in month_keys}

    cursor.execute(
        """
        SELECT id, semana_inicio, semana_fin, ventas_local, ventas_uber, ventas_pedidosya
        FROM ventas_semanales
        WHERE date(semana_fin) >= date(?)
          AND date(semana_inicio) <= date(?)
        ORDER BY date(semana_inicio) ASC, id ASC
        """,
        (f"{anio_int}-01-01", f"{anio_int}-12-31"),
    )
    for row in cursor.fetchall():
        try:
            semana_inicio_dt = datetime.strptime(str(row["semana_inicio"]), "%Y-%m-%d")
            semana_fin_dt = datetime.strptime(str(row["semana_fin"]), "%Y-%m-%d")
        except Exception:
            continue
        if semana_fin_dt < semana_inicio_dt:
            semana_fin_dt = semana_inicio_dt

        tramo_ini = max(semana_inicio_dt, inicio_anio)
        tramo_fin = min(semana_fin_dt, fin_anio)
        if tramo_fin < tramo_ini:
            continue

        total_dias_semana = max(1, int((semana_fin_dt - semana_inicio_dt).days) + 1)
        local_dia = float(row["ventas_local"] or 0) / total_dias_semana
        uber_dia = float(row["ventas_uber"] or 0) / total_dias_semana
        pedidos_dia = float(row["ventas_pedidosya"] or 0) / total_dias_semana
        semana_id = int(row["id"] or 0)

        cursor_dt = tramo_ini
        while cursor_dt <= tramo_fin:
            mes_clave = cursor_dt.strftime("%Y-%m")
            if mes_clave in map_ventas:
                map_ventas[mes_clave]["ventas_local"] += local_dia
                map_ventas[mes_clave]["ventas_uber"] += uber_dia
                map_ventas[mes_clave]["ventas_pedidosya"] += pedidos_dia
                if semana_id > 0:
                    semanas_por_mes[mes_clave].add(semana_id)
            cursor_dt += timedelta(days=1)

    map_compras = {}
    cursor.execute(
        """
        SELECT
            substr(fecha_factura, 1, 7) AS mes_clave,
            COUNT(*) AS documentos,
            COALESCE(SUM(monto_total), 0) AS compras_con_iva
        FROM facturas_archivo
        WHERE COALESCE(eliminado, 0) = 0
          AND date(fecha_factura) >= date(?)
          AND date(fecha_factura) <= date(?)
        GROUP BY substr(fecha_factura, 1, 7)
        ORDER BY mes_clave ASC
        """,
        (f"{anio_int}-01-01", f"{anio_int}-12-31"),
    )
    for row in cursor.fetchall():
        mes_clave = _normalizar_mes_clave_resumen(row["mes_clave"])
        if not mes_clave:
            continue
        map_compras[mes_clave] = {
            "documentos": int(row["documentos"] or 0),
            "compras_con_iva": float(row["compras_con_iva"] or 0),
        }

    for mes in meses_filtrados:
        mes_clave = f"{anio_int}-{mes:02d}"
        v = map_ventas.get(mes_clave, {})
        c = map_compras.get(mes_clave, {})
        cursor.execute(
            """
            INSERT INTO resumen_mensual (
                anio, mes, mes_clave, ventas_local, ventas_uber, ventas_pedidosya,
                compras_con_iva, documentos_compra, semanas_consideradas, dirty, actualizado
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, CURRENT_TIMESTAMP)
            ON CONFLICT(anio, mes) DO UPDATE SET
                mes_clave = excluded.mes_clave,
                ventas_local = excluded.ventas_local,
                ventas_uber = excluded.ventas_uber,
                ventas_pedidosya = excluded.ventas_pedidosya,
                compras_con_iva = excluded.compras_con_iva,
                documentos_compra = excluded.documentos_compra,
                semanas_consideradas = excluded.semanas_consideradas,
                dirty = 0,
                actualizado = CURRENT_TIMESTAMP
            """,
            (
                anio_int,
                mes,
                mes_clave,
                round(float(v.get("ventas_local") or 0), 6),
                round(float(v.get("ventas_uber") or 0), 6),
                round(float(v.get("ventas_pedidosya") or 0), 6),
                round(float(c.get("compras_con_iva") or 0), 6),
                int(c.get("documentos") or 0),
                len(semanas_por_mes.get(mes_clave) or set()),
            ),
        )


def _asegurar_resumen_mensual_materializado(cursor, anio_int):
    anio_int = int(anio_int)
    cursor.execute("SELECT COUNT(*) AS total FROM resumen_mensual WHERE anio = ?", (anio_int,))
    total_registros = int(cursor.fetchone()["total"] or 0)
    cursor.execute(
        """
        SELECT COUNT(*) AS total
        FROM resumen_mensual
        WHERE anio = ? AND COALESCE(dirty, 1) = 1
        """,
        (anio_int,),
    )
    total_dirty = int(cursor.fetchone()["total"] or 0)
    if total_registros < 12:
        _recalcular_resumen_mensual_anio_cursor(cursor, anio_int)
        return
    if total_dirty > 0:
        cursor.execute(
            """
            SELECT mes
            FROM resumen_mensual
            WHERE anio = ? AND COALESCE(dirty, 1) = 1
            ORDER BY mes ASC
            """,
            (anio_int,),
        )
        meses_dirty = [int(row["mes"] or 0) for row in cursor.fetchall() if int(row["mes"] or 0) > 0]
        if meses_dirty:
            _recalcular_resumen_mensual_anio_cursor(cursor, anio_int, meses_objetivo=meses_dirty)


def obtener_anios_tributarios_disponibles():
    conn = get_db()
    cursor = conn.cursor()
    try:
        anios = set()

        cursor.execute(
            """
            SELECT DISTINCT substr(fecha_factura, 1, 4) AS anio
            FROM facturas_archivo
            WHERE COALESCE(eliminado, 0) = 0
              AND fecha_factura IS NOT NULL
              AND trim(fecha_factura) <> ''
            """
        )
        for row in cursor.fetchall():
            raw = str(row["anio"] or "").strip()
            if raw.isdigit():
                anio = int(raw)
                if 2000 <= anio <= 2100:
                    anios.add(anio)

        cursor.execute(
            """
            SELECT DISTINCT substr(semana_inicio, 1, 4) AS anio
            FROM ventas_semanales
            WHERE semana_inicio IS NOT NULL
              AND trim(semana_inicio) <> ''
            """
        )
        for row in cursor.fetchall():
            raw = str(row["anio"] or "").strip()
            if raw.isdigit():
                anio = int(raw)
                if 2000 <= anio <= 2100:
                    anios.add(anio)

        cursor.execute(
            """
            SELECT DISTINCT anio
            FROM facturas_sii_ajustes
            WHERE anio IS NOT NULL
            """
        )
        for row in cursor.fetchall():
            try:
                anio = int(row["anio"] or 0)
            except (TypeError, ValueError):
                continue
            if 2000 <= anio <= 2100:
                anios.add(anio)

        cursor.execute(
            """
            SELECT DISTINCT anio
            FROM resumen_mensual
            WHERE anio IS NOT NULL
            """
        )
        for row in cursor.fetchall():
            try:
                anio = int(row["anio"] or 0)
            except (TypeError, ValueError):
                continue
            if 2000 <= anio <= 2100:
                anios.add(anio)

        anios.add(int(datetime.now().year))
        return sorted(anios, reverse=True)
    finally:
        conn.close()


def obtener_resumen_sii_facturas(anio=None, iva_pct=0.19, comision_apps_pct=0.30, ppm_pct=0.0):
    meses_es = (
        "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
        "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre",
    )

    anio_int = _normalizar_anio_tributario_sii(anio)

    tasa_iva = _normalizar_tasa_decimal(iva_pct, default=0.19, campo="IVA")
    tasa_comision = _normalizar_tasa_decimal(comision_apps_pct, default=0.30, campo="Comision apps")
    tasa_ppm = _normalizar_tasa_decimal(ppm_pct, default=0.0, campo="PPM")

    fecha_desde = f"{anio_int}-01-01"
    fecha_hasta = f"{anio_int}-12-31"
    divisor_iva = 1.0 + tasa_iva if (1.0 + tasa_iva) > 0 else 1.0

    conn = get_db()
    cursor = conn.cursor()
    try:
        ajustes_por_mes = {}
        cursor.execute(
            """
            SELECT mes_clave, valores_json
            FROM facturas_sii_ajustes
            WHERE anio = ?
            ORDER BY mes_clave ASC
            """,
            (anio_int,),
        )
        for row in cursor.fetchall():
            mes_clave = str(row["mes_clave"] or "").strip()
            if not mes_clave:
                continue
            try:
                payload = json.loads(row["valores_json"] or "{}")
            except Exception:
                payload = {}
            try:
                ajustes_por_mes[mes_clave] = _limpiar_valores_ajuste_sii(payload)
            except ValueError:
                ajustes_por_mes[mes_clave] = {}

        _asegurar_resumen_mensual_materializado(cursor, anio_int)
        month_keys = [f"{anio_int}-{mes:02d}" for mes in range(1, 13)]
        cursor.execute(
            """
            SELECT
                mes_clave,
                ventas_local,
                ventas_uber,
                ventas_pedidosya,
                compras_con_iva,
                documentos_compra
            FROM resumen_mensual
            WHERE anio = ?
            ORDER BY mes ASC
            """,
            (anio_int,),
        )
        map_materializado = {
            str(row["mes_clave"]): {
                "ventas_local": float(row["ventas_local"] or 0),
                "ventas_uber": float(row["ventas_uber"] or 0),
                "ventas_pedidosya": float(row["ventas_pedidosya"] or 0),
                "compras_con_iva": float(row["compras_con_iva"] or 0),
                "documentos": int(row["documentos_compra"] or 0),
            }
            for row in cursor.fetchall()
        }
        cursor.execute(
            """
            SELECT COUNT(*) AS total
            FROM ventas_semanales
            WHERE date(semana_fin) >= date(?)
              AND date(semana_inicio) <= date(?)
            """,
            (fecha_desde, fecha_hasta),
        )
        semanas_consideradas = int(cursor.fetchone()["total"] or 0)

        mensual = []
        totales = {
            "documentos_compra": 0,
            "ventas_local": 0.0,
            "ventas_uber": 0.0,
            "ventas_pedidosya": 0.0,
            "ventas_apps": 0.0,
            "ventas_brutas": 0.0,
            "comision_apps": 0.0,
            "ventas_netas_comision": 0.0,
            "ventas_netas_sin_iva": 0.0,
            "compras_con_iva": 0.0,
            "compras_sin_iva": 0.0,
            "iva_debito_estimado": 0.0,
            "iva_credito_estimado": 0.0,
            "iva_neto_estimado": 0.0,
            "remanente_credito": 0.0,
            "ppm_estimado": 0.0,
            "resultado_operacional": 0.0,
            "flujo_post_impuestos": 0.0,
        }

        for idx, mes_clave in enumerate(month_keys):
            base_mes = map_materializado.get(mes_clave, {})
            ventas_local = max(0.0, float(base_mes.get("ventas_local") or 0))
            ventas_uber = max(0.0, float(base_mes.get("ventas_uber") or 0))
            ventas_pedidosya = max(0.0, float(base_mes.get("ventas_pedidosya") or 0))
            compras_con_iva = max(0.0, float(base_mes.get("compras_con_iva") or 0))
            documentos = int(base_mes.get("documentos") or 0)

            ventas_apps = ventas_uber + ventas_pedidosya
            ventas_brutas = ventas_local + ventas_apps
            comision_apps = ventas_apps * tasa_comision
            ventas_netas_comision = ventas_brutas - comision_apps

            iva_debito = ventas_netas_comision * tasa_iva / divisor_iva
            compras_sin_iva = compras_con_iva / divisor_iva if compras_con_iva > 0 else 0.0
            iva_credito = compras_con_iva - compras_sin_iva
            iva_neto = max(0.0, iva_debito - iva_credito)
            remanente_credito = max(0.0, iva_credito - iva_debito)

            ventas_netas_sin_iva = ventas_netas_comision - iva_debito
            ppm_estimado = max(0.0, ventas_netas_sin_iva) * tasa_ppm
            resultado_operacional = ventas_netas_sin_iva - compras_sin_iva
            flujo_post_impuestos = resultado_operacional - iva_neto - ppm_estimado

            estado_iva = "sin movimiento"
            if iva_neto > 0:
                estado_iva = "pagar"
            elif remanente_credito > 0:
                estado_iva = "remanente"

            row_mes = {
                "mes_clave": mes_clave,
                "mes_num": idx + 1,
                "mes_label": f"{meses_es[idx]} {anio_int}",
                "documentos_compra": documentos,
                "ventas_local": round(ventas_local, 2),
                "ventas_uber": round(ventas_uber, 2),
                "ventas_pedidosya": round(ventas_pedidosya, 2),
                "ventas_apps": round(ventas_apps, 2),
                "ventas_brutas": round(ventas_brutas, 2),
                "comision_apps": round(comision_apps, 2),
                "ventas_netas_comision": round(ventas_netas_comision, 2),
                "ventas_netas_sin_iva": round(ventas_netas_sin_iva, 2),
                "compras_con_iva": round(compras_con_iva, 2),
                "compras_sin_iva": round(compras_sin_iva, 2),
                "iva_debito_estimado": round(iva_debito, 2),
                "iva_credito_estimado": round(iva_credito, 2),
                "iva_neto_estimado": round(iva_neto, 2),
                "remanente_credito": round(remanente_credito, 2),
                "ppm_estimado": round(ppm_estimado, 2),
                "resultado_operacional": round(resultado_operacional, 2),
                "flujo_post_impuestos": round(flujo_post_impuestos, 2),
                "estado_iva": estado_iva,
                "ajuste_manual": False,
            }

            ajuste = ajustes_por_mes.get(mes_clave) or {}
            if ajuste:
                for field in SII_AJUSTE_EDITABLE_FIELDS:
                    if field in ajuste:
                        row_mes[field] = round(float(ajuste[field] or 0), 2)
                iva_debito_editado = float(row_mes.get("iva_debito_estimado") or 0)
                iva_credito_editado = float(row_mes.get("iva_credito_estimado") or 0)
                row_mes["iva_neto_estimado"] = round(
                    max(0.0, iva_debito_editado - iva_credito_editado),
                    2,
                )
                row_mes["resultado_operacional"] = round(
                    float(row_mes.get("ventas_netas_sin_iva") or 0)
                    - float(row_mes.get("compras_sin_iva") or 0),
                    2,
                )
                row_mes["remanente_credito"] = round(
                    max(
                        0.0,
                        iva_credito_editado - iva_debito_editado,
                    ),
                    2,
                )
                row_mes["flujo_post_impuestos"] = round(
                    float(row_mes.get("resultado_operacional") or 0)
                    - float(row_mes.get("iva_neto_estimado") or 0)
                    - float(row_mes.get("ppm_estimado") or 0),
                    2,
                )
                iva_neto_editado = float(row_mes.get("iva_neto_estimado") or 0)
                if iva_neto_editado > 0:
                    row_mes["estado_iva"] = "pagar"
                elif float(row_mes.get("remanente_credito") or 0) > 0:
                    row_mes["estado_iva"] = "remanente"
                else:
                    row_mes["estado_iva"] = "sin movimiento"
                row_mes["ajuste_manual"] = True

            mensual.append(row_mes)

            totales["documentos_compra"] += documentos
            for key in (
                "ventas_local",
                "ventas_uber",
                "ventas_pedidosya",
                "ventas_apps",
                "ventas_brutas",
                "comision_apps",
                "ventas_netas_comision",
                "ventas_netas_sin_iva",
                "compras_con_iva",
                "compras_sin_iva",
                "iva_debito_estimado",
                "iva_credito_estimado",
                "iva_neto_estimado",
                "remanente_credito",
                "ppm_estimado",
                "resultado_operacional",
                "flujo_post_impuestos",
            ):
                totales[key] += float(row_mes[key] or 0)

        for key, value in list(totales.items()):
            if isinstance(value, float):
                totales[key] = round(value, 2)

        alertas = []
        if totales["ventas_brutas"] <= 0:
            alertas.append("No hay ventas semanales registradas para este a\u00f1o. Completa Ventas Semanales para estimar F29/F22.")
        if totales["compras_con_iva"] <= 0:
            alertas.append("No hay compras en Facturas para este a\u00f1o. El IVA cr\u00e9dito quedar\u00e1 en cero.")
        if totales["remanente_credito"] > 0 and totales["iva_neto_estimado"] <= 0:
            alertas.append("Existe remanente de IVA credito estimado en el periodo.")
        if tasa_ppm <= 0:
            alertas.append("PPM estimado en 0%. Si corresponde, define una tasa para simular F29/F22.")

        return {
            "anio": anio_int,
            "periodo": {"desde": fecha_desde, "hasta": fecha_hasta},
            "tasas": {
                "iva_pct": round(tasa_iva * 100, 4),
                "comision_apps_pct": round(tasa_comision * 100, 4),
                "ppm_pct": round(tasa_ppm * 100, 4),
            },
            "meta": {
                "semanas_consideradas": int(semanas_consideradas),
                "facturas_registradas": int(totales["documentos_compra"]),
                "meses_con_ajuste_manual": int(
                    sum(1 for r in mensual if bool(r.get("ajuste_manual")))
                ),
                "meses_con_movimiento": int(
                    sum(
                        1
                        for r in mensual
                        if float(r["ventas_brutas"] or 0) > 0
                        or float(r["compras_con_iva"] or 0) > 0
                    )
                ),
            },
            "totales": totales,
            "mensual": mensual,
            "alertas": alertas,
        }
    finally:
        conn.close()


def _normalizar_fecha_generica(valor, campo="fecha"):
    raw = str(valor or "").strip()
    if not raw:
        raise ValueError(f"La {campo} es obligatoria")
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError(f"{campo.capitalize()} invalida")


def _rango_semana_lunes_domingo(fecha_iso):
    base = datetime.strptime(str(fecha_iso), "%Y-%m-%d")
    inicio = base - timedelta(days=base.weekday())
    fin = inicio + timedelta(days=6)
    return inicio.strftime("%Y-%m-%d"), fin.strftime("%Y-%m-%d")


def _enriquecer_venta_semanal(row, compras_facturadas=0.0, comision_apps_pct=0.30, iva_pct=0.19):
    item = dict(row) if not isinstance(row, dict) else dict(row)
    ventas_local = max(0.0, float(item.get("ventas_local") or 0))
    ventas_uber = max(0.0, float(item.get("ventas_uber") or 0))
    ventas_pedidosya = max(0.0, float(item.get("ventas_pedidosya") or 0))
    compras = max(0.0, float(compras_facturadas or item.get("compras_facturadas") or 0))
    ventas_apps = ventas_uber + ventas_pedidosya
    ventas_brutas_canal = ventas_local + ventas_apps

    ventas_monto = max(0.0, float(item.get("ventas_monto") or ventas_brutas_canal))
    marketing_monto = max(0.0, float(item.get("marketing_monto") or 0))
    otros_descuentos_monto = max(0.0, float(item.get("otros_descuentos_monto") or 0))

    tasa_servicio_pct_raw = float(item.get("tasa_servicio_pct") or 30.0)
    impuesto_tasa_servicio_pct_raw = float(item.get("impuesto_tasa_servicio_pct") or 19.0)
    tasa_servicio_pct = tasa_servicio_pct_raw if tasa_servicio_pct_raw > 1 else (tasa_servicio_pct_raw * 100.0)
    impuesto_tasa_servicio_pct = (
        impuesto_tasa_servicio_pct_raw
        if impuesto_tasa_servicio_pct_raw > 1
        else (impuesto_tasa_servicio_pct_raw * 100.0)
    )
    tasa_servicio = max(0.0, tasa_servicio_pct) / 100.0
    impuesto_tasa_servicio = max(0.0, impuesto_tasa_servicio_pct) / 100.0

    servicio_base = ventas_monto * tasa_servicio
    impuesto_servicio = servicio_base * impuesto_tasa_servicio
    costo_servicio_total = servicio_base + impuesto_servicio
    ventas_netas_servicio = ventas_monto - costo_servicio_total
    ganancia_neta = ventas_monto - marketing_monto - costo_servicio_total - otros_descuentos_monto

    tasa_iva = max(0.0, float(iva_pct or 0))
    divisor_iva = 1.0 + tasa_iva
    iva_credito = (compras * tasa_iva / divisor_iva) if divisor_iva > 0 else 0.0
    iva_neto = max(0.0, impuesto_servicio - iva_credito)
    saldo_vs_compras = ganancia_neta - compras
    saldo_estimado = ganancia_neta

    item["ventas_local"] = round(ventas_local, 2)
    item["ventas_uber"] = round(ventas_uber, 2)
    item["ventas_pedidosya"] = round(ventas_pedidosya, 2)
    item["compras_facturadas"] = round(compras, 2)
    item["ventas_apps"] = round(ventas_apps, 2)
    item["ventas_brutas"] = round(ventas_monto, 2)
    item["ventas_monto"] = round(ventas_monto, 2)
    item["marketing_monto"] = round(marketing_monto, 2)
    item["otros_descuentos_monto"] = round(otros_descuentos_monto, 2)
    item["tasa_servicio_pct"] = round(tasa_servicio_pct, 4)
    item["impuesto_tasa_servicio_pct"] = round(impuesto_tasa_servicio_pct, 4)
    item["servicio_base"] = round(servicio_base, 2)
    item["impuesto_servicio"] = round(impuesto_servicio, 2)
    item["costo_servicio_total"] = round(costo_servicio_total, 2)
    item["ganancia_neta"] = round(ganancia_neta, 2)
    item["ganancia_vs_compras"] = round(saldo_vs_compras, 2)

    # Campos legacy para no romper pantallas antiguas.
    item["comision_apps"] = round(servicio_base, 2)
    item["ventas_netas_comision"] = round(max(0.0, ventas_monto - servicio_base), 2)
    item["ventas_netas_sin_iva"] = round(ventas_netas_servicio, 2)
    item["iva_debito_estimado"] = round(impuesto_servicio, 2)
    item["iva_credito_estimado"] = round(iva_credito, 2)
    item["iva_neto_estimado"] = round(iva_neto, 2)
    item["saldo_vs_compras"] = round(saldo_vs_compras, 2)
    item["saldo_estimado"] = round(saldo_estimado, 2)
    return item


def guardar_venta_semanal(data):
    payload = data or {}
    try:
        fecha_base = _normalizar_fecha_generica(
            payload.get("semana_inicio") or payload.get("fecha") or datetime.now().strftime("%Y-%m-%d"),
            "semana",
        )
    except ValueError as e:
        return {"success": False, "error": str(e)}
    semana_inicio, semana_fin = _rango_semana_lunes_domingo(fecha_base)

    ventas_local = max(0.0, _parse_float(payload.get("ventas_local"), 0))
    ventas_uber = max(0.0, _parse_float(payload.get("ventas_uber"), 0))
    ventas_pedidosya = max(0.0, _parse_float(payload.get("ventas_pedidosya"), 0))
    ventas_monto_base = ventas_local + ventas_uber + ventas_pedidosya
    ventas_monto = max(0.0, _parse_float(payload.get("ventas_monto"), ventas_monto_base))
    if ventas_monto <= 0 and ventas_monto_base > 0:
        ventas_monto = ventas_monto_base
    if ventas_monto_base <= 0 and ventas_monto > 0:
        ventas_local = ventas_monto
        ventas_uber = 0.0
        ventas_pedidosya = 0.0

    marketing_monto = max(0.0, _parse_float(payload.get("marketing_monto"), 0))
    otros_descuentos_monto = max(0.0, _parse_float(payload.get("otros_descuentos_monto"), 0))

    tasa_servicio_pct = max(0.0, _parse_float(payload.get("tasa_servicio_pct"), 30))
    impuesto_tasa_servicio_pct = max(0.0, _parse_float(payload.get("impuesto_tasa_servicio_pct"), 19))
    if tasa_servicio_pct <= 1:
        tasa_servicio_pct *= 100
    if impuesto_tasa_servicio_pct <= 1:
        impuesto_tasa_servicio_pct *= 100

    notas = (payload.get("notas") or "").strip()[:500] or None

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO ventas_semanales (
                semana_inicio, semana_fin, ventas_local, ventas_uber, ventas_pedidosya,
                ventas_monto, marketing_monto, otros_descuentos_monto,
                tasa_servicio_pct, impuesto_tasa_servicio_pct, notas, actualizado
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(semana_inicio) DO UPDATE SET
                semana_fin = excluded.semana_fin,
                ventas_local = excluded.ventas_local,
                ventas_uber = excluded.ventas_uber,
                ventas_pedidosya = excluded.ventas_pedidosya,
                ventas_monto = excluded.ventas_monto,
                marketing_monto = excluded.marketing_monto,
                otros_descuentos_monto = excluded.otros_descuentos_monto,
                tasa_servicio_pct = excluded.tasa_servicio_pct,
                impuesto_tasa_servicio_pct = excluded.impuesto_tasa_servicio_pct,
                notas = excluded.notas,
                actualizado = CURRENT_TIMESTAMP
            """,
            (
                semana_inicio,
                semana_fin,
                ventas_local,
                ventas_uber,
                ventas_pedidosya,
                ventas_monto,
                marketing_monto,
                otros_descuentos_monto,
                tasa_servicio_pct,
                impuesto_tasa_servicio_pct,
                notas,
            ),
        )
        cursor.execute(
            """
            SELECT *
            FROM ventas_semanales
            WHERE semana_inicio = ?
            LIMIT 1
            """,
            (semana_inicio,),
        )
        row = cursor.fetchone()
        if not row:
            conn.rollback()
            return {"success": False, "error": "No se pudo guardar la venta semanal"}

        cursor.execute(
            """
            SELECT COALESCE(SUM(monto_total), 0) AS total
            FROM facturas_archivo
            WHERE COALESCE(eliminado, 0) = 0
              AND date(fecha_factura) >= date(?)
              AND date(fecha_factura) <= date(?)
            """,
            (semana_inicio, semana_fin),
        )
        row_total = cursor.fetchone()
        compras = float(row_total["total"] or 0) if row_total else 0.0
        _marcar_resumen_mensual_dirty_por_rango_cursor(cursor, semana_inicio, semana_fin)
        conn.commit()
        return {
            "success": True,
            "registro": _enriquecer_venta_semanal(dict(row), compras_facturadas=compras),
        }
    except Exception as e:
        conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        conn.close()


def listar_ventas_semanales(fecha_desde=None, fecha_hasta=None, limite=20):
    conn = get_db()
    cursor = conn.cursor()
    try:
        where = []
        params = []
        if fecha_desde:
            where.append("date(v.semana_fin) >= date(?)")
            params.append(_normalizar_fecha_generica(fecha_desde, "fecha desde"))
        if fecha_hasta:
            where.append("date(v.semana_inicio) <= date(?)")
            params.append(_normalizar_fecha_generica(fecha_hasta, "fecha hasta"))
        where_sql = (" WHERE " + " AND ".join(where)) if where else ""

        sql = f"""
            SELECT
                v.*,
                COALESCE(SUM(f.monto_total), 0) AS compras_facturadas
            FROM ventas_semanales v
            LEFT JOIN facturas_archivo f
              ON COALESCE(f.eliminado, 0) = 0
             AND date(f.fecha_factura) >= date(v.semana_inicio)
             AND date(f.fecha_factura) <= date(v.semana_fin)
            {where_sql}
            GROUP BY
                v.id,
                v.semana_inicio,
                v.semana_fin,
                v.ventas_local,
                v.ventas_uber,
                v.ventas_pedidosya,
                v.ventas_monto,
                v.marketing_monto,
                v.otros_descuentos_monto,
                v.tasa_servicio_pct,
                v.impuesto_tasa_servicio_pct,
                v.notas,
                v.creado,
                v.actualizado
            ORDER BY date(v.semana_inicio) DESC, v.id DESC
        """
        if limite is not None:
            tope = max(1, min(int(limite or 20), 260))
            sql += " LIMIT ?"
            params.append(tope)

        cursor.execute(sql, params)
        rows = cursor.fetchall()
        return [_enriquecer_venta_semanal(dict(row)) for row in rows]
    finally:
        conn.close()


def eliminar_venta_semanal(venta_semanal_id):
    try:
        registro_id = int(venta_semanal_id)
    except (TypeError, ValueError):
        return {"success": False, "error": "Registro semanal invalido"}
    if registro_id <= 0:
        return {"success": False, "error": "Registro semanal invalido"}

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM ventas_semanales WHERE id = ?", (registro_id,))
        existente = cursor.fetchone()
        if not existente:
            return {"success": False, "error": "Registro semanal no encontrado"}

        cursor.execute("DELETE FROM ventas_semanales WHERE id = ?", (registro_id,))
        _marcar_resumen_mensual_dirty_por_rango_cursor(
            cursor,
            existente["semana_inicio"],
            existente["semana_fin"] or existente["semana_inicio"],
        )
        conn.commit()
        return {"success": True, "registro": dict(existente)}
    except Exception as e:
        conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        conn.close()


def obtener_resumen_ventas_vs_compras(fecha_desde=None, fecha_hasta=None, comision_apps_pct=0.30, iva_pct=0.19):
    semanas = listar_ventas_semanales(fecha_desde=fecha_desde, fecha_hasta=fecha_hasta, limite=None)

    totales = {
        "registros": 0,
        "ventas_local": 0.0,
        "ventas_uber": 0.0,
        "ventas_pedidosya": 0.0,
        "ventas_apps": 0.0,
        "ventas_brutas": 0.0,
        "ventas_monto": 0.0,
        "marketing_monto": 0.0,
        "otros_descuentos_monto": 0.0,
        "servicio_base": 0.0,
        "impuesto_servicio": 0.0,
        "costo_servicio_total": 0.0,
        "ganancia_neta": 0.0,
        "ganancia_vs_compras": 0.0,
        "tasa_servicio_pct_prom": 0.0,
        "impuesto_tasa_servicio_pct_prom": 0.0,
        "comision_apps": 0.0,
        "ventas_netas_comision": 0.0,
        "ventas_netas_sin_iva": 0.0,
        "compras_facturadas": 0.0,
        "iva_debito_estimado": 0.0,
        "iva_credito_estimado": 0.0,
        "iva_neto_estimado": 0.0,
        "saldo_vs_compras": 0.0,
        "saldo_estimado": 0.0,
    }

    acumulado_tasa_servicio = 0.0
    acumulado_tasa_impuesto_servicio = 0.0

    for row in semanas:
        totales["registros"] += 1
        acumulado_tasa_servicio += float(row.get("tasa_servicio_pct") or 30.0)
        acumulado_tasa_impuesto_servicio += float(row.get("impuesto_tasa_servicio_pct") or 19.0)
        for key in (
            "ventas_local",
            "ventas_uber",
            "ventas_pedidosya",
            "ventas_apps",
            "ventas_brutas",
            "ventas_monto",
            "marketing_monto",
            "otros_descuentos_monto",
            "servicio_base",
            "impuesto_servicio",
            "costo_servicio_total",
            "ganancia_neta",
            "ganancia_vs_compras",
            "comision_apps",
            "ventas_netas_comision",
            "ventas_netas_sin_iva",
            "compras_facturadas",
            "iva_debito_estimado",
            "iva_credito_estimado",
            "iva_neto_estimado",
            "saldo_vs_compras",
            "saldo_estimado",
        ):
            totales[key] += float(row.get(key) or 0)

    for key, value in list(totales.items()):
        if isinstance(value, float):
            totales[key] = round(value, 2)

    if totales["registros"] > 0:
        totales["tasa_servicio_pct_prom"] = round(acumulado_tasa_servicio / totales["registros"], 4)
        totales["impuesto_tasa_servicio_pct_prom"] = round(
            acumulado_tasa_impuesto_servicio / totales["registros"],
            4,
        )
    else:
        totales["tasa_servicio_pct_prom"] = 30.0
        totales["impuesto_tasa_servicio_pct_prom"] = 19.0

    return {
        "comision_apps_pct": round(float(comision_apps_pct or 0) * 100, 2),
        "iva_pct": round(float(iva_pct or 0) * 100, 2),
        "tasa_servicio_pct": totales["tasa_servicio_pct_prom"],
        "impuesto_tasa_servicio_pct": totales["impuesto_tasa_servicio_pct_prom"],
        "totales": totales,
        "semanas": semanas,
    }

