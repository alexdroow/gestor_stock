import ctypes
import getpass
import math
import os
import subprocess
import sys
import time

from database import (
    get_db,
    migrar_db,
    obtener_config_alertas,
    obtener_recordatorios_agenda_pendientes,
    descartar_recordatorio_agenda,
    convertir_a_base,
)


TASK_NAME = "GestionStockPro-Recordatorios"


def _normalizar_unidad_producto(unidad_raw):
    unidad = str(unidad_raw or "").strip().lower()
    unidad = (
        unidad.replace("\u00e1", "a")
        .replace("\u00e9", "e")
        .replace("\u00ed", "i")
        .replace("\u00f3", "o")
        .replace("\u00fa", "u")
    )
    equivalencias = {
        "u": "unidad",
        "und": "unidad",
        "unid": "unidad",
        "unidad": "unidad",
        "unidades": "unidad",
        "pieza": "unidad",
        "piezas": "unidad",
        "g": "gr",
        "gr": "gr",
        "gramo": "gr",
        "gramos": "gr",
        "kg": "kg",
        "kilo": "kg",
        "kilogramo": "kg",
        "kilogramos": "kg",
        "ml": "ml",
        "cc": "ml",
        "mililitro": "ml",
        "mililitros": "ml",
        "l": "lt",
        "lt": "lt",
        "litro": "lt",
        "litros": "lt",
        "porcion": "porcion",
        "porciones": "porcion",
    }
    return equivalencias.get(unidad, unidad or "unidad")


def _tipo_unidad(unidad_raw):
    unidad = _normalizar_unidad_producto(unidad_raw)
    if unidad in {"mg", "g", "gr", "kg", "oz", "lb"}:
        return "solido"
    if unidad in {"ml", "cc", "lt", "l", "taza", "cda", "cdt"}:
        return "liquido"
    return "generico"


def _unidades_compatibles_porcion(unidad_1, unidad_2):
    return _tipo_unidad(unidad_1) == _tipo_unidad(unidad_2)


def _cantidad_porcion_en_unidad_stock(cantidad, unidad_origen, unidad_destino):
    unidad_origen = _normalizar_unidad_producto(unidad_origen)
    unidad_destino = _normalizar_unidad_producto(unidad_destino)
    cantidad_num = float(cantidad or 0)
    if cantidad_num <= 0:
        return None
    if unidad_origen == unidad_destino:
        return cantidad_num
    if not _unidades_compatibles_porcion(unidad_origen, unidad_destino):
        return None
    cantidad_base = convertir_a_base(cantidad_num, unidad_origen)
    factor_destino = convertir_a_base(1, unidad_destino)
    if not factor_destino:
        return None
    return cantidad_base / factor_destino


def _producto_sin_porcion_disponible(producto):
    stock = float(producto["stock"] or 0)
    unidad_stock = _normalizar_unidad_producto(producto["unidad"] or "unidad")
    porcion_cantidad = float(producto["porcion_cantidad"] or 1)
    porcion_unidad = _normalizar_unidad_producto(producto["porcion_unidad"] or unidad_stock)
    porcion_en_stock = _cantidad_porcion_en_unidad_stock(porcion_cantidad, porcion_unidad, unidad_stock)
    if not porcion_en_stock or porcion_en_stock <= 0:
        return True
    porciones_disponibles = int(math.floor((stock + 1e-9) / porcion_en_stock))
    return porciones_disponibles < 1


def _contar_alertas_productos(cursor):
    cursor.execute(
        """
        SELECT id, stock, stock_minimo, unidad, porcion_cantidad, porcion_unidad
        FROM productos
        """
    )
    filas = cursor.fetchall()

    ids_stock_bajo = set()
    ids_baja_porcion = set()
    for fila in filas:
        producto_id = int(fila["id"] or 0)
        stock = float(fila["stock"] or 0)
        stock_minimo = float(fila["stock_minimo"] or 0)
        if stock <= stock_minimo:
            ids_stock_bajo.add(producto_id)
        if _producto_sin_porcion_disponible(fila):
            ids_baja_porcion.add(producto_id)

    return {
        "total": len(ids_stock_bajo | ids_baja_porcion),
        "stock_bajo": len(ids_stock_bajo),
        "baja_porcion": len(ids_baja_porcion),
    }


def _run_schtasks(args):
    try:
        completed = subprocess.run(
            ["schtasks", *args],
            capture_output=True,
            text=True,
            check=False,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        return completed.returncode == 0
    except Exception:
        return False


def ensure_background_startup():
    """Crea/elimina tarea programada de recordatorios según configuración."""
    if not getattr(sys, "frozen", False):
        return False

    try:
        config = obtener_config_alertas()
    except Exception:
        return False

    if not config.get("inicio_windows", 1):
        _run_schtasks(["/Delete", "/TN", TASK_NAME, "/F"])
        return True

    exe_path = os.path.abspath(sys.executable)
    run_cmd = f'"{exe_path}" --background-agent'
    user = getpass.getuser()
    return _run_schtasks(
        [
            "/Create",
            "/F",
            "/SC",
            "ONLOGON",
            "/RL",
            "LIMITED",
            "/TN",
            TASK_NAME,
            "/TR",
            run_cmd,
            "/RU",
            user,
        ]
    )


def _cargar_alertas_stock_vencimiento():
    conn = get_db()
    cursor = conn.cursor()
    try:
        alertas_prod = _contar_alertas_productos(cursor)
        prod = int(alertas_prod["total"] or 0)
        cursor.execute("SELECT COUNT(*) AS total FROM insumos WHERE stock_minimo > 0 AND stock <= stock_minimo")
        ins = int(cursor.fetchone()["total"] or 0)

        cfg = obtener_config_alertas()
        dias = max(0, int(cfg.get("dias_anticipacion", 2) or 2))
        cursor.execute(
            """
            SELECT COUNT(*) AS total
            FROM productos
            WHERE fecha_vencimiento IS NOT NULL
              AND date(fecha_vencimiento) <= date('now', '+' || ? || ' days')
            """,
            (dias,),
        )
        venc = int(cursor.fetchone()["total"] or 0)
        return {
            "productos_bajo": prod,
            "productos_stock_bajo": int(alertas_prod["stock_bajo"] or 0),
            "productos_baja_porcion": int(alertas_prod["baja_porcion"] or 0),
            "insumos_bajo": ins,
            "por_vencer": venc,
        }
    finally:
        conn.close()


def _notificar_persistente(titulo, mensaje):
    # MB_OKCANCEL | MB_ICONWARNING | MB_TOPMOST
    flags = 0x00000001 | 0x00000030 | 0x00040000
    return ctypes.windll.user32.MessageBoxW(None, mensaje, titulo, flags)


def run_background_agent():
    """Loop en segundo plano para recordatorios cada N minutos."""
    migrar_db()
    while True:
        try:
            cfg = obtener_config_alertas()
            if not cfg.get("notificaciones_activas", 1):
                time.sleep(60)
                continue

            repetir = max(1, int(cfg.get("repetir_minutos", 15) or 15))

            if cfg.get("incluir_agenda", 1):
                eventos = obtener_recordatorios_agenda_pendientes()
                for ev in eventos:
                    hora = ev.get("hora_inicio") or "sin hora"
                    msg = (
                        f"{ev.get('titulo', 'Evento')}\n"
                        f"Fecha: {ev.get('fecha')} {hora}\n\n"
                        "Aceptar = descartar este aviso\nCancelar = volver a avisar"
                    )
                    respuesta = _notificar_persistente("Recordatorio de Agenda", msg)
                    if respuesta == 1:
                        descartar_recordatorio_agenda(ev["id"], ev["ventana_clave"])

            resumen = _cargar_alertas_stock_vencimiento()
            lineas = []
            if cfg.get("incluir_stock_bajo", 1) and (resumen["productos_bajo"] or resumen["insumos_bajo"]):
                lineas.append(
                    f"Stock bajo: {resumen['productos_bajo']} productos, {resumen['insumos_bajo']} insumos."
                )
                if resumen.get("productos_baja_porcion", 0):
                    lineas.append(
                        f"Sin porcion disponible: {resumen['productos_baja_porcion']} productos."
                    )
            if cfg.get("incluir_vencimientos", 1) and resumen["por_vencer"]:
                lineas.append(f"Próximos a vencer: {resumen['por_vencer']} productos.")

            if lineas:
                _notificar_persistente(
                    "Alertas de Inventario",
                    "\n".join(lineas) + "\n\nAceptar = descartar por ahora",
                )

            time.sleep(repetir * 60)
        except Exception:
            time.sleep(60)
