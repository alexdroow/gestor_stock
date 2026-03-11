import json
import os
import shutil
import sys
from datetime import date, datetime
from io import BytesIO


class StressTester:
    def __init__(self):
        self.failures = []
        self.warnings = []
        self.logs = []

    def log(self, msg):
        self.logs.append(msg)
        print(msg)

    def check(self, condition, title, detail=""):
        if condition:
            self.log(f"[OK] {title}")
        else:
            info = f"{title}" + (f" -> {detail}" if detail else "")
            self.failures.append(info)
            self.log(f"[FAIL] {info}")

    def warn(self, title, detail=""):
        info = f"{title}" + (f" -> {detail}" if detail else "")
        self.warnings.append(info)
        self.log(f"[WARN] {info}")

    @staticmethod
    def response_json(resp):
        try:
            return resp.get_json(silent=True)
        except Exception:
            return None


def _query_one(get_db, sql, params=()):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(sql, params)
    row = cur.fetchone()
    conn.close()
    return row


def main():
    tester = StressTester()
    base_tmp_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".stress_tmp")
    os.makedirs(base_tmp_dir, exist_ok=True)
    temp_dir = os.path.join(base_tmp_dir, f"gestor_stock_stress_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}")
    os.makedirs(temp_dir, exist_ok=True)
    os.environ["GESTIONSTOCK_DATA_DIR"] = temp_dir
    os.environ.pop("GESTIONSTOCK_DB_PATH", None)

    tester.log(f"[INFO] Directorio temporal de prueba: {temp_dir}")

    try:
        from app import app
        from database import get_db

        app.testing = True
        client = app.test_client()

        tag = datetime.now().strftime("%H%M%S")
        today = date.today().strftime("%Y-%m-%d")

        # 1) Smoke pages
        pages = [
            "/",
            "/productos",
            "/insumos",
            "/produccion",
            "/agenda",
            "/ventas",
            "/historial-ventas",
            "/reportes",
            "/facturas",
            "/alertas",
            "/settings",
        ]
        for p in pages:
            resp = client.get(p)
            tester.check(resp.status_code == 200, f"GET {p}", f"status={resp.status_code}")

        # 2) Baseline APIs
        baseline_apis = [
            "/api/estadisticas",
            "/api/productos/todos",
            "/api/productos/stock-disponible",
            "/api/compras-pendientes",
            "/api/produccion/historial",
            "/api/alertas/contador",
            "/api/alertas/vencimiento",
            "/api/alertas/config",
            "/api/alertas/recordatorios",
            "/api/reportes/ventas-semanal",
            "/api/reportes/top-productos",
            "/api/reportes/produccion",
            "/api/reportes/insumos-agregados",
            "/api/reportes/productos-agregados",
            "/api/reportes/mermas-productos",
            "/api/reportes/kardex",
            "/api/reportes/sugerencias-compra",
            "/api/agenda/eventos",
            "/api/agenda/notas",
            "/api/agenda/eventos/proximos",
            "/api/insumo/buscar?codigo=NO_EXISTE",
            "/api/backup/ultimo",
        ]
        for endpoint in baseline_apis:
            resp = client.get(endpoint)
            tester.check(resp.status_code == 200, f"GET {endpoint}", f"status={resp.status_code}")

        # 2.1) Crear por modo AJAX (JSON) sin redirección
        ajax_product_name = f"ProductoAjax_{tag}"
        resp = client.post(
            "/api/producto/agregar",
            data={
                "nombre_producto": ajax_product_name,
                "stock_producto": "1",
                "stock_minimo": "0",
                "unidad": "unidad",
                "porcion_cantidad": "1",
                "porcion_unidad": "unidad",
            },
            headers={"Accept": "application/json", "X-Requested-With": "fetch"},
        )
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Crear producto (modo AJAX)")
        ajax_product_id = data.get("producto_id")
        if ajax_product_id:
            resp = client.post(f"/api/producto/{ajax_product_id}/eliminar")
            data = tester.response_json(resp) or {}
            tester.check(resp.status_code == 200 and data.get("success"), "Eliminar producto AJAX")

        ajax_insumo_code = f"AJX{tag}"
        ajax_insumo_name = f"InsumoAjax_{tag}"
        resp = client.post(
            "/api/insumo/agregar",
            data={
                "codigo_barra": ajax_insumo_code,
                "nombre_insumo": ajax_insumo_name,
                "stock_insumo": "1",
                "stock_minimo": "0",
                "unidad": "unidad",
                "precio_unitario": "1000",
                "cantidad_comprada": "1",
                "unidad_compra": "unidad",
                "cantidad_por_scan": "1",
                "unidad_por_scan": "unidad",
            },
            headers={"Accept": "application/json", "X-Requested-With": "fetch"},
        )
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Crear insumo (modo AJAX)")
        ajax_insumo_id = data.get("insumo_id")
        if ajax_insumo_id:
            resp = client.post(f"/api/insumo/{ajax_insumo_id}/eliminar")
            data = tester.response_json(resp) or {}
            tester.check(resp.status_code == 200 and data.get("success"), "Eliminar insumo AJAX")

        # 3) Product CRUD + stock + lots + merma
        product_name = f"ProductoQA_{tag}"
        resp = client.post(
            "/api/producto/agregar",
            data={
                "nombre_producto": product_name,
                "stock_producto": "12",
                "stock_minimo": "2",
                "unidad": "unidad",
                "vencimiento_cantidad": "5",
                "vencimiento_tipo": "dias",
                "alerta_previa": "1",
            },
        )
        tester.check(resp.status_code in (302, 303), "Crear producto", f"status={resp.status_code}")

        row = _query_one(get_db, "SELECT id FROM productos WHERE nombre = ? ORDER BY id DESC LIMIT 1", (product_name,))
        tester.check(row is not None, "Producto creado en BD")
        if row is None:
            raise RuntimeError("No se pudo continuar: producto no creado")
        product_id = int(row["id"])

        resp = client.get(f"/api/producto/{product_id}/detalle")
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Detalle producto")

        resp = client.post(f"/api/producto/{product_id}/stock", json={"cantidad": -2})
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Descontar stock producto")

        resp = client.post(f"/api/producto/{product_id}/stock", json={"cantidad": 3})
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Sumar stock producto")

        resp = client.post(
            f"/api/producto/{product_id}/actualizar",
            json={
                "nombre": f"{product_name}_Editado",
                "stock_minimo": 1,
                "unidad": "unidad",
                "alerta_dias": 2,
                "precio": 2500,
                "vida_util_dias": 7,
            },
        )
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Actualizar producto")

        resp = client.post(
            f"/api/producto/{product_id}/agregar-lote",
            data={"cantidad": "4", "vencimiento_cantidad": "3", "vencimiento_tipo": "dias"},
        )
        tester.check(resp.status_code in (302, 303), "Agregar lote producto", f"status={resp.status_code}")

        resp = client.get(f"/api/producto/{product_id}/lotes")
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Listar lotes producto")

        resp = client.post("/api/producto/merma", json={"producto_id": product_id, "cantidad": 1, "motivo": "Merma", "detalle": "stress"})
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Registrar merma de producto")
        merma_id = data.get("id")
        if merma_id:
            resp = client.post(f"/api/producto/merma/{merma_id}/revertir")
            data = tester.response_json(resp) or {}
            tester.check(resp.status_code == 200 and data.get("success"), "Revertir merma")

        # duplicate and delete duplicate
        resp = client.post(f"/api/producto/{product_id}/duplicar")
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Duplicar producto")

        dup = _query_one(get_db, "SELECT id FROM productos WHERE nombre LIKE ? ORDER BY id DESC LIMIT 1", (f"{product_name}_Editado (Copia)%",))
        if dup:
            dup_id = int(dup["id"])
            resp = client.post(f"/api/producto/{dup_id}/eliminar")
            data = tester.response_json(resp) or {}
            tester.check(resp.status_code == 200 and data.get("success"), "Eliminar producto duplicado")
        else:
            tester.warn("No se encontró producto duplicado para eliminar")

        # 4) Insumos CRUD + scanner/lote rapido
        insumo_code = f"CODQA{tag}"
        insumo_name = f"InsumoQA_{tag}"
        resp = client.post(
            "/api/insumo/agregar",
            data={
                "codigo_barra": insumo_code,
                "nombre_insumo": insumo_name,
                "stock_insumo": "8",
                "stock_minimo": "1",
                "unidad": "kg",
                "precio_unitario": "12000",
                "cantidad_comprada": "1",
                "unidad_compra": "kg",
                "precio_incluye_iva": "on",
                "cantidad_por_scan": "1",
                "unidad_por_scan": "kg",
            },
        )
        tester.check(resp.status_code in (302, 303), "Crear insumo", f"status={resp.status_code}")

        ins_row = _query_one(get_db, "SELECT id FROM insumos WHERE codigo_barra = ? ORDER BY id DESC LIMIT 1", (insumo_code,))
        tester.check(ins_row is not None, "Insumo creado en BD")
        if ins_row is None:
            raise RuntimeError("No se pudo continuar: insumo no creado")
        insumo_id = int(ins_row["id"])

        resp = client.get(f"/api/insumo/{insumo_id}/detalle")
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Detalle insumo")

        resp = client.post(f"/api/insumo/{insumo_id}/stock", json={"cantidad": -1})
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Descontar stock insumo")

        resp = client.post(f"/api/insumo/{insumo_id}/stock", json={"cantidad": 2})
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Sumar stock insumo")

        resp = client.post(
            f"/api/insumo/{insumo_id}/actualizar",
            json={
                "codigo_barra": insumo_code,
                "nombre": f"{insumo_name}_Editado",
                "stock": 9,
                "stock_minimo": 2,
                "unidad": "kg",
                "precio_unitario": 13000,
                "cantidad_comprada": 1,
                "unidad_compra": "kg",
                "precio_incluye_iva": True,
                "cantidad_por_scan": 1,
                "unidad_por_scan": "kg",
            },
        )
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Actualizar insumo")

        resp = client.get(f"/api/insumo/buscar?codigo={insumo_code}")
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("encontrado"), "Buscar insumo por código")

        resp = client.post("/api/insumo/escanear-avanzado", json={"codigo": insumo_code, "cantidad": 0.5, "unidad": "kg"})
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Escáner avanzado insumo")

        new_scan_code = f"SCN{tag}"
        resp = client.post(
            "/api/insumo/crear-desde-escaner",
            json={
                "codigo_barra": new_scan_code,
                "nombre": f"InsumoScanner_{tag}",
                "stock": 3,
                "stock_minimo": 1,
                "unidad": "kg",
                "precio_unitario": 8000,
                "cantidad_comprada": 1,
                "unidad_compra": "kg",
                "precio_incluye_iva": 1,
                "cantidad_por_scan": 1,
                "unidad_por_scan": "kg",
            },
        )
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Crear insumo desde escáner")
        scan_insumo_id = data.get("id")

        resp = client.post(f"/api/insumo/{insumo_id}/scan-default", json={"cantidad_por_scan": 1.25, "unidad_por_scan": "kg"})
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Actualizar scan-default insumo")

        resp = client.post(
            "/api/insumo/lote-rapido/confirmar",
            json={
                "items": [
                    {
                        "insumo_id": insumo_id,
                        "codigo_barra": insumo_code,
                        "nombre": f"{insumo_name}_Editado",
                        "cantidad": 1.1,
                        "unidad": "kg",
                        "precio_unitario": 13000,
                        "cantidad_comprada": 1,
                        "unidad_compra": "kg",
                        "precio_incluye_iva": True,
                    },
                    {
                        "codigo_barra": f"LOTE{tag}",
                        "nombre": f"InsumoLote_{tag}",
                        "cantidad": 2,
                        "unidad": "kg",
                        "precio_unitario": 4000,
                        "cantidad_comprada": 1,
                        "unidad_compra": "kg",
                        "precio_incluye_iva": True,
                    },
                ]
            },
        )
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Confirmar lote rápido de insumos")

        if scan_insumo_id:
            resp = client.post(f"/api/insumo/{scan_insumo_id}/eliminar")
            data = tester.response_json(resp) or {}
            tester.check(resp.status_code == 200 and data.get("success"), "Eliminar insumo creado por escáner")

        # 5) Compras pendientes
        resp = client.post(
            "/api/compras-pendientes",
            json={
                "insumo_id": insumo_id,
                "nombre": f"{insumo_name}_Editado",
                "cantidad": 2,
                "unidad": "kg",
                "precio_unitario": 14000,
                "precio_incluye_iva": True,
                "estado": "pendiente",
                "nota": "stress",
            },
        )
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Agregar compra pendiente")

        list_resp = client.get("/api/compras-pendientes")
        list_data = tester.response_json(list_resp) or {}
        tester.check(list_resp.status_code == 200 and list_data.get("success"), "Listar compras pendientes")

        item_id = None
        if list_data.get("items"):
            item_id = list_data["items"][0]["id"]
            upd = client.put(
                f"/api/compras-pendientes/{item_id}",
                json={"cantidad": 3, "estado": "comprado", "nota": "actualizado"},
            )
            upd_data = tester.response_json(upd) or {}
            tester.check(upd.status_code == 200 and upd_data.get("success"), "Actualizar compra pendiente")

            dele = client.delete(f"/api/compras-pendientes/{item_id}")
            dele_data = tester.response_json(dele) or {}
            tester.check(dele.status_code == 200 and dele_data.get("success"), "Eliminar compra pendiente")

        # agregar en lote + finalizar
        resp = client.post(
            "/api/compras-pendientes",
            json={"items": [{"nombre": "Azucar QA", "cantidad": 1, "unidad": "kg", "precio_unitario": 5000, "precio_incluye_iva": True}]},
        )
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Agregar lote compras pendientes")

        resp = client.post("/api/compras-pendientes/finalizar", json={})
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Finalizar compras pendientes")

        resp = client.post("/api/compras-pendientes/limpiar", json={"solo_comprados": False})
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Limpiar compras pendientes")

        # 5.1) Crear receta por modo AJAX (JSON) y eliminar
        receta_ajax_name = f"RecetaAjax_{tag}"
        resp = client.post(
            "/api/receta/crear",
            data={
                "nombre": receta_ajax_name,
                "producto_id": str(product_id),
                "rendimiento": "1",
                "insumos[0][id]": str(insumo_id),
                "insumos[0][tipo]": "insumo",
                "insumos[0][cantidad]": "0.2",
                "insumos[0][unidad]": "kg",
            },
            headers={"Accept": "application/json", "X-Requested-With": "fetch"},
        )
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Crear receta (modo AJAX)")
        receta_ajax_id = data.get("receta_id")
        if receta_ajax_id:
            resp = client.post(f"/api/receta/{receta_ajax_id}/eliminar")
            data = tester.response_json(resp) or {}
            tester.check(resp.status_code == 200 and data.get("success"), "Eliminar receta AJAX")

        # 6) Recetas + produccion
        receta_name = f"RecetaQA_{tag}"
        resp = client.post(
            "/api/receta/crear",
            data={
                "nombre": receta_name,
                "producto_id": str(product_id),
                "rendimiento": "1.5",
                "insumos[0][id]": str(insumo_id),
                "insumos[0][tipo]": "insumo",
                "insumos[0][cantidad]": "0.5",
                "insumos[0][unidad]": "kg",
            },
        )
        tester.check(resp.status_code in (302, 303), "Crear receta", f"status={resp.status_code}")

        rec_row = _query_one(get_db, "SELECT id FROM recetas WHERE nombre = ? ORDER BY id DESC LIMIT 1", (receta_name,))
        tester.check(rec_row is not None, "Receta creada en BD")
        if rec_row is None:
            raise RuntimeError("No se pudo continuar: receta no creada")
        receta_id = int(rec_row["id"])

        resp = client.get(f"/api/receta/{receta_id}/detalle")
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Detalle receta")

        resp = client.get(f"/api/receta/{receta_id}/costo")
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Costo receta")

        resp = client.post(
            f"/api/receta/{receta_id}/actualizar",
            json={
                "nombre": f"{receta_name}_Editada",
                "producto_id": product_id,
                "rendimiento": 2,
                "items": [{"tipo": "insumo", "id": insumo_id, "cantidad": 0.4, "unidad": "kg"}],
            },
        )
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Actualizar receta")

        resp = client.post(
            f"/api/receta/{receta_id}/producir",
            json={"cantidad": 1, "cantidad_resultado": 1, "fecha_vencimiento": today},
        )
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Producir receta")
        produccion_id = data.get("produccion_id")

        resp = client.get("/api/produccion/historial")
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Historial producción")

        if produccion_id:
            resp = client.post(f"/api/produccion/{produccion_id}/eliminar")
            data = tester.response_json(resp) or {}
            tester.check(resp.status_code == 200 and data.get("success"), "Revertir producción desde historial")

        resp = client.post(f"/api/receta/{receta_id}/eliminar")
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Eliminar receta")

        # 7) Ventas + historial
        # asegurar stock disponible
        client.post(f"/api/producto/{product_id}/stock", json={"cantidad": 3})
        resp = client.post("/api/venta/procesar", json={"items": [{"id": product_id, "cantidad": 1}]})
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Procesar venta")
        venta_id = data.get("venta_id")

        if venta_id:
            resp = client.get(f"/api/venta/{venta_id}")
            data = tester.response_json(resp) or {}
            tester.check(resp.status_code == 200 and data.get("success"), "Detalle venta")

            resp = client.post(f"/api/venta/{venta_id}/eliminar")
            data = tester.response_json(resp) or {}
            tester.check(resp.status_code == 200 and data.get("success"), "Anular venta")
        else:
            tester.warn("No se obtuvo venta_id para probar detalle/anulación")

        # 8) Agenda + notas
        evento_titulo = f"EventoQA_{tag}"
        evento = {
            "tipo": "pedido",
            "titulo": evento_titulo,
            "fecha": today,
            "hora_inicio": "10:00",
            "hora_fin": "11:00",
            "hora_entrega": "11:30",
            "cliente": "Cliente QA",
            "telefono": "123456",
            "es_envio": True,
            "direccion": "Calle QA 123",
            "ingredientes": "detalle",
            "total": 10000,
            "abono": 3000,
            "motivo": "Prueba",
            "alerta_minutos": 60,
        }
        resp = client.post("/api/agenda/evento", json=evento)
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Crear evento agenda")
        evento_id = data.get("id")

        resp = client.get("/api/agenda/eventos")
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Listar eventos agenda")

        if evento_id:
            evento["id"] = evento_id
            evento["titulo"] = f"{evento_titulo}_Editado"
            resp = client.post("/api/agenda/evento", json=evento)
            data = tester.response_json(resp) or {}
            tester.check(resp.status_code == 200 and data.get("success"), "Actualizar evento agenda")

            resp = client.post(
                "/api/alertas/recordatorio/descartar",
                json={"evento_id": evento_id, "ventana_clave": f"{today}T10:00"},
            )
            data = tester.response_json(resp) or {}
            tester.check(resp.status_code == 200 and data.get("success"), "Descartar recordatorio agenda")

            resp = client.delete(f"/api/agenda/evento/{evento_id}")
            data = tester.response_json(resp) or {}
            tester.check(resp.status_code == 200 and data.get("success"), "Eliminar evento agenda")

        nota = {"titulo": f"NotaQA_{tag}", "contenido": "contenido prueba", "fijada": True, "estado": "activa"}
        resp = client.post("/api/agenda/nota", json=nota)
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Crear nota agenda")
        nota_id = data.get("id")

        resp = client.get("/api/agenda/notas")
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Listar notas agenda")

        if nota_id:
            resp = client.delete(f"/api/agenda/nota/{nota_id}")
            data = tester.response_json(resp) or {}
            tester.check(resp.status_code == 200 and data.get("success"), "Eliminar nota agenda")

        # 9) Alert config
        cfg_resp = client.get("/api/alertas/config")
        cfg_data = tester.response_json(cfg_resp) or {}
        tester.check(cfg_resp.status_code == 200 and cfg_data.get("success"), "Obtener config alertas")
        if cfg_data.get("success"):
            cfg = cfg_data.get("config", {})
            cfg["repetir_minutos"] = 15
            cfg["dias_anticipacion"] = 2
            post = client.post("/api/alertas/config", json=cfg)
            post_data = tester.response_json(post) or {}
            tester.check(post.status_code == 200 and post_data.get("success"), "Guardar config alertas")

        # 10) Facturas
        resp = client.post(
            "/api/facturas/subir",
            data={
                "proveedor": f"ProveedorQA_{tag}",
                "fecha_factura": today,
                "numero_factura": f"F-{tag}",
                "monto_total": "12345",
                "observacion": "prueba",
                "archivos": (BytesIO(b"%PDF-1.4\\n%stress\\n"), f"factura_{tag}.pdf"),
            },
            content_type="multipart/form-data",
        )
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Subir factura")

        fac_row = _query_one(
            get_db,
            "SELECT id FROM facturas_archivo WHERE proveedor = ? ORDER BY id DESC LIMIT 1",
            (f"ProveedorQA_{tag}",),
        )
        tester.check(fac_row is not None, "Factura registrada en BD")

        if fac_row:
            factura_id = int(fac_row["id"])
            view = client.get(f"/facturas/archivo/{factura_id}")
            tester.check(view.status_code == 200, "Visualizar archivo factura", f"status={view.status_code}")
            dl = client.get(f"/facturas/archivo/{factura_id}?download=1")
            tester.check(dl.status_code == 200, "Descargar archivo factura", f"status={dl.status_code}")
            dele = client.post(f"/api/facturas/{factura_id}/eliminar")
            dele_data = tester.response_json(dele) or {}
            tester.check(dele.status_code == 200 and dele_data.get("success"), "Eliminar factura")

        # 11) Backup endpoints
        resp = client.post("/api/backup/crear")
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and data.get("success"), "Crear backup manual")

        resp = client.get("/api/backup/ultimo")
        data = tester.response_json(resp) or {}
        tester.check(resp.status_code == 200 and isinstance(data.get("ultimo"), str), "Consultar último backup")

        # 12) Mapa endpoint
        bad_map = client.get("/api/mapa/static?lat=999&lon=0")
        tester.check(bad_map.status_code == 400, "Validación mapa coordenadas inválidas")

        map_resp = client.get("/api/mapa/static?lat=-33.45&lon=-70.66&w=320&h=180")
        if map_resp.status_code == 200:
            tester.check(str(map_resp.headers.get("Content-Type", "")).startswith("image/"), "Mapa estático devuelve imagen")
        elif map_resp.status_code == 502:
            tester.warn("Mapa estático no disponible en entorno de prueba", "status=502")
        else:
            tester.check(False, "Mapa estático", f"status={map_resp.status_code}")

        # 13) PDF compras
        pdf_resp = client.post(
            "/api/lista-compras/pdf",
            json={
                "items": [
                    {
                        "nombre": "Harina QA",
                        "cantidad": 2,
                        "unidad": "kg",
                        "precio": 1000,
                        "total": 2000,
                        "precio_incluye_iva": True,
                    }
                ]
            },
        )
        if pdf_resp.status_code == 200:
            tester.check("application/pdf" in str(pdf_resp.headers.get("Content-Type", "")), "Generar PDF lista compras")
        else:
            data = tester.response_json(pdf_resp) or {}
            tester.check(False, "Generar PDF lista compras", f"status={pdf_resp.status_code}, error={data.get('error')}")

        # 14) Final report
        print("")
        print("========== RESUMEN STRESS TEST ==========")
        print(f"FALLAS: {len(tester.failures)}")
        print(f"ADVERTENCIAS: {len(tester.warnings)}")
        if tester.failures:
            print("- Detalle de fallas:")
            for f in tester.failures:
                print(f"  * {f}")
        if tester.warnings:
            print("- Advertencias:")
            for w in tester.warnings:
                print(f"  * {w}")

        return 1 if tester.failures else 0
    finally:
        try:
            shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
