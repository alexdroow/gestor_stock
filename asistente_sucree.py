import json
import re
import unicodedata
from datetime import datetime, timedelta
from urllib.parse import quote
from zoneinfo import ZoneInfo


MESES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5,
    "junio": 6, "julio": 7, "agosto": 8, "septiembre": 9,
    "setiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}

DIAS = {
    "lunes": 0, "martes": 1, "miercoles": 2, "miércoles": 2,
    "jueves": 3, "viernes": 4, "sabado": 5, "sábado": 5, "domingo": 6,
}


def registrar_asistente_sucree(app, deps):
    render_template = deps["render_template"]
    request = deps["request"]
    jsonify = deps["jsonify"]
    get_db = deps["get_db"]
    catalogo_publico = deps["_catalogo_torta_publico"]
    validar_payload = deps["_validar_payload_catalogo_torta"]
    cfg_tienda = deps["_obtener_tienda_personalizacion"]
    cfg_agenda = deps["_obtener_cfg_agenda_tienda"]
    calcular_disponibilidad = deps["_calcular_disponibilidad_agenda_tienda"]
    normalizar_email = deps["_normalizar_email"]
    normalizar_telefono = deps["_normalizar_telefono_cl"]
    crear_pdf_reserva = deps.get("_crear_pdf_reserva_agenda_tienda")
    crear_backup = deps.get("crear_backup")
    public_base_url = str(deps.get("PUBLIC_BASE_URL") or "https://pasteleriasucree.cl").rstrip("/")
    whatsapp_pasteleria = "56964330546"

    def fmt_clp(value):
        try:
            n = int(round(float(value or 0)))
        except (TypeError, ValueError):
            n = 0
        return ("$%s" % format(n, ",")).replace(",", ".")

    def fmt_fecha(fecha):
        raw = str(fecha or "").strip()
        try:
            return datetime.strptime(raw, "%Y-%m-%d").strftime("%d-%m-%Y")
        except Exception:
            return raw or "-"

    def norm(texto):
        raw = str(texto or "").strip().lower()
        raw = unicodedata.normalize("NFKD", raw)
        raw = "".join(ch for ch in raw if not unicodedata.combining(ch))
        raw = re.sub(r"[^a-z0-9@._+:/\-\s]", " ", raw)
        return re.sub(r"\s+", " ", raw).strip()

    def slug(texto):
        return re.sub(r"[^a-z0-9]+", " ", norm(texto)).strip()

    def keyword_slug(texto):
        words = [w for w in slug(texto).split() if w not in {"de", "del", "la", "las", "el", "los", "y"}]
        return " ".join(words)

    def ratio(query, target):
        q = set(slug(query).split())
        t = set(slug(target).split())
        if not q or not t:
            return 0.0
        return float(len(q & t)) / float(max(1, min(len(q), len(t))))

    def ensure_tables(cur):
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS asistente_kb (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pregunta TEXT NOT NULL,
                respuesta TEXT NOT NULL,
                activo INTEGER DEFAULT 1,
                creado_en TEXT DEFAULT CURRENT_TIMESTAMP,
                actualizado_en TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS asistente_unknown (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pregunta TEXT NOT NULL,
                contexto_json TEXT,
                estado TEXT DEFAULT 'pendiente',
                respuesta_sugerida TEXT,
                creado_en TEXT DEFAULT CURRENT_TIMESTAMP,
                actualizado_en TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def registrar_desconocida(pregunta, contexto=None):
        conn = None
        try:
            pregunta = str(pregunta or "").strip()[:700]
            if not pregunta:
                return
            conn = get_db()
            cur = conn.cursor()
            ensure_tables(cur)
            ctx = json.dumps(contexto or {}, ensure_ascii=False)[:4000]
            cur.execute(
                "INSERT INTO asistente_unknown (pregunta, contexto_json, estado) VALUES (?, ?, 'pendiente')",
                (pregunta, ctx),
            )
            conn.commit()
        except Exception:
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()

    def buscar_kb(mensaje):
        conn = None
        try:
            conn = get_db()
            cur = conn.cursor()
            ensure_tables(cur)
            cur.execute(
                "SELECT pregunta, respuesta FROM asistente_kb WHERE COALESCE(activo, 1) = 1 ORDER BY actualizado_en DESC, id DESC LIMIT 200"
            )
            best = None
            best_score = 0.0
            for row in cur.fetchall():
                score = ratio(mensaje, row["pregunta"])
                if score > best_score:
                    best_score = score
                    best = row
            if best and best_score >= 0.55:
                return str(best["respuesta"] or "").strip()
        except Exception:
            pass
        finally:
            if conn:
                conn.close()
        return ""

    def buscar_cliente_por_email(email):
        conn = None
        try:
            email = normalizar_email(email)
            if not email:
                return None
            conn = get_db()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, nombre, email, telefono, direccion_default, direccion_lat, direccion_lng
                FROM tienda_clientes
                WHERE LOWER(TRIM(email)) = LOWER(TRIM(?))
                  AND COALESCE(activo, 1) = 1
                ORDER BY actualizado_en DESC, id DESC
                LIMIT 1
                """,
                (email,),
            )
            row = cur.fetchone()
            return dict(row) if row else None
        except Exception:
            return None
        finally:
            if conn:
                conn.close()

    def aplicar_cliente_draft(draft, cliente):
        if not cliente:
            return draft
        draft = dict(draft or {})
        if not draft.get("nombre") and str(cliente.get("nombre") or "").strip():
            draft["nombre"] = str(cliente.get("nombre") or "").strip()
        tel = normalizar_telefono(cliente.get("telefono"))
        if not draft.get("telefono") and tel:
            draft["telefono"] = tel
        if not draft.get("direccion") and str(cliente.get("direccion_default") or "").strip():
            draft["direccion"] = str(cliente.get("direccion_default") or "").strip()
        draft["cliente_encontrado"] = True
        draft["cliente_id"] = int(cliente.get("id") or 0)
        return draft

    def cargar_catalogo():
        cfg = cfg_tienda()
        catalogo = catalogo_publico((cfg or {}).get("catalogo_torta") or {})
        return catalogo if isinstance(catalogo, dict) else {}

    def match_row(texto, rows, min_score=0.45):
        if not texto:
            return None
        best = None
        best_score = 0.0
        texto_slug = slug(texto)
        for row in rows or []:
            nombre = str((row or {}).get("nombre") or "")
            rid = str((row or {}).get("id") or "")
            score = max(ratio(texto, nombre), ratio(texto, rid), ratio(texto, nombre + " " + rid))
            nombre_slug = slug(nombre)
            if nombre_slug and nombre_slug in texto_slug:
                score = max(score, 1.0)
            if score > best_score:
                best_score = score
                best = row
        return best if best and best_score >= min_score else None

    def find_categoria(catalogo, categoria_id):
        cid = str(categoria_id or "").strip().lower()
        for cat in catalogo.get("categorias") or []:
            if str(cat.get("id") or "").strip().lower() == cid:
                return cat
        return None

    def rows_categoria(catalogo, key, categoria):
        rows = list(catalogo.get(key) or [])
        if not categoria:
            return rows
        cid = str(categoria.get("id") or "").strip().lower()
        if key == "sizes":
            return [r for r in rows if str(r.get("categoria_id") or "").strip().lower() == cid]
        id_key = {"sabores": "sabores_ids", "extras": "extras_ids", "toppers": "toppers_ids"}.get(key)
        if id_key and bool(categoria.get("use_category_ingredients")):
            allowed = {str(x or "").strip().lower() for x in (categoria.get(id_key) or []) if str(x or "").strip()}
            return [r for r in rows if str(r.get("id") or "").strip().lower() in allowed]
        return rows

    def list_lines(rows, label_func, empty_text):
        out = []
        for row in rows or []:
            label = label_func(row)
            if label:
                out.append("- " + label)
        return "\n".join(out) if out else "- " + empty_text

    def disponibilidad_texto(items):
        if not items:
            return "No encontre cupos disponibles en los proximos dias."
        lines = ["Estas son algunas fechas con horas tentativas:"]
        for item in items:
            lines.append("")
            lines.append("- %s" % fmt_fecha(item.get("fecha")))
            for hora in item.get("horas") or []:
                lines.append("  - %s" % hora)
        return "\n".join(lines)

    def whatsapp_url(texto):
        msg = str(texto or "Hola, necesito ayuda para agendar una torta.").strip()
        return "https://wa.me/%s?text=%s" % (whatsapp_pasteleria, quote(msg))

    def hora_fin_estimada(hora_inicio, minutos=60):
        try:
            parts = str(hora_inicio or "").split(":")
            h = int(parts[0])
            m = int(parts[1] if len(parts) > 1 else 0)
            total = max(0, min(23 * 60 + 59, h * 60 + m + int(minutos or 60)))
            return "%02d:%02d" % (total // 60, total % 60)
        except Exception:
            return str(hora_inicio or "")

    def validar_hora_cotizacion(draft):
        fecha = str(draft.get("fecha") or "").strip()
        hora = str(draft.get("hora_inicio") or "").strip()[:5]
        if not fecha or not hora:
            return {"ok": False, "error": "Falta fecha u hora.", "horas": []}
        horas = horas_disponibles(fecha, limite=30)
        if draft.get("cotizacion_evento_id"):
            return {"ok": True, "horas": horas}
        if hora in horas:
            return {"ok": True, "horas": horas}
        return {"ok": False, "error": "La hora %s ya no esta disponible para %s." % (hora, fmt_fecha(fecha)), "horas": horas}

    def guia_agendar_texto(catalogo, draft=None):
        draft = dict(draft or {})
        categorias = catalogo.get("categorias") or []
        lines = [
            "Perfecto, te ayudo a agendar una torta.",
            "",
            "Para avanzar necesito estos datos:",
            "- Tipo de torta",
            "- Tamano o cantidad de personas",
            "- Relleno/sabor",
            "- Fecha",
            "- Hora",
            "- Nombre",
            "- Telefono",
            "- Correo",
            "- Retiro o despacho",
            "",
            "Tipos disponibles:",
        ]
        lines.append(list_lines(categorias, lambda c: str(c.get("nombre") or ""), "sin tipos cargados"))
        if draft.get("fecha"):
            horas = horas_disponibles(draft.get("fecha"), limite=6)
            if horas:
                lines.extend(["", "Horas tentativas para %s:" % fmt_fecha(draft.get("fecha"))])
                lines.extend(["- " + h for h in horas])
        lines.extend([
            "",
            "Puedes escribirlo en una sola frase. Ejemplo:",
            "Quiero torta bizcocho 25 personas, manjar, sin topper, para el 19 de septiembre a las 19 hrs, soy Ana, +569..., correo@ejemplo.cl",
        ])
        return "\n".join(lines)

    def parse_fecha(texto):
        txt = norm(texto)
        now = datetime.now(ZoneInfo("America/Santiago"))
        m = re.search(r"\b(20\d{2})[-/](\d{1,2})[-/](\d{1,2})\b", txt)
        if m:
            try:
                return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).date().isoformat()
            except ValueError:
                pass
        m = re.search(r"\b(\d{1,2})[-/](\d{1,2})(?:[-/](20\d{2}))?\b", txt)
        if m:
            year = int(m.group(3) or now.year)
            try:
                dt = datetime(year, int(m.group(2)), int(m.group(1))).date()
                if dt < now.date() and not m.group(3):
                    dt = datetime(year + 1, int(m.group(2)), int(m.group(1))).date()
                return dt.isoformat()
            except ValueError:
                pass
        m = re.search(r"\b(\d{1,2})\s*(?:de\s*)?(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre)(?:\s*(?:de\s*)?(20\d{2}))?\b", txt)
        if m:
            year = int(m.group(3) or now.year)
            month = int(MESES.get(m.group(2)) or 0)
            try:
                dt = datetime(year, month, int(m.group(1))).date()
                if dt < now.date() and not m.group(3):
                    dt = datetime(year + 1, month, int(m.group(1))).date()
                return dt.isoformat()
            except ValueError:
                pass
        if "manana" in txt or "mañana" in txt:
            return (now.date() + timedelta(days=1)).isoformat()
        if "hoy" in txt:
            return now.date().isoformat()
        for dia, weekday in DIAS.items():
            if re.search(r"\b" + re.escape(norm(dia)) + r"\b", txt):
                delta = (weekday - now.weekday()) % 7
                if delta == 0 or "proximo" in txt or "proxima" in txt:
                    delta = delta or 7
                return (now.date() + timedelta(days=delta)).isoformat()
        return ""

    def parse_hora(texto):
        txt = norm(texto)
        m = re.search(r"\b(\d{1,2})[:.](\d{2})\b", txt)
        if m:
            h = int(m.group(1))
            minute = int(m.group(2))
            if 0 <= h <= 23 and 0 <= minute <= 59:
                return "%02d:%02d" % (h, minute)
        m = re.search(r"\b(?:a\s+las\s+|hora\s*)?(\d{1,2})\s*(?:h|hrs|horas?)\b", txt)
        if m:
            h = int(m.group(1))
            if 0 <= h <= 23:
                return "%02d:00" % h
        return ""

    def parse_contacto(texto):
        email = ""
        telefono = ""
        m = re.search(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", str(texto or ""), flags=re.I)
        if m:
            email = normalizar_email(m.group(0))
        phones = re.findall(r"(?:\+?56)?\s*9?\s*(?:\d[\s\-\.]*){8,9}", str(texto or ""))
        for ph in phones:
            clean = normalizar_telefono(ph)
            if clean:
                telefono = clean
                break
        return email, telefono

    def detectar_entrega(texto):
        txt = norm(texto)
        if any(x in txt for x in ["despacho", "delivery", "enviar", "envio", "domicilio"]):
            return "despacho"
        if any(x in txt for x in ["retiro", "retirar", "tienda", "local"]):
            return "retiro"
        return ""

    def actualizar_draft(draft, mensaje, catalogo):
        draft = dict(draft or {})
        texto = str(mensaje or "")
        fecha = parse_fecha(texto)
        hora = parse_hora(texto)
        email, telefono = parse_contacto(texto)
        entrega = detectar_entrega(texto)
        if fecha:
            draft["fecha"] = fecha
        if hora:
            draft["hora_inicio"] = hora
        if email:
            draft["email"] = email
            cliente = buscar_cliente_por_email(email)
            if cliente:
                draft = aplicar_cliente_draft(draft, cliente)
            else:
                draft["cliente_encontrado"] = False
        if telefono:
            draft["telefono"] = telefono
        if entrega:
            draft["entrega_tipo"] = entrega
            draft["entrega_confirmada"] = True
        if not draft.get("entrega_tipo"):
            draft["entrega_tipo"] = "retiro"
        m = re.search(r"\b(?:cliente|nombre|soy|me llamo)\s+([A-Za-zÁÉÍÓÚÜÑáéíóúüñ ]{2,80})", texto, flags=re.I)
        if m:
            nombre = re.sub(r"\s+", " ", m.group(1)).strip(" .,-")[:80]
            nombre = re.split(r"\b(?:telefono|fono|correo|email|mail)\b", nombre, flags=re.I)[0].strip(" .,-")
            if len(nombre) >= 2:
                draft["nombre"] = nombre
        size = None
        m = re.search(r"\b(\d{1,3})\s*(?:personas|pers|pax)\b", norm(texto))
        if m:
            size = match_row(m.group(1) + " personas", catalogo.get("sizes") or [], min_score=0.25)
        if not size:
            size = match_row(texto, catalogo.get("sizes") or [], min_score=0.55)
        if size:
            draft["size_id"] = str(size.get("id") or "")
            if size.get("categoria_id"):
                draft["categoria_id"] = str(size.get("categoria_id") or "")
        categoria = match_row(texto, catalogo.get("categorias") or [], min_score=0.55)
        if categoria:
            draft["categoria_id"] = str(categoria.get("id") or "")
        actuales = list(draft.get("sabor_ids") or [])
        texto_key = keyword_slug(texto)
        encontrados = []
        for sb in catalogo.get("sabores") or []:
            sb_key = keyword_slug(sb.get("nombre"))
            if sb_key and sb_key in texto_key:
                encontrados.append(sb)
        if not encontrados:
            sabor = match_row(texto, catalogo.get("sabores") or [], min_score=0.58)
            if sabor:
                encontrados.append(sabor)
        encontrados.sort(key=lambda sb: texto_key.find(keyword_slug(sb.get("nombre"))) if keyword_slug(sb.get("nombre")) in texto_key else 9999)
        for sabor in encontrados:
            sid = str(sabor.get("id") or "")
            if sid and sid not in actuales:
                actuales.append(sid)
        if actuales:
            draft["sabor_ids"] = actuales[:3]
        topper = match_row(texto, catalogo.get("toppers") or [], min_score=0.58)
        if topper:
            draft["topper_id"] = str(topper.get("id") or "")
        elif "sin topper" in norm(texto):
            for tp in catalogo.get("toppers") or []:
                if "sin" in slug(tp.get("nombre")) and "topper" in slug(tp.get("nombre")):
                    draft["topper_id"] = str(tp.get("id") or "")
                    break
        extras = list(draft.get("extra_items") or [])
        extra = match_row(texto, catalogo.get("extras") or [], min_score=0.62)
        if extra:
            eid = str(extra.get("id") or "")
            if eid and not any(str(x.get("id") or "") == eid for x in extras if isinstance(x, dict)):
                extras.append({"id": eid, "qty": 1})
            draft["extra_items"] = extras[:8]
        if "sin extra" in norm(texto) or "sin extras" in norm(texto):
            draft["extra_items"] = []
        return draft

    def payload_torta(draft):
        return {
            "categoria_id": str(draft.get("categoria_id") or ""),
            "size_id": str(draft.get("size_id") or ""),
            "sabor_ids": list(draft.get("sabor_ids") or []),
            "extra_items": list(draft.get("extra_items") or []),
            "topper_id": str(draft.get("topper_id") or ""),
            "referencia_urls": [],
            "nota": str(draft.get("nota") or ""),
        }

    def crear_ingredientes_cotizacion(draft, resumen):
        categoria = resumen.get("categoria") or {}
        size = resumen.get("size") or {}
        sabores = resumen.get("sabores") or []
        extras = resumen.get("extras") or []
        topper = resumen.get("topper") or None
        lines = [
            "Cotizacion generada por asistente Sucree",
            "Email: %s" % (draft.get("email") or "-"),
            "Entrega: %s" % ("Despacho" if draft.get("entrega_tipo") == "despacho" else "Retiro"),
            "--- Resumen de cotizacion (cliente) ---",
            "Categoria: %s" % (categoria.get("nombre") or "-"),
            "Tamano: %s (%s)" % (size.get("nombre") or "-", fmt_clp(size.get("precio") or 0)),
            "Sabores:",
        ]
        if sabores:
            for sb in sabores:
                lines.append("- %s (%s)" % (sb.get("nombre") or "-", fmt_clp(sb.get("precio") or 0)))
        else:
            lines.append("- -")
        lines.append("Extras:")
        if extras:
            for ex in extras:
                qty = int(ex.get("qty") or 0)
                lines.append("- %s x%s (%s)" % (ex.get("nombre") or "-", qty, fmt_clp(float(ex.get("precio") or 0) * qty)))
        else:
            lines.append("- -")
        lines.append("Topper:")
        if topper:
            lines.append("- %s (%s)" % (topper.get("nombre") or "-", fmt_clp(topper.get("precio") or 0)))
        else:
            lines.append("- Sin topper")
        lines.append("Subtotal estimado productos: %s" % fmt_clp(resumen.get("subtotal") or 0))
        builder_raw = payload_torta(draft)
        lines.append("--- Builder JSON ---")
        lines.append(json.dumps(builder_raw, ensure_ascii=False, separators=(",", ":")))
        lines.append("Total estimado pedido: %s" % fmt_clp(resumen.get("subtotal") or 0))
        return "\n".join(lines)

    def pdf_cotizacion_url(draft, resumen):
        if not callable(crear_pdf_reserva) or not resumen:
            return ""
        try:
            codigo = "COT-%s-%s" % (
                re.sub(r"[^0-9]", "", str(draft.get("fecha") or ""))[:8] or datetime.now(ZoneInfo("America/Santiago")).strftime("%Y%m%d"),
                datetime.now(ZoneInfo("America/Santiago")).strftime("%H%M%S"),
            )
            reserva = {
                "id": 0,
                "documento_tipo": "cotizacion",
                "tipo": "torta",
                "titulo": "Cotizacion asistente",
                "fecha": draft.get("fecha") or "",
                "hora_inicio": draft.get("hora_inicio") or "",
                "hora_entrega": draft.get("hora_inicio") or "",
                "cliente": draft.get("nombre") or "Cliente por confirmar",
                "telefono": draft.get("telefono") or "",
                "direccion": draft.get("direccion") or "",
                "es_envio": 1 if draft.get("entrega_tipo") == "despacho" else 0,
                "ingredientes": crear_ingredientes_cotizacion(draft, resumen),
                "total": float(resumen.get("subtotal") or 0),
                "abono": 0,
                "motivo": "Cotizacion generada por asistente Sucree",
                "estado": "borrador",
                "codigo_pedido": codigo,
                "codigo_operacion": codigo,
            }
            filename = crear_pdf_reserva(reserva)
            return "%s/static/tienda_pedidos_pdf/%s" % (public_base_url, quote(filename))
        except Exception:
            return ""

    def guardar_cotizacion_agenda(draft, resumen):
        if not resumen:
            return {"success": False, "error": "No hay cotizacion valida"}
        try:
            from database import guardar_evento_agenda
            size = resumen.get("size") or {}
            categoria = resumen.get("categoria") or {}
            codigo = str(draft.get("cotizacion_codigo") or "").strip()
            if not codigo:
                codigo = "COT-%s-%s" % (
                    re.sub(r"[^0-9]", "", str(draft.get("fecha") or ""))[:8] or datetime.now(ZoneInfo("America/Santiago")).strftime("%Y%m%d"),
                    datetime.now(ZoneInfo("America/Santiago")).strftime("%H%M%S"),
                )
            evento = {
                "tipo": "torta",
                "titulo": "Cotizacion - %s" % (size.get("nombre") or categoria.get("nombre") or "Torta"),
                "fecha": draft.get("fecha") or "",
                "hora_inicio": draft.get("hora_inicio") or "",
                "hora_fin": hora_fin_estimada(draft.get("hora_inicio"), 60),
                "hora_entrega": draft.get("hora_inicio") or "",
                "cliente": draft.get("nombre") or "Cliente por confirmar",
                "telefono": draft.get("telefono") or "",
                "es_envio": 1 if draft.get("entrega_tipo") == "despacho" else 0,
                "direccion": draft.get("direccion") or "",
                "ingredientes": crear_ingredientes_cotizacion(draft, resumen) + "\nEstado interno: COTIZACION_ASISTENTE_REQUIERE_REVISION",
                "total": float(resumen.get("subtotal") or 0),
                "abono": 0,
                "motivo": "Cotizacion asistente - requiere revision interna",
                "alerta_minutos": 1440,
                "estado": "borrador",
                "codigo_pedido": codigo,
                "codigo_operacion": codigo,
            }
            try:
                eid = int(draft.get("cotizacion_evento_id") or 0)
            except Exception:
                eid = 0
            if eid > 0:
                evento["id"] = eid
            res = guardar_evento_agenda(evento)
            if not res.get("success"):
                return res
            evento_id = int(res.get("id") or eid or 0)
            codigo_final = str(res.get("codigo_pedido") or codigo).strip()
            draft["cotizacion_evento_id"] = evento_id
            draft["cotizacion_codigo"] = codigo_final
            evento["id"] = evento_id
            evento["codigo_pedido"] = codigo_final
            evento["documento_tipo"] = "cotizacion"
            filename = crear_pdf_reserva(evento) if callable(crear_pdf_reserva) else ""
            pdf_url = "%s/static/tienda_pedidos_pdf/%s" % (public_base_url, quote(filename)) if filename else ""
            draft["cotizacion_pdf_url"] = pdf_url
            if callable(crear_backup):
                try:
                    crear_backup()
                except Exception:
                    pass
            return {"success": True, "id": evento_id, "codigo_pedido": codigo_final, "pdf_url": pdf_url}
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    def cotizar(draft, catalogo):
        try:
            if not draft.get("size_id") or not draft.get("sabor_ids"):
                return None, ""
            return validar_payload(payload_torta(draft), catalogo), ""
        except Exception as exc:
            return None, str(exc)

    def resumen_texto(draft, resumen):
        if not resumen:
            return ""
        size = resumen.get("size") or {}
        categoria = resumen.get("categoria") or {}
        sabores = ", ".join(str(x.get("nombre") or "") for x in (resumen.get("sabores") or []) if x)
        extras_rows = resumen.get("extras") or []
        extras = ", ".join((str(x.get("nombre") or "") + " x" + str(int(x.get("qty") or 1))) for x in extras_rows) or "Sin extras"
        topper = resumen.get("topper") or {}
        topper_txt = str(topper.get("nombre") or "Sin topper")
        return "\n".join([
            "Ya tengo esta cotizacion preliminar:",
            "",
            "Producto: %s" % (str(categoria.get("nombre") or "Torta").strip() or "Torta"),
            "Tamano: %s" % (size.get("nombre") or "-"),
            "Rellenos: %s" % (sabores or "-"),
            "Extras: %s" % extras,
            "Topper: %s" % topper_txt,
            "Precio productos: %s" % fmt_clp(resumen.get("subtotal") or 0),
            "Cliente: %s" % (draft.get("nombre") or "-"),
            "Contacto: %s / %s" % (draft.get("telefono") or "-", draft.get("email") or "-"),
            "Entrega: %s" % (("despacho" if draft.get("entrega_tipo") == "despacho" else "retiro") if draft.get("entrega_confirmada") else "por confirmar"),
            "Fecha: %s" % fmt_fecha(draft.get("fecha")),
            "Hora: %s" % (draft.get("hora_inicio") or "-"),
        ])

    def cliente_estado_texto(draft):
        if draft.get("email") and draft.get("cliente_encontrado") is True:
            datos = []
            if draft.get("nombre"):
                datos.append("nombre")
            if draft.get("telefono"):
                datos.append("telefono")
            if draft.get("direccion"):
                datos.append("direccion")
            return "Cliente encontrado por correo. Complete automaticamente: %s." % (", ".join(datos) if datos else "datos disponibles")
        if draft.get("email") and draft.get("cliente_encontrado") is False:
            if not draft.get("nombre") or not draft.get("telefono"):
                return "No encontre ese correo en la base de clientes. Necesito nombre y telefono para continuar."
            return "No encontre ese correo en la base de clientes. Usare los datos que ingresaste para esta cotizacion."
        return ""

    def faltantes(draft, resumen):
        out = []
        if not draft.get("email"):
            out.append("correo")
        if not draft.get("size_id"):
            out.append("tamano de torta")
        if not draft.get("sabor_ids"):
            out.append("relleno/sabor")
        if not draft.get("fecha"):
            out.append("fecha")
        if not draft.get("hora_inicio"):
            out.append("hora")
        if not draft.get("nombre"):
            out.append("nombre")
        if not draft.get("telefono"):
            out.append("telefono")
        if not draft.get("entrega_confirmada"):
            out.append("retiro o despacho")
        if draft.get("entrega_tipo") == "despacho" and not draft.get("direccion"):
            out.append("direccion de despacho")
        if draft.get("size_id") and draft.get("sabor_ids") and not resumen:
            out.append("opciones validas del catalogo")
        return out

    def horas_disponibles(fecha, limite=8):
        conn = None
        try:
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", str(fecha or "")):
                return []
            cfg = cfg_agenda()
            conn = get_db()
            cur = conn.cursor()
            disp = calcular_disponibilidad(cur, cfg, fecha, fecha)
            mapa = ((disp.get("mapa") or {}).get(fecha) or {})
            horas = []
            for hora, slot in sorted(mapa.items()):
                if bool((slot or {}).get("disponible")):
                    horas.append(str(hora))
            return horas[:int(limite or 8)]
        except Exception:
            return []
        finally:
            if conn:
                conn.close()

    def proximas_fechas():
        today = datetime.now(ZoneInfo("America/Santiago")).date()
        out = []
        for offset in range(10):
            fecha = (today + timedelta(days=offset)).isoformat()
            horas = horas_disponibles(fecha, limite=4)
            if horas:
                out.append({"fecha": fecha, "horas": horas})
            if len(out) >= 3:
                break
        return out

    def reservar(draft):
        if draft.get("entrega_tipo") == "despacho":
            return {"success": False, "error": "Para despacho necesito que el cliente confirme la direccion desde el mapa de agenda."}
        payload = {
            "fecha": draft.get("fecha"),
            "hora_inicio": draft.get("hora_inicio"),
            "nombre": draft.get("nombre"),
            "email": draft.get("email"),
            "telefono": draft.get("telefono"),
            "tipo": "torta",
            "detalle": "Reserva creada desde asistente Sucree",
            "catalogo_torta": payload_torta(draft),
            "entrega_tipo": draft.get("entrega_tipo") or "retiro",
            "direccion": "",
            "direccion_confirmada": False,
        }
        try:
            with app.test_client() as client:
                rv = client.post("/api/tienda/agenda/reservar", json=payload)
                data = rv.get_json(silent=True) or {}
                if rv.status_code >= 400:
                    data.setdefault("success", False)
                return data
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    def registrar_cotizacion_completa(draft, resumen):
        disponibilidad = validar_hora_cotizacion(draft)
        if not disponibilidad.get("ok"):
            horas = disponibilidad.get("horas") or []
            if horas:
                return {
                    "ok": False,
                    "reply": "\n".join([
                        disponibilidad.get("error") or "Esa hora no esta disponible.",
                        "",
                        "Estas son horas disponibles para ese mismo dia:",
                        "\n".join("- " + h for h in horas),
                        "",
                        "Dime cual prefieres para actualizar la cotizacion.",
                    ]),
                    "suggestions": horas[:6],
                }
            link = whatsapp_url("Hola Sucree, necesito ayuda porque no encontre horas disponibles para agendar mi torta.")
            return {
                "ok": False,
                "reply": "\n".join([
                    "Para %s no veo horas disponibles en la agenda." % fmt_fecha(draft.get("fecha")),
                    "",
                    "Por favor comunicate con la pasteleria por WhatsApp para revisar una alternativa:",
                    link,
                ]),
                "suggestions": [],
                "whatsapp_url": link,
            }

        guardado = guardar_cotizacion_agenda(draft, resumen)
        if not guardado.get("success"):
            return {
                "ok": False,
                "reply": "No pude dejar la cotizacion en agenda aun: %s" % (guardado.get("error") or "error desconocido"),
                "error": guardado.get("error"),
            }
        codigo = str(guardado.get("codigo_pedido") or draft.get("cotizacion_codigo") or "").strip()
        pdf_url = str(guardado.get("pdf_url") or draft.get("cotizacion_pdf_url") or "").strip()
        lines = [
            "Cotizacion registrada en agenda como pendiente de revision.",
            "Codigo: %s" % (codigo or "-"),
            "",
            "El equipo debe revisarla y confirmarla internamente antes de que quede como pedido confirmado.",
        ]
        if pdf_url:
            lines.extend(["", "PDF de cotizacion:", pdf_url])
        return {
            "ok": True,
            "reply": "\n".join(lines),
            "pdf_url": pdf_url,
            "agenda_id": guardado.get("id"),
            "codigo_pedido": codigo,
        }

    def catalogo_texto(catalogo, categoria_id=""):
        categoria = find_categoria(catalogo, categoria_id)
        if not categoria:
            lines = [
                "Primero elige el tipo de torta que quieres revisar:",
                "",
                list_lines(catalogo.get("categorias") or [], lambda c: str(c.get("nombre") or ""), "sin tipos cargados"),
                "",
                "Escribe, por ejemplo:",
                "- Catalogo bizcocho",
                "- Catalogo panqueque",
                "- Catalogo mil hojas",
            ]
            return "\n".join(lines)

        sizes = rows_categoria(catalogo, "sizes", categoria)
        sabores = rows_categoria(catalogo, "sabores", categoria)
        extras = rows_categoria(catalogo, "extras", categoria)
        toppers = rows_categoria(catalogo, "toppers", categoria)
        lines = [
            "Catalogo para %s" % (categoria.get("nombre") or "torta"),
            "",
            "Tamanos y precios:",
            list_lines(sizes, lambda s: "%s - %s" % (s.get("nombre") or "Tamano", fmt_clp(s.get("precio") or 0)), "sin tamanos cargados"),
            "",
            "Rellenos disponibles:",
            list_lines(sabores, lambda s: "%s%s" % (s.get("nombre") or "", (" - " + fmt_clp(s.get("precio") or 0)) if float(s.get("precio") or 0) > 0 else ""), "sin rellenos cargados"),
            "",
            "Extras disponibles:",
            list_lines(extras, lambda e: "%s - %s" % (e.get("nombre") or "", fmt_clp(e.get("precio") or 0)), "sin extras cargados"),
            "",
            "Toppers disponibles:",
            list_lines(toppers, lambda t: "%s - %s" % (t.get("nombre") or "", fmt_clp(t.get("precio") or 0)), "sin toppers cargados"),
            "",
            "Para cotizar, dime tamano, relleno, topper, fecha y hora.",
        ]
        return "\n".join(lines)

    def sugerencias_catalogo(catalogo):
        out = []
        for cat in catalogo.get("categorias") or []:
            nombre = str(cat.get("nombre") or "").strip()
            cid = str(cat.get("id") or "").strip()
            if nombre:
                base = nombre
            elif cid:
                base = cid.replace("-", " ")
            else:
                continue
            out.append("Catalogo " + base)
        return out[:8]

    def sugerencias_faltantes(faltan):
        mapa = {
            "correo": "Ingresar correo del cliente",
            "nombre": "Ingresar nombre del cliente",
            "telefono": "Ingresar telefono del cliente",
            "fecha": "Ver horas disponibles",
            "hora": "Horas disponibles",
            "tamano de torta": "Ver catalogo y precios",
            "relleno/sabor": "Ver rellenos disponibles",
            "retiro o despacho": "Indicar retiro o despacho",
            "direccion de despacho": "Ingresar direccion de despacho",
        }
        out = []
        for item in faltan or []:
            label = mapa.get(item)
            if label and label not in out:
                out.append(label)
        return out[:6]

    def sugerencias_desde_respuesta(reply):
        texto = str(reply or "")
        out = []
        seen = set()

        def add(label):
            clean = str(label or "").strip().strip(".")
            clean = re.sub(r"^[-•]\s*", "", clean).strip()
            if not clean or len(clean) > 90:
                return
            key = clean.lower()
            if key in seen:
                return
            seen.add(key)
            out.append(clean)

        capture = False
        mode = ""
        for raw in texto.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
            line = str(raw or "").strip()
            low = norm(line)
            if any(x in low for x in ["escribe por ejemplo", "puedes escribir", "si quieres puedes escribir", "opciones", "tipos disponibles"]):
                capture = True
                mode = "tipos" if "tipos disponibles" in low else "ejemplos"
                continue
            if not line:
                continue
            m = re.match(r"^[-•]\s+(.+)$", line)
            if m and capture:
                label = m.group(1)
                if mode == "tipos":
                    label = re.sub(r"(?i)\btortas?\b", "", label).strip()
                    label = "Catalogo " + label
                add(label)
        if "confirmar reserva" in norm(texto):
            add("confirmar reserva")
        if "correo" in norm(texto) and ("necesito" in norm(texto) or "por favor" in norm(texto)):
            add("Ingresar correo del cliente")
        if "horas disponibles" in norm(texto) or "horas tentativas" in norm(texto):
            add("Horas disponibles")
        if "catalogo" in norm(texto) or "precios" in norm(texto):
            add("Ver catalogo y precios")
        if "agendar torta" in norm(texto):
            add("Agendar torta")
        return out[:8]

    def chat_logic(message, draft):
        catalogo = cargar_catalogo()
        msg = str(message or "").strip()
        nmsg = norm(msg)
        draft = actualizar_draft(draft or {}, msg, catalogo)
        resumen, err = cotizar(draft, catalogo)
        faltan = faltantes(draft, resumen)

        if not msg:
            return {"reply": "Escribeme que torta necesitas y para que fecha. Te ayudo a cotizar y revisar horas disponibles.", "draft": draft}
        kb = buscar_kb(msg)
        if kb:
            return {"reply": kb, "draft": draft, "type": "knowledge"}
        agendar_intent = any(x in nmsg for x in ["agendar", "reservar", "hacer pedido", "crear pedido", "pedir torta", "agendar torta", "quiero una torta"])
        consulta_intent = any(x in nmsg for x in ["consulta", "consultas", "ayuda", "que puedes hacer", "como funciona", "menu"])
        catalogo_intent = any(x in nmsg for x in ["catalogo", "opciones", "precios", "precio", "sabores", "rellenos", "tamanos", "tamaños", "tamano"])
        disponibilidad_intent = any(x in nmsg for x in ["hora disponible", "horas disponibles", "disponibilidad", "agenda", "cuando puedo", "fecha disponible", "horarios"])

        if consulta_intent:
            return {
                "reply": "\n".join([
                    "Puedo ayudarte con estas opciones:",
                    "",
                    "- Agendar torta",
                    "- Ver catalogo y precios",
                    "- Revisar horas disponibles",
                    "- Ordenar una cotizacion con datos desordenados",
                    "- Confirmar una reserva cuando falten cero datos",
                    "",
                    "Escribe una de esas opciones o cuentame que torta necesitas.",
                ]),
                "draft": draft,
            }

        if catalogo_intent:
            reply = catalogo_texto(catalogo, categoria_id=draft.get("categoria_id") or "")
            if resumen:
                reply += "\n\n" + resumen_texto(draft, resumen)
            return {"reply": reply, "draft": draft, "quote": resumen}

        if disponibilidad_intent and not agendar_intent:
            if draft.get("fecha"):
                horas = horas_disponibles(draft.get("fecha"), limite=10)
                if horas:
                    reply = "\n".join(
                        ["Horas tentativas para %s:" % fmt_fecha(draft.get("fecha")), ""]
                        + ["- " + h for h in horas]
                        + ["", "Dime cual prefieres."]
                    )
                else:
                    reply = "Para %s no veo cupos disponibles.\n\nPuedo revisar otra fecha si me indicas una." % fmt_fecha(draft.get("fecha"))
            else:
                reply = disponibilidad_texto(proximas_fechas())
            return {"reply": reply, "draft": draft}

        if agendar_intent and faltan:
            reply = guia_agendar_texto(catalogo, draft)
            if resumen:
                reply += "\n\n" + resumen_texto(draft, resumen)
                reply += "\n\nPara continuar falta:\n" + "\n".join("- " + x for x in faltan)
            return {"reply": reply, "draft": draft, "quote": resumen}
        if any(x in nmsg for x in ["catalogo", "opciones", "precios", "precio", "sabores", "rellenos", "tamanos", "tamaños"]):
            reply = catalogo_texto(catalogo)
            if resumen:
                reply += "\n\n" + resumen_texto(draft, resumen)
            return {"reply": reply, "draft": draft, "quote": resumen}
        if any(x in nmsg for x in ["hora disponible", "horas disponibles", "disponibilidad", "agenda", "cuando puedo", "fecha disponible"]):
            if draft.get("fecha"):
                horas = horas_disponibles(draft.get("fecha"), limite=10)
                if horas:
                    reply = "Para %s tengo estas horas tentativas: %s. Dime cual prefieres." % (draft.get("fecha"), ", ".join(horas))
                else:
                    reply = "Para %s no veo cupos disponibles. Puedo revisar otra fecha si me indicas una." % draft.get("fecha")
            else:
                prox = proximas_fechas()
                parts = ["%s: %s" % (x["fecha"], ", ".join(x["horas"])) for x in prox]
                reply = "Estas son algunas fechas con horas tentativas: %s" % (" | ".join(parts) or "no encontre cupos en los proximos dias")
            return {"reply": reply, "draft": draft}
        confirmar = any(x in nmsg for x in ["confirmar", "reservar", "agendar", "crear pedido", "hacer pedido"])
        if confirmar and not faltan:
            cierre = registrar_cotizacion_completa(draft, resumen)
            reply = resumen_texto(draft, resumen) + "\n\n" + cierre.get("reply", "")
            return {
                "reply": reply,
                "draft": draft,
                "quote": resumen,
                "pdf_url": cierre.get("pdf_url") or draft.get("cotizacion_pdf_url") or "",
                "agenda_id": cierre.get("agenda_id"),
                "suggestions": cierre.get("suggestions") or [],
                "whatsapp_url": cierre.get("whatsapp_url") or "",
                "error": cierre.get("error"),
            }
        if resumen:
            if not draft.get("email"):
                reply = "\n".join([
                    "Ya tengo la base de la torta, pero antes de continuar necesito el correo del cliente.",
                    "",
                    "Con ese correo revisare si ya existe en la base de datos para completar nombre, telefono y direccion si estan guardados.",
                    "",
                    "Por favor escribe el correo para seguir con la cotizacion.",
                ])
                return {"reply": reply, "draft": draft, "quote": resumen}
            reply = resumen_texto(draft, resumen)
            cliente_msg = cliente_estado_texto(draft)
            if cliente_msg:
                reply += "\n\n" + cliente_msg
            if draft.get("fecha"):
                horas = horas_disponibles(draft.get("fecha"), limite=6)
                if horas:
                    reply += "\n\nHoras tentativas disponibles para %s:\n%s" % (
                        fmt_fecha(draft.get("fecha")),
                        "\n".join("- " + h for h in horas),
                    )
            if faltan:
                reply += "\n\nPara continuar falta:\n%s" % "\n".join("- " + x for x in faltan)
            else:
                cierre = registrar_cotizacion_completa(draft, resumen)
                reply += "\n\n" + cierre.get("reply", "")
                return {
                    "reply": reply,
                    "draft": draft,
                    "quote": resumen,
                    "pdf_url": cierre.get("pdf_url") or draft.get("cotizacion_pdf_url") or "",
                    "agenda_id": cierre.get("agenda_id"),
                    "suggestions": cierre.get("suggestions") or [],
                    "whatsapp_url": cierre.get("whatsapp_url") or "",
                    "error": cierre.get("error"),
                }
            return {"reply": reply, "draft": draft, "quote": resumen, "pdf_url": draft.get("cotizacion_pdf_url") or ""}
        meaningful = any(draft.get(k) for k in ["fecha", "hora_inicio", "email", "telefono", "nombre", "size_id", "sabor_ids"])
        if meaningful:
            reply = "Voy ordenando la informacion."
            if err:
                reply += " %s." % err
            if faltan:
                reply += "\n\nPara continuar falta:\n%s" % "\n".join("- " + x for x in faltan)
            if draft.get("fecha"):
                horas = horas_disponibles(draft.get("fecha"), limite=5)
                if horas:
                    reply += "\n\nHoras tentativas para %s:\n%s" % (
                        fmt_fecha(draft.get("fecha")),
                        "\n".join("- " + h for h in horas),
                    )
            return {"reply": reply, "draft": draft}
        registrar_desconocida(msg, {"draft": draft})
        return {
            "reply": "No entendi bien eso todavia. Lo deje registrado para que el equipo lo revise y pueda aprender esa respuesta.\n\nSi quieres, puedes escribir:\n- Agendar torta\n- Ver catalogo y precios\n- Horas disponibles",
            "draft": draft,
            "unknown": True,
        }

    @app.route("/asistente-sucree")
    def asistente_sucree():
        return render_template("asistente_sucree.html")

    @app.route("/ventas/asistente-conocimiento")
    def asistente_sucree_conocimiento():
        return render_template("asistente_conocimiento.html")

    @app.route("/api/asistente/public/chat", methods=["POST"])
    def api_asistente_public_chat():
        try:
            data = request.get_json(silent=True) or {}
            msg = str(data.get("message") or "").strip()[:1200]
            draft = data.get("draft") if isinstance(data.get("draft"), dict) else {}
            out = chat_logic(msg, draft)
            if "suggestions" not in out or out.get("suggestions") is None:
                out["suggestions"] = sugerencias_desde_respuesta(out.get("reply"))
            payload = {"success": True}
            payload.update(out)
            return jsonify(payload)
        except Exception as exc:
            registrar_desconocida(str((request.get_json(silent=True) or {}).get("message") or ""), {"error": str(exc)})
            return jsonify({"success": False, "error": "El asistente tuvo un problema temporal. Intentalo nuevamente."}), 500

    @app.route("/api/asistente/admin/pendientes", methods=["GET", "POST"])
    def api_asistente_admin_pendientes():
        conn = None
        try:
            conn = get_db()
            cur = conn.cursor()
            ensure_tables(cur)
            if request.method == "POST":
                data = request.get_json(silent=True) or {}
                pregunta = str(data.get("pregunta") or "").strip()[:700]
                respuesta = str(data.get("respuesta") or "").strip()[:2500]
                unknown_id = int(data.get("id") or 0)
                if not pregunta or not respuesta:
                    return jsonify({"success": False, "error": "Pregunta y respuesta son obligatorias"}), 400
                cur.execute("INSERT INTO asistente_kb (pregunta, respuesta, activo) VALUES (?, ?, 1)", (pregunta, respuesta))
                if unknown_id > 0:
                    cur.execute(
                        "UPDATE asistente_unknown SET estado = 'resuelto', respuesta_sugerida = ?, actualizado_en = CURRENT_TIMESTAMP WHERE id = ?",
                        (respuesta, unknown_id),
                    )
                conn.commit()
                return jsonify({"success": True})
            cur.execute("SELECT id, pregunta, contexto_json, estado, creado_en FROM asistente_unknown ORDER BY id DESC LIMIT 100")
            rows = [dict(r) for r in cur.fetchall()]
            return jsonify({"success": True, "pendientes": rows})
        except Exception as exc:
            if conn:
                conn.rollback()
            return jsonify({"success": False, "error": str(exc)}), 500
        finally:
            if conn:
                conn.close()
