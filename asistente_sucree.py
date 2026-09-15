import hashlib
import json
import re
import unicodedata
import uuid
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
    cumple_anticipacion_reserva = deps.get("_cumple_anticipacion_reserva")
    minutos_anticipacion_reserva = deps.get("_minutos_anticipacion_reserva")
    topper_requiere_96h = deps.get("_topper_requiere_96h")
    seguimiento_payload = deps.get("_seguimiento_agenda_payload")
    seguimiento_label = deps.get("_seguimiento_agenda_label")
    upsert_cliente_tienda = deps.get("_upsert_cliente_tienda_cursor")
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

    def ensure_column(cur, table, column, ddl):
        cur.execute("PRAGMA table_info(%s)" % table)
        cols = {str(r[1]) for r in cur.fetchall()}
        if column not in cols:
            cur.execute("ALTER TABLE %s ADD COLUMN %s" % (table, ddl))

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
        for column, ddl in [
            ("keywords", "keywords TEXT"),
            ("categoria", "categoria TEXT DEFAULT 'general'"),
            ("ejemplos_json", "ejemplos_json TEXT"),
            ("uso_count", "uso_count INTEGER DEFAULT 0"),
            ("ultimo_uso", "ultimo_uso TEXT"),
        ]:
            ensure_column(cur, "asistente_kb", column, ddl)
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
        for column, ddl in [
            ("pregunta_norm", "pregunta_norm TEXT"),
            ("conversation_id", "conversation_id TEXT"),
            ("respuesta_actual", "respuesta_actual TEXT"),
            ("tipo_evento", "tipo_evento TEXT DEFAULT 'no_entendido'"),
            ("intent_json", "intent_json TEXT"),
            ("confianza", "confianza REAL DEFAULT 0"),
            ("veces", "veces INTEGER DEFAULT 1"),
            ("prioridad", "prioridad TEXT DEFAULT 'normal'"),
        ]:
            ensure_column(cur, "asistente_unknown", column, ddl)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS asistente_conversaciones (
                conversation_id TEXT PRIMARY KEY,
                canal TEXT DEFAULT 'web',
                estado TEXT DEFAULT 'activa',
                mensajes_total INTEGER DEFAULT 0,
                desconocidas_total INTEGER DEFAULT 0,
                ultimo_tipo TEXT,
                resumen_json TEXT,
                creado_en TEXT DEFAULT CURRENT_TIMESTAMP,
                actualizado_en TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS asistente_mensajes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id TEXT,
                pregunta TEXT,
                respuesta TEXT,
                tipo_respuesta TEXT,
                entendido INTEGER DEFAULT 1,
                confianza REAL DEFAULT 1,
                intent_json TEXT,
                draft_json TEXT,
                creado_en TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_asistente_unknown_norm ON asistente_unknown(pregunta_norm, estado)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_asistente_mensajes_conv ON asistente_mensajes(conversation_id, id)")

    def crear_conversation_id():
        return "ASC-%s" % uuid.uuid4().hex[:18].upper()

    def fingerprint_text(texto):
        base = slug(texto)
        return hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()[:20] if base else ""

    def confidence_from_output(out, intent=None):
        out = dict(out or {})
        tipo = str(out.get("type") or "").strip()
        if out.get("unknown"):
            return 0.08
        if tipo == "invalid_catalog_option":
            return 0.82
        if out.get("quote"):
            return 0.95
        if tipo in {"knowledge", "tracking", "tracking_email", "conversation_closed"}:
            return 0.92
        if intent and any(intent.get(k) for k in ["agendar", "catalogo", "disponibilidad", "consulta", "confirmar"]):
            return 0.76
        return 0.58

    def registrar_desconocida(pregunta, contexto=None):
        conn = None
        try:
            pregunta = str(pregunta or "").strip()[:700]
            if not pregunta:
                return
            contexto = contexto or {}
            conn = get_db()
            cur = conn.cursor()
            ensure_tables(cur)
            pnorm = fingerprint_text(pregunta)
            ctx = json.dumps(contexto, ensure_ascii=False)[:6000]
            conversation_id = str(contexto.get("conversation_id") or "").strip()[:80]
            respuesta_actual = str(contexto.get("respuesta_actual") or "").strip()[:2500]
            tipo_evento = str(contexto.get("motivo") or contexto.get("tipo_evento") or "no_entendido").strip()[:80]
            intent_json = json.dumps(contexto.get("intent") or {}, ensure_ascii=False)[:3000]
            confianza = float(contexto.get("confianza") or 0)
            row = None
            if pnorm:
                cur.execute(
                    "SELECT id, veces FROM asistente_unknown WHERE pregunta_norm = ? AND estado IN ('pendiente','revisar') ORDER BY id DESC LIMIT 1",
                    (pnorm,),
                )
                row = cur.fetchone()
            if row:
                cur.execute(
                    """
                    UPDATE asistente_unknown
                    SET veces = COALESCE(veces, 1) + 1,
                        contexto_json = ?, conversation_id = COALESCE(NULLIF(?, ''), conversation_id),
                        respuesta_actual = COALESCE(NULLIF(?, ''), respuesta_actual),
                        tipo_evento = ?, intent_json = ?, confianza = ?, actualizado_en = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (ctx, conversation_id, respuesta_actual, tipo_evento, intent_json, confianza, int(row["id"])),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO asistente_unknown
                    (pregunta, pregunta_norm, contexto_json, estado, conversation_id, respuesta_actual, tipo_evento, intent_json, confianza, veces, prioridad)
                    VALUES (?, ?, ?, 'pendiente', ?, ?, ?, ?, ?, 1, ?)
                    """,
                    (pregunta, pnorm, ctx, conversation_id, respuesta_actual, tipo_evento, intent_json, confianza, "alta" if confianza < 0.25 else "normal"),
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
                """
                SELECT id, pregunta, respuesta, keywords, categoria, uso_count
                FROM asistente_kb
                WHERE COALESCE(activo, 1) = 1
                ORDER BY actualizado_en DESC, id DESC
                LIMIT 350
                """
            )
            msg_norm = slug(mensaje)
            msg_words = set(msg_norm.split())
            best = None
            best_score = 0.0
            for row in cur.fetchall():
                pregunta_score = ratio(mensaje, row["pregunta"])
                keywords = [slug(x) for x in re.split(r"[,;\n]+", str(row["keywords"] or "")) if slug(x)]
                keyword_score = 0.0
                for kw in keywords:
                    kw_words = set(kw.split())
                    if kw and kw in msg_norm:
                        keyword_score = max(keyword_score, 1.0)
                    elif kw_words:
                        keyword_score = max(keyword_score, len(msg_words & kw_words) / max(1, len(kw_words)))
                score = max(pregunta_score, keyword_score)
                if score > best_score:
                    best_score = score
                    best = row
            if best and best_score >= 0.55:
                try:
                    cur.execute(
                        "UPDATE asistente_kb SET uso_count = COALESCE(uso_count, 0) + 1, ultimo_uso = CURRENT_TIMESTAMP WHERE id = ?",
                        (int(best["id"]),),
                    )
                    conn.commit()
                except Exception:
                    conn.rollback()
                return str(best["respuesta"] or "").strip()
        except Exception:
            pass
        finally:
            if conn:
                conn.close()
        return ""

    def registrar_interaccion_asistente(conversation_id, pregunta, out, draft_entrada=None, user_agent=""):
        conn = None
        try:
            conversation_id = str(conversation_id or "").strip()[:80] or crear_conversation_id()
            pregunta = str(pregunta or "").strip()[:1200]
            out = dict(out or {})
            draft_salida = out.get("draft") if isinstance(out.get("draft"), dict) else {}
            intent = inferir_intenciones(norm(pregunta), draft_entrada or {}) if pregunta else {}
            confianza = confidence_from_output(out, intent)
            entendido = 0 if out.get("unknown") else 1
            tipo = str(out.get("type") or ("unknown" if out.get("unknown") else "reply")).strip()[:80]
            respuesta = str(out.get("reply") or "").strip()[:4000]
            estado_conv = "cerrada" if out.get("closed") else "activa"
            conn = get_db()
            cur = conn.cursor()
            ensure_tables(cur)
            cur.execute(
                """
                INSERT INTO asistente_conversaciones
                (conversation_id, canal, estado, mensajes_total, desconocidas_total, ultimo_tipo, resumen_json)
                VALUES (?, 'web', ?, 0, 0, ?, ?)
                ON CONFLICT(conversation_id) DO UPDATE SET
                    estado = excluded.estado,
                    ultimo_tipo = excluded.ultimo_tipo,
                    resumen_json = excluded.resumen_json,
                    actualizado_en = CURRENT_TIMESTAMP
                """,
                (conversation_id, estado_conv, tipo, json.dumps({"draft": draft_salida, "user_agent": str(user_agent or "")[:300]}, ensure_ascii=False)[:7000]),
            )
            cur.execute(
                """
                INSERT INTO asistente_mensajes
                (conversation_id, pregunta, respuesta, tipo_respuesta, entendido, confianza, intent_json, draft_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    conversation_id,
                    pregunta,
                    respuesta,
                    tipo,
                    entendido,
                    confianza,
                    json.dumps(intent, ensure_ascii=False)[:3000],
                    json.dumps(draft_salida, ensure_ascii=False)[:7000],
                ),
            )
            cur.execute(
                """
                UPDATE asistente_conversaciones
                SET mensajes_total = COALESCE(mensajes_total, 0) + 1,
                    desconocidas_total = COALESCE(desconocidas_total, 0) + ?,
                    actualizado_en = CURRENT_TIMESTAMP
                WHERE conversation_id = ?
                """,
                (0 if entendido else 1, conversation_id),
            )
            conn.commit()
            if not entendido:
                registrar_desconocida(pregunta, {
                    "conversation_id": conversation_id,
                    "draft": draft_salida,
                    "draft_entrada": draft_entrada or {},
                    "intent": intent,
                    "respuesta_actual": respuesta,
                    "motivo": "no_entendido",
                    "confianza": confianza,
                })
            return conversation_id
        except Exception:
            if conn:
                conn.rollback()
            return conversation_id
        finally:
            if conn:
                conn.close()


    def merge_drafts_conservador(base, nuevo):
        merged = dict(base or {})
        nuevo = dict(nuevo or {})
        for key, value in nuevo.items():
            if value is None or value == "" or value == []:
                continue
            if key in {"sabor_ids", "extra_items", "extras", "incompatibilidades"}:
                if value:
                    merged[key] = value
                continue
            merged[key] = value
        return merged

    def recuperar_draft_conversacion(conversation_id):
        conn = None
        try:
            cid = str(conversation_id or "").strip()[:80]
            if not cid:
                return {}
            conn = get_db()
            cur = conn.cursor()
            ensure_tables(cur)
            cur.execute(
                """
                SELECT draft_json
                FROM asistente_mensajes
                WHERE conversation_id = ? AND draft_json IS NOT NULL AND TRIM(draft_json) <> ''
                ORDER BY id DESC
                LIMIT 1
                """,
                (cid,),
            )
            row = cur.fetchone()
            if row:
                try:
                    data = json.loads(row["draft_json"] or "{}")
                    return data if isinstance(data, dict) else {}
                except Exception:
                    return {}
            cur.execute("SELECT resumen_json FROM asistente_conversaciones WHERE conversation_id = ? LIMIT 1", (cid,))
            row = cur.fetchone()
            if row:
                try:
                    data = json.loads(row["resumen_json"] or "{}")
                    draft = data.get("draft") if isinstance(data, dict) else {}
                    return draft if isinstance(draft, dict) else {}
                except Exception:
                    return {}
        except Exception:
            return {}
        finally:
            if conn:
                conn.close()
        return {}

    def reconciliar_draft_catalogo(draft, catalogo):
        draft = dict(draft or {})
        categoria = find_categoria(catalogo, draft.get("categoria_id") or draft.get("tamano_invalido_categoria_id") or "")
        personas = draft.get("personas") or draft.get("tamano_invalido")
        if categoria and personas and not draft.get("size_id"):
            candidatos = sizes_por_personas(catalogo, personas, categoria)
            if len(candidatos) == 1:
                draft["size_id"] = str(candidatos[0].get("id") or "")
                draft.pop("tamano_invalido", None)
                draft.pop("tamano_invalido_categoria_id", None)
                draft = clear_incompatibilidades(draft, {"tama?o", "tamano"})
        if draft.get("sabor_ids"):
            categoria = find_categoria(catalogo, draft.get("categoria_id") or "")
            if categoria:
                validos = {str(x.get("id") or "") for x in rows_categoria(catalogo, "sabores", categoria)}
                if validos:
                    filtrados = [sid for sid in (draft.get("sabor_ids") or []) if str(sid) in validos]
                    if filtrados:
                        draft["sabor_ids"] = filtrados[:3]
        return draft

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

    def registrar_cliente_desde_draft(draft):
        draft = dict(draft or {})
        email = normalizar_email(draft.get("email"))
        telefono = normalizar_telefono(draft.get("telefono"))
        nombre = str(draft.get("nombre") or "").strip()[:80]
        direccion = str(draft.get("direccion") or "").strip()[:240]
        if not email or not telefono or len(nombre) < 2:
            return draft
        if draft.get("entrega_tipo") == "despacho" and not direccion:
            return draft
        if draft.get("cliente_id") and draft.get("cliente_encontrado") is True:
            return draft
        conn = None
        try:
            conn = get_db()
            cur = conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            cliente = None
            if callable(upsert_cliente_tienda):
                try:
                    cliente = upsert_cliente_tienda(
                        cur,
                        nombre=nombre,
                        email=email,
                        telefono=telefono,
                        email_confirmado=0,
                        direccion_default=direccion,
                    )
                except Exception:
                    cliente = None
            if not cliente:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS tienda_clientes (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        nombre TEXT DEFAULT '',
                        email TEXT NOT NULL,
                        telefono TEXT NOT NULL,
                        activo INTEGER NOT NULL DEFAULT 1,
                        creado_en TEXT DEFAULT CURRENT_TIMESTAMP,
                        actualizado_en TEXT DEFAULT CURRENT_TIMESTAMP,
                        ultimo_login TEXT,
                        direccion_default TEXT,
                        direccion_lat REAL,
                        direccion_lng REAL,
                        UNIQUE(email, telefono)
                    )
                    """
                )
                for ddl in [
                    "ALTER TABLE tienda_clientes ADD COLUMN direccion_default TEXT",
                    "ALTER TABLE tienda_clientes ADD COLUMN direccion_lat REAL",
                    "ALTER TABLE tienda_clientes ADD COLUMN direccion_lng REAL",
                    "ALTER TABLE tienda_clientes ADD COLUMN ultimo_login TEXT",
                ]:
                    try:
                        cur.execute(ddl)
                    except Exception:
                        pass
                cur.execute(
                    """
                    INSERT INTO tienda_clientes (
                        nombre, email, telefono, direccion_default, activo, actualizado_en, ultimo_login
                    )
                    VALUES (?, ?, ?, NULLIF(?, ''), 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT(email, telefono) DO UPDATE SET
                        nombre = excluded.nombre,
                        direccion_default = COALESCE(excluded.direccion_default, tienda_clientes.direccion_default),
                        activo = 1,
                        actualizado_en = CURRENT_TIMESTAMP,
                        ultimo_login = CURRENT_TIMESTAMP
                    """,
                    (nombre, email, telefono, direccion),
                )
                cur.execute(
                    """
                    SELECT id, nombre, email, telefono, direccion_default, direccion_lat, direccion_lng
                    FROM tienda_clientes
                    WHERE LOWER(TRIM(email)) = LOWER(TRIM(?))
                      AND TRIM(telefono) = TRIM(?)
                    LIMIT 1
                    """,
                    (email, telefono),
                )
                row = cur.fetchone()
                cliente = dict(row) if row else None
            conn.commit()
            if cliente:
                draft = aplicar_cliente_draft(draft, cliente)
            draft["cliente_encontrado"] = True
            draft["cliente_registrado_por_asistente"] = True
            return draft
        except Exception:
            if conn:
                conn.rollback()
            return draft
        finally:
            if conn:
                conn.close()

    def detectar_codigo_pedido(texto):
        raw = str(texto or "").upper()
        m = re.search(r"\b(?:AGD|COT)-\d{8}-[A-Z0-9]{6,}\b", raw)
        if m:
            return m.group(0).strip()
        m = re.search(r"\b(?:AGD|COT)[\s\-]*(\d{8})[\s\-]*([A-Z0-9]{6,})\b", raw)
        if m:
            prefix = raw[m.start():m.start() + 3]
            return "%s-%s-%s" % (prefix, m.group(1), m.group(2))
        return ""

    def descripcion_estado_seguimiento(estado):
        mapa = {
            "pendiente": "Tu pedido fue registrado y esta pendiente de revision.",
            "recepcionado": "Pasteleria Sucree ya reviso la informacion de tu pedido.",
            "produccion": "Tu pedido esta siendo preparado por nuestro equipo.",
            "espera_envio": "Tu pedido esta listo para coordinar entrega o retiro.",
            "despachado": "Tu pedido ya fue despachado o va camino a destino.",
            "entregado": "Tu pedido fue entregado correctamente.",
        }
        return mapa.get(str(estado or "").strip().lower(), "Tu pedido esta registrado en nuestro sistema.")


    def buscar_pedidos_activos_por_email(email, limite=5):
        email = normalizar_email(email)
        if not email:
            return []
        conn = None
        try:
            conn = get_db()
            cur = conn.cursor()
            hoy = datetime.now(ZoneInfo("America/Santiago")).date().isoformat()
            like_email = "%Email: %s%" % email
            cur.execute(
                """
                SELECT id, tipo, titulo, fecha, hora_inicio, hora_entrega, cliente, telefono,
                       direccion, ingredientes, total, abono, estado, codigo_pedido,
                       codigo_operacion, seguimiento_estado, es_envio, creado
                FROM agenda_eventos
                WHERE (
                    LOWER(TRIM(COALESCE(cliente_email, ''))) = LOWER(TRIM(?))
                    OR ingredientes LIKE ?
                )
                  AND COALESCE(NULLIF(TRIM(estado), ''), 'activo') NOT IN ('entregado', 'cancelado', 'anulado')
                  AND (COALESCE(fecha, '') = '' OR fecha >= ?)
                ORDER BY COALESCE(fecha, '') ASC, COALESCE(hora_inicio, hora_entrega, '') ASC, id DESC
                LIMIT ?
                """,
                (email, like_email, hoy, int(limite or 5)),
            )
            return [dict(row) for row in cur.fetchall()]
        except Exception:
            return []
        finally:
            if conn:
                conn.close()

    def respuesta_seguimiento_por_email(email):
        email = normalizar_email(email)
        if not email:
            return "Para revisar tus pedidos activos, enviame el correo usado al agendar o el codigo del pedido."
        pedidos = buscar_pedidos_activos_por_email(email)
        if not pedidos:
            return "No encontre pedidos activos asociados a %s. Si tienes el codigo del pedido, enviamelo y lo reviso directamente." % email
        lines = ["Estos son los pedidos activos que encontre para %s:" % email, ""]
        for ev in pedidos:
            codigo = str(ev.get("codigo_pedido") or ev.get("codigo_operacion") or ("#%s" % ev.get("id"))).strip()
            estado = str(ev.get("seguimiento_estado") or ev.get("estado") or "pendiente").strip().lower()
            label = seguimiento_label(estado) if callable(seguimiento_label) else estado.replace("_", " ").title()
            titulo = str(ev.get("titulo") or "Pedido Sucree").strip()
            fecha = fmt_fecha(ev.get("fecha"))
            hora = str(ev.get("hora_entrega") or ev.get("hora_inicio") or "-").strip()[:5] or "-"
            url = "%s/seguimiento/%s" % (public_base_url, quote(codigo)) if codigo and not codigo.startswith("#") else "%s/seguimiento" % public_base_url
            lines.extend([
                "- %s" % codigo,
                "  Pedido: %s" % titulo,
                "  Estado: %s" % label,
                "  Fecha y hora: %s %s" % (fecha, hora),
                "  Seguimiento: %s" % url,
            ])
        if len(pedidos) >= 5:
            lines.extend(["", "Si no ves tu pedido, enviame el codigo exacto para buscarlo directamente."])
        return "\n".join(lines)

    def consultar_estado_pedido(codigo):
        codigo = str(codigo or "").strip().upper()
        if not codigo:
            return ""
        seguimiento_url = "%s/seguimiento/%s" % (public_base_url, quote(codigo))
        try:
            from database import obtener_evento_agenda_por_codigo
            evento = obtener_evento_agenda_por_codigo(codigo)
        except Exception:
            evento = None
        if not evento:
            return "\n".join([
                "No encontre un pedido con el codigo %s." % codigo,
                "",
                "Revisa que el codigo este escrito completo. Tambien puedes intentar desde el link de seguimiento:",
                seguimiento_url,
            ])
        payload = seguimiento_payload(evento) if callable(seguimiento_payload) else {}
        estado = str((payload or {}).get("estado") or evento.get("seguimiento_estado") or "pendiente").strip().lower()
        label = str((payload or {}).get("estado_label") or "").strip()
        if not label:
            label = seguimiento_label(estado) if callable(seguimiento_label) else estado.replace("_", " ").title()
        titulo = str((payload or {}).get("titulo") or evento.get("titulo") or "Pedido Sucree").strip()
        fecha = fmt_fecha((payload or {}).get("fecha") or evento.get("fecha"))
        hora = str((payload or {}).get("hora") or evento.get("hora_entrega") or evento.get("hora_inicio") or "-").strip()[:5] or "-"
        modalidad = str((payload or {}).get("modalidad") or ("Despacho" if evento.get("es_envio") else "Retiro en tienda")).strip()
        lines = [
            "Estado de tu pedido %s:" % codigo,
            "",
            "Pedido: %s" % titulo,
            "Estado actual: %s" % label,
            descripcion_estado_seguimiento(estado),
            "",
            "Fecha: %s" % fecha,
            "Hora: %s" % hora,
            "Modalidad: %s" % modalidad,
            "",
            "Tambien puedes revisar el avance actualizado desde este link:",
            seguimiento_url,
        ]
        return "\n".join(lines)

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

    def texto_catalogo_limpio(texto):
        limpio = slug(texto)
        stop = {
            "catalogo", "catalogos", "precio", "precios", "opcion", "opciones",
            "ver", "revisar", "mostrar", "quiero", "torta", "tortas", "de", "del",
        }
        return " ".join(w for w in limpio.split() if w not in stop)

    def detectar_categoria_catalogo(texto, catalogo):
        consulta = texto_catalogo_limpio(texto)
        if not consulta:
            return None
        tokens = set(consulta.split())
        consulta_compacta = consulta.replace(" ", "")
        familias = [
            ("mil hojas", {"mil", "hojas"}),
            ("milhojas", {"milhojas"}),
            ("sin azucar", {"sin", "azucar"}),
            ("bizcocho", {"bizcocho"}),
            ("panqueque", {"panqueque"}),
            ("ganache", {"ganache"}),
            ("puntillismo", {"puntillismo"}),
            ("cuatro leches", {"cuatro", "leches"}),
        ]
        best = None
        best_score = 0
        for cat in catalogo.get("categorias") or []:
            nombre_slug = slug(cat.get("nombre"))
            nombre_compacto = nombre_slug.replace(" ", "")
            nombre_tokens = set(nombre_slug.split())
            score = len(tokens & nombre_tokens) * 10
            for _, fam_tokens in familias:
                if fam_tokens <= tokens and fam_tokens <= nombre_tokens:
                    score += 20 + len(fam_tokens)
            if consulta and consulta in nombre_slug:
                score += 30
            if consulta_compacta and consulta_compacta in nombre_compacto:
                score += 30
            if consulta_compacta == "milhojas" and ("milhojas" in nombre_compacto or {"mil", "hojas"} <= nombre_tokens):
                score += 35
            if score > best_score:
                best = cat
                best_score = score
        return best if best_score > 0 else match_row(consulta, catalogo.get("categorias") or [], min_score=0.35)

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

    def sizes_por_personas(catalogo, personas, categoria=None):
        try:
            personas = int(personas or 0)
        except (TypeError, ValueError):
            return []
        rows = rows_categoria(catalogo, "sizes", categoria) if categoria else list(catalogo.get("sizes") or [])
        out = []
        for row in rows:
            nombre_size = norm(row.get("nombre") or "")
            if re.search(r"\b%s\b" % re.escape(str(personas)), nombre_size):
                out.append(row)
        return out

    def palabras_catalogo_existentes(catalogo):
        valores = []
        for key in ["categorias", "sizes", "sabores", "extras", "toppers"]:
            for row in catalogo.get(key) or []:
                valores.append(slug(row.get("nombre") or ""))
                valores.append(slug(row.get("id") or ""))
        return [v for v in valores if v]

    def texto_contiene_opcion_catalogo(texto_norm, catalogo):
        t = slug(texto_norm)
        return any(v and v in t for v in palabras_catalogo_existentes(catalogo))

    def add_incompatibilidad(draft, campo, valor, mensaje=""):
        draft = dict(draft or {})
        items = [x for x in (draft.get("incompatibilidades") or []) if isinstance(x, dict)]
        key = "%s:%s" % (campo, slug(valor))
        if not any(str(x.get("key") or "") == key for x in items):
            items.append({"key": key, "campo": campo, "valor": str(valor or "").strip(), "mensaje": str(mensaje or "").strip()})
        draft["incompatibilidades"] = items[:5]
        return draft

    def clear_incompatibilidades(draft, campos=None):
        draft = dict(draft or {})
        if not campos:
            draft.pop("incompatibilidades", None)
            return draft
        campos = {str(x or "") for x in campos}
        draft["incompatibilidades"] = [x for x in (draft.get("incompatibilidades") or []) if str((x or {}).get("campo") or "") not in campos]
        if not draft.get("incompatibilidades"):
            draft.pop("incompatibilidades", None)
        return draft

    def extraer_valor_despues_de(texto, palabras):
        raw = str(texto or "")
        patron = r"\b(?:%s)\b\s*(?:de|del|con|:|-)?\s+(.+)" % "|".join(re.escape(p) for p in palabras)
        m = re.search(patron, raw, flags=re.I)
        if not m:
            return ""
        valor = re.split(r"\b(?:para|fecha|hora|correo|email|telefono|teléfono|fono|nombre|retiro|despacho|direccion|dirección|sin topper|topper|extra|extras)\b", m.group(1), flags=re.I)[0]
        return re.sub(r"\s+", " ", valor).strip(" .,-:;")[:80]

    def respuesta_incompatibilidades(draft, catalogo):
        items = [x for x in (draft.get("incompatibilidades") or []) if isinstance(x, dict)]
        if not items:
            return ""
        first = items[0]
        campo = str(first.get("campo") or "opcion")
        valor = str(first.get("valor") or "").strip()
        categoria = find_categoria(catalogo, draft.get("categoria_id") or draft.get("tamano_invalido_categoria_id") or "")
        lines = []
        if valor:
            lines.append("No puedo avanzar con %s: %s, porque no existe o no está disponible en el sistema." % (campo, valor))
        else:
            lines.append("No puedo avanzar con ese dato porque no existe o no está disponible en el sistema.")
        lines.append("Antes de seguir necesito que elijas una opción válida del catálogo.")
        if campo in {"tipo de torta", "producto"}:
            lines.extend(["", "Tipos disponibles:", list_lines(catalogo.get("categorias") or [], lambda c: str(c.get("nombre") or ""), "sin tipos cargados")])
        elif campo in {"relleno", "sabor"}:
            sabores = rows_categoria(catalogo, "sabores", categoria) if categoria else list(catalogo.get("sabores") or [])
            lines.extend(["", "Rellenos disponibles%s:" % ((" para " + str(categoria.get("nombre") or "")) if categoria else ""), list_lines(sabores[:12], lambda r: str(r.get("nombre") or ""), "sin rellenos cargados")])
        elif campo == "topper":
            toppers = rows_categoria(catalogo, "toppers", categoria) if categoria else list(catalogo.get("toppers") or [])
            lines.extend(["", "Toppers disponibles:", list_lines(toppers[:10], lambda r: str(r.get("nombre") or ""), "sin toppers cargados")])
        elif campo == "extra":
            extras = rows_categoria(catalogo, "extras", categoria) if categoria else list(catalogo.get("extras") or [])
            lines.extend(["", "Extras disponibles:", list_lines(extras[:10], lambda r: str(r.get("nombre") or ""), "sin extras cargados")])
        else:
            _, sizes = opciones_tamano_para_draft(draft, catalogo or {})
            lines.extend(["", "Tamaños disponibles:", list_lines(sizes[:10], lambda r: "%s - %s" % (r.get("nombre") or "Tamaño", fmt_clp(r.get("precio") or 0)), "sin tamaños cargados")])
        return "\n".join(lines)

    def sugerencias_incompatibilidades(draft, catalogo):
        items = [x for x in (draft.get("incompatibilidades") or []) if isinstance(x, dict)]
        if not items:
            return []
        campo = str(items[0].get("campo") or "")
        categoria = find_categoria(catalogo, draft.get("categoria_id") or draft.get("tamano_invalido_categoria_id") or "")
        key = "categorias"
        if campo in {"relleno", "sabor"}:
            key = "sabores"
        elif campo == "topper":
            key = "toppers"
        elif campo == "extra":
            key = "extras"
        elif campo in {"tamaño", "tamano"}:
            key = "sizes"
        rows = rows_categoria(catalogo, key, categoria) if key != "categorias" else list(catalogo.get("categorias") or [])
        return [str(x.get("nombre") or "").strip() for x in rows[:6] if str(x.get("nombre") or "").strip()]

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

    def info_anticipacion_torta(draft=None, catalogo=None):
        draft = dict(draft or {})
        catalogo = catalogo or {}
        categoria = find_categoria(catalogo, draft.get("categoria_id") or "")
        topper = None
        topper_id = str(draft.get("topper_id") or "").strip()
        if topper_id:
            for row in catalogo.get("toppers") or []:
                if str(row.get("id") or "").strip() == topper_id:
                    topper = row
                    break
        try:
            min_horas_categoria = int(float((categoria or {}).get("min_lead_hours") or 0))
        except (TypeError, ValueError):
            min_horas_categoria = 0
        if callable(topper_requiere_96h):
            requiere_topper = bool(topper_requiere_96h(topper_id=topper_id, topper_nombre=(topper or {}).get("nombre")))
        else:
            texto_topper = slug("%s %s" % (topper_id, (topper or {}).get("nombre") or ""))
            requiere_topper = bool(texto_topper and "sin topper" not in texto_topper)
        if callable(minutos_anticipacion_reserva):
            try:
                minutos = int(minutos_anticipacion_reserva(
                    "torta",
                    topper_requiere_96h=requiere_topper,
                    min_horas_categoria=min_horas_categoria,
                ))
            except Exception:
                minutos = max(48, min_horas_categoria) * 60
        else:
            minutos = max(96 if requiere_topper else 48, min_horas_categoria) * 60
        horas = max(1, int((minutos + 59) // 60))
        return {
            "horas": horas,
            "min_horas_categoria": min_horas_categoria,
            "topper_96h": requiere_topper,
        }

    def cumple_anticipacion_torta(fecha, hora, cfg, draft=None, catalogo=None):
        info = info_anticipacion_torta(draft, catalogo)
        if callable(cumple_anticipacion_reserva):
            try:
                return bool(cumple_anticipacion_reserva(
                    fecha,
                    hora,
                    "torta",
                    cfg_agenda=cfg,
                    now_local=datetime.now(ZoneInfo("America/Santiago")),
                    topper_requiere_96h=bool(info.get("topper_96h")),
                    min_horas_categoria=int(info.get("min_horas_categoria") or 0),
                ))
            except Exception:
                pass
        try:
            slot_dt = datetime.strptime("%s %s" % (fecha, hora), "%Y-%m-%d %H:%M").replace(tzinfo=ZoneInfo("America/Santiago"))
            min_dt = datetime.now(ZoneInfo("America/Santiago")) + timedelta(hours=int(info.get("horas") or 48))
            return slot_dt >= min_dt
        except Exception:
            return False

    def mensaje_anticipacion_torta(draft=None, catalogo=None):
        info = info_anticipacion_torta(draft, catalogo)
        horas = int(info.get("horas") or 48)
        if info.get("topper_96h"):
            return "Las tortas con topper requieren minimo %s horas de anticipacion." % horas
        return "Las tortas requieren minimo %s horas de anticipacion." % horas

    def validar_hora_cotizacion(draft, catalogo=None):
        fecha = str(draft.get("fecha") or "").strip()
        hora = str(draft.get("hora_inicio") or "").strip()[:5]
        if not fecha or not hora:
            return {"ok": False, "error": "Falta fecha u hora.", "horas": []}
        detalle = horas_disponibles_detalle(fecha, limite=30, draft=draft, catalogo=catalogo)
        horas = detalle.get("horas") or []
        if draft.get("cotizacion_evento_id"):
            return {"ok": True, "horas": horas}
        if hora in horas:
            return {"ok": True, "horas": horas}
        try:
            if not cumple_anticipacion_torta(fecha, hora, cfg_agenda(), draft=draft, catalogo=catalogo):
                return {"ok": False, "error": mensaje_anticipacion_torta(draft, catalogo), "horas": horas, "anticipacion": True}
        except Exception:
            pass
        if detalle.get("bloqueadas_anticipacion"):
            return {"ok": False, "error": mensaje_anticipacion_torta(draft, catalogo), "horas": horas, "anticipacion": True}
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
        if draft.get("fecha") and not draft.get("hora_inicio"):
            horas = horas_disponibles(draft.get("fecha"), limite=6, draft=draft, catalogo=catalogo)
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

    def limpiar_nombre_cliente(valor):
        nombre = re.sub(r"\s+", " ", str(valor or "")).strip(" .,-:;")[:80]
        nombre = re.split(r"\b(?:telefono|tel[eé]fono|fono|celular|whatsapp|correo|email|mail|direccion|direcci[oó]n|despacho)\b", nombre, flags=re.I)[0].strip(" .,-:;")
        nombre = re.sub(r"\b(?:y\s+mi|mi|y)\s*$", "", nombre, flags=re.I).strip(" .,-:;")
        if len(nombre) < 2 or re.search(r"\d|@", nombre) or len(nombre.split()) > 6:
            return ""
        return nombre

    def parse_nombre_cliente(texto, draft=None):
        raw = str(texto or "").strip()
        patrones = [
            r"\b(?:nombre|cliente)\s*(?:es|:|-)?\s+([A-Za-zÁÉÍÓÚÜÑáéíóúüñ ]{2,80})",
            r"\b(?:soy|me llamo|mi nombre es)\s+([A-Za-zÁÉÍÓÚÜÑáéíóúüñ ]{2,80})",
        ]
        for patron in patrones:
            m = re.search(patron, raw, flags=re.I)
            if m:
                nombre = limpiar_nombre_cliente(m.group(1))
                if nombre:
                    return nombre
        draft = dict(draft or {})
        if draft.get("email") and draft.get("cliente_encontrado") is False and not draft.get("nombre"):
            if not parse_fecha(raw) and not parse_hora(raw) and "@" not in raw and not normalizar_telefono(raw):
                low = norm(raw)
                bloqueadas = ["catalogo", "horas", "disponible", "bizcocho", "panqueque", "relleno", "topper", "despacho", "retiro", "direccion", "precio"]
                if not any(x in low for x in bloqueadas):
                    return limpiar_nombre_cliente(raw)
        return ""

    def parse_direccion_cliente(texto, draft=None):
        raw = str(texto or "").strip()
        patrones = [
            r"\b(?:direccion|direcci[oó]n|domicilio)\s*(?:es|:|-)?\s+(.+)",
            r"\b(?:despacho|delivery|enviar|envio|env[ií]o)\s+(?:a|en|para)?\s*(.+)",
        ]
        for patron in patrones:
            m = re.search(patron, raw, flags=re.I)
            if m:
                direccion = re.split(r"\b(?:telefono|tel[eé]fono|fono|celular|whatsapp|correo|email|mail|nombre|cliente)\b", m.group(1), flags=re.I)[0]
                direccion = re.sub(r"\s+", " ", direccion).strip(" .,-:;")[:240]
                if len(direccion) >= 6:
                    return direccion
        draft = dict(draft or {})
        if draft.get("entrega_tipo") == "despacho" and not draft.get("direccion"):
            if not parse_fecha(raw) and not parse_hora(raw) and "@" not in raw and not normalizar_telefono(raw):
                low = norm(raw)
                pistas = ["calle", "pasaje", "avenida", "av ", "villa", "depto", "departamento", "casa", "maipu", "santiago"]
                if len(raw) >= 8 and (re.search(r"\d", raw) or any(x in low for x in pistas)):
                    return re.sub(r"\s+", " ", raw).strip(" .,-:;")[:240]
        return ""

    def actualizar_draft(draft, mensaje, catalogo):
        draft = dict(draft or {})
        texto = str(mensaje or "")
        fecha = parse_fecha(texto)
        hora = parse_hora(texto)
        email, telefono = parse_contacto(texto)
        entrega = detectar_entrega(texto)
        nombre_detectado = parse_nombre_cliente(texto, draft)
        direccion_detectada = parse_direccion_cliente(texto, draft)
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
        if nombre_detectado:
            draft["nombre"] = nombre_detectado
        if direccion_detectada:
            draft["direccion"] = direccion_detectada
        texto_norm = norm(texto)
        categoria = detectar_categoria_catalogo(texto, catalogo) or match_row(texto, catalogo.get("categorias") or [], min_score=0.55)
        if categoria:
            draft = clear_incompatibilidades(draft, {"tipo de torta", "producto"})
        elif any(x in texto_norm for x in ["red velvet", "cheesecake", "kuchen", "pie", "cupcake", "helado", "brownie", "tiramis", "selva negra"]):
            valor_tipo = extraer_valor_despues_de(texto, ["torta", "pastel", "producto", "quiero", "necesito"]) or texto
            draft = add_incompatibilidad(draft, "tipo de torta", valor_tipo)
        categoria_para_tamano = categoria or find_categoria(catalogo, draft.get("categoria_id") or "")
        size = None
        m = re.search(r"\b(\d{1,3})\s*(?:persona|personas|pers|pax)\b", norm(texto))
        if m:
            personas = int(m.group(1))
            draft["personas"] = personas
            draft.pop("tamano_invalido", None)
            draft.pop("tamano_invalido_categoria_id", None)
            draft = clear_incompatibilidades(draft, {"tamaño", "tamano"})
            if categoria_para_tamano:
                candidatos = sizes_por_personas(catalogo, personas, categoria_para_tamano)
                if len(candidatos) == 1:
                    size = candidatos[0]
                if not size:
                    draft["tamano_invalido"] = personas
                    draft["tamano_invalido_categoria_id"] = str(categoria_para_tamano.get("id") or "")
                    draft = add_incompatibilidad(draft, "tamaño", "%s personas" % personas)
            else:
                candidatos_globales = sizes_por_personas(catalogo, personas)
                if not candidatos_globales:
                    draft["tamano_invalido"] = personas
                    draft = add_incompatibilidad(draft, "tamaño", "%s personas" % personas)
                elif len(candidatos_globales) == 1:
                    size = candidatos_globales[0]
        # No elegir un tama?o por similitud si el cliente no indic? personas o un tama?o expl?cito.
        if not size and not m and not draft.get("personas") and any(x in texto_norm for x in ["persona", "personas", "pax", "tamano", "tama?o"]):
            size = match_row(texto, catalogo.get("sizes") or [], min_score=0.70)
        if size:
            draft["size_id"] = str(size.get("id") or "")
            draft.pop("tamano_invalido", None)
            draft.pop("tamano_invalido_categoria_id", None)
            draft = clear_incompatibilidades(draft, {"tamaño", "tamano"})
            if size.get("categoria_id"):
                draft["categoria_id"] = str(size.get("categoria_id") or "")
        if categoria:
            draft["categoria_id"] = str(categoria.get("id") or "")
            size_id_actual = str(draft.get("size_id") or "").strip()
            if size_id_actual:
                size_actual = None
                for row in catalogo.get("sizes") or []:
                    if str(row.get("id") or "").strip() == size_id_actual:
                        size_actual = row
                        break
                if size_actual and str(size_actual.get("categoria_id") or "").strip().lower() != str(categoria.get("id") or "").strip().lower():
                    draft.pop("size_id", None)
        if categoria and draft.get("personas") and not draft.get("size_id") and not draft.get("tamano_invalido"):
            personas = int(draft.get("personas") or 0)
            candidatos = sizes_por_personas(catalogo, personas, categoria)
            if len(candidatos) == 1:
                draft["size_id"] = str(candidatos[0].get("id") or "")
            else:
                draft["tamano_invalido"] = personas
                draft["tamano_invalido_categoria_id"] = str(categoria.get("id") or "")
                draft = add_incompatibilidad(draft, "tamaño", "%s personas" % personas)
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
        if encontrados:
            draft = clear_incompatibilidades(draft, {"relleno", "sabor"})
        elif any(x in texto_norm for x in ["relleno", "sabor", "saborizado"]):
            valor_sabor = extraer_valor_despues_de(texto, ["relleno", "rellenos", "sabor", "sabores"])
            if valor_sabor:
                draft = add_incompatibilidad(draft, "relleno", valor_sabor)
        if actuales:
            draft["sabor_ids"] = actuales[:3]
        topper = match_row(texto, catalogo.get("toppers") or [], min_score=0.58)
        if topper:
            draft["topper_id"] = str(topper.get("id") or "")
            draft = clear_incompatibilidades(draft, {"topper"})
        elif "sin topper" in texto_norm:
            for tp in catalogo.get("toppers") or []:
                if "sin" in slug(tp.get("nombre")) and "topper" in slug(tp.get("nombre")):
                    draft["topper_id"] = str(tp.get("id") or "")
                    draft = clear_incompatibilidades(draft, {"topper"})
                    break
        elif "topper" in texto_norm:
            valor_topper = extraer_valor_despues_de(texto, ["topper", "adorno", "decoracion", "decoraci?n"]) or "topper solicitado"
            draft = add_incompatibilidad(draft, "topper", valor_topper)
        extras = list(draft.get("extra_items") or [])
        extra = match_row(texto, catalogo.get("extras") or [], min_score=0.62)
        if extra:
            eid = str(extra.get("id") or "")
            if eid and not any(str(x.get("id") or "") == eid for x in extras if isinstance(x, dict)):
                extras.append({"id": eid, "qty": 1})
            draft["extra_items"] = extras[:8]
            draft = clear_incompatibilidades(draft, {"extra"})
        elif any(x in texto_norm for x in ["extra", "extras"]):
            valor_extra = extraer_valor_despues_de(texto, ["extra", "extras"])
            if valor_extra and "sin" not in slug(valor_extra):
                draft = add_incompatibilidad(draft, "extra", valor_extra)
        if "sin extra" in texto_norm or "sin extras" in texto_norm:
            draft["extra_items"] = []
            draft = clear_incompatibilidades(draft, {"extra"})
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
        if draft.get("cliente_registrado_por_asistente"):
            return "Perfecto, ya tengo tus datos de contacto para continuar con la cotizacion."
        if draft.get("email") and draft.get("cliente_encontrado") is True:
            datos = []
            if draft.get("nombre"):
                datos.append("nombre")
            if draft.get("telefono"):
                datos.append("telefono")
            if draft.get("direccion"):
                datos.append("direccion")
            return "Con ese correo pude completar: %s. Revisa que este correcto antes de enviar." % (", ".join(datos) if datos else "datos disponibles")
        if draft.get("email") and draft.get("cliente_encontrado") is False:
            if not draft.get("nombre") or not draft.get("telefono"):
                return "Necesito tu nombre y telefono para continuar con la solicitud."
            if draft.get("entrega_tipo") == "despacho" and not draft.get("direccion"):
                return "Ya tengo nombre y telefono; para despacho necesito la direccion completa."
            return "Usare los datos que ingresaste para preparar esta cotizacion."
        return ""

    def opciones_tamano_para_draft(draft, catalogo):
        categoria = find_categoria(catalogo, draft.get("categoria_id") or draft.get("tamano_invalido_categoria_id") or "")
        sizes = rows_categoria(catalogo, "sizes", categoria) if categoria else list(catalogo.get("sizes") or [])
        return categoria, sizes

    def respuesta_tamano_invalido(draft, catalogo):
        personas = str(draft.get("tamano_invalido") or draft.get("personas") or "").strip()
        categoria, sizes = opciones_tamano_para_draft(draft, catalogo or {})
        lines = []
        if personas:
            lines.append("No tengo una torta de %s personas cargada en el catálogo." % personas)
        else:
            lines.append("Ese tamaño no está cargado en el catálogo.")
        if categoria:
            lines.append("Para %s, estas son las opciones disponibles:" % (categoria.get("nombre") or "ese tipo de torta"))
        else:
            lines.append("Estas son las opciones disponibles:")
        lines.append(list_lines(sizes[:10], lambda s: "%s - %s" % (s.get("nombre") or "Tamaño", fmt_clp(s.get("precio") or 0)), "sin tamaños cargados"))
        lines.extend([
            "",
            "Para continuar, elige uno de esos tamaños o indícanos otra opción del catálogo.",
        ])
        return "\n".join(lines)

    def sugerencias_tamano_invalido(draft, catalogo):
        _, sizes = opciones_tamano_para_draft(draft, catalogo or {})
        out = []
        for row in sizes[:6]:
            nombre = str(row.get("nombre") or "").strip()
            if nombre:
                out.append(nombre)
        return out or ["Ver catalogo y precios"]

    def diagnostico_catalogo_invalido(draft, catalogo, err=""):
        categoria = find_categoria(catalogo, draft.get("categoria_id") or "")
        sizes = rows_categoria(catalogo, "sizes", categoria) if categoria else list(catalogo.get("sizes") or [])
        sabores = rows_categoria(catalogo, "sabores", categoria) if categoria else list(catalogo.get("sabores") or [])
        parts = ["Ya tengo tamaño y relleno, pero esa combinación no coincide con una opción válida del catálogo."]
        if categoria:
            parts.append("Estoy revisando el tipo: %s." % (categoria.get("nombre") or "torta"))
        else:
            parts.append("Necesito confirmar primero el tipo de torta, por ejemplo: bizcocho, panqueque o mil hojas.")
        if err:
            parts.append("Detalle interno: %s" % str(err)[:180])
        if sizes:
            parts.append("Tamaños disponibles para ese tipo:")
            parts.append(list_lines(sizes[:8], lambda size: str(size.get("nombre") or "Tamaño"), "sin tamaños cargados"))
        if sabores:
            parts.append("Rellenos disponibles para ese tipo:")
            parts.append(list_lines(sabores[:10], lambda sabor: str(sabor.get("nombre") or "Relleno"), "sin rellenos cargados"))
        parts.append("Para corregirlo, escribe solo el dato que quieres cambiar, por ejemplo: tamaño o relleno.")
        return "\n".join([p for p in parts if p])

    def es_faltante_diagnostico(item):
        txt = str(item or "")
        return "\n" in txt or txt.startswith("Ya tengo tamaño") or txt.startswith("Necesito ajustar")

    def faltantes_simples(faltan):
        return [x for x in (faltan or []) if not es_faltante_diagnostico(x)]

    def faltantes_diagnosticos(faltan):
        return [x for x in (faltan or []) if es_faltante_diagnostico(x)]

    def texto_faltantes_destacado(faltan):
        simples = faltantes_simples(faltan)
        if not simples:
            return ""
        return "Para continuar falta:\n%s" % "\n".join("- " + str(x) for x in simples)

    def faltantes(draft, resumen, catalogo=None, err=""):
        out = []
        if draft.get("incompatibilidades"):
            out.append("opcion incompatible")
        if not draft.get("email"):
            out.append("correo")
        if not draft.get("size_id"):
            if draft.get("tamano_invalido"):
                out.append("tamano no disponible")
            elif draft.get("personas") and not draft.get("categoria_id"):
                out.append("tipo de torta para %s personas" % draft.get("personas"))
            else:
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
            out.append(diagnostico_catalogo_invalido(draft, catalogo or {}, err))
        return out

    def hora_elegida_disponible(draft, catalogo=None):
        if not draft.get("fecha") or not draft.get("hora_inicio"):
            return False
        return bool(validar_hora_cotizacion(draft, catalogo=catalogo).get("ok"))

    def respuesta_disponibilidad(draft, catalogo=None, limite=10):
        fecha = str(draft.get("fecha") or "").strip()
        hora = str(draft.get("hora_inicio") or "").strip()[:5]
        if fecha and hora:
            validacion = validar_hora_cotizacion(draft, catalogo=catalogo)
            if validacion.get("ok"):
                return "La hora %s para %s esta disponible y queda tomada como horario elegido. No necesito mostrarte horas tentativas; sigo con los datos que falten para completar la solicitud." % (hora, fmt_fecha(fecha))
            horas = validacion.get("horas") or []
            if horas:
                return "\n".join([
                    validacion.get("error") or "Esa hora no esta disponible.",
                    "",
                    "Para ese mismo dia puedo ofrecer:",
                    "\n".join("- " + h for h in horas[:limite]),
                    "",
                    "Elige una de esas horas y actualizo la solicitud.",
                ])
            return validacion.get("error") or respuesta_sin_horas(fecha, {}, draft=draft, catalogo=catalogo)
        if fecha:
            detalle = horas_disponibles_detalle(fecha, limite=limite, draft=draft, catalogo=catalogo)
            horas = detalle.get("horas") or []
            if horas:
                return "\n".join(["Horas disponibles para %s:" % fmt_fecha(fecha), ""] + ["- " + h for h in horas] + ["", "Dime cual prefieres."])
            return respuesta_sin_horas(fecha, detalle, draft=draft, catalogo=catalogo)
        return disponibilidad_texto(proximas_fechas(draft=draft, catalogo=catalogo))

    def horas_disponibles_detalle(fecha, limite=8, draft=None, catalogo=None):
        conn = None
        out = {
            "horas": [],
            "bloqueadas_anticipacion": 0,
            "bloqueadas_ocupacion": 0,
            "slots_libres_base": 0,
        }
        try:
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", str(fecha or "")):
                return out
            cfg = cfg_agenda()
            conn = get_db()
            cur = conn.cursor()
            disp = calcular_disponibilidad(cur, cfg, fecha, fecha)
            mapa = ((disp.get("mapa") or {}).get(fecha) or {})
            horas = []
            for hora, slot in sorted(mapa.items()):
                if bool((slot or {}).get("disponible")):
                    out["slots_libres_base"] += 1
                    if cumple_anticipacion_torta(fecha, str(hora), cfg, draft=draft, catalogo=catalogo):
                        horas.append(str(hora))
                    else:
                        out["bloqueadas_anticipacion"] += 1
                else:
                    out["bloqueadas_ocupacion"] += 1
            out["horas"] = horas[:int(limite or 8)]
            return out
        except Exception:
            return out
        finally:
            if conn:
                conn.close()

    def horas_disponibles(fecha, limite=8, draft=None, catalogo=None):
        return horas_disponibles_detalle(fecha, limite=limite, draft=draft, catalogo=catalogo).get("horas") or []

    def proximas_fechas(draft=None, catalogo=None):
        today = datetime.now(ZoneInfo("America/Santiago")).date()
        out = []
        cfg = cfg_agenda()
        days_ahead = max(10, int((cfg or {}).get("days_ahead") or 10))
        for offset in range(days_ahead):
            fecha = (today + timedelta(days=offset)).isoformat()
            horas = horas_disponibles(fecha, limite=4, draft=draft, catalogo=catalogo)
            if horas:
                out.append({"fecha": fecha, "horas": horas})
            if len(out) >= 3:
                break
        return out

    def respuesta_sin_horas(fecha, detalle=None, draft=None, catalogo=None):
        detalle = detalle or {}
        bloqueada_por_fecha = False
        try:
            bloqueada_por_fecha = not cumple_anticipacion_torta(fecha, "23:59", cfg_agenda(), draft=draft, catalogo=catalogo)
        except Exception:
            bloqueada_por_fecha = False
        if int(detalle.get("bloqueadas_anticipacion") or 0) > 0 or bloqueada_por_fecha:
            return "%s\n\nPara %s no puedo ofrecer horas porque no cumple el plazo minimo de anticipacion. Puedo revisar otra fecha mas adelante si me indicas una." % (
                mensaje_anticipacion_torta(draft, catalogo),
                fmt_fecha(fecha),
            )
        return "Para %s no veo cupos disponibles.\n\nPuedo revisar otra fecha si me indicas una." % fmt_fecha(fecha)

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

    def registrar_cotizacion_completa(draft, resumen, catalogo=None):
        disponibilidad = validar_hora_cotizacion(draft, catalogo=catalogo)
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
            if disponibilidad.get("anticipacion"):
                return {
                    "ok": False,
                    "reply": "\n".join([
                        disponibilidad.get("error") or mensaje_anticipacion_torta(draft, catalogo),
                        "",
                        "Para %s a las %s no puedo registrar la cotizacion porque no cumple el plazo minimo de agenda para tortas.",
                        "Indica una fecha mas adelante y reviso las horas disponibles.",
                    ]) % (fmt_fecha(draft.get("fecha")), str(draft.get("hora_inicio") or "-")[:5]),
                    "suggestions": ["Horas disponibles"],
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
                "reply": "No pude registrar la solicitud en este momento. Tus datos siguen guardados en esta conversacion; intenta nuevamente o habla con el equipo.",
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
        prioridad = [
            "tipo de torta",
            "tamano de torta",
            "relleno/sabor",
            "fecha",
            "hora",
            "retiro o despacho",
            "direccion de despacho",
            "correo",
            "nombre",
            "telefono",
        ]
        faltan_list = [str(x or "") for x in (faltan or [])]
        out = []
        for key in prioridad:
            for item in faltan_list:
                if key == "tipo de torta" and item.startswith("tipo de torta"):
                    label = "Elegir tipo de torta"
                elif item == key:
                    label = mapa.get(item)
                else:
                    label = None
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
        if "codigo completo del pedido" in norm(texto) or "seguimiento" in norm(texto):
            add("Consultar estado de pedido")
        return out[:8]

    def tiene_palabra(texto_norm, palabras):
        tokens = set(str(texto_norm or "").split())
        return any(p in tokens or p in texto_norm for p in palabras)

    def es_saludo_simple(texto_norm):
        if not texto_norm:
            return False
        saludos = {"hola", "buenas", "buenos dias", "buen dia", "buenas tardes", "buenas noches", "holi", "hello"}
        palabras_accion = ["agendar", "reservar", "cotizar", "precio", "catalogo", "catálogo", "torta", "pastel", "queque", "horas", "pedido", "seguimiento", "necesito", "quiero", "encargar", "comprar", "pax", "personas"]
        return tiene_palabra(texto_norm, saludos) and not any(x in texto_norm for x in palabras_accion)

    def es_agradecimiento(texto_norm):
        gracias = ["gracias", "muchas gracias", "te pasaste", "perfecto gracias", "ok gracias", "super gracias"]
        return any(x == texto_norm or x in texto_norm for x in gracias) and len(texto_norm.split()) <= 5

    def es_despedida(texto_norm):
        despedidas = ["adios", "chao", "chau", "hasta luego", "nos vemos", "bye", "me despido"]
        return any(x == texto_norm or x in texto_norm for x in despedidas) and len(texto_norm.split()) <= 5

    def detectar_campo_edicion(texto_norm):
        t = str(texto_norm or "").strip()
        if not t:
            return ""
        mapas = [
            ("fecha", ["fecha", "dia", "cambiar fecha", "modificar fecha", "otra fecha"]),
            ("hora", ["hora", "horario", "cambiar hora", "modificar hora", "otra hora"]),
            ("relleno", ["relleno", "rellenos", "sabor", "sabores", "manjar", "crema", "frambuesa", "lucuma", "mango"]),
            ("tamano", ["tamano", "tamaño", "personas", "pax", "cantidad", "porciones"]),
            ("tipo", ["tipo", "bizcocho", "panqueque", "mil hojas", "milhojas", "tradicional"]),
            ("topper", ["topper", "sin topper", "adorno", "decoracion", "placa"]),
            ("extras", ["extra", "extras", "nuez", "ganache", "chips", "fruta"]),
            ("nombre", ["nombre", "cliente", "persona", "contacto"]),
            ("telefono", ["telefono", "teléfono", "fono", "celular", "whatsapp", "numero", "número"]),
            ("correo", ["correo", "email", "mail"]),
            ("entrega", ["entrega", "retiro", "despacho", "delivery", "modalidad"]),
            ("direccion", ["direccion", "dirección", "domicilio", "ubicacion", "ubicación"]),
        ]
        for campo, palabras in mapas:
            if any(p in t for p in palabras):
                return campo
        return ""

    def prompt_edicion(campo):
        mensajes = {
            "fecha": "Claro. Indícame la nueva fecha, por ejemplo: 20 de septiembre.",
            "hora": "Claro. Indícame la nueva hora, por ejemplo: 18:00.",
            "relleno": "Claro. Indícame el nuevo relleno o sabor que quieres usar.",
            "tamano": "Claro. Indícame el nuevo tamaño, por ejemplo: 15 personas.",
            "tipo": "Claro. Indícame el tipo de torta: bizcocho, panqueque o mil hojas.",
            "topper": "Claro. Indícame si quieres topper o sin topper.",
            "extras": "Claro. Indícame qué extra quieres agregar o quitar.",
            "nombre": "Claro. Escríbeme el nombre correcto del cliente.",
            "telefono": "Claro. Escríbeme el teléfono correcto.",
            "correo": "Claro. Escríbeme el correo correcto.",
            "entrega": "Claro. Indícame si será retiro en tienda o despacho.",
            "direccion": "Claro. Escríbeme la dirección completa de despacho.",
        }
        return mensajes.get(campo, "Claro. Indícame qué dato quieres modificar.")

    def limpiar_dato_para_edicion(draft, campo):
        draft = dict(draft or {})
        if campo in {"fecha", "hora"}:
            if campo == "fecha":
                draft.pop("fecha", None)
            if campo == "hora":
                draft.pop("hora_inicio", None)
            draft.pop("cotizacion_evento_id", None)
            draft.pop("cotizacion_codigo", None)
            draft.pop("cotizacion_pdf_url", None)
        elif campo in {"relleno", "tipo", "tamano", "extras", "topper"}:
            if campo == "relleno":
                draft["sabor_ids"] = []
            if campo == "tipo":
                draft.pop("categoria_id", None)
                draft.pop("size_id", None)
                draft.pop("personas", None)
                draft["sabor_ids"] = []
            if campo == "tamano":
                draft.pop("size_id", None)
                draft.pop("personas", None)
                draft.pop("tamano_invalido", None)
                draft.pop("tamano_invalido_categoria_id", None)
            if campo == "extras":
                draft["extras"] = []
            if campo == "topper":
                draft.pop("topper_id", None)
            draft.pop("cotizacion_evento_id", None)
            draft.pop("cotizacion_codigo", None)
            draft.pop("cotizacion_pdf_url", None)
        elif campo in {"nombre", "telefono", "correo", "direccion", "entrega"}:
            if campo == "nombre":
                draft.pop("nombre", None)
            if campo == "telefono":
                draft.pop("telefono", None)
            if campo == "correo":
                draft.pop("email", None)
                draft.pop("cliente_encontrado", None)
            if campo == "direccion":
                draft.pop("direccion", None)
                draft["entrega_tipo"] = "despacho"
                draft["entrega_confirmada"] = True
            if campo == "entrega":
                draft.pop("entrega_confirmada", None)
            draft.pop("cotizacion_evento_id", None)
            draft.pop("cotizacion_codigo", None)
            draft.pop("cotizacion_pdf_url", None)
        return draft

    def cambios_relevantes_entrada(prev, nuevo):
        keys = [
            "fecha", "hora_inicio", "email", "telefono", "nombre", "direccion", "entrega_tipo",
            "entrega_confirmada", "categoria_id", "size_id", "personas", "topper_id", "tamano_invalido",
        ]
        for key in keys:
            if prev.get(key) != nuevo.get(key):
                return True
        for key in ["sabor_ids", "extras"]:
            if list(prev.get(key) or []) != list(nuevo.get(key) or []):
                return True
        return False

    def respuesta_no_entendida(draft=None):
        return "No entendí ese dato con seguridad. Para evitar dejar una cotización incorrecta, dime qué quieres hacer: cambiar fecha, cambiar relleno, ver catálogo, revisar horas disponibles o hablar con el equipo."

    def es_edicion_corta(texto_norm, campo):
        t = str(texto_norm or "").strip()
        if not t or not campo:
            return False
        if len(t.split()) <= 3 and not parse_fecha(t) and not parse_hora(t) and not re.search(r"\d{1,3}\s*(?:persona|personas|pax)", t):
            return True
        return any(t == x for x in [
            "cambiar " + campo, "modificar " + campo, "editar " + campo, "corregir " + campo,
        ])

    def inferir_intenciones(texto_norm, draft=None):
        draft = dict(draft or {})
        tiene_fecha = bool(parse_fecha(texto_norm))
        tiene_hora = bool(parse_hora(texto_norm))
        tiene_personas = bool(re.search(r"\b\d{1,3}\s*(?:persona|personas|pers|pax)\b", texto_norm))
        palabras_torta = [
            "torta", "tortas", "pastel", "pasteles", "queque", "bizcocho", "bizcochuelo", "panqueque",
            "mil hojas", "milhojas", "mil hoja", "tradicional", "relleno", "rellenos", "sabor", "sabores",
            "manjar", "crema", "ganache", "lucuma", "lúcuma", "frambuesa", "mango", "topper", "personas",
            "persona", "pax", "porciones", "porcion", "porción",
        ]
        palabras_agenda = [
            "agendar", "agenda", "reservar", "reserva", "apartar", "guardar hora", "tomar hora", "encargar",
            "encargo", "pedir", "pedido", "ordenar", "cotizar", "cotizacion", "cotización", "presupuesto",
            "quiero", "quisiera", "necesito", "busco", "me gustaria", "me gustaría", "comprar", "hacer una torta",
            "preparar una torta", "solicitar", "crear solicitud", "hacer solicitud",
        ]
        palabras_catalogo = ["catalogo", "catálogo", "carta", "menu", "menú", "precio", "precios", "vale", "valor", "cuanto", "cuánto", "opciones", "tipos", "sabores", "rellenos", "tamanos", "tamaños", "tamano", "tamaño"]
        palabras_horas = ["hora", "horas", "horario", "horarios", "disponible", "disponibles", "disponibilidad", "cupos", "cupo", "agenda", "cuando puedo", "cuándo puedo", "fecha disponible", "hay hora", "tienen hora"]
        texto_torta = any(x in texto_norm for x in palabras_torta)
        accion_directa = any(x in texto_norm for x in ["agendar", "reservar", "hacer pedido", "crear pedido", "encargar", "cotizar", "cotizacion"])
        agendar = accion_directa or (any(x in texto_norm for x in palabras_agenda) and (texto_torta or tiene_personas or tiene_fecha or tiene_hora))
        agendar = agendar or (texto_torta and (tiene_personas or tiene_fecha or tiene_hora or draft.get("size_id") or draft.get("personas")))
        catalogo = any(x in texto_norm for x in palabras_catalogo) or ("tradicional" in texto_norm and texto_torta and not tiene_fecha and not tiene_hora)
        disponibilidad = any(x in texto_norm for x in palabras_horas) and (tiene_fecha or tiene_hora or "disponible" in texto_norm or "cupos" in texto_norm or "horario" in texto_norm)
        confirmar = any(x in texto_norm for x in ["confirmar", "si confirmo", "sí confirmo", "esta bien", "está bien", "ok enviar", "enviar solicitud", "enviar", "registrar", "crear pedido", "hacer pedido", "reservar ahora", "listo enviar", "todo correcto"])
        consulta = any(x in texto_norm for x in ["ayuda", "menu", "menú", "que puedes hacer", "qué puedes hacer", "como funciona", "cómo funciona", "consulta", "consultas", "opciones de ayuda"])
        return {
            "agendar": bool(agendar),
            "catalogo": bool(catalogo),
            "disponibilidad": bool(disponibilidad),
            "confirmar": bool(confirmar),
            "consulta": bool(consulta),
            "tiene_torta": bool(texto_torta),
            "tiene_fecha": tiene_fecha,
            "tiene_hora": tiene_hora,
            "tiene_personas": tiene_personas,
        }

    def respuesta_faltantes_contextual(draft, faltan, catalogo=None):
        faltan = list(faltan or [])
        if not faltan:
            return ""
        catalogo = catalogo or {}
        if "opcion incompatible" in faltan:
            return respuesta_incompatibilidades(draft, catalogo)
        if "tamano no disponible" in faltan:
            return respuesta_tamano_invalido(draft, catalogo)
        if any(str(x).startswith("tipo de torta") for x in faltan):
            categorias = list_lines(catalogo.get("categorias") or [], lambda c: str(c.get("nombre") or ""), "sin tipos cargados")
            return "Tengo la cantidad de personas. Para avanzar necesito que elijas el tipo de torta:\n%s\n\nPuedes escribir, por ejemplo: bizcocho 15 personas con manjar." % categorias
        prioridad = [
            ("tamano de torta", "¿Para cuantas personas necesitas la torta?"),
            ("relleno/sabor", "¿Que relleno o sabor quieres?"),
            ("fecha", "¿Para que fecha la necesitas?"),
            ("hora", "¿A que hora te acomoda el retiro o despacho?"),
            ("correo", "Falta tu correo para continuar."),
            ("nombre", "Falta tu nombre para continuar."),
            ("telefono", "Falta tu telefono para continuar."),
            ("retiro o despacho", "¿La quieres para retiro en tienda o despacho?"),
            ("direccion de despacho", "Para despacho necesito la direccion completa."),
        ]
        for key, msg in prioridad:
            if key in faltan:
                return msg
        diagnosticos = faltantes_diagnosticos(faltan)
        if diagnosticos:
            return "\n\n".join(str(x) for x in diagnosticos)
        return texto_faltantes_destacado(faltan)

    def chat_logic(message, draft):
        catalogo = cargar_catalogo()
        msg = str(message or "").strip()
        nmsg = norm(msg)
        draft_inicial = dict(draft or {})

        if draft_inicial.get("cotizacion_finalizada"):
            return {
                "reply": "La cotización ya fue enviada y el PDF quedó disponible en esta conversación. Si necesitas otro pedido o una nueva cotización, inicia una nueva conversación.",
                "draft": draft_inicial,
                "type": "conversation_closed",
                "closed": True,
            }

        resumen_pre, err_pre = cotizar(draft_inicial, catalogo)
        faltan_pre = faltantes(draft_inicial, resumen_pre, catalogo, err_pre)
        campo_edicion = detectar_campo_edicion(nmsg)
        if resumen_pre and not faltan_pre and campo_edicion and es_edicion_corta(nmsg, campo_edicion):
            draft_edit = limpiar_dato_para_edicion(draft_inicial, campo_edicion)
            draft_edit["editando_campo"] = campo_edicion
            return {
                "reply": prompt_edicion(campo_edicion),
                "draft": draft_edit,
                "type": "edit_prompt",
                "suggestions": sugerencias_faltantes(faltantes(draft_edit, None, catalogo, "")) or [],
            }

        campo_pendiente = str(draft_inicial.get("editando_campo") or "").strip()
        base_para_actualizar = limpiar_dato_para_edicion(draft_inicial, campo_pendiente) if campo_pendiente else draft_inicial
        base_para_actualizar.pop("editando_campo", None)
        draft = actualizar_draft(base_para_actualizar, msg, catalogo)
        draft = reconciliar_draft_catalogo(draft, catalogo)
        cambio_detectado = cambios_relevantes_entrada(base_para_actualizar, draft)
        if campo_pendiente and not cambio_detectado:
            draft["editando_campo"] = campo_pendiente
            return {
                "reply": prompt_edicion(campo_pendiente),
                "draft": draft,
                "type": "edit_prompt",
                "suggestions": sugerencias_faltantes(faltantes(draft, None, catalogo, "")) or [],
            }
        draft = registrar_cliente_desde_draft(draft)
        resumen, err = cotizar(draft, catalogo)
        faltan = faltantes(draft, resumen, catalogo, err)

        if "opcion incompatible" in faltan:
            return {
                "reply": respuesta_incompatibilidades(draft, catalogo),
                "draft": draft,
                "type": "invalid_catalog_option",
                "suggestions": sugerencias_incompatibilidades(draft, catalogo) or sugerencias_catalogo(catalogo),
            }

        if "tamano no disponible" in faltan:
            return {
                "reply": respuesta_tamano_invalido(draft, catalogo),
                "draft": draft,
                "type": "invalid_catalog_option",
                "suggestions": sugerencias_tamano_invalido(draft, catalogo),
            }

        if not msg:
            return {"reply": "Escribeme que torta necesitas y para que fecha. Te ayudo a cotizar y revisar horas disponibles.", "draft": draft}
        codigo_seguimiento = detectar_codigo_pedido(msg)
        seguimiento_intent = any(x in nmsg for x in ["estado pedido", "estado de pedido", "seguimiento", "seguir pedido", "como va", "donde va", "mi pedido"])
        if codigo_seguimiento:
            return {
                "reply": consultar_estado_pedido(codigo_seguimiento),
                "draft": draft,
                "type": "tracking",
                "tracking_url": "%s/seguimiento/%s" % (public_base_url, quote(codigo_seguimiento)),
            }
        if seguimiento_intent:
            if draft.get("email"):
                draft.pop("tracking_pending", None)
                return {
                    "reply": respuesta_seguimiento_por_email(draft.get("email")),
                    "draft": draft,
                    "type": "tracking_email",
                    "suggestions": ["Enviar codigo de pedido", "Agendar torta"],
                }
            draft["tracking_pending"] = True
            return {
                "reply": "\n".join([
                    "Puedo revisar el estado de tu pedido.",
                    "",
                    "Enviame el correo usado al agendar para buscar tus pedidos activos.",
                    "Si no aparece con el correo, tambien puedes enviarme el codigo completo del pedido, por ejemplo:",
                    "- AGD-20260916-000103",
                    "",
                    "Tambien puedes revisar directamente aqui:",
                    "%s/seguimiento" % public_base_url,
                ]),
                "draft": draft,
                "type": "tracking_help",
                "suggestions": ["Consultar por correo", "Enviar codigo de pedido"],
            }
        if draft.get("tracking_pending") and draft.get("email"):
            draft.pop("tracking_pending", None)
            return {
                "reply": respuesta_seguimiento_por_email(draft.get("email")),
                "draft": draft,
                "type": "tracking_email",
                "suggestions": ["Enviar codigo de pedido", "Agendar torta"],
            }
        if es_saludo_simple(nmsg):
            return {
                "reply": "Hola, soy el asistente de Sucree. Puedo ayudarte a agendar una torta, revisar catalogo y precios, o consultar horas disponibles.\n\n¿Que necesitas preparar?",
                "draft": draft,
                "suggestions": ["Agendar torta", "Ver catalogo y precios", "Horas disponibles"],
            }
        if es_agradecimiento(nmsg):
            return {
                "reply": "Con gusto. Si quieres, puedo seguir ayudandote a completar la cotizacion o revisar horas disponibles.",
                "draft": draft,
                "suggestions": ["Agendar torta", "Horas disponibles"],
            }
        if es_despedida(nmsg):
            return {"reply": "Gracias por escribir a Sucree. Cuando necesites agendar o revisar tu pedido, estare aqui para ayudarte.", "draft": draft}

        kb = buscar_kb(msg)
        if kb:
            return {"reply": kb, "draft": draft, "type": "knowledge"}

        intent = inferir_intenciones(nmsg, draft)
        agendar_intent = intent.get("agendar")
        consulta_intent = intent.get("consulta")
        catalogo_intent = intent.get("catalogo")
        disponibilidad_intent = intent.get("disponibilidad")

        if consulta_intent:
            return {
                "reply": "\n".join([
                    "Puedo ayudarte con estas opciones:",
                    "",
                    "- Agendar torta",
                    "- Ver catalogo y precios",
                    "- Revisar horas disponibles",
                    "- Consultar estado de pedido con codigo",
                    "- Ordenar una cotizacion aunque escribas los datos desordenados",
                    "",
                    "Puedes escribir algo como: quiero torta bizcocho 15 personas con manjar para el 30 de septiembre a las 18:00.",
                ]),
                "draft": draft,
                "suggestions": ["Agendar torta", "Ver catalogo y precios", "Horas disponibles"],
            }

        if disponibilidad_intent and not agendar_intent:
            return {"reply": respuesta_disponibilidad(draft, catalogo=catalogo, limite=10), "draft": draft}

        if catalogo_intent and not (agendar_intent and (draft.get("size_id") or draft.get("personas") or draft.get("fecha") or draft.get("hora_inicio"))):
            categoria_msg = detectar_categoria_catalogo(msg, catalogo)
            if categoria_msg:
                draft["categoria_id"] = str(categoria_msg.get("id") or "")
            reply = catalogo_texto(catalogo, categoria_id=draft.get("categoria_id") or "")
            if "tradicional" in nmsg and not categoria_msg:
                reply = "Para torta tradicional primero elige el tipo base que prefieres.\n\n" + reply
            return {"reply": reply, "draft": draft, "suggestions": sugerencias_catalogo(catalogo)}

        if agendar_intent and faltan:
            detalle = respuesta_faltantes_contextual(draft, faltan, catalogo)
            reply = "Perfecto, voy armando tu solicitud."
            if hora_elegida_disponible(draft, catalogo):
                reply += "\n\n" + respuesta_disponibilidad(draft, catalogo=catalogo, limite=6)
            if detalle:
                reply += "\n\n" + detalle
            return {"reply": reply, "draft": draft, "suggestions": sugerencias_faltantes(faltan) or sugerencias_catalogo(catalogo)}

        confirmar = (nmsg.strip() in {"si", "sí", "ok", "okay", "dale", "correcto"}) or any(x in nmsg for x in ["confirmar", "confirmo", "confirmar reserva", "si confirmo", "sí confirmo", "esta bien", "está bien", "todo bien", "enviar solicitud", "enviar cotizacion", "registrar solicitud", "reservar ahora"])
        if confirmar:
            resumen_confirm, err_confirm = cotizar(draft, catalogo)
            faltan_confirm = faltantes(draft, resumen_confirm, catalogo, err_confirm)
            if not resumen_confirm or faltan_confirm:
                detalle = respuesta_faltantes_contextual(draft, faltan_confirm, catalogo)
                reply = "Antes de enviar la solicitud necesito completar y revisar estos datos."
                if detalle:
                    reply += "\n\n" + detalle
                return {
                    "reply": reply,
                    "draft": draft,
                    "suggestions": sugerencias_faltantes(faltan_confirm) or sugerencias_catalogo(catalogo),
                }
            cierre = registrar_cotizacion_completa(draft, resumen_confirm, catalogo=catalogo)
            if cierre.get("ok"):
                reply = resumen_texto(draft, resumen_confirm) + "\n\n" + cierre.get("reply", "")
            else:
                reply = cierre.get("reply", "No pude registrar la solicitud en este momento.")
            if cierre.get("ok"):
                draft["cotizacion_finalizada"] = True
            return {
                "reply": reply,
                "draft": draft,
                "quote": resumen_confirm if cierre.get("ok") else None,
                "pdf_url": cierre.get("pdf_url") or draft.get("cotizacion_pdf_url") or "",
                "agenda_id": cierre.get("agenda_id"),
                "codigo_pedido": cierre.get("codigo_pedido") or draft.get("cotizacion_codigo") or "",
                "suggestions": cierre.get("suggestions") or [],
                "whatsapp_url": cierre.get("whatsapp_url") or "",
                "error": cierre.get("error"),
                "closed": bool(cierre.get("ok")),
            }
        if not cambio_detectado and not any([agendar_intent, consulta_intent, catalogo_intent, disponibilidad_intent]):
            registrar_desconocida(msg, {"draft": draft, "motivo": "sin_cambios_relevantes"})
            return {
                "reply": respuesta_no_entendida(draft),
                "draft": draft,
                "unknown": True,
                "suggestions": ["Cambiar fecha", "Cambiar relleno", "Ver catalogo y precios", "Horas disponibles", "Hablar con el equipo"],
            }

        if resumen:
            if faltan:
                detalle = respuesta_faltantes_contextual(draft, faltan, catalogo)
                reply = "Ya tengo parte de la cotizacion."
                cliente_msg = cliente_estado_texto(draft)
                if cliente_msg:
                    reply += "\n\n" + cliente_msg
                if draft.get("fecha") and not draft.get("hora_inicio"):
                    reply += "\n\n" + respuesta_disponibilidad(draft, catalogo=catalogo, limite=6)
                elif hora_elegida_disponible(draft, catalogo):
                    reply += "\n\nHora confirmada: %s para %s." % (str(draft.get("hora_inicio") or "")[:5], fmt_fecha(draft.get("fecha")))
                if detalle:
                    reply += "\n\n" + detalle
                return {"reply": reply, "draft": draft, "suggestions": sugerencias_faltantes(faltan) or sugerencias_catalogo(catalogo)}

            disponibilidad = validar_hora_cotizacion(draft, catalogo=catalogo)
            if not disponibilidad.get("ok"):
                horas = disponibilidad.get("horas") or []
                if horas:
                    reply = "Esa hora no esta disponible. Estas son horas disponibles para ese mismo dia:\n%s\n\nDime cual prefieres para actualizar la cotizacion." % "\n".join("- " + h for h in horas)
                    return {"reply": reply, "draft": draft, "suggestions": horas[:6]}
                if disponibilidad.get("anticipacion"):
                    return {
                        "reply": "%s\n\nIndica una fecha mas adelante y reviso las horas disponibles." % (disponibilidad.get("error") or mensaje_anticipacion_torta(draft, catalogo)),
                        "draft": draft,
                        "suggestions": ["Horas disponibles"],
                    }
                link = whatsapp_url("Hola Sucree, necesito ayuda porque no encontre horas disponibles para agendar mi torta.")
                return {
                    "reply": "Para %s no veo horas disponibles en la agenda. Por favor comunicate con la pasteleria por WhatsApp para revisar una alternativa:\n%s" % (fmt_fecha(draft.get("fecha")), link),
                    "draft": draft,
                    "whatsapp_url": link,
                }

            reply = resumen_texto(draft, resumen)
            cliente_msg = cliente_estado_texto(draft)
            if cliente_msg:
                reply += "\n\n" + cliente_msg
            reply += "\n\nYa tengo toda la información mínima. Revisa el resumen. Si quieres modificar algo, puedes escribir solo: nombre, fecha, hora, relleno, tamaño, topper, entrega, dirección, teléfono o correo. Si está correcto, presiona Enviar solicitud."
            return {"reply": reply, "draft": draft, "quote": resumen, "suggestions": ["Enviar solicitud", "Fecha", "Relleno", "Nombre", "Hora", "Tamaño"]}
        meaningful = any(draft.get(k) for k in ["fecha", "hora_inicio", "email", "telefono", "nombre", "size_id", "sabor_ids", "personas", "categoria_id"])
        if meaningful:
            reply = "Voy ordenando la informacion."
            if err:
                reply += " Necesito que revisemos una opcion del catalogo para continuar."
            if faltan:
                destacado = texto_faltantes_destacado(faltan)
                diagnosticos = faltantes_diagnosticos(faltan)
                if destacado:
                    reply += "\n\n" + destacado
                if diagnosticos:
                    reply += "\n\n" + "\n\n".join(str(x) for x in diagnosticos)
            if draft.get("fecha") and not draft.get("hora_inicio"):
                reply += "\n\n" + respuesta_disponibilidad(draft, catalogo=catalogo, limite=5)
            elif hora_elegida_disponible(draft, catalogo):
                reply += "\n\nHora confirmada: %s para %s." % (str(draft.get("hora_inicio") or "")[:5], fmt_fecha(draft.get("fecha")))
            return {"reply": reply, "draft": draft}
        registrar_desconocida(msg, {"draft": draft})
        return {
            "reply": respuesta_no_entendida(draft),
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
        data = request.get_json(silent=True) or {}
        msg = str(data.get("message") or "").strip()[:1200]
        draft = data.get("draft") if isinstance(data.get("draft"), dict) else {}
        conversation_id = str(data.get("conversation_id") or draft.get("conversation_id") or "").strip()[:80]
        if not conversation_id:
            conversation_id = crear_conversation_id()
        draft_recuperado = recuperar_draft_conversacion(conversation_id)
        draft = merge_drafts_conservador(draft_recuperado, draft)
        draft["conversation_id"] = conversation_id
        try:
            draft_entrada = dict(draft or {})
            out = chat_logic(msg, draft)
            out_draft = out.get("draft") if isinstance(out.get("draft"), dict) else {}
            out_draft["conversation_id"] = conversation_id
            out["draft"] = out_draft
            if "suggestions" not in out or out.get("suggestions") is None:
                out["suggestions"] = sugerencias_desde_respuesta(out.get("reply"))
            registrar_interaccion_asistente(
                conversation_id,
                msg,
                out,
                draft_entrada=draft_entrada,
                user_agent=request.headers.get("User-Agent", ""),
            )
            payload = {"success": True, "conversation_id": conversation_id}
            payload.update(out)
            return jsonify(payload)
        except Exception as exc:
            registrar_desconocida(msg, {"error": str(exc), "conversation_id": conversation_id, "motivo": "exception"})
            return jsonify({"success": False, "conversation_id": conversation_id, "error": "El asistente tuvo un problema temporal. Intentalo nuevamente."}), 500

    @app.route("/api/asistente/admin/pendientes", methods=["GET", "POST"])
    def api_asistente_admin_pendientes():
        conn = None

        def parse_json(raw, fallback):
            try:
                return json.loads(raw) if raw else fallback
            except Exception:
                return fallback

        try:
            conn = get_db()
            cur = conn.cursor()
            ensure_tables(cur)
            if request.method == "POST":
                data = request.get_json(silent=True) or {}
                action = str(data.get("action") or "save").strip().lower()
                unknown_id = int(data.get("id") or 0)
                if action in {"ignore", "ignorar"}:
                    if unknown_id <= 0:
                        return jsonify({"success": False, "error": "Falta el ID de la pregunta"}), 400
                    cur.execute(
                        "UPDATE asistente_unknown SET estado = 'ignorado', actualizado_en = CURRENT_TIMESTAMP WHERE id = ?",
                        (unknown_id,),
                    )
                    conn.commit()
                    return jsonify({"success": True})
                if action in {"reopen", "reabrir"}:
                    if unknown_id <= 0:
                        return jsonify({"success": False, "error": "Falta el ID de la pregunta"}), 400
                    cur.execute(
                        "UPDATE asistente_unknown SET estado = 'pendiente', actualizado_en = CURRENT_TIMESTAMP WHERE id = ?",
                        (unknown_id,),
                    )
                    conn.commit()
                    return jsonify({"success": True})

                pregunta = str(data.get("pregunta") or "").strip()[:700]
                respuesta = str(data.get("respuesta") or "").strip()[:2500]
                keywords = str(data.get("keywords") or "").strip()[:1200]
                categoria = str(data.get("categoria") or "general").strip()[:80] or "general"
                if not pregunta or not respuesta:
                    return jsonify({"success": False, "error": "Pregunta y respuesta son obligatorias"}), 400
                ejemplos = [pregunta]
                for item in re.split(r"[,;\n]+", keywords):
                    item = str(item or "").strip()
                    if item and item not in ejemplos:
                        ejemplos.append(item)
                cur.execute(
                    """
                    INSERT INTO asistente_kb (pregunta, respuesta, keywords, categoria, ejemplos_json, activo)
                    VALUES (?, ?, ?, ?, ?, 1)
                    """,
                    (pregunta, respuesta, keywords, categoria, json.dumps(ejemplos[:20], ensure_ascii=False)),
                )
                if unknown_id > 0:
                    cur.execute(
                        """
                        UPDATE asistente_unknown
                        SET estado = 'resuelto', respuesta_sugerida = ?, actualizado_en = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (respuesta, unknown_id),
                    )
                conn.commit()
                return jsonify({"success": True})

            cur.execute(
                """
                SELECT id, pregunta, contexto_json, estado, respuesta_sugerida, creado_en, actualizado_en,
                       pregunta_norm, conversation_id, respuesta_actual, tipo_evento, intent_json,
                       confianza, veces, prioridad
                FROM asistente_unknown
                ORDER BY CASE estado WHEN 'pendiente' THEN 0 WHEN 'revisar' THEN 1 WHEN 'resuelto' THEN 2 ELSE 3 END,
                         COALESCE(veces, 1) DESC, id DESC
                LIMIT 160
                """
            )
            pendientes = []
            for r in cur.fetchall():
                row = dict(r)
                row["contexto"] = parse_json(row.get("contexto_json"), {})
                row["intent"] = parse_json(row.get("intent_json"), {})
                pendientes.append(row)

            cur.execute(
                """
                SELECT id, pregunta, respuesta, keywords, categoria, activo, uso_count, ultimo_uso, creado_en, actualizado_en
                FROM asistente_kb
                ORDER BY COALESCE(activo, 1) DESC, actualizado_en DESC, id DESC
                LIMIT 120
                """
            )
            kb_rows = [dict(r) for r in cur.fetchall()]

            cur.execute(
                """
                SELECT conversation_id, canal, estado, mensajes_total, desconocidas_total, ultimo_tipo, creado_en, actualizado_en
                FROM asistente_conversaciones
                ORDER BY actualizado_en DESC
                LIMIT 80
                """
            )
            conversaciones = [dict(r) for r in cur.fetchall()]

            cur.execute("SELECT COUNT(*) AS c FROM asistente_unknown WHERE estado IN ('pendiente','revisar')")
            pendientes_count = int((cur.fetchone() or {"c": 0})["c"] or 0)
            cur.execute("SELECT COUNT(*) AS c FROM asistente_conversaciones")
            conversaciones_count = int((cur.fetchone() or {"c": 0})["c"] or 0)
            cur.execute("SELECT COUNT(*) AS c FROM asistente_mensajes")
            mensajes_count = int((cur.fetchone() or {"c": 0})["c"] or 0)
            cur.execute("SELECT COUNT(*) AS c FROM asistente_kb WHERE COALESCE(activo, 1) = 1")
            kb_count = int((cur.fetchone() or {"c": 0})["c"] or 0)

            return jsonify({
                "success": True,
                "pendientes": pendientes,
                "kb": kb_rows,
                "conversaciones": conversaciones,
                "stats": {
                    "pendientes": pendientes_count,
                    "conversaciones": conversaciones_count,
                    "mensajes": mensajes_count,
                    "respuestas": kb_count,
                },
            })
        except Exception as exc:
            if conn:
                conn.rollback()
            return jsonify({"success": False, "error": str(exc)}), 500
        finally:
            if conn:
                conn.close()

    @app.route("/api/asistente/admin/conversaciones/<conversation_id>", methods=["GET"])
    def api_asistente_admin_conversacion(conversation_id):
        conn = None
        try:
            conn = get_db()
            cur = conn.cursor()
            ensure_tables(cur)
            cid = str(conversation_id or "").strip()[:80]
            cur.execute(
                """
                SELECT id, pregunta, respuesta, tipo_respuesta, entendido, confianza, intent_json, draft_json, creado_en
                FROM asistente_mensajes
                WHERE conversation_id = ?
                ORDER BY id ASC
                LIMIT 300
                """,
                (cid,),
            )
            rows = []
            for r in cur.fetchall():
                row = dict(r)
                try:
                    row["intent"] = json.loads(row.get("intent_json") or "{}")
                except Exception:
                    row["intent"] = {}
                try:
                    row["draft"] = json.loads(row.get("draft_json") or "{}")
                except Exception:
                    row["draft"] = {}
                rows.append(row)
            return jsonify({"success": True, "conversation_id": cid, "mensajes": rows})
        except Exception as exc:
            return jsonify({"success": False, "error": str(exc)}), 500
        finally:
            if conn:
                conn.close()
