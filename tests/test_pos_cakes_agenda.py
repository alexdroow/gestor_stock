import unittest
import os
import sqlite3
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import app as app_module
import database


class PosCakesAgendaTests(unittest.TestCase):
    def setUp(self):
        self.client = app_module.app.test_client()
        with self.client.session_transaction() as session:
            session[app_module._ADMIN_SESSION_KEY] = True

    def test_mobile_pos_adds_tortas_tab_and_embeds_mobile_agenda_lazily(self):
        response = self.client.get('/admin/pos/mobile')

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['X-POS-Mobile-Version'], 'mobile-pos-20261019-11')
        self.assertIn(b'data-tab="tortas"', response.data)
        self.assertIn(b'data-src="/agenda?pos_embed=1"', response.data)
        self.assertIn(b'data-tab="redes"', response.data)
        self.assertIn(b'data-src="/admin/redes?pos_embed=1"', response.data)
        self.assertIn(b"frame.dataset.loaded!=='1'", response.data)

    def test_updated_pos_apk_is_available_as_version_1412(self):
        page = self.client.get('/descargar-pos-android')
        self.assertEqual(page.status_code, 200)
        self.assertIn('1.4.12', page.get_data(as_text=True))
        apk = self.client.get('/descargas/SucreePOS-Android-prueba.apk')
        self.assertEqual(apk.status_code, 200)
        self.assertEqual(apk.headers.get('X-POS-APK-Version'), '1.4.12')
        self.assertTrue(apk.data.startswith(b'PK'))
        apk.close()
        apk.close()

    def test_redes_admin_can_render_embedded_inside_mobile_pos(self):
        response = self.client.get('/admin/redes?pos_embed=1')

        self.assertEqual(response.status_code, 200)
        self.assertIn(b'class="embedded"', response.data)
        self.assertIn(b'target="_top"', response.data)
        self.assertIn(b'csrf-token', response.data)

    def test_embedded_agenda_uses_sucreestock_mobile_view_and_returns_to_mobile_pos(self):
        response = self.client.get('/agenda?pos_embed=1')

        self.assertEqual(response.status_code, 200)
        self.assertIn(b'@media (max-width: 760px)', response.data)
        self.assertIn(b'class="agenda-mobile-bottom-nav"', response.data)
        self.assertIn(b'href="/admin/pos/mobile?tab=home" target="_top"', response.data)
        self.assertIn('Volver al POS'.encode(), response.data)

    def test_registered_agenda_blocks_are_visible_on_month_week_and_mobile_calendars(self):
        template = (Path(__file__).resolve().parents[1] / 'templates' / 'agenda.html').read_text(encoding='utf-8')
        self.assertIn('function bloqueosEnRangoAgenda(', template)
        self.assertIn('...bloqueosEnRangoAgenda({ inicio: primerDia, fin: ultimoDia })', template)
        self.assertIn("${bloqueos.length ? 'blocked' : ''}", template)
        self.assertIn('class="mobile-week-blocked"', template)
        self.assertIn('def guardar_evento_agenda(', (Path(__file__).resolve().parents[1] / 'database.py').read_text(encoding='utf-8'))

    def test_unconfirmed_orders_are_explicitly_labeled_in_calendar_views(self):
        template = (Path(__file__).resolve().parents[1] / 'templates' / 'agenda.html').read_text(encoding='utf-8')
        self.assertIn("esPedidoPendientePorConfirmar(ev) ? '\\u23F3 POR CONFIRMAR · '", template)
        self.assertIn('content: "Sin confirmar"', template)
        self.assertIn('labelEstadoOperativoAgenda(status)', template)

    def test_agenda_reuses_its_existing_58mm_ticket_for_native_pos_printing(self):
        template = (Path(__file__).resolve().parents[1] / 'templates' / 'agenda.html').read_text(encoding='utf-8')

        self.assertIn('@page { size: 58mm auto; margin: 3mm; }', template)
        self.assertIn('<main class="ticket">', template)
        self.assertIn('const ticketHtml = construirHtmlImpresion58(evento);', template)
        self.assertIn('window.SucreePosAndroid.printAgendaTicket(String(idNum), ticketHtml);', template)
        self.assertIn('win.document.write(ticketHtml);', template)

    def test_mobile_pos_download_endpoint_returns_pdf_attachment(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, 'agenda.sqlite')
            pdf_root = os.path.join(temp_dir, 'static')
            os.makedirs(os.path.join(pdf_root, 'tienda_pedidos_pdf'))
            conn = sqlite3.connect(db_path)
            conn.execute('''CREATE TABLE agenda_eventos (
                id INTEGER PRIMARY KEY, tipo TEXT, fecha TEXT, hora_inicio TEXT, hora_fin TEXT,
                hora_entrega TEXT, cliente TEXT, telefono TEXT, direccion TEXT, ingredientes TEXT,
                total REAL, abono REAL, codigo_operacion TEXT, codigo_pedido TEXT
            )''')
            conn.execute("INSERT INTO agenda_eventos VALUES (1,'torta','2026-11-10','12:00','13:00','12:00','Cliente','+56912345678','','Detalle',30000,0,'OPA-1','AGD-1')")
            conn.commit()
            conn.close()

            def create_test_pdf(event):
                target = os.path.join(pdf_root, 'tienda_pedidos_pdf', 'cotizacion-test.pdf')
                with open(target, 'wb') as pdf:
                    pdf.write(b'%PDF-1.4 test')
                return 'cotizacion-test.pdf'

            with patch.object(database, 'DB_PATH', db_path), \
                 patch.object(app_module, 'static_dir', pdf_root), \
                 patch.object(app_module, '_crear_pdf_reserva_agenda_tienda', side_effect=create_test_pdf):
                response = self.client.get('/api/agenda/evento/1/pdf?download=1')

            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.mimetype, 'application/pdf')
            self.assertIn('attachment;', response.headers.get('Content-Disposition', ''))
            self.assertTrue(response.data.startswith(b'%PDF-'))
            response.close()

    def test_agenda_pdf_download_uses_android_bridge_instead_of_popup(self):
        template = (Path(__file__).resolve().parents[1] / 'templates' / 'agenda.html').read_text(encoding='utf-8')
        self.assertIn("nativeBridge.downloadAgendaPdf(String(idNum))", template)
        self.assertIn("/api/agenda/evento/${idNum}/pdf?download=1", template)
        self.assertIn('descargarPdfAgenda(eventoEditando.id)', template)
        self.assertIn('descargarPdfAgenda(pedidoBusquedaActual.id)', template)
        native = (Path(__file__).resolve().parents[1] / 'android_pos' / 'app' / 'src' / 'main' / 'java' / 'cl' / 'sucree' / 'pos' / 'MainActivity.kt').read_text(encoding='utf-8')
        self.assertIn('fun downloadAgendaPdf(orderId: String)', native)
        self.assertIn('DownloadManager.Request.VISIBILITY_VISIBLE_NOTIFY_COMPLETED', native)

    def test_cake_catalog_admin_has_category_and_per_category_size_order_controls(self):
        response = self.client.get('/ventas/admin-catalogo-torta')

        self.assertEqual(response.status_code, 200)
        self.assertIn('data-move-record', response.get_data(as_text=True))
        self.assertIn('de las categorías', response.get_data(as_text=True))
        self.assertIn('de los tamaños dentro de su categoría', response.get_data(as_text=True))

    def test_public_cake_catalog_keeps_saved_category_and_size_order(self):
        catalog = app_module._catalogo_torta_publico({
            'categorias': [
                {'id': 'bizcocho', 'nombre': 'Bizcocho', 'activo': True},
                {'id': 'panqueque', 'nombre': 'Panqueque', 'activo': True},
            ],
            'sizes': [
                {'id': 'b-30', 'categoria_id': 'bizcocho', 'nombre': '30 personas', 'activo': True},
                {'id': 'p-15', 'categoria_id': 'panqueque', 'nombre': '15 personas', 'activo': True},
                {'id': 'b-15', 'categoria_id': 'bizcocho', 'nombre': '15 personas', 'activo': True},
            ],
        })

        self.assertEqual([row['id'] for row in catalog['categorias']], ['bizcocho', 'panqueque'])
        self.assertEqual([row['id'] for row in catalog['sizes']], ['b-30', 'p-15', 'b-15'])

    def test_agenda_torta_whatsapp_lets_admin_choose_and_save_transfer_accounts(self):
        template = (Path(__file__).resolve().parents[1] / 'templates' / 'agenda.html').read_text(encoding='utf-8')
        self.assertIn('id="modal-transferencia-agenda"', template)
        self.assertIn('function enviarResumenTortaWhatsApp()', template)
        self.assertIn("if (tipo === 'torta')", template)

        with tempfile.TemporaryDirectory() as temp_dir, patch.object(database, 'DB_PATH', os.path.join(temp_dir, 'agenda.sqlite')):
            initial = self.client.get('/api/agenda/whatsapp-mensajes').get_json()
            self.assertTrue(initial['templates']['cuentas_transferencia'])
            accounts = [
                {'nombre': 'Cuenta empresa', 'datos': 'Titular: Sucree SpA\nBanco: BCI\nCuenta corriente: 123456'}
            ]
            saved = self.client.post('/api/agenda/whatsapp-mensajes', json={
                'confirmacion': initial['templates']['confirmacion'],
                'pdf': initial['templates']['pdf'],
                'cuentas_transferencia': accounts,
            })
            self.assertEqual(saved.status_code, 200)
            self.assertEqual(saved.get_json()['cuentas_transferencia'], accounts)
            reloaded = self.client.get('/api/agenda/whatsapp-mensajes').get_json()
            self.assertEqual(reloaded['templates']['cuentas_transferencia'], accounts)

    def test_transfer_accounts_are_not_publicly_disclosed_or_editable(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.object(database, 'DB_PATH', os.path.join(temp_dir, 'agenda.sqlite')):
            with self.client.session_transaction() as session:
                session.pop(app_module._ADMIN_SESSION_KEY, None)
            response = self.client.get('/api/agenda/whatsapp-mensajes')
            self.assertEqual(response.status_code, 401)
            denied = self.client.post('/api/agenda/whatsapp-mensajes', json={
                'cuentas_transferencia': [{'nombre': 'No autorizado', 'datos': 'Datos'}]
            })
            self.assertEqual(denied.status_code, 401)

    def test_existing_agenda_event_date_cannot_change_without_explicit_confirmation(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.object(database, 'DB_PATH', os.path.join(temp_dir, 'agenda.sqlite')):
            conn = sqlite3.connect(database.DB_PATH)
            conn.executescript('''
                CREATE TABLE agenda_eventos (
                    id INTEGER PRIMARY KEY, tipo TEXT, titulo TEXT, fecha TEXT, hora_inicio TEXT,
                    hora_fin TEXT, hora_entrega TEXT, cliente TEXT, telefono TEXT, es_envio INTEGER,
                    direccion TEXT, ingredientes TEXT, total REAL, abono REAL, motivo TEXT,
                    alerta_minutos INTEGER, estado TEXT, codigo_operacion TEXT, codigo_pedido TEXT
                );
                CREATE TABLE agenda_recordatorios_descartados (evento_id INTEGER);
            ''')
            for event_id, state, original_date in [(1, 'pendiente', '2026-10-30'), (2, 'completado', '2026-10-18')]:
                conn.execute(
                    '''INSERT INTO agenda_eventos (id, tipo, titulo, fecha, hora_inicio, hora_fin, hora_entrega,
                       cliente, telefono, es_envio, total, abono, alerta_minutos, estado, codigo_operacion, codigo_pedido)
                       VALUES (?, 'torta', ?, ?, '12:00', '13:00', '12:00', 'Cliente', '+56912345678', 0,
                               30000, 10000, 1440, ?, ?, ?)''',
                    (event_id, f'Torta {state}', original_date, state, f'OPA-{event_id}', f'AGD-{event_id}'),
                )
            conn.commit()
            conn.close()
            for event_id, state, original_date in [(1, 'pendiente', '2026-10-30'), (2, 'completado', '2026-10-18')]:
                event = {
                    'tipo': 'torta', 'titulo': f'Torta {state}', 'fecha': original_date,
                    'hora_inicio': '12:00', 'hora_fin': '13:00', 'hora_entrega': '12:00',
                    'cliente': 'Cliente', 'telefono': '+56912345678', 'estado': state,
                    'total': 30000, 'abono': 10000, 'codigo_operacion': f'OPA-{event_id}', 'codigo_pedido': f'AGD-{event_id}',
                }
                changed = {**event, 'id': event_id, 'fecha': '2026-11-02'}
                rejected = database.guardar_evento_agenda(dict(changed))
                self.assertFalse(rejected['success'])
                self.assertTrue(rejected['requiere_confirmacion_fecha'])
                confirmed = database.guardar_evento_agenda({**changed, 'confirmar_cambio_fecha': True})
                self.assertTrue(confirmed['success'])

    def test_agenda_edit_prompts_before_changing_active_or_past_order_date(self):
        template = (Path(__file__).resolve().parents[1] / 'templates' / 'agenda.html').read_text(encoding='utf-8')
        self.assertIn('La fecha del pedido cambiará de', template)
        self.assertIn('confirmar_cambio_fecha: confirmarCambioFecha', template)
        self.assertIn('confirmar_cambio_fecha: fechaModificada', template)

    def test_personal_agenda_reminders_save_and_reject_past_due_dates(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.object(database, 'DB_PATH', os.path.join(temp_dir, 'agenda.sqlite')):
            conn = sqlite3.connect(database.DB_PATH)
            conn.execute('''CREATE TABLE agenda_notas (
                id INTEGER PRIMARY KEY AUTOINCREMENT, titulo TEXT NOT NULL, contenido TEXT,
                tipo TEXT DEFAULT 'texto', checklist_json TEXT, color TEXT, fijada INTEGER,
                recordatorio TEXT, estado TEXT, creada TEXT DEFAULT CURRENT_TIMESTAMP,
                actualizada TEXT DEFAULT CURRENT_TIMESTAMP
            )''')
            conn.commit()
            conn.close()

            future = (datetime.now(database.ZONA_HORARIA_CHILE).replace(tzinfo=None) + timedelta(hours=2)).strftime('%Y-%m-%dT%H:%M')
            saved = database.guardar_nota_agenda({
                'titulo': 'Revisar pedido', 'contenido': 'Confirmar decoración', 'tipo': 'recordatorio',
                'estado': 'activa', 'recordatorio': future,
            })
            self.assertTrue(saved['success'])
            notes = database.obtener_notas_agenda()
            self.assertEqual(notes[0]['tipo'], 'recordatorio')
            self.assertEqual(notes[0]['titulo'], 'Revisar pedido')

            past = (datetime.now(database.ZONA_HORARIA_CHILE).replace(tzinfo=None) - timedelta(minutes=5)).strftime('%Y-%m-%dT%H:%M')
            rejected = database.guardar_nota_agenda({
                'titulo': 'Vencido', 'tipo': 'recordatorio', 'estado': 'activa', 'recordatorio': past,
            })
            self.assertFalse(rejected['success'])

    def test_personal_reminders_render_in_calendar_and_sync_to_native_alarm_bridge(self):
        template = (Path(__file__).resolve().parents[1] / 'templates' / 'agenda.html').read_text(encoding='utf-8')
        receiver = (Path(__file__).resolve().parents[1] / 'android_pos' / 'app' / 'src' / 'main' / 'java' / 'cl' / 'sucree' / 'pos' / 'AgendaReminderReceiver.kt').read_text(encoding='utf-8')
        manifest = (Path(__file__).resolve().parents[1] / 'android_pos' / 'app' / 'src' / 'main' / 'AndroidManifest.xml').read_text(encoding='utf-8')
        self.assertIn('function nuevoRecordatorioPersonal()', template)
        self.assertIn('function recordatoriosPersonalesActivos()', template)
        self.assertIn("raw.slice(0, 10) >= hoy", template)
        self.assertIn('proximoDia.setHours(24, 0, 1, 0)', template)
        self.assertIn('syncAgendaReminders(JSON.stringify', template)
        self.assertIn('setExactAndAllowWhileIdle', receiver)
        self.assertIn('ACTION_BOOT_COMPLETED', receiver.replace('Intent.ACTION_BOOT_COMPLETED', 'ACTION_BOOT_COMPLETED'))
        self.assertIn('android.intent.action.BOOT_COMPLETED', manifest)

    def test_pos_background_alerts_cover_store_orders_and_agenda_quotes(self):
        root = Path(__file__).resolve().parents[1]
        native = (root / 'android_pos' / 'app' / 'src' / 'main' / 'java' / 'cl' / 'sucree' / 'pos' / 'OrderAlertReceiver.kt').read_text(encoding='utf-8')
        activity = (root / 'android_pos' / 'app' / 'src' / 'main' / 'java' / 'cl' / 'sucree' / 'pos' / 'MainActivity.kt').read_text(encoding='utf-8')
        pos = (root / 'templates' / 'pos_admin_mobile.html').read_text(encoding='utf-8')
        agenda = (root / 'templates' / 'agenda.html').read_text(encoding='utf-8')
        manifest = (root / 'android_pos' / 'app' / 'src' / 'main' / 'AndroidManifest.xml').read_text(encoding='utf-8')
        self.assertIn('POLL_INTERVAL_MS = 60 * 1000L', native)
        self.assertIn('POLL_INTERVAL_SECONDS = 5L', (root / 'android_pos' / 'app' / 'src' / 'main' / 'java' / 'cl' / 'sucree' / 'pos' / 'PosMonitoringService.kt').read_text(encoding='utf-8'))
        self.assertIn('foregroundServiceType="dataSync"', manifest)
        self.assertIn('FOREGROUND_SERVICE_DATA_SYNC', manifest)
        self.assertIn('setOngoing(true)', (root / 'android_pos' / 'app' / 'src' / 'main' / 'java' / 'cl' / 'sucree' / 'pos' / 'PosMonitoringService.kt').read_text(encoding='utf-8'))
        self.assertIn('setSound(sound, audio)', native)
        self.assertIn('vibrationPattern = longArrayOf(0, 350, 180, 350)', native)
        self.assertIn('ALERT_REPEAT_INTERVAL_MS = 5 * 60 * 1000L', native)
        self.assertIn('/api/tienda/admin/pedidos-chat-activos', native)
        self.assertIn('/api/pos/monitor/agenda', native)
        service = (root / 'android_pos' / 'app' / 'src' / 'main' / 'java' / 'cl' / 'sucree' / 'pos' / 'PosMonitoringService.kt').read_text(encoding='utf-8')
        self.assertIn('Notification.FLAG_NO_CLEAR', service)
        self.assertIn('pos_attention_alerts_v3', native)
        self.assertIn('Solicitudes nuevas sin confirmar: $quotes', service)
        self.assertIn('Nuevas (${status.pendingQuotes})', service)
        self.assertIn('UNRESOLVED_QUOTES_KEY = "new_unconfirmed_quote_ids"', native)
        self.assertIn('unresolved.add(id)', native)
        self.assertIn('unresolved.retainAll(activeIds)', native)
        self.assertIn('return unresolved.size', native)
        self.assertIn('eventState in setOf("pendiente", "borrador")', native)
        self.assertIn('setDeleteIntent(dismissedNotificationPendingIntent())', service)
        self.assertIn('MonitoringNotificationDismissedReceiver', manifest)
        self.assertIn('scheduleRepeat(context, key, ALERT_REPEAT_INTERVAL_MS)', native)
        self.assertIn('val isCocktail = type.contains("cocteler") || type == "cocktail"', native)
        self.assertIn('event.optString("ingredientes").contains("reserva de cocteleria", true)', native)
        self.assertIn('!baseline && onlineReservation &&', native)
        self.assertIn('RECENT_RESERVATION_WINDOW_MS = 15 * 60 * 1000L', native)
        self.assertIn('"Nuevo pedido de cocteleria" else "Nueva cotizacion de torta"', native)
        self.assertIn('putExtra("attention_key", key)', native)
        self.assertIn('onNewAgendaQuote', activity)
        self.assertIn('startBackgroundAlertMonitoring()', pos)
        self.assertIn('syncActiveOrderAlerts', pos)
        self.assertIn('syncActiveAgendaQuotes', agenda)
        self.assertIn('cl.sucree.pos.action.POLL_NEW_ORDERS', manifest)

    def test_pos_monitor_agenda_feed_requires_admin_session(self):
        anonymous = app_module.app.test_client().get('/api/pos/monitor/agenda')
        self.assertEqual(anonymous.status_code, 401)
        conn = sqlite3.connect(':memory:')
        conn.row_factory = sqlite3.Row
        conn.execute('''CREATE TABLE agenda_eventos (
            id INTEGER PRIMARY KEY, tipo TEXT, fecha TEXT, cliente TEXT, motivo TEXT,
            estado TEXT, codigo_operacion TEXT, creado TEXT, ingredientes TEXT
        )''')
        conn.execute(
            '''INSERT INTO agenda_eventos
               (id, tipo, fecha, cliente, motivo, estado, codigo_operacion, creado, ingredientes)
               VALUES (1, 'cocteleria', '2026-11-05', 'Cliente', 'Reserva cliente tienda online',
                       'pendiente', 'OPA-TI-TEST', '2026-11-05 12:00:00', 'Reserva de cocteleria')'''
        )
        conn.execute(
            '''INSERT INTO agenda_eventos
               (id, tipo, fecha, cliente, motivo, estado, codigo_operacion, creado, ingredientes)
               VALUES (2, 'cocteleria', '2026-11-05', 'Confirmado', 'Reserva cliente tienda online',
                       'confirmado', 'OPA-TI-CONFIRMED', '2026-11-05 12:00:00', 'Reserva de cocteleria')'''
        )
        conn.execute(
            '''INSERT INTO agenda_eventos
               (id, tipo, fecha, cliente, motivo, estado, codigo_operacion, creado, ingredientes)
               VALUES (3, 'torta', '2026-11-05', 'Borrador', 'Reserva cliente tienda online',
                       'borrador', 'OPA-TI-DRAFT', '2026-11-05 12:00:00', '')'''
        )
        with patch.object(app_module, 'get_db', return_value=conn):
            response = self.client.get('/api/pos/monitor/agenda')
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()['success'])
        eventos = response.get_json()['eventos']
        self.assertEqual(len(eventos), 2)
        self.assertEqual({event['estado'] for event in eventos}, {'pendiente', 'borrador'})
        self.assertNotIn('OPA-TI-CONFIRMED', {event['codigo_operacion'] for event in eventos})


if __name__ == '__main__':
    unittest.main()
