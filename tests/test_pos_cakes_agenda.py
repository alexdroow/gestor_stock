import unittest
import os
import sqlite3
import tempfile
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

    def test_agenda_reuses_its_existing_58mm_ticket_for_native_pos_printing(self):
        template = (Path(__file__).resolve().parents[1] / 'templates' / 'agenda.html').read_text(encoding='utf-8')

        self.assertIn('@page { size: 58mm auto; margin: 3mm; }', template)
        self.assertIn('<main class="ticket">', template)
        self.assertIn('const ticketHtml = construirHtmlImpresion58(evento);', template)
        self.assertIn('window.SucreePosAndroid.printAgendaTicket(String(idNum), ticketHtml);', template)
        self.assertIn('win.document.write(ticketHtml);', template)

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


if __name__ == '__main__':
    unittest.main()
