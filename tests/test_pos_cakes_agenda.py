import unittest
from pathlib import Path

import app as app_module


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


if __name__ == '__main__':
    unittest.main()
