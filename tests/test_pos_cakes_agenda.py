import unittest
from unittest.mock import patch

import app as app_module


class PosCakesAgendaTests(unittest.TestCase):
    def setUp(self):
        self.client = app_module.app.test_client()
        with self.client.session_transaction() as session:
            session[app_module._ADMIN_SESSION_KEY] = True

    def test_mobile_pos_adds_tortas_tab_and_embeds_mobile_agenda_lazily(self):
        response = self.client.get('/admin/pos/mobile')

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['X-POS-Mobile-Version'], 'mobile-pos-20261018-7')
        self.assertIn(b'data-tab="tortas"', response.data)
        self.assertIn(b'data-src="/tienda/agendapedidos?pos_embed=1"', response.data)
        self.assertIn(b"frame.dataset.loaded!=='1'", response.data)

    def test_embedded_agenda_renders_mobile_reservation_section_without_store_shell(self):
        with patch.object(
            app_module,
            '_obtener_tienda_personalizacion',
            return_value=app_module._default_tienda_personalizacion(),
        ):
            response = self.client.get('/tienda/agendapedidos?pos_embed=1')

        self.assertEqual(response.status_code, 200)
        self.assertIn(b'pos-agenda-embed', response.data)
        self.assertIn(b'id="sec-agenda-reservas"', response.data)
        self.assertIn(b'body.pos-agenda-embed .mobile-bottom-nav', response.data)


if __name__ == '__main__':
    unittest.main()
