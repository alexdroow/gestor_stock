import unittest

import app as app_module


class PosCakesAgendaTests(unittest.TestCase):
    def setUp(self):
        self.client = app_module.app.test_client()
        with self.client.session_transaction() as session:
            session[app_module._ADMIN_SESSION_KEY] = True

    def test_mobile_pos_adds_tortas_tab_and_embeds_mobile_agenda_lazily(self):
        response = self.client.get('/admin/pos/mobile')

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['X-POS-Mobile-Version'], 'mobile-pos-20261018-8')
        self.assertIn(b'data-tab="tortas"', response.data)
        self.assertIn(b'data-src="/agenda?pos_embed=1"', response.data)
        self.assertIn(b"frame.dataset.loaded!=='1'", response.data)

    def test_embedded_agenda_uses_sucreestock_mobile_view_and_returns_to_mobile_pos(self):
        response = self.client.get('/agenda?pos_embed=1')

        self.assertEqual(response.status_code, 200)
        self.assertIn(b'@media (max-width: 760px)', response.data)
        self.assertIn(b'class="agenda-mobile-bottom-nav"', response.data)
        self.assertIn(b'href="/admin/pos/mobile?tab=home" target="_top"', response.data)
        self.assertIn('Volver al POS'.encode(), response.data)


if __name__ == '__main__':
    unittest.main()
