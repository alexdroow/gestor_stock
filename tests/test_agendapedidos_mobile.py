import unittest
import re

import app


class AgendaPedidosMobileTests(unittest.TestCase):
    def setUp(self):
        self.client = app.app.test_client()

    @staticmethod
    def _body_classes(response):
        html = response.get_data(as_text=True)
        match = re.search(r"<body[^>]*\bclass=\"([^\"]*)\"", html)
        return set(match.group(1).split()) if match else set()

    def test_mobile_agendapedidos_receives_mobile_redesign_scope(self):
        response = self.client.get(
            "/tienda/agendapedidos",
            headers={"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X)"},
        )

        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        classes = self._body_classes(response)
        self.assertIn("tienda-agendapedidos-oficial", classes)
        self.assertIn("tienda-preview-agenda", classes)
        self.assertIn("tienda-preview-draft", classes)
        self.assertIn("mobile-ui", classes)
        self.assertIn("agenda-cake-products-heading", html)
        self.assertIn("tienda-agendapedidos-oficial .agenda-cake-catalog-grid", html)
        self.assertIn("#modal-agenda-cake-builder.active .cake-builder-stepper", html)
        self.assertIn('class="agenda-cake-reference-details" open', html)
        self.assertIn("#modal-agenda-cake-cart.active #agenda-days-list", html)
        self.assertIn("Confirmar reserva", html)

    def test_desktop_agendapedidos_does_not_receive_mobile_body_class(self):
        response = self.client.get(
            "/tienda/agendapedidos",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/131.0.0.0"},
        )

        self.assertEqual(response.status_code, 200)
        classes = self._body_classes(response)
        self.assertIn("tienda-agendapedidos-oficial", classes)
        self.assertIn("tienda-preview-agenda", classes)
        self.assertIn("tienda-preview-draft", classes)
        self.assertNotIn("mobile-ui", classes)

    def test_regular_store_does_not_receive_agendapedidos_redesign_scope(self):
        response = self.client.get(
            "/tienda",
            headers={"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X)"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertNotIn("tienda-agendapedidos-oficial", self._body_classes(response))


if __name__ == "__main__":
    unittest.main()
