import re
import unittest

import app


class TiendaMobileNavigationTests(unittest.TestCase):
    def setUp(self):
        self.client = app.app.test_client()

    def test_mobile_store_has_correct_sucree_wordmark_and_catalog_link(self):
        response = self.client.get(
            "/tienda",
            headers={"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X)"},
        )

        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        body = re.search(r"<body[^>]*\bclass=\"([^\"]*)\"", html)
        self.assertIn("mobile-ui", body.group(1) if body else "")
        self.assertIn('content: "Sucr\\00e9\\65" !important;', html)
        self.assertIn('href=\"/tienda/tortas-listas\">Catalogo Tortas</a>', html)
        self.assertIn('href=\"/redes\">Redes Sucrée</a>', html)
        self.assertIn("function abrirMenuMobile()", html)
        self.assertIn("window.matchMedia?.(\"(max-width: 899px), (pointer: coarse)\")", html)

    def test_menu_button_uses_script_listener_and_not_inline_handler(self):
        response = self.client.get(
            "/tienda",
            headers={"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X)"},
        )

        html = response.get_data(as_text=True)
        self.assertIn('id="btn-mobile-menu" aria-label="Abrir menu"', html)
        self.assertNotIn('id="btn-mobile-menu" onclick=', html)
        self.assertIn("btnMobileMenuEl.addEventListener(\"click\", (event)", html)


if __name__ == "__main__":
    unittest.main()
