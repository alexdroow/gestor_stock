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

    def test_mobile_store_has_persistent_login_and_account_entry(self):
        response = self.client.get(
            "/tienda",
            headers={"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X)"},
        )
        html = response.get_data(as_text=True)
        self.assertIn('id="mobile-account-access"', html)
        self.assertIn('id="mobile-account-access-label">Iniciar sesi&oacute;n</span>', html)
        self.assertIn('body.mobile-ui:not(.tienda-preview-agenda) .mobile-account-access', html)
        self.assertNotIn('body.mobile-ui:not(.tienda-preview-agenda):not(.tienda-preview-draft) .mobile-account-access', html)
        self.assertIn('id="mobile-account-nav-label">Iniciar sesi&oacute;n</strong>', html)
        self.assertIn('id="mobile-account-nav-label"', html)
        self.assertIn('mobileAccountAccessLabelEl.textContent = logged ? "Mi cuenta" : "Iniciar sesi\\u00f3n"', html)
        self.assertIn('mobileAccountNavLabelEl.textContent = logged ? "Mi cuenta" : "Iniciar sesi\\u00f3n"', html)
        self.assertIn('id="modal-cuenta"', html)
        self.assertIn('id="cuenta-email"', html)
        self.assertIn('id="cuenta-telefono"', html)

    def test_active_coupon_popup_is_limited_to_once_per_customer_every_24_hours(self):
        response = self.client.get("/tienda")
        html = response.get_data(as_text=True)
        self.assertIn("function mostrarAvisoCuponActivo(cupones)", html)
        self.assertIn("24 * 60 * 60 * 1000", html)
        self.assertIn("localStorage.setItem(storageKey, String(ahora))", html)
        self.assertIn('id = "cupon-activo-modal"', html)
        self.assertIn("if (cupones.length > 0) mostrarAvisoCuponActivo(cupones)", html)
        self.assertNotIn("mostrarAvisoCuponValoracion", html)

    def test_desktop_store_exposes_login_and_authenticated_account_menu(self):
        response = self.client.get("/tienda", headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/130"})
        html = response.get_data(as_text=True)
        self.assertIn('id="btn-iniciar-sesion">Iniciar sesi&oacute;n</button>', html)
        self.assertIn('id="btn-cuenta" style="display:none;">Mi cuenta</button>', html)
        self.assertIn('btnCuentaEl.textContent = "Mi cuenta"', html)
        self.assertIn('btnIniciarSesionEl.style.display = logged ? "none" : ""', html)


if __name__ == "__main__":
    unittest.main()
