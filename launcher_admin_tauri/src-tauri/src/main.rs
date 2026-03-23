#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use tauri::{Manager, WindowUrl};
use url::Url;

const ROUTES_PERMITIDAS: [&str; 6] = [
    "/ventas/admin-personalizacion",
    "/ventas/admin-catalogo",
    "/ventas/admin-catalogo-torta",
    "/ventas/admin-envios",
    "/agenda",
    "/ventas",
];

fn normalizar_base_url(base_url: &str) -> Result<String, String> {
    let raw = base_url.trim().trim_end_matches('/');
    if raw.is_empty() {
        return Err("Debes indicar una URL base valida".to_string());
    }
    let parsed = Url::parse(raw).map_err(|_| "URL base invalida".to_string())?;
    let scheme = parsed.scheme().to_ascii_lowercase();
    if scheme != "https" && scheme != "http" {
        return Err("La URL base debe iniciar con http:// o https://".to_string());
    }
    if parsed.host_str().is_none() {
        return Err("La URL base no contiene host valido".to_string());
    }
    Ok(raw.to_string())
}

fn construir_admin_url(base_url: &str, route: &str) -> Result<Url, String> {
    let base = normalizar_base_url(base_url)?;
    let route_clean = format!("/{}", route.trim().trim_start_matches('/'));
    if !ROUTES_PERMITIDAS.contains(&route_clean.as_str()) {
        return Err("Ruta no permitida en este launcher".to_string());
    }
    let full = format!("{}{}", base, route_clean);
    Url::parse(&full).map_err(|_| "No se pudo construir la URL admin".to_string())
}

#[tauri::command]
fn open_admin_panel(app: tauri::AppHandle, base_url: String, route: String) -> Result<(), String> {
    let url = construir_admin_url(&base_url, &route)?;

    if let Some(win) = app.get_window("admin-panel") {
        let js = format!("window.location.replace({:?});", url.as_str());
        win.eval(&js)
            .map_err(|e| format!("No se pudo navegar ventana admin: {}", e))?;
        win.show().map_err(|e| format!("No se pudo mostrar ventana: {}", e))?;
        win.set_focus().map_err(|e| format!("No se pudo enfocar ventana: {}", e))?;
        return Ok(());
    }

    tauri::WindowBuilder::new(
        &app,
        "admin-panel",
        WindowUrl::External(url),
    )
    .title("Admin Panel - Gestor Stock")
    .inner_size(1360.0, 900.0)
    .min_inner_size(980.0, 700.0)
    .resizable(true)
    .build()
    .map_err(|e| format!("No se pudo abrir panel admin: {}", e))?;

    Ok(())
}

fn main() {
    tauri::Builder::default()
        .on_page_load(|window, payload| {
            let url_txt = payload.url().to_string();
            if url_txt.contains("/agenda") {
                let _ = window.eval(
                    r##"
                    (() => {
                        try {
                            if (window.__SUCREE_LAUNCHER_AGENDA_PATCH__) return;
                            const p = String(window.location.pathname || "");
                            if (!p.startsWith("/agenda")) return;
                            window.__SUCREE_LAUNCHER_AGENDA_PATCH__ = true;

                            const STYLE_ID = "sucree-launcher-agenda-force-style";
                            let st = document.getElementById(STYLE_ID);
                            if (!st) {
                                st = document.createElement("style");
                                st.id = STYLE_ID;
                                st.textContent = `
                                    html { zoom: var(--sucree-launcher-zoom, 0.82) !important; }
                                    .main-content {
                                        padding: 18px 22px !important;
                                    }
                                    .agenda-layout {
                                        grid-template-columns: minmax(0, 1fr) !important;
                                        gap: 12px !important;
                                    }
                                    .calendar-card {
                                        min-height: 520px !important;
                                    }
                                    .calendar-header {
                                        padding: 14px 16px !important;
                                    }
                                    .calendar-nav {
                                        gap: 10px !important;
                                    }
                                    .calendar-nav h2 {
                                        min-width: 170px !important;
                                        font-size: 29px !important;
                                    }
                                    .calendar-grid {
                                        grid-template-rows: 34px repeat(6, minmax(82px, 1fr)) !important;
                                    }
                                    .calendar-day {
                                        min-height: 82px !important;
                                        padding: 6px !important;
                                    }
                                    .day-event {
                                        font-size: 10px !important;
                                        line-height: 1.2 !important;
                                        max-width: 100% !important;
                                        overflow: hidden !important;
                                        text-overflow: ellipsis !important;
                                        white-space: nowrap !important;
                                    }
                                    .side-panel {
                                        display: grid !important;
                                        grid-template-columns: repeat(3, minmax(210px, 1fr)) !important;
                                        gap: 12px !important;
                                    }
                                    @media (max-width: 1600px) {
                                        .side-panel { grid-template-columns: 1fr !important; }
                                    }
                                    #sucree-launcher-zoom-tools {
                                        position: fixed;
                                        right: 14px;
                                        top: 14px;
                                        z-index: 2000;
                                        display: inline-flex;
                                        align-items: center;
                                        gap: 6px;
                                        background: rgba(15, 23, 42, 0.94);
                                        border: 1px solid rgba(148, 163, 184, 0.3);
                                        border-radius: 10px;
                                        padding: 7px;
                                        box-shadow: 0 10px 25px rgba(15, 23, 42, 0.35);
                                    }
                                    #sucree-launcher-zoom-tools button {
                                        border: 1px solid #475569;
                                        background: #1e293b;
                                        color: #f8fafc;
                                        border-radius: 8px;
                                        min-width: 32px;
                                        height: 30px;
                                        cursor: pointer;
                                        font-size: 12px;
                                        font-weight: 800;
                                    }
                                    #sucree-launcher-zoom-tools button:hover {
                                        border-color: #f59e0b;
                                    }
                                    #sucree-launcher-zoom-value {
                                        color: #f8fafc;
                                        min-width: 50px;
                                        text-align: center;
                                        font-size: 12px;
                                        font-weight: 800;
                                    }
                                `;
                                document.head.appendChild(st);
                            }

                            const ZOOM_KEY = "sucree_launcher_zoom_agenda_runtime";
                            const clamp = (v) => Math.max(0.70, Math.min(1.10, Number(v) || 0.82));
                            const apply = (v, save = true) => {
                                const z = clamp(v);
                                document.documentElement.style.setProperty("--sucree-launcher-zoom", String(z));
                                const valueEl = document.getElementById("sucree-launcher-zoom-value");
                                if (valueEl) valueEl.textContent = `${Math.round(z * 100)}%`;
                                if (save) {
                                    try { localStorage.setItem(ZOOM_KEY, String(z)); } catch (_) {}
                                }
                                return z;
                            };

                            let current = 0.82;
                            try {
                                const saved = Number(localStorage.getItem(ZOOM_KEY));
                                if (Number.isFinite(saved)) current = clamp(saved);
                            } catch (_) {}
                            current = apply(current, false);

                            if (!document.getElementById("sucree-launcher-zoom-tools")) {
                                const tools = document.createElement("div");
                                tools.id = "sucree-launcher-zoom-tools";
                                tools.innerHTML = `
                                    <button type="button" id="slz-minus">-</button>
                                    <span id="sucree-launcher-zoom-value">${Math.round(current * 100)}%</span>
                                    <button type="button" id="slz-plus">+</button>
                                    <button type="button" id="slz-reset">Reset</button>
                                `;
                                document.body.appendChild(tools);
                                tools.querySelector("#slz-minus")?.addEventListener("click", () => current = apply(current - 0.02));
                                tools.querySelector("#slz-plus")?.addEventListener("click", () => current = apply(current + 0.02));
                                tools.querySelector("#slz-reset")?.addEventListener("click", () => current = apply(0.82));
                            }
                        } catch (_) {}
                    })();
                    "##,
                );
            }
        })
        .invoke_handler(tauri::generate_handler![open_admin_panel])
        .run(tauri::generate_context!())
        .expect("error while running launcher admin");
}
