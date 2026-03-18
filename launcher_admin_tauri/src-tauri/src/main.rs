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
        .invoke_handler(tauri::generate_handler![open_admin_panel])
        .run(tauri::generate_context!())
        .expect("error while running launcher admin");
}
