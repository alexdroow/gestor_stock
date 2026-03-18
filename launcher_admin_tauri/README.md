# Launcher Admin Sucree (Tauri)

Este módulo crea un **launcher de escritorio** para abrir solo rutas administrativas de tu sistema (sin tienda pública).

## Rutas habilitadas
- `/ventas/admin-personalizacion`
- `/ventas/admin-catalogo`
- `/ventas/admin-catalogo-torta`
- `/ventas/admin-envios`
- `/agenda`
- `/ventas`

## Requisitos (Windows)
1. Node.js LTS
2. Rust (rustup)
3. Microsoft C++ Build Tools (Visual Studio Build Tools)

## Desarrollo
```bash
cd launcher_admin_tauri
npm install
npm run tauri:dev
```

## Compilar `.exe` instalable
```bash
cd launcher_admin_tauri
npm install
npm run tauri:build
```

Salida esperada (MSI):
- `launcher_admin_tauri/src-tauri/target/release/bundle/msi/*.msi`

## Icono
Se usa:
- `launcher_admin_tauri/src-tauri/icons/icon.ico`

(Se copió desde `assets/icon.ico` del proyecto.)

## Uso
- Abre el Launcher.
- Deja o cambia la URL base (por defecto: `https://alexdroow.pythonanywhere.com`).
- Haz clic en el panel admin que quieres abrir.

## Seguridad
Este launcher valida en código que solo se puedan abrir rutas administrativas permitidas.
