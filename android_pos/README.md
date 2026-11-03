# Sucrée POS Android

Aplicación Android genérica para abrir el POS móvil oficial de Sucrée. Usa un WebView del sistema y el backend existente; no mantiene otro catálogo, carrito de ventas ni base de datos de ventas.

## Compatibilidad

- Android 8.0 (API 26) en adelante, incluido Android 13.
- No limita el fabricante ni la arquitectura del terminal.
- La interfaz de ventas es `/admin/pos/mobile` en `https://pasteleriasucree.cl`.
- El usuario inicia sesión en el sitio; Android conserva las cookies seguras del WebView.

## Seguridad y comportamiento

- Solo permite navegación HTTPS dentro de `pasteleriasucree.cl` y `www.pasteleriasucree.cl`.
- WhatsApp se abre como aplicación externa; los demás dominios no se cargan dentro de la APK.
- Desactiva contenido mixto, acceso a archivos locales y cookies de terceros.
- No contiene credenciales, tokens ni claves de firma. El puente JavaScript nativo solo atiende impresión de tickets de Agenda y avisos de nuevos pedidos.
- La venta continúa guardándose una sola vez en el backend actual.

## Compilar APK de prueba

Requisitos: JDK 17 o superior y Android SDK con plataforma 35.

En Windows PowerShell:

```powershell
cd android_pos
.\gradlew.bat assembleDebug
```

En Bash:

```bash
cd android_pos
./gradlew assembleDebug
```

APK de depuración:

```text
app/build/outputs/apk/debug/app-debug.apk
```

La APK debug sirve para instalación y validación interna. No distribuirla como versión operativa. Un release debe firmarse con el keystore privado de Sucrée; nunca guardar ese archivo ni sus contraseñas en Git.

## Impresión

En iMin Swift 2 se usa el SDK oficial iMin Printer v2 y papel de 58 mm. Otros Android conservan la ruta Android Print. Las reimpresiones se identifican como copia y no vuelven a crear la venta.

## Avisos de pedidos

La APK consulta pedidos y cotizaciones pendientes mediante alarmas nativas mientras está en segundo plano. Los avisos se repiten cada cinco minutos hasta abrir la notificación, atender el pedido o confirmar la cotización. Requiere permisos de Notificaciones y Alarmas exactas; Android puede retrasar alarmas por ahorro de batería y no ejecuta tareas si el usuario fuerza la detención de la app. El sondeo requiere que siga vigente la sesión administrativa.
