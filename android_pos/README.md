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
- No contiene credenciales, tokens, claves de firma ni un puente JavaScript nativo.
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

Tras confirmar la venta, la APK permite imprimir una comanda por el servicio estándar de impresión de Android, con papel configurado a 58 mm cuando el servicio lo admite. El estado del trabajo se registra localmente separado de la venta; una reimpresión queda marcada como copia y nunca vuelve a crear el pedido. La app informa si Android completa, bloquea o falla el trabajo.

Android no define una API universal para controlar la impresora térmica integrada en todos los terminales. Si un equipo no expone su impresora como servicio Android Print, se necesita identificar el fabricante/modelo e integrar su SDK/adaptador oficial. La venta no depende de que la impresora esté disponible.
