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

Al abrir Sucree POS se inicia un servicio Android en primer plano. La notificacion persistente muestra el estado y la hora de la ultima consulta, y ofrece accesos directos a Pedidos y Agenda. Mientras el servicio esta activo consulta pedidos y reservas cada cinco segundos; las alarmas nativas quedan como respaldo cada minuto. Los avisos individuales se repiten cada cinco minutos hasta abrirlos o atender el pedido. El canal de alertas solicita sonido y vibracion, sujetos al volumen y a los ajustes de notificaciones de Android. Android/Wear OS puede reflejar esas notificaciones en un reloj si la sincronizacion esta habilitada en el dispositivo. La consulta de pedidos requiere una sesion administrativa vigente. El servicio reduce la suspension del proceso, pero Android puede retrasar la red con ahorro extremo de bateria y no ejecuta tareas si el usuario fuerza la detencion de la app.
