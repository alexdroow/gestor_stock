#  Gestor de Stock Pro - Guía de Instalación

Esta guía te ayudará a convertir tu aplicación Flask en un **programa profesional instalable** para Windows.

---

##  Requisitos Previos

Antes de comenzar, necesitas instalar:

### 1. Python 3.10 o superior
- Descarga desde: https://www.python.org/downloads/
- **IMPORTANTE**: Marca "Add Python to PATH" durante la instalación

### 2. Inno Setup 6 (para crear el instalador)
- Descarga desde: https://jrsoftware.org/isdl.php
- Instala con las opciones por defecto

---

##  Pasos para Crear el Instalador

### Paso 1: Preparar tu proyecto

1. Copia todos los archivos de esta carpeta a tu proyecto:
   ```
    TuProyecto/
   |--  requirements.txt      <- NUEVO
   |--  main.py               <- NUEVO (punto de entrada)
   |--  build.py              <- NUEVO
   |--  GestionStock.spec     <- NUEVO
   |--  installer.iss         <- NUEVO
   |--  app.py                <- Tu código
   |--  database.py           <- Tu código
   |--  backup.py             <- Tu código
   |--  templates/            <- Tus HTML
   `--  static/               <- Tus CSS/JS
   ```

2. **Opcional**: Agrega un icono a tu aplicación:
   - Crea una carpeta `assets/`
   - Coloca tu icono como `assets/icon.ico` (formato .ico, 256x256 recomendado)
   - Si no tienes icono, el instalador funcionará igual

### Paso 2: Instalar dependencias

Abre una terminal (CMD o PowerShell) en la carpeta de tu proyecto y ejecuta:

```bash
pip install -r requirements.txt
```

### Paso 3: Probar en modo desarrollo

Antes de empaquetar, verifica que todo funcione:

```bash
python main.py
```

Si se abre la ventana correctamente, ¡estás listo para empaquetar!

### Paso 4: Crear el ejecutable

Ejecuta el script de build:

```bash
python build.py
```

Este proceso:
1. [OK] Limpia builds anteriores
2. [OK] Verifica dependencias
3. [OK] Crea el ejecutable con PyInstaller
4. [OK] Genera el instalador con Inno Setup

**Tiempo estimado**: 3-5 minutos

### Paso 5: Distribuir tu aplicación

Después del build, encontrarás:

```
 dist/
`--  GestionStockPro.exe         <- Ejecutable portable

 Output/
`--  GestionStockPro_Setup.exe  <- Instalador profesional
```

**Opciones de distribución:**

| Opción | Cuándo usar | Tamaño |
|--------|-------------|--------|
| **Instalador** (.exe) | Para clientes finales, instalación profesional | ~25 MB |
| **Portable** (carpeta zip) | Para uso en USB o sin instalar | ~25 MB |

---

##  Estructura del Instalador Profesional

El instalador creado incluye:

- [OK] **Acceso directo en el menú Inicio**
- [OK] **Opción de icono en el escritorio**
- [OK] **Desinstalador integrado** (Panel de Control -> Programas)
- [OK] **Base de datos en AppData** (persistente entre actualizaciones)
- [OK] **Verificación de instancias** (evita abrir 2 veces)

### Ubicación de datos del usuario

```
C:\Users\[TuUsuario]\AppData\Local\GestionStockPro\
|--  stock.db              <- Base de datos
`--  backups\              <- Copias de seguridad
```

---

##  Solución de Problemas

### Error: "No module named 'pywebview'"
```bash
pip install pywebview
```

### Error: "PyInstaller no encontrado"
```bash
pip install pyinstaller
```

### Error: "Inno Setup no encontrado"
- Descárgalo de https://jrsoftware.org/isdl.php
- Instálalo en la ruta por defecto
- El build.py detectará automáticamente Inno Setup

### La ventana no se abre
1. Verifica que Flask funcione: `python app.py`
2. Revisa que los templates estén en la carpeta `templates/`
3. Ejecuta en modo debug: cambia `debug=False` a `debug=True` en `main.py`

### El ejecutable es muy grande
- El archivo `.spec` ya excluye librerías grandes (numpy, matplotlib, etc.)
- Usa compresión UPX (activada por defecto)
- Tamaño típico: 25-40 MB

---

##  Actualizar la Aplicación

Para lanzar una nueva versión:

1. **Actualiza la versión** en estos archivos:
   - `main.py` (línea con "v4.0")
   - `GestionStock.spec` (APP_VERSION)
   - `installer.iss` (MyAppVersion)

2. **Ejecuta el build**:
   ```bash
   python build.py
   ```

3. **Distribuye el nuevo instalador**

**Nota**: Los datos del usuario (base de datos) se mantienen al actualizar.

---

##  Consejos Adicionales

### Personalizar el instalador

Edita `installer.iss` para:
- Cambiar el nombre del programa
- Agregar tu logo
- Modificar el texto de bienvenida
- Cambiar la carpeta de instalación por defecto

### Agregar auto-actualizaciones

Para actualizaciones automáticas, considera:
- [pyupdater](https://github.com/Digital-Sapphire/PyUpdater) (avanzado)
- Sistema simple: verificar versión en un servidor y descargar nuevo instalador

### Firmar el ejecutable (opcional, recomendado)

Para que Windows no muestre "Desconocido":
1. Compra un certificado de firma de código
2. Firma el .exe con `signtool.exe`
3. Alternativa gratuita: [Certum](https://en.sklep.certum.pl/data-safety/code-signing-certificates/open-source-code-signing.html) para open source

---

##  Soporte

Si tienes problemas:
1. Revisa los mensajes de error en la consola
2. Verifica que todos los archivos estén en su lugar
3. Asegúrate de tener las versiones correctas de Python y dependencias

---

## [OK] Checklist Final

Antes de distribuir tu aplicación:

- [ ] Probaste el ejecutable en tu PC
- [ ] Probaste el instalador en una PC limpia (o máquina virtual)
- [ ] Verificaste que la base de datos se crea correctamente
- [ ] Comprobaste que los backups funcionan
- [ ] El icono aparece correctamente
- [ ] El desinstalador funciona

---

**¡Listo! Tu Gestor de Stock ahora es una aplicación profesional.** 
