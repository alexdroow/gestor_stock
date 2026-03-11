#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import tempfile
import time
import webbrowser
from datetime import datetime

# Silenciar stderr nativo (FFmpeg/OpenCV) en modo app.
# Se puede desactivar con: GESTIONSTOCK_SUPPRESS_STDERR=0
if os.environ.get("GESTIONSTOCK_SUPPRESS_STDERR", "1").strip().lower() not in ("0", "false", "no", "off"):
    try:
        _null_stderr = open(os.devnull, "w", encoding="utf-8", buffering=1)
        os.dup2(_null_stderr.fileno(), 2)
        sys.stderr = _null_stderr
    except Exception:
        pass

# =============================================================================
# DIAGNÓSTICO - Usar ruta segura
# =============================================================================

# Usar directorio temporal de Windows (siempre existe y tiene permisos)
DIAG_FILE = os.path.join(tempfile.gettempdir(), 'GestionStock_DIAG.txt')

def diag(msg):
    try:
        with open(DIAG_FILE, 'a', encoding='utf-8') as f:
            f.write(f"{datetime.now()}: {msg}\n")
    except Exception as e:
        # Si falla el diagnóstico, al menos intentar mostrar en consola
        print(f"DIAG ERROR: {e}")

diag("=" * 50)
diag("INICIANDO APLICACION")
diag(f"Python: {sys.version}")
diag(f"Frozen: {getattr(sys, 'frozen', False)}")


def habilitar_site_packages_global():
    """
    Si se ejecuta dentro de un venv sin dependencias, agrega el site-packages
    del Python base para poder iniciar offline.
    """
    if getattr(sys, "prefix", "") == getattr(sys, "base_prefix", ""):
        return False

    candidatos = [
        os.path.join(sys.base_prefix, "Lib", "site-packages"),
        os.path.join(
            sys.base_prefix,
            "lib",
            f"python{sys.version_info.major}.{sys.version_info.minor}",
            "site-packages",
        ),
    ]

    agregados = []
    for ruta in candidatos:
        if os.path.isdir(ruta) and ruta not in sys.path:
            sys.path.append(ruta)
            agregados.append(ruta)

    if agregados:
        diag(f"Site-packages global agregado: {agregados}")
        return True
    return False


habilitar_site_packages_global()

# Modo agente en segundo plano (sin UI)
if "--background-agent" in sys.argv:
    try:
        from background_agent import run_background_agent
        diag("Iniciando en modo background-agent")
        run_background_agent()
        sys.exit(0)
    except Exception as e:
        diag(f"ERROR background-agent: {e}")
        sys.exit(1)

FORCE_BROWSER = (
    "--browser" in sys.argv
    or os.environ.get("GESTIONSTOCK_FORCE_BROWSER", "").strip().lower() in ("1", "true", "yes", "on")
)
if FORCE_BROWSER:
    diag("Modo navegador forzado por parametro/entorno")

WEBVIEW_AVAILABLE = True
webview = None
try:
    import webview as _webview
    webview = _webview
    diag("Import webview: OK")
except Exception as e:
    WEBVIEW_AVAILABLE = False
    diag(f"ERROR import webview: {e}")
    print("[INFO] pywebview no disponible. Se usara modo navegador.")

try:
    import threading
    import socket
    diag("Imports basicos: OK")
except Exception as e:
    diag(f"ERROR imports basicos: {e}")
    raise

# =============================================================================
# CONFIGURACION
# =============================================================================

try:
    from config import DATA_DIR, DB_PATH
    diag(f"Config importado: DATA_DIR={DATA_DIR}")
except ImportError as e:
    diag(f"Config no encontrado, usando fallback: {e}")
    
    def get_data_directory():
        if getattr(sys, 'frozen', False):
            local_appdata = os.getenv("LOCALAPPDATA")
            if local_appdata:
                data_dir = os.path.join(local_appdata, 'GestionStockPro')
            else:
                data_dir = os.path.join(os.path.expanduser('~'), 'AppData', 'Local', 'GestionStockPro')
        else:
            data_dir = os.path.dirname(os.path.abspath(__file__))
        os.makedirs(data_dir, exist_ok=True)
        return data_dir
    
    DATA_DIR = get_data_directory()
    DB_PATH = os.path.join(DATA_DIR, 'stock.db')

os.environ['GESTIONSTOCK_DATA_DIR'] = DATA_DIR
os.environ['GESTIONSTOCK_DB_PATH'] = DB_PATH

diag(f"DATA_DIR: {DATA_DIR}")
diag(f"DB_PATH: {DB_PATH}")

# Perfil dedicado de WebView2 para evitar errores de inicializacion
WEBVIEW2_PROFILE_DIR = os.path.join(DATA_DIR, "webview2_profile")
try:
    os.makedirs(WEBVIEW2_PROFILE_DIR, exist_ok=True)
    os.environ.setdefault("WEBVIEW2_USER_DATA_FOLDER", WEBVIEW2_PROFILE_DIR)
    diag(f"WEBVIEW2_USER_DATA_FOLDER: {WEBVIEW2_PROFILE_DIR}")
except Exception as e:
    diag(f"No se pudo preparar perfil WebView2: {e}")

try:
    from background_agent import ensure_background_startup
    ensure_background_startup()
except Exception as e:
    diag(f"No se pudo registrar tarea de inicio: {e}")

# =============================================================================
# RESTO DEL CODIGO
# =============================================================================

def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        s.listen(1)
        port = s.getsockname()[1]
    return port

FLASK_PORT = find_free_port()
FLASK_URL = f'http://127.0.0.1:{FLASK_PORT}'

diag(f"Puerto seleccionado: {FLASK_PORT}")

flask_ready = threading.Event()
flask_start_error = None

def start_flask():
    global flask_start_error
    try:
        if getattr(sys, 'frozen', False):
            sys.path.insert(0, sys._MEIPASS)
        
        diag("Importando database...")
        from database import init_db
        import database
        database.DB_PATH = DB_PATH
        
        diag("Importando app...")
        from app import app
        
        diag("Inicializando DB...")
        init_db()
        
        diag("DB inicializada correctamente")
        flask_ready.set()
        
        diag(f"Iniciando servidor en {FLASK_PORT}")
        app.run(host='127.0.0.1', port=FLASK_PORT, debug=False, use_reloader=False, threaded=True)
        
    except Exception as e:
        flask_start_error = str(e)
        diag(f"ERROR en Flask: {e}")
        import traceback
        diag(traceback.format_exc())
        flask_ready.set()

diag("Iniciando thread de Flask...")
threading.Thread(target=start_flask, daemon=True).start()

diag("Esperando que Flask este listo...")
if not flask_ready.wait(timeout=30):
    diag("TIMEOUT esperando Flask")
    sys.exit(1)

if flask_start_error:
    diag(f"No se puede iniciar la app Flask: {flask_start_error}")
    sys.exit(1)

diag("Flask listo, creando ventana...")

def abrir_en_navegador():
    try:
        ok = webbrowser.open(FLASK_URL, new=2)
        if ok:
            diag("Modo navegador abierto correctamente")
        else:
            diag("Modo navegador intento abrir, pero el sistema devolvio false")
        print(f"[INFO] App disponible en: {FLASK_URL}")
        return True
    except Exception as e:
        diag(f"ERROR abriendo navegador: {e}")
        return False


def mantener_servidor_activo():
    diag("Servidor activo en modo navegador. Presiona Ctrl+C para cerrar.")
    try:
        while True:
            time.sleep(0.8)
    except KeyboardInterrupt:
        diag("Cierre manual desde consola (Ctrl+C)")


if FORCE_BROWSER or not WEBVIEW_AVAILABLE:
    if abrir_en_navegador():
        mantener_servidor_activo()
        diag("Aplicacion cerrada normalmente (modo navegador)")
    else:
        diag("No se pudo abrir navegador en modo fallback")
        sys.exit(1)
else:
    try:
        window = webview.create_window(
            title='Sucr\u00e9eStock',
            url=FLASK_URL,
            width=1400,
            height=900
        )

        diag("Ventana creada, iniciando webview...")
        webview.start(debug=False, http_server=False)
        diag("Aplicacion cerrada normalmente")

    except Exception as e:
        diag(f"ERROR en webview: {e}")
        import traceback
        diag(traceback.format_exc())
        print("[WARN] Fallo WebView2. Abriendo en navegador...")
        if abrir_en_navegador():
            mantener_servidor_activo()
            diag("Aplicacion cerrada normalmente (fallback navegador)")
        else:
            diag("No se pudo abrir navegador tras fallo de webview")
            sys.exit(1)

diag("FIN DE LA APLICACION")
