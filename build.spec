# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path
from PyInstaller.utils.hooks import collect_submodules


PROJECT_DIR = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
ICON_PATH = PROJECT_DIR / "assets" / "icon.ico"

block_cipher = None


def _safe_collect_submodules(package_name):
    try:
        return collect_submodules(package_name)
    except Exception:
        return []


TUYA_HIDDENIMPORTS = _safe_collect_submodules("tuya_sharing")
CRYPTO_HIDDENIMPORTS = _safe_collect_submodules("cryptography")

EXTRA_HIDDENIMPORTS = sorted(
    set(
        [
            "webview",
            "pytz",
            "reportlab",
            "reportlab.platypus",
            "reportlab.lib.styles",
            "tuya_sharing",
            "pyqrcode",
            "cryptography",
            "cv2",
            "numpy",
            "numpy.core",
            "numpy.typing",
        ]
        + TUYA_HIDDENIMPORTS
        + CRYPTO_HIDDENIMPORTS
    )
)


a = Analysis(
    ["main.py"],
    pathex=[str(PROJECT_DIR)],
    binaries=[],
    datas=[
        (str(PROJECT_DIR / "templates"), "templates"),
        (str(PROJECT_DIR / "static"), "static"),
    ],
    hiddenimports=EXTRA_HIDDENIMPORTS,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=["matplotlib", "pandas", "scipy", "tkinter"],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name="SucreeStock",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=str(ICON_PATH) if ICON_PATH.exists() else None,
)
