#!/usr/bin/env python3
"""Script de build para generar ejecutable e instalador en Windows."""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
SPEC_FILE = PROJECT_ROOT / "GestionStock.spec"
REQUIREMENTS_FILE = PROJECT_ROOT / "requirements.txt"
INNO_SETUP_PATH = Path(r"C:\Program Files (x86)\Inno Setup 6\ISCC.exe")
INSTALLER_SCRIPT = PROJECT_ROOT / "installer.iss"
DIST_DIR = PROJECT_ROOT / "dist"
EXE_NAME = "GestionStockPro.exe"
DATA_BUNDLE_NAME = "GestionStockPro_data_bundle.zip"
PORTABLE_DIR_NAME = "GestionStockPro_Portable"


def run(cmd: list[str], *, check: bool = True) -> subprocess.CompletedProcess:
    """Ejecuta comando en la raiz del proyecto."""
    return subprocess.run(cmd, cwd=PROJECT_ROOT, check=check)


def clean_build() -> None:
    print("[1/5] Limpiando builds anteriores...")
    for folder in ("build", "dist", ".pyinstaller"):
        target = PROJECT_ROOT / folder
        if target.exists():
            shutil.rmtree(target, ignore_errors=True)
            print(f"  - eliminado: {target.name}")

    for pycache in PROJECT_ROOT.rglob("__pycache__"):
        if "venv" in pycache.parts:
            continue
        shutil.rmtree(pycache, ignore_errors=True)


def check_dependencies() -> None:
    print("[2/5] Verificando dependencias...")
    if not REQUIREMENTS_FILE.exists():
        raise FileNotFoundError(f"No existe {REQUIREMENTS_FILE}")
    run([sys.executable, "-m", "pip", "install", "-r", str(REQUIREMENTS_FILE)])


def build_executable() -> None:
    print("[3/5] Construyendo ejecutable...")
    if not SPEC_FILE.exists():
        raise FileNotFoundError(f"No existe {SPEC_FILE}")
    run(
        [
            sys.executable,
            "-m",
            "PyInstaller",
            str(SPEC_FILE.name),
            "--clean",
            "--noconfirm",
        ]
    )


def create_installer() -> None:
    print("[4/5] Construyendo instalador...")
    if not INNO_SETUP_PATH.exists():
        print("  - Inno Setup no encontrado, se omite instalador.")
        return
    if not INSTALLER_SCRIPT.exists():
        raise FileNotFoundError(f"No existe {INSTALLER_SCRIPT}")
    run([str(INNO_SETUP_PATH), str(INSTALLER_SCRIPT.name)])


def create_transfer_package() -> None:
    print("[5/5] Preparando paquete para migrar a otro PC...")
    exe_path = DIST_DIR / EXE_NAME
    if not exe_path.exists():
        raise FileNotFoundError(f"No existe ejecutable para empaquetar: {exe_path}")

    data_bundle_script = PROJECT_ROOT / "tools" / "data_bundle.py"
    bundle_path = DIST_DIR / DATA_BUNDLE_NAME
    if data_bundle_script.exists():
        try:
            run(
                [
                    sys.executable,
                    str(data_bundle_script),
                    "export",
                    "--source",
                    str(PROJECT_ROOT),
                    "--output",
                    str(bundle_path),
                ]
            )
            print(f"  - Bundle de datos generado: {bundle_path.name}")
        except Exception as exc:
            print(f"  - [WARN] No se pudo generar bundle de datos: {exc}")
    else:
        print("  - [WARN] tools/data_bundle.py no existe, se omite bundle de datos.")

    portable_dir = DIST_DIR / PORTABLE_DIR_NAME
    portable_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(exe_path, portable_dir / EXE_NAME)
    if bundle_path.exists():
        shutil.copy2(bundle_path, portable_dir / DATA_BUNDLE_NAME)

    launcher_path = portable_dir / "Abrir_GestionStockPro.bat"
    launcher_path.write_text(
        "@echo off\n"
        "setlocal\n"
        "set \"ROOT=%~dp0\"\n"
        "set \"GESTIONSTOCK_DATA_DIR=%ROOT%data\"\n"
        "if not exist \"%GESTIONSTOCK_DATA_DIR%\" mkdir \"%GESTIONSTOCK_DATA_DIR%\"\n"
        "\"%ROOT%GestionStockPro.exe\"\n",
        encoding="utf-8",
    )

    readme_path = portable_dir / "LEEME_TRASPASO.txt"
    readme_path.write_text(
        "PASOS PARA MOVER A OTRO PC\n"
        "1) Copia esta carpeta completa al otro PC.\n"
        "2) Ejecuta Abrir_GestionStockPro.bat.\n"
        "3) Si existe GestionStockPro_data_bundle.zip, la app importa los datos al primer inicio.\n"
        "4) La data quedara dentro de la carpeta local data/ del paquete portable.\n",
        encoding="utf-8",
    )

    zip_base = DIST_DIR / PORTABLE_DIR_NAME
    portable_zip = Path(
        shutil.make_archive(
            str(zip_base),
            "zip",
            root_dir=str(DIST_DIR),
            base_dir=PORTABLE_DIR_NAME,
        )
    )
    print(f"  - Paquete portable: {portable_dir}")
    print(f"  - ZIP portable: {portable_zip}")


def main() -> None:
    print("=" * 58)
    print("   GESTOR DE STOCK PRO - BUILD")
    print("=" * 58)
    try:
        clean_build()
        check_dependencies()
        build_executable()
        create_installer()
        create_transfer_package()
    except Exception as e:
        print(f"[ERROR] Build fallido: {e}")
        raise SystemExit(1)

    print("=" * 58)
    print("[OK] Build completado")
    print("=" * 58)
    print(f"Ejecutable: {DIST_DIR / EXE_NAME}")
    print(f"Bundle datos: {DIST_DIR / DATA_BUNDLE_NAME}")
    print(f"Portable: {DIST_DIR / PORTABLE_DIR_NAME}")


if __name__ == "__main__":
    main()
