#!/usr/bin/env python3
"""Exporta/importa un bundle de datos de GestionStockPro.

Uso rapido:
  python tools/data_bundle.py export
  python tools/data_bundle.py import --bundle dist/GestionStockPro_data_bundle.zip
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


APP_DATA_DIR_NAME = "GestionStockPro"
DATA_FOLDERS = ("facturas", "backups")
DEFAULT_BUNDLE_NAME = "GestionStockPro_data_bundle.zip"


def _create_temp_workspace(prefix: str, preferred_base: Path | None = None) -> Path:
    """Crea carpeta temporal escribible evitando ACLs conflictivas de tempfile."""
    candidates: list[Path] = []
    env_tmp = os.getenv("GESTIONSTOCK_TMP_DIR")
    if env_tmp:
        candidates.append(Path(env_tmp).expanduser())
    if preferred_base is not None:
        candidates.append(preferred_base)
    candidates.append(Path(tempfile.gettempdir()))
    candidates.append(Path.cwd() / ".tmp_data_bundle")

    tried: list[str] = []
    for base in candidates:
        try:
            root = base.expanduser().resolve()
            root.mkdir(parents=True, exist_ok=True)
            path = root / f"{prefix}{uuid4().hex[:12]}"
            path.mkdir(parents=True, exist_ok=False)
            return path
        except Exception:
            tried.append(str(base))
            continue
    raise RuntimeError(f"No se pudo crear carpeta temporal. Intentado en: {', '.join(tried)}")


def _sqlite_snapshot(src_db: Path, dst_db: Path) -> None:
    src = sqlite3.connect(str(src_db), timeout=30)
    dst = sqlite3.connect(str(dst_db), timeout=30)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()


def _copytree_merge(src: Path, dst: Path, overwrite: bool = False) -> None:
    if not src.exists() or not src.is_dir():
        return
    dst.mkdir(parents=True, exist_ok=True)
    for root, dirs, files in os.walk(src):
        root_path = Path(root)
        rel = root_path.relative_to(src)
        target_root = dst / rel
        target_root.mkdir(parents=True, exist_ok=True)
        for name in dirs:
            (target_root / name).mkdir(parents=True, exist_ok=True)
        for name in files:
            src_file = root_path / name
            dst_file = target_root / name
            if dst_file.exists() and not overwrite:
                continue
            shutil.copy2(src_file, dst_file)


def _default_data_dir() -> Path:
    local_appdata = os.getenv("LOCALAPPDATA")
    if local_appdata:
        return Path(local_appdata) / APP_DATA_DIR_NAME
    return Path.home() / "AppData" / "Local" / APP_DATA_DIR_NAME


def _detect_source_dir(explicit: str | None, project_root: Path) -> Path:
    if explicit:
        path = Path(explicit).expanduser().resolve()
        if not (path / "stock.db").exists():
            raise FileNotFoundError(f"No existe stock.db en la carpeta indicada: {path}")
        return path

    candidates: list[Path] = []
    env_dir = os.getenv("GESTIONSTOCK_DATA_DIR")
    if env_dir:
        candidates.append(Path(env_dir).expanduser().resolve())
    candidates.append(_default_data_dir())
    candidates.append(project_root)

    unique: list[Path] = []
    for c in candidates:
        if c not in unique:
            unique.append(c)

    for c in unique:
        if (c / "stock.db").exists():
            return c
    raise FileNotFoundError(
        "No se encontro stock.db en rutas conocidas. Usa --source para indicar la carpeta de datos."
    )


def export_bundle(source_dir: Path, output_zip: Path) -> Path:
    source_dir = source_dir.resolve()
    db_path = source_dir / "stock.db"
    if not db_path.exists():
        raise FileNotFoundError(f"No existe base de datos: {db_path}")

    output_zip.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = _create_temp_workspace("gs_export_", preferred_base=output_zip.parent / ".tmp")
    try:
        data_root = tmp_path / "data"
        data_root.mkdir(parents=True, exist_ok=True)

        db_snapshot = data_root / "stock.db"
        _sqlite_snapshot(db_path, db_snapshot)

        included_dirs: list[str] = []
        for folder in DATA_FOLDERS:
            src = source_dir / folder
            dst = data_root / folder
            if src.exists() and src.is_dir():
                _copytree_merge(src, dst, overwrite=True)
                included_dirs.append(folder)

        manifest = {
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_dir": str(source_dir),
            "includes": {
                "db": "data/stock.db",
                "dirs": included_dirs,
            },
            "app_data_dir_name": APP_DATA_DIR_NAME,
        }
        (tmp_path / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        with zipfile.ZipFile(output_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for file_path in tmp_path.rglob("*"):
                if file_path.is_file():
                    zf.write(file_path, file_path.relative_to(tmp_path).as_posix())
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)

    return output_zip


def _find_bundle_db(extracted_root: Path) -> Path:
    candidates = [
        extracted_root / "data" / "stock.db",
        extracted_root / "stock.db",
    ]
    for c in candidates:
        if c.exists():
            return c
    raise FileNotFoundError("El bundle no contiene data/stock.db")


def import_bundle(bundle_zip: Path, dest_dir: Path, force: bool = False) -> Path:
    bundle_zip = bundle_zip.resolve()
    if not bundle_zip.exists():
        raise FileNotFoundError(f"No existe bundle: {bundle_zip}")

    dest_dir = dest_dir.resolve()
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_db = dest_dir / "stock.db"
    if dest_db.exists() and not force:
        raise FileExistsError(
            f"Ya existe {dest_db}. Usa --force para sobrescribir datos."
        )

    tmp_path = _create_temp_workspace("gs_import_", preferred_base=dest_dir / ".tmp")
    try:
        with zipfile.ZipFile(bundle_zip, "r") as zf:
            zf.extractall(tmp_path)

        src_db = _find_bundle_db(tmp_path)
        if dest_db.exists():
            dest_db.unlink()
        _sqlite_snapshot(src_db, dest_db)

        for folder in DATA_FOLDERS:
            src = tmp_path / "data" / folder
            if not src.exists():
                src = tmp_path / folder
            if not src.exists():
                continue
            dst = dest_dir / folder
            if force and dst.exists():
                shutil.rmtree(dst, ignore_errors=True)
            _copytree_merge(src, dst, overwrite=force)
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)

    return dest_dir


def parse_args() -> argparse.Namespace:
    project_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Exporta/importa data bundle de GestionStockPro")
    sub = parser.add_subparsers(dest="command", required=True)

    exp = sub.add_parser("export", help="Crea un zip con DB + data")
    exp.add_argument("--source", help="Carpeta origen que contiene stock.db")
    exp.add_argument(
        "--output",
        default=str((project_root / "dist" / DEFAULT_BUNDLE_NAME)),
        help="Ruta del zip a generar",
    )

    imp = sub.add_parser("import", help="Restaura data desde un zip bundle")
    imp.add_argument(
        "--bundle",
        required=True,
        help="Ruta del zip de datos",
    )
    imp.add_argument(
        "--dest",
        default=str(_default_data_dir()),
        help="Carpeta de destino (por defecto AppData local)",
    )
    imp.add_argument(
        "--force",
        action="store_true",
        help="Sobrescribe base y carpetas existentes",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    project_root = Path(__file__).resolve().parents[1]

    if args.command == "export":
        src = _detect_source_dir(args.source, project_root=project_root)
        out = Path(args.output).expanduser().resolve()
        bundle = export_bundle(src, out)
        print(f"[OK] Bundle exportado: {bundle}")
        print(f"[INFO] Fuente usada: {src}")
        return 0

    if args.command == "import":
        bundle = Path(args.bundle).expanduser().resolve()
        dst = Path(args.dest).expanduser().resolve()
        final_dir = import_bundle(bundle, dst, force=bool(args.force))
        print(f"[OK] Data importada en: {final_dir}")
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
