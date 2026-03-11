import os
import shutil
import sqlite3
import sys
import tempfile
import zipfile


APP_VERSION = "4.85"
APP_DISPLAY_NAME = "GestionStockPro"
APP_DATA_DIR_NAME = "GestionStockPro"
MIGRATABLE_DATA_DIRS = ("facturas", "backups")
DATA_BUNDLE_FILENAMES = (
    "GestionStockPro_data_bundle.zip",
    "gestionstock_data_bundle.zip",
    "data_bundle.zip",
)


def _sqlite_copy(src, dst):
    try:
        src_conn = sqlite3.connect(src, timeout=30)
        dst_conn = sqlite3.connect(dst, timeout=30)
        try:
            src_conn.backup(dst_conn)
        finally:
            dst_conn.close()
            src_conn.close()
        return True
    except Exception:
        try:
            shutil.copy2(src, dst)
            return True
        except Exception:
            return False


def _directory_has_files(path):
    if not os.path.isdir(path):
        return False
    for _root, _dirs, files in os.walk(path):
        if files:
            return True
    return False


def _copytree_merge_missing(src, dst):
    if not _directory_has_files(src):
        return False

    src_abs = os.path.abspath(src)
    dst_abs = os.path.abspath(dst)
    if src_abs == dst_abs:
        return False

    copied = False
    os.makedirs(dst_abs, exist_ok=True)
    for root, dirs, files in os.walk(src_abs):
        rel = os.path.relpath(root, src_abs)
        dst_root = dst_abs if rel == "." else os.path.join(dst_abs, rel)
        os.makedirs(dst_root, exist_ok=True)

        for dir_name in dirs:
            os.makedirs(os.path.join(dst_root, dir_name), exist_ok=True)

        for file_name in files:
            src_file = os.path.join(root, file_name)
            dst_file = os.path.join(dst_root, file_name)
            if os.path.exists(dst_file):
                continue
            try:
                shutil.copy2(src_file, dst_file)
                copied = True
            except Exception:
                continue
    return copied


def get_data_directory():
    """Retorna la carpeta de datos persistente de la aplicacion."""
    env_dir = os.getenv("GESTIONSTOCK_DATA_DIR")
    if env_dir:
        data_dir = os.path.abspath(env_dir)
    elif getattr(sys, "frozen", False):
        local_appdata = os.getenv("LOCALAPPDATA")
        if local_appdata:
            data_dir = os.path.join(local_appdata, APP_DATA_DIR_NAME)
        else:
            data_dir = os.path.join(
                os.path.expanduser("~"),
                "AppData",
                "Local",
                APP_DATA_DIR_NAME,
            )
    else:
        data_dir = os.path.dirname(os.path.abspath(__file__))

    os.makedirs(data_dir, exist_ok=True)
    return data_dir


def _legacy_data_directories(data_dir):
    candidates = []
    if getattr(sys, "frozen", False):
        exe_dir = os.path.dirname(os.path.abspath(sys.executable))
        candidates.append(exe_dir)
    candidates.append(os.getcwd())
    candidates.append(os.path.dirname(os.path.abspath(__file__)))

    unique = []
    data_dir_abs = os.path.abspath(data_dir)
    for path in candidates:
        abs_path = os.path.abspath(path)
        if abs_path == data_dir_abs:
            continue
        if abs_path not in unique and os.path.isdir(abs_path):
            unique.append(abs_path)
    return unique


def _legacy_db_candidates(data_dir, legacy_dirs=None):
    roots = legacy_dirs or _legacy_data_directories(data_dir)
    unique = []
    for root in roots:
        legacy_db = os.path.join(root, "stock.db")
        abs_path = os.path.abspath(legacy_db)
        if abs_path not in unique and os.path.exists(abs_path):
            unique.append(abs_path)
    return unique


def _migrate_legacy_side_dirs(data_dir, legacy_dirs):
    for folder in MIGRATABLE_DATA_DIRS:
        dst_dir = os.path.join(data_dir, folder)
        if _directory_has_files(dst_dir):
            continue
        for legacy_root in legacy_dirs:
            src_dir = os.path.join(legacy_root, folder)
            if _copytree_merge_missing(src_dir, dst_dir):
                break


def _find_data_bundle_file(legacy_dirs):
    for root in legacy_dirs:
        for file_name in DATA_BUNDLE_FILENAMES:
            candidate = os.path.join(root, file_name)
            if os.path.isfile(candidate):
                return os.path.abspath(candidate)
    return None


def _extract_data_bundle(bundle_path, dst_dir):
    try:
        with zipfile.ZipFile(bundle_path, "r") as zf:
            zf.extractall(dst_dir)
        return True
    except Exception:
        return False


def _import_data_bundle_if_needed(data_dir, legacy_dirs):
    target_db = os.path.join(data_dir, "stock.db")
    if os.path.exists(target_db):
        return False

    bundle_path = _find_data_bundle_file(legacy_dirs)
    if not bundle_path:
        return False

    with tempfile.TemporaryDirectory(prefix="gs_bundle_") as tmp_dir:
        if not _extract_data_bundle(bundle_path, tmp_dir):
            return False

        db_candidates = [
            os.path.join(tmp_dir, "stock.db"),
            os.path.join(tmp_dir, "data", "stock.db"),
        ]
        src_db = next((p for p in db_candidates if os.path.isfile(p)), None)
        if not src_db:
            return False

        if not _sqlite_copy(src_db, target_db):
            return False

        for folder in MIGRATABLE_DATA_DIRS:
            dst_folder = os.path.join(data_dir, folder)
            for prefix in ("", "data"):
                src_folder = (
                    os.path.join(tmp_dir, folder)
                    if not prefix
                    else os.path.join(tmp_dir, prefix, folder)
                )
                _copytree_merge_missing(src_folder, dst_folder)
    return True


def get_database_path(data_dir, legacy_dirs=None):
    env_db = os.getenv("GESTIONSTOCK_DB_PATH")
    if env_db:
        db_path = os.path.abspath(env_db)
        parent = os.path.dirname(db_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        return db_path

    db_path = os.path.join(data_dir, "stock.db")
    if os.path.exists(db_path):
        return db_path

    for legacy_path in _legacy_db_candidates(data_dir, legacy_dirs=legacy_dirs):
        if legacy_path == os.path.abspath(db_path):
            continue
        if _sqlite_copy(legacy_path, db_path):
            break
    return db_path


DATA_DIR = get_data_directory()
LEGACY_DATA_DIRS = _legacy_data_directories(DATA_DIR)
if getattr(sys, "frozen", False):
    _import_data_bundle_if_needed(DATA_DIR, LEGACY_DATA_DIRS)
DB_PATH = get_database_path(DATA_DIR, legacy_dirs=LEGACY_DATA_DIRS)
if getattr(sys, "frozen", False):
    _migrate_legacy_side_dirs(DATA_DIR, LEGACY_DATA_DIRS)
BACKUP_DIR = os.path.join(os.path.dirname(DB_PATH), "backups")

os.makedirs(BACKUP_DIR, exist_ok=True)


if __name__ == "__main__":
    print(f"DATA_DIR: {DATA_DIR}")
    print(f"DB_PATH: {DB_PATH}")
