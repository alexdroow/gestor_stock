import math


def normalize_unit(unit_raw):
    unit = str(unit_raw or "").strip().lower()
    unit = (
        unit.replace("\u00e1", "a")
        .replace("\u00e9", "e")
        .replace("\u00ed", "i")
        .replace("\u00f3", "o")
        .replace("\u00fa", "u")
    )
    aliases = {
        "u": "unidad",
        "und": "unidad",
        "unid": "unidad",
        "unidad": "unidad",
        "unidades": "unidad",
        "pieza": "unidad",
        "piezas": "unidad",
        "g": "gr",
        "gr": "gr",
        "gramo": "gr",
        "gramos": "gr",
        "kg": "kg",
        "kilo": "kg",
        "kilogramo": "kg",
        "kilogramos": "kg",
        "ml": "ml",
        "cc": "ml",
        "mililitro": "ml",
        "mililitros": "ml",
        "l": "lt",
        "lt": "lt",
        "litro": "lt",
        "litros": "lt",
        "porcion": "porcion",
        "porciones": "porcion",
    }
    return aliases.get(unit, unit or "unidad")


def unit_type(unit_raw):
    unit = normalize_unit(unit_raw)
    if unit in {"mg", "g", "gr", "kg", "oz", "lb"}:
        return "solido"
    if unit in {"ml", "cc", "lt", "l", "taza", "cda", "cdt"}:
        return "liquido"
    return "generico"


def units_compatible(unit_1, unit_2):
    return unit_type(unit_1) == unit_type(unit_2)


def convert_amount(amount, unit_origin, unit_target, convert_to_base_func):
    origin = normalize_unit(unit_origin)
    target = normalize_unit(unit_target)
    try:
        qty = float(amount or 0)
    except (TypeError, ValueError):
        return {"success": False, "error": "Cantidad inválida"}

    if qty <= 0:
        return {"success": False, "error": "La cantidad debe ser mayor a 0"}

    if origin == target:
        return {"success": True, "cantidad": qty, "unidad": target}

    if not units_compatible(origin, target):
        return {"success": False, "error": f"Unidades incompatibles ({origin} vs {target})"}

    base_qty = convert_to_base_func(qty, origin)
    target_factor = convert_to_base_func(1, target)
    if not target_factor:
        return {"success": False, "error": f"No se pudo convertir unidad destino: {target}"}
    return {"success": True, "cantidad": base_qty / target_factor, "unidad": target}


def format_simple_number(value):
    try:
        number = float(value or 0)
    except (TypeError, ValueError):
        return "0"
    if math.isfinite(number) and number.is_integer():
        return str(int(number))
    return f"{number:.4f}".rstrip("0").rstrip(".")
