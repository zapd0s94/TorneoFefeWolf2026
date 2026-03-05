# core/auth.py
from __future__ import annotations
import hmac
import os
import base64
import hashlib
from datetime import datetime
import pytz

from core.database import conectar_google_sheets

TZ = pytz.timezone("America/Argentina/Buenos_Aires")

# ---------------------------
# Hash seguro (PBKDF2)
# ---------------------------

def _hash_secret(secret: str, salt: bytes | None = None, iters: int = 210_000) -> str:
    """
    Devuelve: pbkdf2$iters$salt_b64$hash_b64
    """
    if secret is None:
        secret = ""
    secret = str(secret).strip()
    if not secret:
        return ""

    if salt is None:
        salt = os.urandom(16)

    dk = hashlib.pbkdf2_hmac("sha256", secret.encode("utf-8"), salt, iters, dklen=32)
    return f"pbkdf2${iters}${base64.b64encode(salt).decode('utf-8')}${base64.b64encode(dk).decode('utf-8')}"

def _verify_secret(secret: str, stored: str) -> bool:
    try:
        if not stored or not stored.startswith("pbkdf2$"):
            return False
        _, iters_s, salt_b64, hash_b64 = stored.split("$", 3)
        iters = int(iters_s)
        salt = base64.b64decode(salt_b64.encode("utf-8"))
        expected = base64.b64decode(hash_b64.encode("utf-8"))

        dk = hashlib.pbkdf2_hmac("sha256", str(secret).strip().encode("utf-8"), salt, iters, dklen=32)
        return hmac.compare_digest(dk, expected)
    except Exception:
        return False

# ---------------------------
# Sheets helpers
# ---------------------------

def _open_ws(nombre: str):
    sheet1 = conectar_google_sheets("sheet1")
    if not sheet1:
        return None
    try:
        return sheet1.spreadsheet.worksheet(nombre)
    except Exception:
        return None

def _users_ws():
    return _open_ws("Usuarios")

def _audit_ws():
    return _open_ws("Audit")

def _colmap(ws):
    headers = ws.row_values(1)
    return {h.strip(): i + 1 for i, h in enumerate(headers)}

def audit(usuario: str, accion: str, detalle: str = ""):
    try:
        ws = _audit_ws()
        if not ws:
            return
        ts = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
        ws.append_row([ts, str(usuario), str(accion), str(detalle)], value_input_option="USER_ENTERED")
    except Exception:
        return

# ---------------------------
# Usuarios: lectura
# ---------------------------

def get_user_row(usuario: str):
    ws = _users_ws()
    if not ws:
        return None, "No se pudo conectar a hoja Usuarios."
    try:
        objetivo = str(usuario or "").strip().lower()
        records = ws.get_all_records()
        for idx, r in enumerate(records, start=2):  # fila 1 headers
            u = str(r.get("usuario", "")).strip().lower()
            if u == objetivo:
                return (idx, r), None
        return None, "Usuario no existe."
    except Exception as e:
        return None, f"Error leyendo Usuarios: {e}"

# ---------------------------
# Bootstrap / Login
# ---------------------------

def bootstrap_user(usuario: str, rol: str, password_inicial: str, mother_code: str, copas: int = 0, color: str = "gray"):
    ws = _users_ws()
    if not ws:
        return False, "No se pudo conectar a hoja Usuarios."

    row, _ = get_user_row(usuario)
    if row:
        return False, "Ese usuario ya existe."

    pw_hash = _hash_secret(password_inicial)
    mother_hash = _hash_secret(mother_code)
    ts = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")

    # Importante: esta append_row tiene que coincidir con tus columnas actuales
    # usuario | rol | pw_hash | mother_hash | copas | color | creado | ultimo_login | forzar_cambio | pin_hash
    ws.append_row([usuario, rol, pw_hash, mother_hash, int(copas), color, ts, "", 0, ""], value_input_option="USER_ENTERED")
    audit(usuario, "BOOTSTRAP", f"rol={rol}, copas={copas}")
    return True, "Usuario creado."

def login(usuario: str, password: str):
    found, err = get_user_row(usuario)
    if not found:
        return False, err

    row_idx, data = found
    if not _verify_secret(password, str(data.get("pw_hash", ""))):
        audit(usuario, "LOGIN_FAIL", "password incorrecto")
        return False, "Contraseña incorrecta."

    ws = _users_ws()
    ts = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    try:
        headers = ws.row_values(1)
        col_last = headers.index("ultimo_login") + 1
        ws.update_cell(row_idx, col_last, ts)
    except Exception:
        pass

    audit(usuario, "LOGIN_OK", "")
    perfil = {
        "usuario": str(data.get("usuario", usuario)),
        "rol": str(data.get("rol", "")),
        "copas": int(data.get("copas", 0) or 0),
        "color": str(data.get("color", "gray")),
        "forzar_cambio": int(data.get("forzar_cambio", 0) or 0),
    }
    return True, perfil

def change_password(usuario: str, new_password: str, old_password: str | None = None):
    found, err = get_user_row(usuario)
    if not found:
        return False, err

    row_idx, data = found

    if not new_password or len(str(new_password).strip()) < 4:
        return False, "La nueva contraseña es muy corta."

    force = int(data.get("forzar_cambio", 0) or 0)

    if not old_password or str(old_password).strip() == "":
        if force != 1:
            audit(usuario, "PW_CHANGE_FAIL", "faltó old_password y no estaba forzado")
            return False, "Falta contraseña actual."
    else:
        if not _verify_secret(old_password, str(data.get("pw_hash", ""))):
            audit(usuario, "PW_CHANGE_FAIL", "old password incorrecto")
            return False, "Contraseña actual incorrecta."

    ws = _users_ws()
    cm = _colmap(ws)
    if "pw_hash" not in cm or "forzar_cambio" not in cm:
        return False, "Faltan columnas pw_hash o forzar_cambio en Usuarios."

    ws.update_cell(row_idx, cm["pw_hash"], _hash_secret(new_password))
    ws.update_cell(row_idx, cm["forzar_cambio"], 0)
    audit(usuario, "PW_CHANGE_OK", "forced" if force == 1 else "normal")
    return True, "Contraseña cambiada."

def reset_password_with_mother(usuario: str, mother_code: str, new_password: str):
    found, err = get_user_row(usuario)
    if not found:
        return False, err

    row_idx, data = found
    if not _verify_secret(mother_code, str(data.get("mother_hash", ""))):
        audit(usuario, "PW_RESET_FAIL", "mother incorrecta")
        return False, "Mother code incorrecto."

    ws = _users_ws()
    cm = _colmap(ws)
    ws.update_cell(row_idx, cm["pw_hash"], _hash_secret(new_password))
    ws.update_cell(row_idx, cm["forzar_cambio"], 0)
    audit(usuario, "PW_RESET_OK", "")
    return True, "Contraseña reseteada."

# ---------------------------
# Admin helpers
# ---------------------------

def admin_update_user_fields(usuario_objetivo: str, **fields):
    found, err = get_user_row(usuario_objetivo)
    if not found:
        return False, err

    row_idx, _data = found
    ws = _users_ws()
    cm = _colmap(ws)

    allowed = {"rol", "copas", "color", "forzar_cambio"}
    updates = []
    for k, v in fields.items():
        if k not in allowed:
            continue
        if k not in cm:
            return False, f"Falta la columna '{k}' en la hoja Usuarios."
        if k == "copas":
            v = int(v or 0)
        if k == "forzar_cambio":
            v = int(v or 0)
        updates.append((cm[k], v))

    if not updates:
        return False, "No hay campos válidos para actualizar."

    for col, val in updates:
        ws.update_cell(row_idx, col, val)

    audit("ADMIN", "UPDATE_USER", f"{usuario_objetivo} -> {fields}")
    return True, "Usuario actualizado."

def admin_reset_password(usuario_objetivo: str, new_password: str, forzar_cambio: int = 1):
    found, err = get_user_row(usuario_objetivo)
    if not found:
        return False, err

    row_idx, _data = found
    ws = _users_ws()
    cm = _colmap(ws)

    if "pw_hash" not in cm:
        return False, "Falta la columna 'pw_hash' en Usuarios."
    if "forzar_cambio" not in cm:
        return False, "Falta la columna 'forzar_cambio' en Usuarios."

    ws.update_cell(row_idx, cm["pw_hash"], _hash_secret(new_password))
    ws.update_cell(row_idx, cm["forzar_cambio"], int(forzar_cambio))
    audit("ADMIN", "RESET_PASSWORD", f"{usuario_objetivo} force={forzar_cambio}")
    return True, "Password reseteada."

# ---------------------------
# PIN separado (4 dígitos)
# ---------------------------

def _is_valid_pin(pin: str) -> bool:
    pin = str(pin or "").strip()
    return pin.isdigit() and len(pin) == 4

def set_pin(usuario_objetivo: str, new_pin: str):
    if not _is_valid_pin(new_pin):
        return False, "El PIN debe ser numérico de 4 dígitos."

    found, err = get_user_row(usuario_objetivo)
    if not found:
        return False, err

    row_idx, _data = found
    ws = _users_ws()
    headers = ws.row_values(1)
    if "pin_hash" not in headers:
        return False, "Falta la columna 'pin_hash' en la hoja Usuarios."

    col_pin = headers.index("pin_hash") + 1
    ws.update_cell(row_idx, col_pin, _hash_secret(new_pin))

    audit("ADMIN", "SET_PIN", f"{usuario_objetivo}")
    return True, "PIN actualizado ✅"

def verify_pin(usuario: str, pin: str) -> bool:
    if not _is_valid_pin(pin):
        return False

    found, _ = get_user_row(usuario)
    if not found:
        return False

    _row_idx, data = found
    stored = str(data.get("pin_hash", "")).strip()
    if not stored or not stored.startswith("pbkdf2$"):
        return False

    return _verify_secret(pin, stored)

