"""
core/database.py — Torneo Fefe Wolf 2026
===========================================
CAMBIOS v2 (fix historial):
  - guardar_historial()        → escribe en Google Sheets hoja "Historial"
  - guardar_historial_detalle()→ escribe en Google Sheets hoja "HistorialDetalle"
  - leer_historial_df()        → lee desde Google Sheets hoja "Historial"
  - leer_historial_detalle_df()→ lee desde Google Sheets hoja "HistorialDetalle"
  - borrar_historial_gp()      → borra filas del GP en "Historial"
  - borrar_historial_detalle_gp()→ borra filas del GP en "HistorialDetalle"

  SQLite se mantiene SOLO para locks (archivo local, no necesita persistencia cross-deploy).

ESTRUCTURA esperada en Google Sheets:
  Hoja "Historial"        → columnas: gp | piloto | puntos | ts
  Hoja "HistorialDetalle" → columnas: gp | piloto | etapa  | puntos | ts
"""

import os
import sys
from datetime import datetime
import sqlite3
import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials

# ──────────────────────────────────────────────
# PATHS (SQLite solo para locks)
# ──────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)
LOCK_DB_PATH = os.path.join(ROOT_DIR, "locks.db")


# ==============================================================================
# CONEXIÓN GOOGLE SHEETS
# ==============================================================================

def conectar_google_sheets(nombre_hoja="sheet1"):
    """
    Abre la hoja 'nombre_hoja' del spreadsheet TorneoFefe2026_DB.
    Retorna el objeto worksheet o None si falla.
    """
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    try:
        import json
        creds_dict = json.loads(json.dumps(dict(st.secrets["GCP_SERVICE_ACCOUNT"])))
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
        client = gspread.authorize(creds)
        ss = client.open("TorneoFefe2026_DB")

        if nombre_hoja and nombre_hoja != "sheet1":
            return ss.worksheet(nombre_hoja)
        return ss.sheet1

    except Exception as e:
        st.error(f"❌ ERROR Google Sheets [{nombre_hoja}]: {type(e).__name__}: {e}")
        print(f"ERROR SHEETS [{nombre_hoja}]: {type(e).__name__}: {e}")
        return None


def _ts_ahora() -> str:
    """Timestamp Argentina ISO sin microsegundos."""
    try:
        import pytz
        tz = pytz.timezone("America/Argentina/Buenos_Aires")
        return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.now().replace(microsecond=0).isoformat()


# ==============================================================================
# HISTORIAL RESUMEN — Google Sheets hoja "Historial"
# Columnas: gp | piloto | puntos | ts
# ==============================================================================

def _ensure_historial_headers(sheet):
    """Crea la fila de encabezados si la hoja está vacía."""
    try:
        primera = sheet.row_values(1)
        if not primera or primera[0].strip().lower() != "gp":
            sheet.insert_row(["gp", "piloto", "puntos", "ts"], index=1)
    except Exception as e:
        print(f"WARN _ensure_historial_headers: {e}")


def guardar_historial(gp: str, piloto: str, puntos: int):
    """
    Agrega una fila en la hoja 'Historial' de Google Sheets.
    Reemplaza la escritura en SQLite local.
    """
    sheet = conectar_google_sheets("Historial")
    if sheet is None:
        print(f"ERROR guardar_historial: no se pudo conectar a hoja Historial")
        return

    try:
        _ensure_historial_headers(sheet)
        fila = [
            str(gp).strip(),
            str(piloto).strip(),
            int(puntos),
            _ts_ahora(),
        ]
        sheet.append_row(fila, value_input_option="USER_ENTERED")
    except Exception as e:
        print(f"ERROR guardar_historial ({piloto}, {gp}): {e}")


def borrar_historial_gp(gp: str):
    """
    Elimina TODAS las filas del GP indicado en la hoja 'Historial'.
    Se usa antes de regenerar el historial para evitar duplicados.
    """
    sheet = conectar_google_sheets("Historial")
    if sheet is None:
        return

    gp_s = str(gp).strip()
    try:
        rows = sheet.get_all_values()
        # Recorremos de abajo hacia arriba para no desincronizar índices al borrar
        indices_a_borrar = []
        for idx, row in enumerate(rows, start=1):
            if idx == 1:
                continue  # saltar encabezado
            if row and str(row[0]).strip() == gp_s:
                indices_a_borrar.append(idx)

        # Borrar en orden inverso
        for idx in sorted(indices_a_borrar, reverse=True):
            sheet.delete_rows(idx)

    except Exception as e:
        print(f"ERROR borrar_historial_gp ({gp}): {e}")


def leer_historial(gp=None) -> pd.DataFrame:
    """
    Lee la hoja 'Historial' y retorna un DataFrame.
    Si se pasa gp, filtra por ese GP.
    """
    sheet = conectar_google_sheets("Historial")
    if sheet is None:
        return pd.DataFrame(columns=["gp", "piloto", "puntos", "ts"])

    try:
        rows = sheet.get_all_values()
        if not rows or len(rows) < 2:
            return pd.DataFrame(columns=["gp", "piloto", "puntos", "ts"])

        headers = [h.strip().lower() for h in rows[0]]
        data = rows[1:]
        df = pd.DataFrame(data, columns=headers)

        # Normalizar columnas esperadas
        for col in ["gp", "piloto", "puntos", "ts"]:
            if col not in df.columns:
                df[col] = "" if col != "puntos" else 0

        df["gp"]     = df["gp"].astype(str).str.strip()
        df["piloto"] = df["piloto"].astype(str).str.strip()
        df["puntos"] = pd.to_numeric(df["puntos"], errors="coerce").fillna(0).astype(int)
        df["ts"]     = df["ts"].astype(str).str.strip()

        if gp:
            df = df[df["gp"] == str(gp).strip()]

        return df[["gp", "piloto", "puntos", "ts"]].reset_index(drop=True)

    except Exception as e:
        print(f"ERROR leer_historial: {e}")
        return pd.DataFrame(columns=["gp", "piloto", "puntos", "ts"])


def leer_historial_df(gp=None) -> pd.DataFrame:
    """
    Wrapper principal que usan pantalla_historial_gp() y admin_tools.
    Retorna DataFrame con columnas: gp, piloto, puntos, ts
    """
    try:
        df = leer_historial(gp)
        if df is None or df.empty:
            return pd.DataFrame(columns=["gp", "piloto", "puntos", "ts"])
        return df
    except Exception:
        return pd.DataFrame(columns=["gp", "piloto", "puntos", "ts"])


# ==============================================================================
# HISTORIAL DETALLE — Google Sheets hoja "HistorialDetalle"
# Columnas: gp | piloto | etapa | puntos | ts
# ==============================================================================

def _ensure_historial_detalle_headers(sheet):
    """Crea la fila de encabezados si la hoja está vacía."""
    try:
        primera = sheet.row_values(1)
        if not primera or primera[0].strip().lower() != "gp":
            sheet.insert_row(["gp", "piloto", "etapa", "puntos", "ts"], index=1)
    except Exception as e:
        print(f"WARN _ensure_historial_detalle_headers: {e}")


def guardar_historial_detalle(gp: str, piloto: str, etapa: str, puntos: int):
    """
    Agrega una fila en la hoja 'HistorialDetalle' de Google Sheets.
    Reemplaza la escritura en SQLite local.
    """
    sheet = conectar_google_sheets("HistorialDetalle")
    if sheet is None:
        print(f"ERROR guardar_historial_detalle: no se pudo conectar a hoja HistorialDetalle")
        return

    try:
        _ensure_historial_detalle_headers(sheet)
        fila = [
            str(gp).strip(),
            str(piloto).strip(),
            str(etapa).strip().upper(),
            int(puntos),
            _ts_ahora(),
        ]
        sheet.append_row(fila, value_input_option="USER_ENTERED")
    except Exception as e:
        print(f"ERROR guardar_historial_detalle ({piloto}, {gp}, {etapa}): {e}")


def borrar_historial_detalle_gp(gp: str):
    """
    Elimina TODAS las filas del GP indicado en la hoja 'HistorialDetalle'.
    """
    sheet = conectar_google_sheets("HistorialDetalle")
    if sheet is None:
        return

    gp_s = str(gp).strip()
    try:
        rows = sheet.get_all_values()
        indices_a_borrar = []
        for idx, row in enumerate(rows, start=1):
            if idx == 1:
                continue
            if row and str(row[0]).strip() == gp_s:
                indices_a_borrar.append(idx)

        for idx in sorted(indices_a_borrar, reverse=True):
            sheet.delete_rows(idx)

    except Exception as e:
        print(f"ERROR borrar_historial_detalle_gp ({gp}): {e}")


def leer_historial_detalle(gp=None) -> pd.DataFrame:
    """
    Lee la hoja 'HistorialDetalle' y retorna un DataFrame.
    """
    sheet = conectar_google_sheets("HistorialDetalle")
    if sheet is None:
        return pd.DataFrame(columns=["gp", "piloto", "etapa", "puntos", "ts"])

    try:
        rows = sheet.get_all_values()
        if not rows or len(rows) < 2:
            return pd.DataFrame(columns=["gp", "piloto", "etapa", "puntos", "ts"])

        headers = [h.strip().lower() for h in rows[0]]
        data = rows[1:]
        df = pd.DataFrame(data, columns=headers)

        for col in ["gp", "piloto", "etapa", "puntos", "ts"]:
            if col not in df.columns:
                df[col] = "" if col != "puntos" else 0

        df["gp"]     = df["gp"].astype(str).str.strip()
        df["piloto"] = df["piloto"].astype(str).str.strip()
        df["etapa"]  = df["etapa"].astype(str).str.strip().str.upper()
        df["puntos"] = pd.to_numeric(df["puntos"], errors="coerce").fillna(0).astype(int)
        df["ts"]     = df["ts"].astype(str).str.strip()

        if gp:
            df = df[df["gp"] == str(gp).strip()]

        return df[["gp", "piloto", "etapa", "puntos", "ts"]].reset_index(drop=True)

    except Exception as e:
        print(f"ERROR leer_historial_detalle: {e}")
        return pd.DataFrame(columns=["gp", "piloto", "etapa", "puntos", "ts"])


def leer_historial_detalle_df() -> pd.DataFrame:
    """
    Wrapper principal que usa pantalla_historial_gp() y admin_tools.
    Retorna DataFrame con columnas: gp, piloto, etapa, puntos, ts
    """
    try:
        df = leer_historial_detalle(None)
        if df is None or df.empty:
            return pd.DataFrame(columns=["gp", "piloto", "etapa", "puntos", "ts"])
        return df
    except Exception:
        return pd.DataFrame(columns=["gp", "piloto", "etapa", "puntos", "ts"])


# ==============================================================================
# HELPERS predicciones (sheet1)
# ==============================================================================

def _find_pred_row(sheet, usuario: str, gp: str, etapa: str):
    try:
        values = sheet.get_all_values()
        if not values or len(values) < 2:
            return None

        u = str(usuario).strip()
        g = str(gp).strip()
        e = str(etapa).strip().upper()

        for idx, row in enumerate(values[1:], start=2):
            if len(row) < 4:
                continue
            if row[1].strip() == u and row[2].strip() == g and row[3].strip().upper() == e:
                return idx
        return None
    except Exception:
        return None


def existe_prediccion(usuario: str, gp: str, etapa: str) -> bool:
    sheet = conectar_google_sheets("sheet1")
    if sheet is None:
        return False
    return _find_pred_row(sheet, usuario, gp, etapa) is not None


def guardar_etapa(usuario, gp, etapa, datos, camp_data=None):
    sheet = conectar_google_sheets("sheet1")
    if sheet is None:
        return False, "Error CRÍTICO: No se pudo conectar con la Base de Datos."

    etapa = str(etapa).strip().upper()

    if existe_prediccion(usuario, gp, etapa):
        return False, f"⚠️ Ya enviaste {etapa} para {gp}. No se permite enviar dos veces."

    try:
        import pytz as _ptz_db
        from datetime import datetime as _dt_db
        ts = _dt_db.now(_ptz_db.timezone("America/Argentina/Buenos_Aires")).strftime("%Y-%m-%d %H:%M:%S")
        base = [ts, str(usuario).strip(), str(gp).strip(), etapa]
        datos = datos if isinstance(datos, dict) else {}

        if etapa == "QUALY":
            fila = base + [
                datos.get(1, ""),
                datos.get(2, ""),
                datos.get(3, ""),
                datos.get(4, ""),
                datos.get(5, ""),
                datos.get("colapinto_q", ""),
            ]
            if str(gp).strip() == "01. Gran Premio de Australia":
                cd = camp_data if isinstance(camp_data, dict) else {}
                fila += [cd.get("piloto", ""), cd.get("equipo", "")]
            else:
                fila += ["", ""]

        elif etapa == "SPRINT":
            fila = base + [datos.get(i, "") for i in range(1, 9)]

        elif etapa == "CARRERA":
            fila = base + [datos.get(i, "") for i in range(1, 11)]
            fila += [datos.get("colapinto_r", "")]
            fila += [datos.get("c1", ""), datos.get("c2", ""), datos.get("c3", "")]
            fila += ["", ""]

        else:
            return False, f"Etapa desconocida: {etapa}"

        sheet.append_row(fila, value_input_option="USER_ENTERED")
        return True, "✅ Predicción guardada correctamente."

    except Exception as e:
        return False, f"Error guardando predicción: {e}"


def recuperar_predicciones_piloto(usuario, gp):
    sheet = conectar_google_sheets("sheet1")
    if not sheet:
        return None, None, (None, None)

    try:
        registros = sheet.get_all_values()
    except Exception:
        return None, None, (None, None)

    data_q, data_s, data_r, data_c = {}, {}, {}, {}
    found_q = found_s = found_r = False

    for row in registros[1:]:
        if len(row) < 4:
            continue

        if str(row[1]).strip() != str(usuario).strip() or str(row[2]).strip() != str(gp).strip():
            continue

        etapa = str(row[3]).strip().upper()

        if etapa == "QUALY":
            if len(row) >= 10:
                for i in range(1, 6):
                    data_q[i] = row[3 + i]
                data_q["colapinto_q"] = row[9]
                data_q["camp_piloto"] = row[10] if len(row) > 10 else ""
                data_q["camp_equipo"] = row[11] if len(row) > 11 else ""
                found_q = True

        elif etapa == "SPRINT":
            if len(row) >= 12:
                for i in range(1, 9):
                    data_s[i] = row[3 + i]
                found_s = True

        elif etapa == "CARRERA":
            if len(row) >= 18:
                for i in range(1, 11):
                    data_r[i] = row[3 + i]
                data_r["colapinto_r"] = row[14]
                data_c[1] = row[15]
                data_c[2] = row[16]
                data_c[3] = row[17]
                found_r = True

    res_q = data_q if found_q else None
    res_s = data_s if found_s else None
    res_r = (data_r, data_c) if found_r else (None, None)
    return res_q, res_s, res_r


# ==============================================================================
# DESEMPATE por timestamp (QUIÉN ENVIÓ PRIMERO)
# ==============================================================================

def obtener_ts_prediccion(usuario: str, gp: str, etapa: str):
    sheet = conectar_google_sheets("sheet1")
    if sheet is None:
        return None

    u = str(usuario).strip()
    g = str(gp).strip()
    e = str(etapa).strip().upper()

    try:
        rows = sheet.get_all_values()
    except Exception:
        return None

    for row in rows[1:]:
        if len(row) < 4:
            continue
        if str(row[1]).strip() == u and str(row[2]).strip() == g and str(row[3]).strip().upper() == e:
            ts = (row[0] or "").strip()
            try:
                return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            except Exception:
                try:
                    return datetime.fromisoformat(ts)
                except Exception:
                    return None
    return None


def get_pred_ts(usuario: str, gp: str, etapa: str):
    ts = obtener_ts_prediccion(usuario, gp, etapa)
    return ts if ts is not None else datetime.max


# ==============================================================================
# TABLA POSICIONES (worksheet "Posiciones")
# ==============================================================================

def leer_tabla_posiciones(pilotos_torneo):
    sheet = conectar_google_sheets("Posiciones")
    if not sheet:
        return pd.DataFrame(
            {"Piloto": pilotos_torneo, "Puntos": [0] * len(pilotos_torneo),
             "Qualys": [0] * len(pilotos_torneo), "Sprints": [0] * len(pilotos_torneo),
             "Carreras": [0] * len(pilotos_torneo)}
        )

    try:
        data = sheet.get_all_records()
        if not data:
            raise ValueError("Hoja vacía o sin encabezados")
        return pd.DataFrame(data)
    except Exception:
        return pd.DataFrame(
            {"Piloto": pilotos_torneo, "Puntos": [0] * len(pilotos_torneo),
             "Qualys": [0] * len(pilotos_torneo), "Sprints": [0] * len(pilotos_torneo),
             "Carreras": [0] * len(pilotos_torneo)}
        )


def actualizar_tabla_general(piloto, puntos, gp):
    sheet = conectar_google_sheets("Posiciones")
    if not sheet:
        return False, "No se pudo abrir hoja Posiciones."

    try:
        piloto = str(piloto).strip()
        puntos_nuevos = int(puntos)

        headers = sheet.row_values(1)
        if not headers:
            return False, "Hoja Posiciones sin encabezados."

        colmap = {h.strip(): i + 1 for i, h in enumerate(headers)}

        if "Piloto" not in colmap or "Puntos" not in colmap:
            return False, "Faltan columnas requeridas: Piloto y/o Puntos."

        records = sheet.get_all_values()
        fila_piloto = None
        for r in range(2, len(records) + 1):
            val = (records[r - 1][colmap["Piloto"] - 1] or "").strip()
            if val == piloto:
                fila_piloto = r
                break

        if fila_piloto is None:
            fila_piloto = len(records) + 1
            nueva_fila = [""] * len(headers)
            nueva_fila[colmap["Piloto"] - 1] = piloto
            for k in ["Puntos", "Qualys", "Sprints", "Carreras"]:
                if k in colmap:
                    nueva_fila[colmap[k] - 1] = "0"
            sheet.append_row(nueva_fila, value_input_option="USER_ENTERED")

        cell_val = sheet.cell(fila_piloto, colmap["Puntos"]).value
        pts_actual = int(cell_val) if cell_val not in (None, "", " ") else 0

        nuevo_total = pts_actual + puntos_nuevos
        sheet.update_cell(fila_piloto, colmap["Puntos"], nuevo_total)

        return True, f"✅ {piloto} actualizado: +{puntos_nuevos} pts (Total: {nuevo_total})"
    except Exception as e:
        return False, f"Error actualizando tabla: {e}"


def incrementar_estadistica_posiciones(piloto: str, campo: str, delta: int = 1):
    sheet = conectar_google_sheets("Posiciones")
    if not sheet:
        return False, "No se pudo abrir hoja Posiciones."

    piloto = str(piloto).strip()
    campo = str(campo).strip()

    headers = sheet.row_values(1)
    if not headers:
        return False, "Hoja Posiciones sin encabezados."

    colmap = {h.strip(): i + 1 for i, h in enumerate(headers)}
    if "Piloto" not in colmap or campo not in colmap:
        return False, f"Faltan columnas requeridas: Piloto y/o {campo}"

    records = sheet.get_all_values()
    fila = None
    for r in range(2, len(records) + 1):
        val = (records[r - 1][colmap["Piloto"] - 1] or "").strip()
        if val == piloto:
            fila = r
            break

    if fila is None:
        fila = len(records) + 1
        nueva = [""] * len(headers)
        nueva[colmap["Piloto"] - 1] = piloto
        for k in ["Puntos", "Qualys", "Sprints", "Carreras"]:
            if k in colmap:
                nueva[colmap[k] - 1] = "0"
        sheet.append_row(nueva, value_input_option="USER_ENTERED")

    cur_val = sheet.cell(fila, colmap[campo]).value
    cur_int = int(cur_val) if cur_val not in (None, "", " ") else 0
    sheet.update_cell(fila, colmap[campo], cur_int + int(delta))
    return True, "OK"


# ==============================================================================
# LOCKS — SQLite LOCAL (los locks son efímeros, no necesitan persistencia)
# ==============================================================================

def _lock_db():
    con = sqlite3.connect(LOCK_DB_PATH, check_same_thread=False)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS locks (
            k TEXT PRIMARY KEY,
            ts TEXT NOT NULL
        )
        """
    )
    con.commit()
    return con


def lock_exists(k: str) -> bool:
    con = _lock_db()
    try:
        cur = con.execute("SELECT 1 FROM locks WHERE k = ?", (str(k),))
        return cur.fetchone() is not None
    finally:
        con.close()


def set_lock(k: str):
    con = _lock_db()
    try:
        con.execute(
            "INSERT OR REPLACE INTO locks (k, ts) VALUES (?, ?)",
            (str(k), datetime.now().replace(microsecond=0).isoformat()),
        )
        con.commit()
    finally:
        con.close()


def clear_lock(k: str):
    con = _lock_db()
    try:
        con.execute("DELETE FROM locks WHERE k = ?", (str(k),))
        con.commit()
    finally:
        con.close()


def clear_all_locks():
    con = _lock_db()
    try:
        con.execute("DELETE FROM locks")
        con.commit()
    finally:
        con.close()


# ==============================================================================
# CAMPEONES + DNS
# ==============================================================================

def _norm_txt(x: str) -> str:
    return str(x or "").strip().lower()


def aplicar_bonus_campeones_final(
    gp_final: str,
    piloto_campeon_real: str,
    constructor_campeon_real: str,
    gp_prediccion_campeones: str,
    pilotos_torneo: list
):
    lock_key = f"CHAMP_DONE::{gp_final}"
    if lock_exists(lock_key):
        return False, f"⚠️ Bonus campeones ya aplicado para {gp_final}. Bloqueado."

    pil_real = _norm_txt(piloto_campeon_real)
    con_real = _norm_txt(constructor_campeon_real)

    resumen = []
    for piloto in pilotos_torneo:
        db_qualy, _, _ = recuperar_predicciones_piloto(piloto, gp_prediccion_campeones)

        pred_pil = _norm_txt((db_qualy or {}).get("camp_piloto", ""))
        pred_con = _norm_txt((db_qualy or {}).get("camp_equipo", ""))

        bonus = 0
        if pred_pil and pred_pil == pil_real:
            bonus += 50
            guardar_historial_detalle(gp_final, piloto, "BONUS_CHAMP_PILOTO", 50)
        if pred_con and pred_con == con_real:
            bonus += 25
            guardar_historial_detalle(gp_final, piloto, "BONUS_CHAMP_CONST", 25)

        if bonus:
            actualizar_tabla_general(piloto, bonus, gp_final)

        resumen.append({
            "Piloto": piloto,
            "Pred_Piloto": (db_qualy or {}).get("camp_piloto", ""),
            "Pred_Const": (db_qualy or {}).get("camp_equipo", ""),
            "Bonus": bonus
        })

    set_lock(lock_key)
    return True, pd.DataFrame(resumen)


def detectar_faltantes_por_gp(gp: str, pilotos_torneo: list, gps_sprint: list):
    out = {p: {"QUALY": False, "SPRINT": False, "CARRERA": False} for p in pilotos_torneo}

    sheet = conectar_google_sheets("sheet1")
    if sheet is None:
        return out

    try:
        rows = sheet.get_all_values()
    except Exception:
        return out

    gp_s = str(gp).strip()
    pilotos_set = set(str(p).strip() for p in pilotos_torneo)
    want_sprint = gp in gps_sprint

    for row in rows[1:]:
        if len(row) < 4:
            continue
        usuario = str(row[1]).strip()
        gp_row  = str(row[2]).strip()
        etapa   = str(row[3]).strip().upper()

        if gp_row != gp_s:
            continue
        if usuario not in pilotos_set:
            continue

        if etapa == "QUALY":
            out[usuario]["QUALY"] = True
        elif etapa == "CARRERA":
            out[usuario]["CARRERA"] = True
        elif want_sprint and etapa == "SPRINT":
            out[usuario]["SPRINT"] = True

    return out


def aplicar_sanciones_dns(gp: str, pilotos_torneo: list, gps_sprint: list):
    """
    Detecta quiénes no enviaron predicciones para el GP y aplica -5 pts por etapa faltante.
    Guarda el detalle en HistorialDetalle con etapa='DNS'.
    """
    falt = detectar_faltantes_por_gp(gp, pilotos_torneo, gps_sprint)
    want_sprint = gp in gps_sprint

    rows = []
    for piloto in pilotos_torneo:
        miss = []
        if not falt[piloto]["QUALY"]:
            miss.append("QUALY")
        if want_sprint and not falt[piloto]["SPRINT"]:
            miss.append("SPRINT")
        if not falt[piloto]["CARRERA"]:
            miss.append("CARRERA")

        penalty = -5 * len(miss)
        if penalty != 0:
            actualizar_tabla_general(piloto, penalty, gp)
            guardar_historial_detalle(gp, piloto, "DNS", penalty)

        rows.append({
            "Piloto":      piloto,
            "Faltantes":   ", ".join(miss) if miss else "-",
            "Penalización": penalty
        })

    return pd.DataFrame(rows)
