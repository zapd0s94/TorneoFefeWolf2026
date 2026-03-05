import pandas as pd

# ✅ Unificamos todos los imports de la base de datos en un solo bloque
from core.database import (
    recuperar_predicciones_piloto,
    actualizar_tabla_general,
    incrementar_estadistica_posiciones,
    lock_exists,
    set_lock,
    get_pred_ts,
    guardar_historial,          # <-- ¡Agregado aquí!
    guardar_historial_detalle,
    borrar_historial_gp,
    borrar_historial_detalle_gp
)
from core.scoring import calcular_puntos


def _norm_keys(d):
    if not isinstance(d, dict):
        return {}
    out = {}
    for k, v in d.items():
        if isinstance(k, str) and k.isdigit():
            out[int(k)] = v
        else:
            out[k] = v
    return out


def _winner_por_puntos(rows_cat, gp_calc, etapa):
    """
    rows_cat: lista de dicts {"Piloto":..., "PTS": int}
    Devuelve (piloto_ganador, pts_ganador)
    Empate -> menor ts gana.
    """
    if not rows_cat:
        return None, 0
    max_pts = max(r["PTS"] for r in rows_cat)
    empatados = [r["Piloto"] for r in rows_cat if r["PTS"] == max_pts]
    if len(empatados) == 1:
        return empatados[0], max_pts

    mejor = None
    mejor_ts = None
    for p in empatados:
        ts = get_pred_ts(p, gp_calc, etapa)
        if mejor is None or (ts is not None and mejor_ts is not None and ts < mejor_ts) or (mejor_ts is None and ts is not None):
            mejor = p
            mejor_ts = ts
    return mejor, max_pts


def calcular_y_actualizar_todos(gp_calc: str, oficial: dict, pilotos_torneo: list, gps_sprint: list):
    # ✅ LOCK: 1 sola vez por GP
    lock_key = f"GP_DONE::{gp_calc}"
    if lock_exists(lock_key):
        return pd.DataFrame([{
            "Piloto": "-",
            "Carrera": 0,
            "Const": 0,
            "Carrera+Const": 0,
            "Qualy": 0,
            "Sprint": 0,
            "Total": 0,
            "OK": False,
            "Mensaje": f"⚠️ {gp_calc} ya fue calculado. Bloqueado para evitar doble suma."
        }])

    # 🧹 IMPORTANTE: Limpiamos historial previo de este GP por si hubo un recálculo
    borrar_historial_gp(gp_calc)
    borrar_historial_detalle_gp(gp_calc)

    of_r = {i: oficial.get(f"r{i}", "") for i in range(1, 11)}
    of_q = {i: oficial.get(f"q{i}", "") for i in range(1, 6)}
    of_c = {i: oficial.get(f"c{i}", "") for i in range(1, 4)}
    of_s = {i: oficial.get(f"s{i}", "") for i in range(1, 9)}

    rows = []
    cat_qualy = []
    cat_sprint = []
    cat_carrera = []

    for piloto in pilotos_torneo:
        db_qualy, db_sprint, (db_race, db_const) = recuperar_predicciones_piloto(piloto, gp_calc)

        val_r = _norm_keys(db_race or {})
        val_q = _norm_keys(db_qualy or {})
        val_c = _norm_keys(db_const or {})
        val_s = _norm_keys(db_sprint or {})

        pts_carrera = calcular_puntos("CARRERA", val_r, of_r, val_r.get("colapinto_r"), oficial.get("col_r"))
        pts_const  = calcular_puntos("CONSTRUCTORES", val_c, of_c)
        pts_carr_total = pts_carrera + pts_const

        pts_qualy  = calcular_puntos("QUALY", val_q, of_q, val_q.get("colapinto_q"), oficial.get("col_q"))

        pts_sprint = 0
        if gp_calc in gps_sprint and db_sprint:
            pts_sprint = calcular_puntos("SPRINT", val_s, of_s)

        total = pts_carr_total + pts_qualy + pts_sprint

        # ✅ GUARDAR DETALLE POR ETAPA (SIEMPRE)
        guardar_historial_detalle(gp_calc, piloto, "QUALY", int(pts_qualy))
        guardar_historial_detalle(gp_calc, piloto, "CARRERA", int(pts_carrera))
        guardar_historial_detalle(gp_calc, piloto, "CONSTRUCTORES", int(pts_const))
        guardar_historial_detalle(gp_calc, piloto, "CARRERA_CONST", int(pts_carr_total))
        if gp_calc in gps_sprint:
            guardar_historial_detalle(gp_calc, piloto, "SPRINT", int(pts_sprint))

        # ✅ LA SOLUCIÓN: GUARDAR EL HISTORIAL GENERAL DEL GP
        guardar_historial(str(gp_calc), str(piloto), int(total))

        # Actualizamos la tabla general de posiciones
        ok, msg = actualizar_tabla_general(piloto, total, gp_calc)

        rows.append({
            "Piloto": piloto,
            "Carrera": pts_carrera,
            "Const": pts_const,
            "Carrera+Const": pts_carr_total,
            "Qualy": pts_qualy,
            "Sprint": pts_sprint,
            "Total": total,
            "OK": ok,
            "Mensaje": msg
        })

        cat_qualy.append({"Piloto": piloto, "PTS": pts_qualy})
        cat_carrera.append({"Piloto": piloto, "PTS": pts_carr_total})
        if gp_calc in gps_sprint:
            cat_sprint.append({"Piloto": piloto, "PTS": pts_sprint})

    # ✅ ganadores únicos por categoría
    wq, _ = _winner_por_puntos(cat_qualy, gp_calc, "QUALY")
    if wq:
        incrementar_estadistica_posiciones(wq, "Qualys", 1)

    ws, _ = _winner_por_puntos(cat_sprint, gp_calc, "SPRINT")
    if gp_calc in gps_sprint and ws:
        incrementar_estadistica_posiciones(ws, "Sprints", 1)

    wc, _ = _winner_por_puntos(cat_carrera, gp_calc, "CARRERA")
    if wc:
        incrementar_estadistica_posiciones(wc, "Carreras", 1)

    set_lock(lock_key)
    return pd.DataFrame(rows)


def generar_historial_solo(gp_calc: str, oficial: dict, pilotos_torneo: list, gps_sprint: list):
    """
    Genera/rehace historial SQLite (tabla_historial + historial_detalle) SIN sumar puntos en Google Sheets.
    Útil cuando el GP ya está cerrado por lock, pero querés que Historial por GP muestre datos.
    """
    # 1) limpiamos historial previo de ese GP para no duplicar
    borrar_historial_gp(gp_calc)
    borrar_historial_detalle_gp(gp_calc)

    of_r = {i: oficial.get(f"r{i}", "") for i in range(1, 11)}
    of_q = {i: oficial.get(f"q{i}", "") for i in range(1, 6)}
    of_c = {i: oficial.get(f"c{i}", "") for i in range(1, 4)}
    of_s = {i: oficial.get(f"s{i}", "") for i in range(1, 9)}

    rows = []

    for piloto in pilotos_torneo:
        db_qualy, db_sprint, (db_race, db_const) = recuperar_predicciones_piloto(piloto, gp_calc)

        val_r = _norm_keys(db_race or {})
        val_q = _norm_keys(db_qualy or {})
        val_c = _norm_keys(db_const or {})
        val_s = _norm_keys(db_sprint or {})

        pts_carrera = calcular_puntos("CARRERA", val_r, of_r, val_r.get("colapinto_r"), oficial.get("col_r"))
        pts_const  = calcular_puntos("CONSTRUCTORES", val_c, of_c)
        pts_carr_total = pts_carrera + pts_const
        pts_qualy  = calcular_puntos("QUALY", val_q, of_q, val_q.get("colapinto_q"), oficial.get("col_q"))

        pts_sprint = 0
        if gp_calc in gps_sprint and db_sprint:
            pts_sprint = calcular_puntos("SPRINT", val_s, of_s)

        # Guardamos detalle
        guardar_historial_detalle(gp_calc, piloto, "QUALY", int(pts_qualy))
        guardar_historial_detalle(gp_calc, piloto, "CARRERA", int(pts_carrera))
        guardar_historial_detalle(gp_calc, piloto, "CONSTRUCTORES", int(pts_const))
        guardar_historial_detalle(gp_calc, piloto, "CARRERA_CONST", int(pts_carr_total))
        if gp_calc in gps_sprint:
            guardar_historial_detalle(gp_calc, piloto, "SPRINT", int(pts_sprint))

        total = int(pts_carr_total + pts_qualy + pts_sprint)

        # Guardamos historial total (por GP)
        guardar_historial(gp=str(gp_calc), piloto=str(piloto), puntos=int(total))

        rows.append({
            "Piloto": piloto,
            "Carrera": int(pts_carrera),
            "Const": int(pts_const),
            "Carrera+Const": int(pts_carr_total),
            "Qualy": int(pts_qualy),
            "Sprint": int(pts_sprint),
            "Total": int(total),
            "OK": True,
            "Mensaje": "Historial generado (sin sumar a Posiciones)"
        })

    return pd.DataFrame(rows)