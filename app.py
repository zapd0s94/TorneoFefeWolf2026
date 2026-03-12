import os, sys, base64, hmac, hashlib, secrets, sqlite3, html as _html
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
try:
    import plotly.graph_objects as go
    import plotly.express as px
    _PLOTLY_OK = True
except ImportError:
    _PLOTLY_OK = False
import pytz, requests, concurrent.futures
from datetime import datetime, timedelta
from collections import defaultdict

# ─────────────────────────────────────────────────────────
# 1. PAGE CONFIG — siempre primero
# ─────────────────────────────────────────────────────────
st.set_page_config(page_title="Torneo Fefe Wolf 2026", layout="wide", page_icon="🏆")
st.markdown('<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">',
            unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────
# 2. CSS — cacheado, se inyecta ANTES de cualquier import lento
# ─────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def _css():
    css_path = os.path.join("ui","styles.css")
    if not os.path.exists(css_path): return ""
    with open(css_path,"r",encoding="utf-8") as f: css = f.read()
    img = os.path.join("ui","FORMULEROS.jpg")
    bg  = ""
    if os.path.exists(img):
        with open(img,"rb") as f2: bg = "data:image/jpeg;base64,"+base64.b64encode(f2.read()).decode()
    return css.replace("__LOGIN_BG__", bg)

def load_css():
    c = _css()
    if c: st.markdown(f"<style>{c}</style>", unsafe_allow_html=True)

load_css()   # ← CSS se inyecta inmediatamente. Página ya "se ve".

# ─────────────────────────────────────────────────────────
# 3. LAZY MODULE LOADERS con @st.cache_resource
#    Se ejecutan UNA sola vez (server lifetime) y de forma lazy.
#    F5 → instantáneo porque ya están cacheados.
#    Si GSheets tarda → solo la primera carga paga el precio.
# ─────────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def _mod_auth():
    try:
        from core.auth import (login, change_password, reset_password_with_mother,
                               bootstrap_user, get_user_row, admin_update_user_fields,
                               admin_reset_password, verify_pin, set_pin)
        return dict(login=login, change_password=change_password,
                    reset_password_with_mother=reset_password_with_mother,
                    bootstrap_user=bootstrap_user, get_user_row=get_user_row,
                    admin_update_user_fields=admin_update_user_fields,
                    admin_reset_password=admin_reset_password,
                    verify_pin=verify_pin, set_pin=set_pin)
    except Exception as e:
        return {"_error": str(e)}

@st.cache_resource(show_spinner=False)
def _mod_db():
    try:
        from core.database import (guardar_etapa, recuperar_predicciones_piloto,
                                   leer_tabla_posiciones, actualizar_tabla_general,
                                   guardar_historial, leer_historial_df,
                                   leer_historial_detalle_df, lock_exists, set_lock,
                                   aplicar_sanciones_dns, aplicar_bonus_campeones_final)
        return dict(guardar_etapa=guardar_etapa,
                    recuperar_predicciones_piloto=recuperar_predicciones_piloto,
                    leer_tabla_posiciones=leer_tabla_posiciones,
                    actualizar_tabla_general=actualizar_tabla_general,
                    guardar_historial=guardar_historial,
                    leer_historial_df=leer_historial_df,
                    leer_historial_detalle_df=leer_historial_detalle_df,
                    lock_exists=lock_exists, set_lock=set_lock,
                    aplicar_sanciones_dns=aplicar_sanciones_dns,
                    aplicar_bonus_campeones_final=aplicar_bonus_campeones_final)
    except Exception as e:
        return {"_error": str(e)}

@st.cache_resource(show_spinner=False)
def _mod_admin():
    try:
        from core.admin_tools import calcular_y_actualizar_todos, generar_historial_solo
        return dict(calcular=calcular_y_actualizar_todos, historial=generar_historial_solo)
    except Exception as e:
        return {"_error": str(e)}

@st.cache_resource(show_spinner=False)
def _mod_core():
    try:
        from core.scoring import calcular_puntos
        from core.rules  import obtener_estado_gp
        from core.utils  import normalizar_nombre
        return dict(calcular_puntos=calcular_puntos,
                    obtener_estado_gp=obtener_estado_gp,
                    normalizar_nombre=normalizar_nombre)
    except Exception as e:
        return {"_error": str(e)}

@st.cache_resource(show_spinner=False)
def _mod_mesa():
    try:
        from core.mesa_chica_db import (mc_is_mod, mc_badge_for, mc_is_spam,
                                         mc_add_message, mc_list_messages,
                                         mc_update_message, mc_soft_delete_message,
                                         mc_purge_html_messages, _mc_safe_text,
                                         mc_toggle_like, mc_like_count, mc_user_liked)
        return dict(mc_is_mod=mc_is_mod, mc_badge_for=mc_badge_for,
                    mc_is_spam=mc_is_spam, mc_add_message=mc_add_message,
                    mc_list_messages=mc_list_messages,
                    mc_update_message=mc_update_message,
                    mc_soft_delete_message=mc_soft_delete_message,
                    mc_purge_html_messages=mc_purge_html_messages,
                    _mc_safe_text=_mc_safe_text,
                    mc_toggle_like=mc_toggle_like,
                    mc_like_count=mc_like_count,
                    mc_user_liked=mc_user_liked)
    except Exception as e:
        return {"_error": str(e)}

# Helpers de acceso a módulos lazy (con fallback si error)
def _auth(fn, *a, default=(False,"Módulo no disponible"), timeout=10, **kw):
    m = _mod_auth()
    if "_error" in m or fn not in m: return default
    return _safe_call(m[fn], *a, timeout_sec=timeout, default=default, **kw)

def _db(fn, *a, default=None, timeout=8, **kw):
    m = _mod_db()
    if "_error" in m or fn not in m: return default
    return _safe_call(m[fn], *a, timeout_sec=timeout, default=default, **kw)

def _core(fn, *a, default=None, timeout=4, **kw):
    m = _mod_core()
    if "_error" in m or fn not in m: return default
    return _safe_call(m[fn], *a, timeout_sec=timeout, default=default, **kw)

def _safe_call(fn, *a, timeout_sec=8, default=None, **kw):
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(fn, *a, **kw)
        try:    return fut.result(timeout=timeout_sec)
        except: return default

# ─────────────────────────────────────────────────────────
# 4. CONSTANTES
# ─────────────────────────────────────────────────────────
TZ = pytz.timezone("America/Argentina/Buenos_Aires")
API_BASE = "http://127.0.0.1:8000"

HORARIOS_CARRERA = {
    "01. Gran Premio de Australia":     "2026-03-08 01:00",
    "02. Gran Premio de China":         "2026-03-15 04:00",
    "03. Gran Premio de Japón":         "2026-03-29 02:00",
    "04. Gran Premio de Baréin":        "2026-04-12 12:00",
    "05. Gran Premio de Arabia Saudita":"2026-04-19 14:00",
    "06. Gran Premio de Miami":         "2026-05-03 17:00",
    "07. Gran Premio de Canadá":        "2026-05-24 17:00",
    "08. Gran Premio de Mónaco":        "2026-06-07 10:00",
    "09. Gran Premio de Barcelona":     "2026-06-14 10:00",
    "10. Gran Premio de Austria":       "2026-06-28 10:00",
    "11. Gran Premio de Gran Bretaña":  "2026-07-05 11:00",
    "12. Gran Premio de Bélgica":       "2026-07-19 10:00",
    "13. Gran Premio de Hungría":       "2026-07-26 10:00",
    "14. Gran Premio de los Países Bajos":"2026-08-23 10:00",
    "15. Gran Premio de Italia":        "2026-09-06 10:00",
    "16. Gran Premio de Madrid":        "2026-09-13 10:00",
    "17. Gran Premio de Azerbaiyán":    "2026-09-27 08:00",
    "18. Gran Premio de Singapur":      "2026-10-11 09:00",
    "19. Gran Premio de los Estados Unidos":"2026-10-25 17:00",
    "20. Gran Premio de México":        "2026-11-01 17:00",
    "21. Gran Premio de Brasil":        "2026-11-08 14:00",
    "22. Gran Premio de Las Vegas":     "2026-11-21 01:00",
    "23. Gran Premio de Qatar":         "2026-11-29 13:00",
    "24. Gran Premio de Abu Dabi":      "2026-12-06 10:00",
}
GPS_OFICIALES = list(HORARIOS_CARRERA.keys())
GPS_SPRINT = [
    "02. Gran Premio de China","06. Gran Premio de Miami",
    "07. Gran Premio de Canadá","11. Gran Premio de Gran Bretaña",
    "14. Gran Premio de los Países Bajos","18. Gran Premio de Singapur",
]
PILOTOS_TORNEO = ["Checo Perez","Nicki Lauda","Valteri Bottas","Lando Norris","Fernando Alonso"]
GRILLA_2026 = {
    "MCLAREN":      ["Lando Norris","Oscar Piastri"],
    "RED BULL":     ["Max Verstappen","Isack Hadjar"],
    "MERCEDES":     ["Kimi Antonelli","George Russell"],
    "FERRARI":      ["Charles Leclerc","Lewis Hamilton"],
    "WILLIAMS":     ["Alex Albon","Carlos Sainz"],
    "ASTON MARTIN": ["Lance Stroll","Fernando Alonso"],
    "RACING BULLS": ["Liam Lawson","Arvid Lindblad"],
    "HAAS":         ["Oliver Bearman","Esteban Ocon"],
    "AUDI":         ["Nico Hulkenberg","Gabriel Bortoleto"],
    "ALPINE":       ["Pierre Gasly","Franco Colapinto"],
    "CADILLAC":     ["Checo Perez","Valteri Bottas"],
}
ESCALA_CARRERA_JUEGO = {1:25,2:18,3:15,4:12,5:10,6:8,7:6,8:4,9:2,10:1}
PILOTO_COLORS = {
    "Checo Perez":"#D4AF37","Nicki Lauda":"#1E90FF","Lando Norris":"#FFA500","Fernando Alonso":"#FF4444",
    "Valteri Bottas":"#00CFFF",
}
TEAM_COLORS = {
    "MCLAREN":"#FF8000","RED BULL":"#3671C6","MERCEDES":"#00D2BE",
    "FERRARI":"#DC0000","WILLIAMS":"#005AFF","ASTON MARTIN":"#006F62",
    "RACING BULLS":"#2B4562","HAAS":"#B6BABD","AUDI":"#00E676",
    "ALPINE":"#FF4FD8","CADILLAC":"#E6C200",
}
TEAM_LOGOS_SVG = {
    "MCLAREN":"MCL","RED BULL":"RBR","MERCEDES":"AMG","FERRARI":"SF",
    "WILLIAMS":"WRC","ASTON MARTIN":"AMR","RACING BULLS":"RB",
    "HAAS":"HAA","AUDI":"AUD","ALPINE":"ALP","CADILLAC":"CAD",
}
DRIVER_PHOTOS = {
    # F1 official 2025 headshots (stable CDN)
    "Lando Norris":      "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/mclaren/lannor01/2026mclarenlannor01right.webp",
    "Oscar Piastri":     "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/mclaren/oscpia01/2026mclarenoscpia01right.webp",
    "Max Verstappen":    "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/redbullracing/maxver01/2026redbullracingmaxver01right.webp",
    "Isack Hadjar": "https://media.formula1.com/image/upload/c_lfill,w_440/q_auto/v1740000000/common/f1/2026/redbullracing/isahad01/2026redbullracingisahad01right.webp",
    "George Russell":    "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/mercedes/georus01/2026mercedesgeorus01right.webp",
    "Kimi Antonelli": "https://media.formula1.com/image/upload/c_lfill,w_440/q_auto/v1740000000/common/f1/2026/mercedes/andant01/2026mercedesandant01right.webp",
    "Charles Leclerc":   "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/ferrari/chalec01/2026ferrarichalec01right.webp",
    "Lewis Hamilton":    "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/ferrari/lewham01/2026ferrarilewham01right.webp",
    "Alex Albon":        "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/williams/alealb01/2026williamsalealb01right.webp",
    "Carlos Sainz":      "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/williams/carsai01/2026williamscarsai01right.webp",
    "Lance Stroll":      "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/astonmartin/lanstr01/2026astonmartinlanstr01right.webp",
    "Fernando Alonso":   "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/astonmartin/feralo01/2026astonmartinferalo01right.webp",
    "Liam Lawson":       "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/racingbulls/lialaw01/2026racingbullslialaw01right.webp",
    "Arvid Lindblad":    "https://media.formula1.com/image/upload/c_lfill,w_440/q_auto/v1740000000/common/f1/2026/racingbulls/arvlin01/2026racingbullsarvlin01right.webp",
    "Oliver Bearman":    "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/haas/olibea01/2026haasolibea01right.webp",
    "Esteban Ocon":      "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/haas/estoco01/2026haasestoco01right.webp",
    "Nico Hulkenberg":   "https://media.formula1.com/image/upload/c_lfill,w_440/q_auto/v1740000000/common/f1/2026/audi/nichul01/2026audinichul01right.webp",
    "Gabriel Bortoleto": "https://media.formula1.com/image/upload/c_lfill,w_440/q_auto/v1740000000/common/f1/2026/audi/gabbor01/2026audigabbor01right.webp",
    "Pierre Gasly":      "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/alpine/piegas01/2026alpinepiegas01right.webp",
    "Franco Colapinto":  "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/alpine/fracol01/2026alpinefracol01right.webp",
    "Checo Perez":       "https://media.formula1.com/image/upload/c_lfill,w_440/q_auto/v1740000000/common/f1/2026/cadillac/serper01/2026cadillacserper01right.webp",
    "Valteri Bottas":    "https://media.formula1.com/image/upload/c_lfill,w_440/q_auto/v1740000000/common/f1/2026/cadillac/valbot01/2026cadillacvalbot01right.webp",
}
MESA_CHICA_MODS   = {"Valteri Bottas","Lando Norris","Fernando Alonso"}
MESA_CHICA_BADGES = {
    "Valteri Bottas": {"tipo":"fipf",      "label":"MIEMBRO FIPF","stars":"★"},
    "Lando Norris":   {"tipo":"fipf",      "label":"MIEMBRO FIPF","stars":""},
    "Fernando Alonso":{"tipo":"fipf",      "label":"MIEMBRO FIPF","stars":""},
    "Checo Perez":    {"tipo":"formulero", "label":"FORMULERO",   "stars":"★★★"},
    "Nicki Lauda":    {"tipo":"formulero", "label":"FORMULERO",   "stars":"★"},
}
CALENDARIO_VISUAL = [
    {"Fecha":"06-08 Mar","Gran Premio":"GP Australia",      "Circuito":"Melbourne",          "Formato":"Clásico"},
    {"Fecha":"13-15 Mar","Gran Premio":"GP China",          "Circuito":"Shanghai",           "Formato":"⚡ SPRINT"},
    {"Fecha":"27-29 Mar","Gran Premio":"GP Japón",          "Circuito":"Suzuka",             "Formato":"Clásico"},
    {"Fecha":"10-12 Abr","Gran Premio":"GP Bahréin",        "Circuito":"Sakhir",             "Formato":"Clásico"},
    {"Fecha":"17-19 Abr","Gran Premio":"GP Arabia Saudita", "Circuito":"Jeddah",             "Formato":"Clásico"},
    {"Fecha":"01-03 May","Gran Premio":"GP Miami",          "Circuito":"Miami",              "Formato":"⚡ SPRINT"},
    {"Fecha":"22-24 May","Gran Premio":"GP Canadá",         "Circuito":"Montreal",           "Formato":"⚡ SPRINT"},
    {"Fecha":"05-07 Jun","Gran Premio":"GP Mónaco",         "Circuito":"Montecarlo",         "Formato":"Clásico"},
    {"Fecha":"12-14 Jun","Gran Premio":"GP España",         "Circuito":"Barcelona",          "Formato":"Clásico"},
    {"Fecha":"26-28 Jun","Gran Premio":"GP Austria",        "Circuito":"Spielberg",          "Formato":"Clásico"},
    {"Fecha":"03-05 Jul","Gran Premio":"GP Gran Bretaña",   "Circuito":"Silverstone",        "Formato":"⚡ SPRINT"},
    {"Fecha":"17-19 Jul","Gran Premio":"GP Bélgica",        "Circuito":"Spa",                "Formato":"Clásico"},
    {"Fecha":"24-26 Jul","Gran Premio":"GP Hungría",        "Circuito":"Budapest",           "Formato":"Clásico"},
    {"Fecha":"21-23 Ago","Gran Premio":"GP Países Bajos",   "Circuito":"Zandvoort",          "Formato":"⚡ SPRINT"},
    {"Fecha":"04-06 Sep","Gran Premio":"GP Italia",         "Circuito":"Monza",              "Formato":"Clásico"},
    {"Fecha":"11-13 Sep","Gran Premio":"GP Madrid",         "Circuito":"Madrid",             "Formato":"Clásico"},
    {"Fecha":"25-27 Sep","Gran Premio":"GP Azerbaiyán",     "Circuito":"Bakú",               "Formato":"Clásico"},
    {"Fecha":"09-11 Oct","Gran Premio":"GP Singapur",       "Circuito":"Marina Bay",         "Formato":"⚡ SPRINT"},
    {"Fecha":"23-25 Oct","Gran Premio":"GP Estados Unidos", "Circuito":"Austin",             "Formato":"Clásico"},
    {"Fecha":"30-01 Nov","Gran Premio":"GP México",         "Circuito":"Hermanos Rodríguez", "Formato":"Clásico"},
    {"Fecha":"06-08 Nov","Gran Premio":"GP Brasil",         "Circuito":"Interlagos",         "Formato":"Clásico"},
    {"Fecha":"19-21 Nov","Gran Premio":"GP Las Vegas",      "Circuito":"Las Vegas",          "Formato":"Clásico"},
    {"Fecha":"27-29 Nov","Gran Premio":"GP Qatar",          "Circuito":"Lusail",             "Formato":"Clásico"},
    {"Fecha":"04-06 Dic","Gran Premio":"GP Abu Dabi",       "Circuito":"Yas Marina",         "Formato":"Clásico"},
]

# ─────────────────────────────────────────────────────────
# 5. AUTH TOKENS (local, sin DB, sin GSheets)
# ─────────────────────────────────────────────────────────
def _auth_secret():
    try:
        from streamlit.errors import StreamlitSecretNotFoundError
        s = st.secrets.get("AUTH_SECRET", None)
    except Exception: s = None
    return s or os.getenv("AUTH_SECRET","DEV_SECRET_FW_2026_CAMBIAR")

def _b64u(b): return base64.urlsafe_b64encode(b).decode().rstrip("=")
def _b64ud(s):
    pad = "="*(-len(s)%4)
    return base64.urlsafe_b64decode((s+pad).encode())

def auth_create_token(usuario, hours=168):
    exp = int((datetime.utcnow()+timedelta(hours=hours)).timestamp())
    payload = f"{usuario}|{exp}|{secrets.token_urlsafe(8)}".encode()
    sig = hmac.new(_auth_secret().encode(), payload, hashlib.sha256).digest()
    return f"{_b64u(payload)}.{_b64u(sig)}"

def auth_user_from_token(token):
    try:
        if not token or "." not in token: return None
        p64,s64 = token.split(".",1)
        payload = _b64ud(p64); sig = _b64ud(s64)
        good = hmac.new(_auth_secret().encode(), payload, hashlib.sha256).digest()
        if not hmac.compare_digest(sig,good): return None
        usuario,exp_str,_ = payload.decode().split("|",2)
        if int(exp_str) < int(datetime.utcnow().timestamp()): return None
        return usuario
    except: return None

# ─────────────────────────────────────────────────────────
# 6. PERFIL CACHEADO (GSheets, TTL 5 min)
# ─────────────────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner=False)
def _get_perfil(usuario: str):
    m = _mod_auth()
    if "_error" in m or "get_user_row" not in m: return None
    def _do():
        result, _ = m["get_user_row"](usuario)
        if not result: return None
        data = result[1] if (isinstance(result,tuple) and len(result)==2) else result
        if isinstance(data,dict):
            return {"usuario":str(data.get("usuario",usuario)),
                    "rol":str(data.get("rol","")),
                    "copas":int(data.get("copas",0) or 0),
                    "color":str(data.get("color","gray")),
                    "forzar_cambio":int(data.get("forzar_cambio",0) or 0)}
        if isinstance(data,(list,tuple)) and len(data)>0:
            return {"usuario":data[0],
                    "rol":data[1] if len(data)>1 else "",
                    "copas":int(data[2]) if len(data)>2 else 0,
                    "color":data[3] if len(data)>3 else "white",
                    "forzar_cambio":int(data[4]) if len(data)>4 else 0}
        return None
    return _safe_call(_do, timeout_sec=30, default=None)

# ─────────────────────────────────────────────────────────
# 7. HELPERS UI
# ─────────────────────────────────────────────────────────
def qp_get(k): v=st.query_params.get(k,None); return (v[0] if isinstance(v,list) else v)
def qp_set(k,v): qp=dict(st.query_params); qp[k]=v; st.query_params.update(qp)

def is_logged_in(): return st.session_state.get("perfil") is not None
def is_admin():
    rol = str((st.session_state.get("perfil") or {}).get("rol","")).lower()
    return "admin" in rol or "comisario" in rol

def logout():
    try: st.query_params.clear()
    except: pass
    for k in ("perfil","usuario","_tok_done"): st.session_state[k] = None if k!="_tok_done" else False
    try: _get_perfil.clear()
    except: pass
    components.html('<script>localStorage.removeItem("fw_token");</script>',height=0)

def _driver_avatar_html(nombre, color="#a855f7", size=48):
    url = DRIVER_PHOTOS.get(nombre,"")
    ini = "".join(w[0] for w in nombre.split()[:2]).upper()
    if url:
        return (f'<div style="width:{size}px;height:{size}px;border-radius:50%;overflow:hidden;'
                f'border:2px solid {color};flex-shrink:0;">'
                f'<img src="{url}" width="{size}" height="{size}" style="object-fit:cover;" '
                f'onerror="this.style.display=\'none\'"/></div>')
    return (f'<div style="width:{size}px;height:{size}px;border-radius:50%;'
            f'background:{color}33;border:2px solid {color};display:flex;align-items:center;'
            f'justify-content:center;font-weight:900;color:{color};font-size:{size//3}px;'
            f'flex-shrink:0;">{ini}</div>')

def render_dark_table(df):
    df2 = df.copy()
    if {"Piloto","Puntos"}.issubset(df2.columns):
        df2 = df2.sort_values("Puntos",ascending=False).reset_index(drop=True)
        medals={0:"🥇",1:"🥈",2:"🥉"}
        df2.insert(0,"#",[medals.get(i,f"{i+1}") for i in range(len(df2))])
    h = (df2.to_html(index=False,escape=False)
         .replace('<table border="1" class="dataframe">','<table class="fw-table">'))
    st.markdown(f'<div class="fw-table-wrap">{h}</div>', unsafe_allow_html=True)

def normalizar_keys_num(d):
    if not isinstance(d,dict): return {}
    return {int(k) if (isinstance(k,str) and k.isdigit()) else k:v for k,v in d.items()}

def calcular_constructores_auto(of_r, grilla, escala, top_n=3):
    m = _mod_core()
    nn = m.get("normalizar_nombre", lambda x:x.lower().strip()) if "_error" not in m else lambda x:x.lower().strip()
    d2t = {nn(d):t for t,ds in grilla.items() for d in ds}
    tp = defaultdict(int)
    for pos,pts in escala.items():
        p = nn(of_r.get(pos,""))
        if p and p in d2t: tp[d2t[p]] += int(pts)
    ranking = sorted(tp.items(),key=lambda x:(-x[1],x[0]))
    return [t for t,_ in ranking[:top_n]], dict(tp)

def _mc_safe(s):
    m = _mod_mesa()
    if "_error" not in m and "_mc_safe_text" in m:
        try: return m["_mc_safe_text"](s)
        except: pass
    return _html.escape(s or "").replace("\n","<br>")

def _mc_badge(u):
    m = _mod_mesa()
    if "_error" not in m and "mc_badge_for" in m:
        try: return m["mc_badge_for"](u)
        except: pass
    b = MESA_CHICA_BADGES.get(u,{"tipo":"formulero","label":"FORMULERO","stars":""})
    return b["tipo"],b["label"],b["stars"]

def _mc_is_mod(u):
    m = _mod_mesa()
    if "_error" not in m and "mc_is_mod" in m:
        try: return m["mc_is_mod"](u)
        except: pass
    return u in MESA_CHICA_MODS

def flecha_arriba():
    components.html("""
    <style>
      .fw-top{position:fixed;right:18px;bottom:22px;width:52px;height:52px;border-radius:50%;
        border:2px solid rgba(246,195,73,.90);
        background:radial-gradient(circle at 30% 30%,rgba(246,195,73,.95),rgba(168,85,247,.30));
        box-shadow:0 0 0 6px rgba(168,85,247,.10),0 14px 40px rgba(0,0,0,.60);
        display:flex;align-items:center;justify-content:center;cursor:pointer;
        z-index:999999;transition:all .15s ease;user-select:none;}
      .fw-top:hover{transform:translateY(-3px);}
      .fw-top span{font-size:22px;font-weight:900;color:#10131d;}
    </style>
    <div class="fw-top"
      onclick="try{var d=window.parent.document;[d.querySelector('section.main'),d.querySelector('[data-testid=stAppViewContainer]')].forEach(function(e){if(e)e.scrollTo({top:0,behavior:'smooth'});});}catch(e){}"
      title="Ir arriba"><span>↑</span></div>""", height=0)

# ─────────────────────────────────────────────────────────
# 8. LOGIN + SIDEBAR
# ─────────────────────────────────────────────────────────
def sidebar_login_block():
    for k,d in [("perfil",None),("usuario",None),("_tok_done",False)]:
        if k not in st.session_state: st.session_state[k] = d

    # Restaurar desde token URL — puro cómputo local (sin GSheets)
    token = qp_get("t")
    if not is_logged_in() and token and not st.session_state["_tok_done"]:
        st.session_state["_tok_done"] = True
        u = auth_user_from_token(token)
        if u:
            with st.spinner("Restaurando sesión..."):
                perfil = _get_perfil(u)   # cacheado + timeout 30s
            if perfil:
                st.session_state["perfil"] = perfil
                st.session_state["usuario"] = perfil["usuario"]
                st.rerun()
            else:
                try: st.query_params.clear()
                except: pass

    # Reponer token si se fue
    if is_logged_in() and not qp_get("t"):
        u2 = (st.session_state.get("perfil") or {}).get("usuario","")
        if u2: qp_set("t", auth_create_token(u2))

    # ── NO LOGUEADO ──────────────────────────────────────
    if not is_logged_in():
        st.markdown("""
    <style>
    .tabla_historial_dark {
        width: 100%;
        border-collapse: collapse;
        background: rgba(7, 10, 25, 0.96);
        color: #e8ecff;
        border: 1px solid rgba(212, 175, 55, 0.25);
        border-radius: 14px;
        overflow: hidden;
        margin-top: 10px;
        margin-bottom: 12px;
        font-size: 14px;
    }

    .tabla_historial_dark th {
        background: linear-gradient(90deg, rgba(212,175,55,0.18), rgba(255,255,255,0.03));
        color: #ffdd7a;
        text-align: left;
        padding: 12px 14px;
        border-bottom: 1px solid rgba(212,175,55,0.22);
        font-weight: 800;
    }
    
    .tabla_historial_dark td {
        padding: 11px 14px;
        color: #e8ecff;
        border-bottom: 1px solid rgba(255,255,255,0.06);
        background: rgba(255,255,255,0.02);
    }

    .tabla_historial_dark tr:hover td {
        background: rgba(255,221,122,0.05);
    }

    .hist_car_track {
        position: relative;
        width: 100%;
        height: 30px;
        margin: 4px 0 12px 0;
        overflow: hidden;
        border-radius: 999px;
        background: linear-gradient(90deg, rgba(255,255,255,0.02), rgba(212,175,55,0.08), rgba(255,255,255,0.02));
        border: 1px solid rgba(212,175,55,0.18);
    }

    .hist_car {
        position: absolute;
        left: -60px;
        top: 3px;
        font-size: 21px;
        animation: histCarMove 6s linear infinite;
    }

    @keyframes histCarMove {
        0% { left: -60px; }
        100% { left: calc(100% + 60px); }
    }

    .flecha_subir_dorada {
        position: fixed;
        right: 24px;
        bottom: 22px;
        width: 46px;
        height: 46px;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        text-decoration: none;
        font-size: 22px;
        font-weight: 900;
        color: #1a1200;
        background: linear-gradient(180deg, #ffe38a 0%, #d4af37 100%);
        border: 1px solid rgba(255,240,180,0.65);
        box-shadow: 0 0 18px rgba(212,175,55,0.40);
        z-index: 9999;
    }

    .flecha_subir_dorada:hover {
        transform: scale(1.06);
        box-shadow: 0 0 24px rgba(212,175,55,0.58);
    }

    section[data-testid="stSidebar"],
    [data-testid="stSidebarCollapsedControl"] {
        display: none !important;
    }
    button[kind="primary"]{
            background:linear-gradient(90deg,#e10600,#ff3b3b);
                                       border:none;
                                       color:white;
                                       font-weight:700;
                                       }
            button[kind="primary"]:hover{
                background:linear-gradient(90deg,#ff3b3b,#ff6a6a);
                                           transform:scale(1.03);
                                           }
    </style>
    """, unsafe_allow_html=True)

        st.markdown("""
        <div class="fw-login-page">
          <div class="fw-login-shell">
            <div class="fw-login-card">
              <div class="fw-login-overlay"></div>
              <div class="fw-login-inner">
                <div class="fw-login-title">
                  🏆
                </div>
              </div>
            </div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        _,c2,_ = st.columns([1.1,1,1.1])
        with c2:
            # Header del formulario (solo visual, sin div wrapper)
            st.markdown("""
            <div class="fw-login-form-header">
              <div class="fw-login-form-title">🏎️ &nbsp;INICIA SESIÓN</div>
              <div class="fw-login-form-sub">Ingresá para hacer tus pronósticos</div>
            </div>
            """, unsafe_allow_html=True)

            m = _mod_auth()
            if "_error" in m:
                st.error(f"⚠️ Error cargando módulo auth: {m['_error']}")
            else:
                u_in = st.text_input("Usuario", key="li_u", placeholder="Tu nombre de piloto")
                p_in = st.text_input("Contraseña", type="password", key="li_p", placeholder="••••••••")

                if st.button("⚡  ENTRAR AL TORNEO", key="li_btn", use_container_width=True):
                    if not u_in or not p_in:
                        st.error("Completá usuario y contraseña.")
                    else:
                        with st.spinner("🔄 Verificando credenciales... (puede tardar ~20 seg)"):
                            ok, res = _safe_call(
                                m["login"], u_in, p_in,
                                timeout_sec=45,
                                default=(False, "⏱️ El servidor tardó demasiado. Reintentá — generalmente es solo la primera vez.")
                            )
                        if ok:
                            st.session_state["perfil"]  = res
                            st.session_state["usuario"] = res["usuario"]
                            qp_set("t", auth_create_token(res["usuario"]))
                            st.rerun()
                        else:
                            st.error(f"⛔ {res}")

                with st.expander("🔑 ¿Olvidaste tu contraseña?"):
                    u2  = st.text_input("Usuario",            key="rp_u")
                    mc  = st.text_input("Mother code",  type="password", key="rp_mc")
                    np1 = st.text_input("Nueva contraseña", type="password", key="rp_np1")
                    np2 = st.text_input("Repetir",      type="password", key="rp_np2")
                    if st.button("Resetear contraseña", key="rp_btn"):
                        if np1 != np2:
                            st.error("Las contraseñas no coinciden.")
                        elif len((np1 or "").strip()) < 4:
                            st.error("Mínimo 4 caracteres.")
                        else:
                            with st.spinner("Procesando..."):
                                ok2, msg2 = _safe_call(
                                    m["reset_password_with_mother"], u2, mc, np1,
                                    timeout_sec=45, default=(False, "Tiempo de espera agotado.")
                                )
                            st.success("✅ Contraseña actualizada. Ya podés iniciar sesión.") if ok2 else st.error(msg2)

        st.stop()

    # ── LOGUEADO — SIDEBAR ───────────────────────────────
    perfil = st.session_state["perfil"] or {}
    usr   = perfil.get("usuario","")
    rol   = perfil.get("rol","Piloto")
    copas = int(perfil.get("copas",0) or 0)
    color = PILOTO_COLORS.get(usr,"#a855f7")
    trofeos = "🏆"*copas if copas else "—"

    st.sidebar.markdown(f"""
    <div class="sidebar-profile-card" style="border-color:{color}55;">
      <div class="sidebar-profile-name" style="color:{color};">{usr}</div>
      <div class="sidebar-profile-role">{rol}</div>
      <div class="sidebar-profile-trophies">{trofeos}</div>
      <div class="sidebar-profile-stat">🏆 {copas} Título{"s" if copas!=1 else ""}</div>
    </div>""", unsafe_allow_html=True)

    m = _mod_auth()
    if "_error" not in m and "change_password" in m:
        with st.sidebar.expander("🔐 Cambiar contraseña"):
            op=st.text_input("Contraseña actual",type="password",key="cpw_old")
            n1=st.text_input("Nueva contraseña",type="password",key="cpw_n1")
            n2=st.text_input("Repetir nueva",type="password",key="cpw_n2")
            if st.button("Guardar cambio",key="cpw_btn"):
                if n1!=n2: st.sidebar.error("No coinciden.")
                elif len((n1 or "").strip())<4: st.sidebar.error("Mínimo 4 caracteres.")
                else:
                    ok,msg = _safe_call(m["change_password"],usr,n1,op,timeout_sec=10,default=(False,"Timeout"))
                    st.sidebar.success("✅ Actualizada.") if ok else st.sidebar.error(msg)

    st.sidebar.markdown('<div class="sidebar-logout-wrap">', unsafe_allow_html=True)
    if st.sidebar.button("🚪 Cerrar sesión", use_container_width=True, key="btn_logout"):
        logout(); st.rerun()
    st.sidebar.markdown("</div>", unsafe_allow_html=True)

    if os.getenv("FW_SETUP","0")=="1":
        with st.sidebar.expander("🛠️ Bootstrap Admin"):
            bpw=st.text_input("Password",type="password",key="boot_pw")
            bmc=st.text_input("Mother code",type="password",key="boot_mc")
            if st.button("Crear Admin Checo",key="boot_btn") and "_error" not in m:
                found,_=_safe_call(m["get_user_row"],"Checo Perez",timeout_sec=6,default=(False,None))
                if found: st.warning("Ya existe.")
                else:
                    ok,msg=_safe_call(m["bootstrap_user"],"Checo Perez","Comisario | Administrador",bpw,bmc,copas=3,color="gold",timeout_sec=10,default=(False,"Timeout"))
                    st.success(msg) if ok else st.error(msg)

    if is_admin() and "_error" not in m:
        with st.sidebar.expander("👤 Crear usuario (Admin)"):
            nu=st.text_input("Usuario",key="nu_u"); nr=st.selectbox("Rol",["Piloto","Comisario | Administrador"],key="nu_r")
            np=st.text_input("Password",type="password",key="nu_p"); nm=st.text_input("Mother code",type="password",key="nu_m")
            nc=st.number_input("Títulos",0,99,0,key="nu_c"); ncl=st.text_input("Color","white",key="nu_cl")
            if st.button("Crear usuario",key="nu_btn"):
                ok,msg=_safe_call(m["bootstrap_user"],nu,nr,np,nm,copas=int(nc),color=ncl,timeout_sec=10,default=(False,"Timeout"))
                st.success(msg) if ok else st.error(msg)
        with st.sidebar.expander("🔐 Setear PIN (Admin)"):
            piu=st.selectbox("Usuario",PILOTOS_TORNEO,key="pin_u")
            piv=st.text_input("Nuevo PIN",type="password",max_chars=4,key="pin_v")
            if st.button("Guardar PIN",key="pin_btn") and "set_pin" in m:
                ok,msg=_safe_call(m["set_pin"],piu,piv,timeout_sec=45,default=(False,"Tiempo agotado. Reintentá."))
                st.success(msg) if ok else st.error(msg)

    st.sidebar.divider()

# ─────────────────────────────────────────────────────────
# 9. PANTALLAS
# ─────────────────────────────────────────────────────────
def pantalla_inicio():
    st.markdown("""
    <div class="hero">
      <div class="hero-title">🏆 TORNEO DE PREDICCIONES</div>
      <div class="hero-subtitle">FEFE WOLF 2026</div>
      <div class="hero-foot">© 2026 Derechos Reservados — Fundado por <b>Checo Perez</b></div>
    </div>""", unsafe_allow_html=True)
    st.markdown("""<div class="section-title">📜 EL LEGADO DE FEFE WOLF</div>
    <div class="card gold-left">
      <div class="card-title">EN EL PRINCIPIO, HUBO RUIDO DE MOTORES...</div>
      <div class="card-text">
        Corría el año 2021. El mundo estaba cambiando, y la Fórmula 1 vivía una de sus batallas más feroces.
        En ese caos, cinco amigos decidieron que ser espectadores no era suficiente. Necesitaban ser protagonistas.<br><br>
        Bajo la visión fundacional de <b>Checo Perez</b>, se creó este santuario: un lugar donde la amistad se mide en puntos
        y el honor se juega en cada curva.<br><br>
        Pero este torneo no sería posible sin nuestra guía eterna: <b>Fefe Wolf</b>. Aunque no esté físicamente en el paddock,
        su espíritu competitivo impregna cada decisión. Es el líder espiritual que recuerda que nunca hay que levantar el pie.<br><br>
        <b>LOS CINCO ELEGIDOS:</b> Checo, Lauda, Bottas, Lando y Alonso.<br>
        No corremos por dinero. Corremos por el derecho sagrado de decir "te lo dije" el domingo por la tarde.<br><br>
        Hemos visto campeones ascender y caer. Vimos a Lauda y a Fefe Wolf compartir la gloria del 21. Vimos el dominio
        implacable de Checo, actual Tri Campeón. Vimos la sorpresa táctica y caída de Bottas.
        Ahora, en 2026, Audi ruge, Cadillac desafía al sistema y Colapinto lleva la bandera argentina.
        <i>¿Quién tendrá la audacia para reclamar el trono este año?</i>
      </div>
    </div>""", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>👑 EN MEMORIA DEL REY FEFE WOLF</div>", unsafe_allow_html=True)
    _,c2,_ = st.columns([1,2,1])
    with c2:
        try: st.image("IMAGENFEFE.jfif", use_container_width=True)
        except: st.info("Subí 'IMAGENFEFE.jfif' para mostrarla.")
    st.markdown("<div class='section-title'>🏛️ LA CÚPULA OFICIAL 2026</div>", unsafe_allow_html=True)
    _,d2,_ = st.columns([1,2,1])
    with d2:
        try: st.image("IMAGENCUPULA.jfif", use_container_width=True)
        except: st.info("Subí 'IMAGENCUPULA.jfif' para mostrarla.")
    st.markdown("<div class='section-title'>🏎️ PILOTOS EN PARRILLA</div>", unsafe_allow_html=True)
    cols = st.columns(5)
    for i,p in enumerate(PILOTOS_TORNEO):
        c = PILOTO_COLORS.get(p,"#a855f7")
        with cols[i]:
            st.markdown(f'<div class="pilot-chip" style="border-color:{c}44;"><div class="pilot-name" style="color:{c};">{p}</div><div class="pilot-role">Piloto Formulero</div></div>',
                        unsafe_allow_html=True)


def pantalla_calendario():
    st.title("📅 CALENDARIO TEMPORADA 2026")
    _,c2,_ = st.columns([1,2,1])
    with c2:
        try: st.image("IMAGENCALENDARIO.jfif", caption="Mapa de la Temporada")
        except: st.info("Subí 'IMAGENCALENDARIO.jfif'.")
    st.divider()
    render_dark_table(pd.DataFrame(CALENDARIO_VISUAL))


def pantalla_pilotos_y_escuderias():
    st.markdown('<div class="section-title">🏎️ PILOTOS Y EQUIPOS 2026</div>', unsafe_allow_html=True)

    tab_pil, tab_eq = st.tabs(["👤 Pilotos F1 2026", "🏎️ Constructores 2026"])

    with tab_pil:
        # Grid de pilotos al estilo F1.com
        all_drivers = []
        for equipo, pilotos in GRILLA_2026.items():
            color = TEAM_COLORS.get(equipo, "#A855F7")
            abbr  = TEAM_LOGOS_SVG.get(equipo, equipo[:3])
            for num_idx, pil in enumerate(pilotos):
                all_drivers.append((pil, equipo, color, abbr, num_idx + 1))

        cards_html = '<div class="f1-drivers-grid">'
        for pil, equipo, color, abbr, num in all_drivers:
            photo = DRIVER_PHOTOS.get(pil, "")
            initials = "".join(p[0] for p in pil.split()[:2]).upper()
            nacionalidades = {
                "Lando Norris": "🇬🇧", "Oscar Piastri": "🇦🇺", "Max Verstappen": "🇳🇱",
                "Isack Hadjar": "🇫🇷", "Kimi Antonelli": "🇮🇹", "George Russell": "🇬🇧",
                "Charles Leclerc": "🇲🇨", "Lewis Hamilton": "🇬🇧", "Alex Albon": "🇹🇭",
                "Carlos Sainz": "🇪🇸", "Lance Stroll": "🇨🇦", "Fernando Alonso": "🇪🇸",
                "Liam Lawson": "🇳🇿", "Arvid Lindblad": "🇸🇪", "Oliver Bearman": "🇬🇧",
                "Esteban Ocon": "🇫🇷", "Nico Hulkenberg": "🇩🇪", "Gabriel Bortoleto": "🇧🇷",
                "Pierre Gasly": "🇫🇷", "Franco Colapinto": "🇦🇷",
                "Checo Perez": "🇲🇽", "Valteri Bottas": "🇫🇮",
            }
            flag = nacionalidades.get(pil, "🌍")
            last_name = pil.split()[-1].upper()
            first_name = " ".join(pil.split()[:-1])
            # Color strip top + team badge
            img_html = f'<img src="{photo}" class="f1d-photo" onerror="this.style.display=\'none\';this.nextElementSibling.style.display=\'flex\';" loading="lazy">'
            fallback = f'<div class="f1d-fallback" style="display:none;color:{color};">{initials}</div>'
            cards_html += f"""
            <div class="f1d-card fade-up" style="--tc:{color}">
              <div class="f1d-top-stripe"></div>
              <div class="f1d-team-badge">{abbr}</div>
              <div class="f1d-photo-wrap">{img_html}{fallback}</div>
              <div class="f1d-info">
                <div class="f1d-flag">{flag}</div>
                <div class="f1d-firstname">{first_name}</div>
                <div class="f1d-lastname">{last_name}</div>
                <div class="f1d-team-name">{equipo}</div>
              </div>
            </div>"""
        cards_html += "</div>"
        st.markdown(cards_html, unsafe_allow_html=True)

    with tab_eq:
        # Sección de equipos al estilo F1.com
        TEAM_DESCRIPTIONS = {
            "MCLAREN":      "El equipo de Woking vuelve como favorito con Norris y Piastri. MCL60 dominó la segunda mitad de 2025.",
            "RED BULL":     "El gigante de Milton Keynes. Verstappen busca su quinto título con el joven Hadjar como compañero.",
            "MERCEDES":     "La flecha plateada renace. Antonelli, la gran apuesta, junto a Russell en el W17.",
            "FERRARI":      "La Scuderia con Hamilton y Leclerc: la alineación más mediática de la historia de la F1.",
            "WILLIAMS":     "Albon y Sainz, una dupla sólida para llevar a Williams de regreso a la lucha por puntos.",
            "ASTON MARTIN": "El millonario proyecto de Lawrence Stroll con Alonso y su hijo Lance. AMR26 promete.",
            "RACING BULLS": "El equipo B de Red Bull. Lawson y Lindblad, dos jóvenes hambrientos de puntos.",
            "HAAS":         "Bearman y Ocon, dos europeos con hambre de demostrar. La escudería americana apuesta al futuro.",
            "AUDI":         "El debutante de lujo. Hulkenberg y Bortoleto encabezan el proyecto más ambicioso de la era moderna.",
            "ALPINE":       "Gasly y Colapinto — el argentino que tiene a toda Latinoamérica de su lado. ¡Vamos Franco!",
            "CADILLAC":     "El regreso de Checo a la grilla, ahora con Bottas. El equipo americano desafía al establishment.",
        }
        # F1 CDN car images 2025 (fallback silhouette if 2026 not yet available)
        TEAM_CARS = {
            "MCLAREN":      "https://media.formula1.com/image/upload/c_lfill,w_3392/q_auto/v1740000000/common/f1/2026/mclaren/2026mclarencarright.webp",
            "RED BULL":     "https://media.formula1.com/image/upload/c_lfill,w_3392/q_auto/v1740000000/common/f1/2026/redbullracing/2026redbullracingcarright.webp",
            "MERCEDES":     "https://media.formula1.com/image/upload/c_lfill,w_3392/q_auto/v1740000000/common/f1/2026/mercedes/2026mercedescarright.webp",
            "FERRARI":      "https://media.formula1.com/image/upload/c_lfill,w_3392/q_auto/v1740000000/common/f1/2026/ferrari/2026ferraricarright.webp",
            "WILLIAMS":     "https://media.formula1.com/image/upload/c_lfill,w_3392/q_auto/v1740000000/common/f1/2026/williams/2026williamscarright.webp",
            "ASTON MARTIN": "https://media.formula1.com/image/upload/c_lfill,w_3392/q_auto/v1740000000/common/f1/2026/astonmartin/2026astonmartincarright.webp",
            "RACING BULLS": "https://media.formula1.com/image/upload/c_lfill,w_3392/q_auto/v1740000000/common/f1/2026/racingbulls/2026racingbullscarright.webp",
            "HAAS":         "https://media.formula1.com/image/upload/c_lfill,w_3392/q_auto/v1740000000/common/f1/2026/haas/2026haascarright.webp",
            "AUDI":         "https://media.formula1.com/image/upload/c_lfill,w_3392/q_auto/v1740000000/common/f1/2026/audi/2026audicarright.webp",
            "ALPINE":       "https://media.formula1.com/image/upload/c_lfill,w_3392/q_auto/v1740000000/common/f1/2026/alpine/2026alpinecarright.webp",
            "CADILLAC":     "https://media.formula1.com/image/upload/c_lfill,w_3392/q_auto/v1740000000/common/f1/2026/cadillac/2026cadillaccarright.webp",
        }
        teams_list = list(GRILLA_2026.items())
        for i in range(0, len(teams_list), 2):
            cols = st.columns(2, gap="large")
            for j, (equipo, pilotos) in enumerate(teams_list[i:i+2]):
                color = TEAM_COLORS.get(equipo, "#A855F7")
                abbr  = TEAM_LOGOS_SVG.get(equipo, equipo[:3])
                desc  = TEAM_DESCRIPTIONS.get(equipo, "")
                car   = TEAM_CARS.get(equipo, "")
                p1, p2 = pilotos[0], pilotos[1]
                car_html = f'<img src="{car}" class="tfc-car-img" loading="lazy" onerror="this.style.display=\'none\'">' if car else ""
                with cols[j]:
                    st.markdown(f"""
                    <div class="team-full-card fade-up" style="--tc:{color}">
                      <div class="tfc-header">
                        <div class="tfc-stripe"></div>
                        <div class="tfc-abbr" style="color:{color}">{abbr}</div>
                        <div class="tfc-name">{equipo}</div>
                      </div>
                      <div class="tfc-car-wrap">{car_html}</div>
                      <div class="tfc-drivers">
                        <div class="tfc-driver">
                          <div class="tfc-num" style="color:{color}">01</div>
                          <div class="tfc-dname">{p1}</div>
                        </div>
                        <div class="tfc-driver">
                          <div class="tfc-num" style="color:{color}">02</div>
                          <div class="tfc-dname">{p2}</div>
                        </div>
                      </div>
                      <div class="tfc-desc">{desc}</div>
                    </div>""", unsafe_allow_html=True)



def pantalla_reglamento():
    st.title("📜 REGLAMENTO OFICIAL 2026")
    st.markdown("""
<div class="card fade-up" style="border-color:rgba(217,70,239,.35);">
<div class="card-text">

### ⚠️ REGLA DE ESCRITURA HA SIDO ELIMINADA
**LOS NOMBRES YA NO DEBEN ESCRIBIRSE EXACTAMENTE IGUAL A LA SECCIÓN 'PILOTOS Y ESCUDERÍAS'.**
**El sistema lo detecta automaticamente, aunque siempre deben verificar que esté correcto.**

</div></div><div style="height:12px;"></div>
<div class="card fade-up"><div class="card-text">

### ⚔️ SISTEMA DE PUNTUACIÓN

**🏁 CARRERA:** 1°(25) 2°(18) 3°(15) 4°(12) 5°(10) 6°(8) 7°(6) 8°(4) 9°(2) 10°(1) · Pleno +5 Pts

**⏱️ CLASIFICACIÓN:** 1°(15) 2°(10) 3°(7) 4°(5) 5°(3) · Pleno +5 Pts

**⚡ SPRINT:** 1°(8) 2°(7) 3°(6) 4°(5) 5°(4) 6°(3) 7°(2) 8°(1) · Pleno +3 Pts

**🛠️ CONSTRUCTORES:** 1°(10) 2°(5) 3°(2) · Pleno +3 Pts

**🧉 REGLA COLAPINTO:** Acierto exacto Qualy **+10 Pts** · Acierto exacto Carrera **+20 Pts**

**🏆 CAMPEONES:** Piloto campeón **+50 Pts** · Constructor campeón **+25 Pts**

</div></div><div style="height:12px;"></div>
<div class="card fade-up" style="border-color:rgba(255,64,64,.35);"><div class="card-text">

### ⛔ SANCIONES D.N.S.
Falta en Clasificación: **-5 Pts** · Falta en Sprint: **-5 Pts** · Falta en Carrera/Constructores: **-5 Pts**

**Desempate:** En caso de igualdad de puntos, ganará la predicción el Formulero que haya envíado primero.

</div></div>""", unsafe_allow_html=True)


def pantalla_muro():
    st.markdown("""
    <div class="hof-wrap">
      <div class="hof-title">🏆 MURO DE CAMPEONES</div>
      <div class="hof-subtitle">👑 HALL OF FAME</div>
    </div>
    <div class="hof-wrap">
      <div class="hof-card fade-up"><div class="hof-row"><span class="hof-medal">🏅</span><div class="hof-name">CHECO PEREZ</div></div><div class="hof-stars">★★★</div><div class="hof-meta">TriCampeón: 2022 · 2023 · 2025</div></div>
      <div class="hof-card fade-up"><div class="hof-row"><span class="hof-medal">🏅</span><div class="hof-name">VALTERI BOTTAS</div></div><div class="hof-stars">★</div><div class="hof-meta">Campeón: 2024</div></div>
      <div class="hof-card fade-up"><div class="hof-row"><span class="hof-medal">🏅</span><div class="hof-name">FEFE WOLF</div></div><div class="hof-stars">★</div><div class="hof-meta">Campeón: 2021</div></div>
      <div class="hof-card fade-up"><div class="hof-row"><span class="hof-medal">🏅</span><div class="hof-name">NICKI LAUDA</div></div><div class="hof-stars">★</div><div class="hof-meta">Campeón: 2021</div></div>
    </div>""", unsafe_allow_html=True)


def pantalla_tabla_posiciones():
    st.markdown('<div class="section-title">📊 TABLA GENERAL 2026</div>', unsafe_allow_html=True)
    if st.button("🔄 Actualizar tabla", key="btn_ref_tabla"):
        st.cache_data.clear(); st.rerun()

    @st.cache_data(ttl=120, show_spinner="Cargando tabla…")
    def _get():
        m = _mod_db()
        if "_error" in m: return None
        return _safe_call(m["leer_tabla_posiciones"], PILOTOS_TORNEO, timeout_sec=8, default=None)

    df = _get()
    if df is None or (hasattr(df,"empty") and df.empty):
        df = pd.DataFrame({"Piloto":PILOTOS_TORNEO,"Puntos":[0]*5,"Qualys":[0]*5,"Sprints":[0]*5,"Carreras":[0]*5})
    if "Puntos" in df.columns:
        df = df.sort_values("Puntos",ascending=False).reset_index(drop=True)
    if len(df)>=3:
        p1,p2,p3 = df.iloc[0],df.iloc[1],df.iloc[2]
        def _pod(col,row,medal,extra=""):
            c=PILOTO_COLORS.get(row["Piloto"],"#a855f7")
            with col:
                st.markdown(f'<div class="card fade-up" style="text-align:center;border-color:{c}44;{extra}"><div style="font-size:32px;">{medal}</div><div style="font-weight:900;color:{c};font-size:15px;">{row["Piloto"]}</div><div style="font-size:26px;font-weight:900;color:#ffdd7a;">{int(row.get("Puntos",0))}</div><div style="font-size:11px;color:rgba(232,236,255,.55);">PTS</div></div>',unsafe_allow_html=True)
        c1,c2,c3=st.columns(3)
        _pod(c1,p2,"🥈"); _pod(c2,p1,"🥇","border-color:#D4AF3788;"); _pod(c3,p3,"🥉")
        render_dark_table(df)
    if "Puntos" in df.columns and not df.empty and _PLOTLY_OK:
        colors = [PILOTO_COLORS.get(p, "#a855f7") for p in df["Piloto"]]
        fig = go.Figure(go.Bar(
            x=df["Piloto"], y=df["Puntos"],
            marker_color=colors,
            marker_line_width=0,
            text=df["Puntos"], textposition="outside",
            textfont=dict(color="#ffdd7a", size=14, family="Inter"),
        ))
        fig.update_layout(
            height=260, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=10, r=10, t=10, b=10),
            xaxis=dict(tickfont=dict(color="#e8ecff", size=12), showgrid=False, zeroline=False),
            yaxis=dict(tickfont=dict(color="#a9b2d6", size=11), showgrid=True,
                       gridcolor="rgba(246,195,73,0.08)", zeroline=False),
            showlegend=False,
        )
        fig.update_traces(marker_cornerradius="5%")
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def pantalla_historial_gp():
    st.markdown('<div class="section-title">📈 HISTORIAL POR GRAN PREMIO</div>', unsafe_allow_html=True)
    st.markdown('<div id="top"></div>', unsafe_allow_html=True)

    @st.cache_data(ttl=120, show_spinner="Cargando historial…")
    def _get():
        m = _mod_db()
        if "_error" in m: return pd.DataFrame(), pd.DataFrame()
        h = _safe_call(m["leer_historial_df"], timeout_sec=8, default=pd.DataFrame())
        d = _safe_call(m["leer_historial_detalle_df"], timeout_sec=8, default=pd.DataFrame())
        return h, d

    df_hist, df_det = _get()
    if df_hist is None or (hasattr(df_hist,"empty") and df_hist.empty):
        st.markdown("""<div class="card fade-up" style="text-align:center;padding:40px;">
          <div style="font-size:56px;">🏎️</div>
          <div style="font-weight:900;font-size:20px;color:#ffdd7a;">La temporada 2026 está por comenzar</div>
          <div style="color:rgba(232,236,255,.65);margin-top:10px;font-size:14px;">
            El historial se activa tras el primer cómputo oficial.</div></div>""",
            unsafe_allow_html=True)
        return

    df_hist=df_hist.copy(); df_hist.columns=[c.lower().strip() for c in df_hist.columns]
    df_hist["puntos"]=pd.to_numeric(df_hist["puntos"],errors="coerce").fillna(0).astype(int)
    df_hist["gp"]=df_hist["gp"].astype(str).str.strip()
    df_hist["piloto"]=df_hist["piloto"].astype(str).str.strip()
    df_hist=df_hist.groupby(["gp","piloto"],as_index=False)["puntos"].sum()
    gp_ord={g:i for i,g in enumerate(GPS_OFICIALES)}
    df_hist["_ord"]=df_hist["gp"].map(gp_ord).fillna(99)
    df_hist=df_hist.sort_values(["_ord","piloto"]).drop(columns="_ord")
    gps_j=[g for g in GPS_OFICIALES if g in df_hist["gp"].values]
    short={g:g.split(". ",1)[-1].strip() if ". " in g else g for g in GPS_OFICIALES}

    tab_evo,tab_gp,tab_pers,tab_stats=st.tabs(["📉 Evolución","🏁 Por GP","👤 Personal","🏅 Stats"])

    with tab_evo:
        
        pivot=df_hist.pivot_table(index="gp",columns="piloto",values="puntos",fill_value=0)
        pivot=pivot.reindex([g for g in GPS_OFICIALES if g in pivot.index])
        cumdf = pivot.cumsum()
        short_idx = [short.get(g,g) for g in cumdf.index]
        if _PLOTLY_OK:
            fig = go.Figure()
            for pil in cumdf.columns:
                c = PILOTO_COLORS.get(pil, "#a855f7")
                fig.add_trace(go.Scatter(
                    x=short_idx, y=cumdf[pil].values,
                    mode="lines+markers", name=pil,
                    line=dict(color=c, width=3),
                    marker=dict(color=c, size=7),
                ))
            fig.update_layout(
                height=380, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=10,r=10,t=10,b=60),
                xaxis=dict(tickfont=dict(color="#a9b2d6",size=11), tickangle=-42,
                           showgrid=False, zeroline=False),
                yaxis=dict(tickfont=dict(color="#a9b2d6",size=11),
                           title=dict(text="Pts Acum.", font=dict(color="#ffdd7a")),
                           showgrid=True, gridcolor="rgba(246,195,73,0.08)", zeroline=False),
                legend=dict(font=dict(color="#e8ecff"), bgcolor="rgba(0,0,0,0.3)",
                            bordercolor="rgba(246,195,73,.2)", borderwidth=1),
            )
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        cd=pivot.cumsum().copy(); cd.index=cd.index.map(short); cd.index.name="GP"
        st.markdown(
            cd.to_html(classes="tabla_historial_dark", border=0),
            unsafe_allow_html=True
            )

    with tab_gp:
        gp_sel=st.selectbox("GP:",gps_j,key="hist_gp_sel",format_func=lambda x:short.get(x,x))
        df_gp=df_hist[df_hist["gp"]==gp_sel].sort_values("puntos",ascending=False).reset_index(drop=True)
        if df_gp.empty: st.warning("Sin datos.")
        else:
            gan=df_gp.iloc[0]; cg=PILOTO_COLORS.get(gan["piloto"],"#D4AF37")
            st.markdown(f'<div class="card fade-up" style="text-align:center;border-color:{cg}55;padding:22px;"><div style="font-size:38px;">🏆</div><div style="font-weight:900;font-size:18px;color:{cg};">Ganador: {gan["piloto"]}</div><div style="font-size:32px;font-weight:900;color:#ffdd7a;">{gan["puntos"]} pts</div></div>',unsafe_allow_html=True)
            ds=df_gp[["piloto","puntos"]].rename(columns={"piloto":"Piloto","puntos":"Puntos"}); ds.index=range(1,len(ds)+1)
            st.markdown(
                ds.to_html(classes="tabla_historial_dark", border=0),
                unsafe_allow_html=True
                )
            if df_det is not None and not (hasattr(df_det,"empty") and df_det.empty):
                ddt=df_det.copy(); ddt.columns=[c.lower().strip() for c in ddt.columns]
                ddt=ddt[ddt["gp"]==gp_sel]
                if not ddt.empty:
                    pv=ddt.pivot_table(index="piloto",columns="etapa",values="puntos",fill_value=0,aggfunc="sum").reset_index()
                    pv.columns.name = None
                    st.markdown(
                        pv.to_html(classes="tabla_historial_dark", border=0, index=False),
                        unsafe_allow_html=True
                        )
            df_gp["Color"]=df_gp["piloto"].map(PILOTO_COLORS).fillna("#a855f7")
            if _PLOTLY_OK:
                fig2 = go.Figure(go.Bar(
                    x=df_gp["piloto"], y=df_gp["puntos"],
                    marker_color=df_gp["Color"].tolist(),
                    text=df_gp["puntos"], textposition="outside",
                    textfont=dict(color="#ffdd7a", size=13),
                ))
                fig2.update_layout(
                    height=220, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    margin=dict(l=10,r=10,t=10,b=10), showlegend=False,
                    xaxis=dict(tickfont=dict(color="#e8ecff",size=12), showgrid=False),
                    yaxis=dict(tickfont=dict(color="#a9b2d6",size=11), showgrid=True,
                               gridcolor="rgba(246,195,73,0.08)", zeroline=False),
                )
                st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False})

    with tab_pers:
        usr_d=(st.session_state.get("perfil") or {}).get("usuario","")
        opc=[p for p in PILOTOS_TORNEO if p in df_hist["piloto"].values] or PILOTOS_TORNEO
        idx=opc.index(usr_d) if usr_d in opc else 0
        pil=st.selectbox("Piloto:",opc,index=idx,key="hist_pers")
        cp=PILOTO_COLORS.get(pil,"#a855f7")
        df_p=df_hist[df_hist["piloto"]==pil].copy()
        df_p["gp_s"]=df_p["gp"].map(short).fillna(df_p["gp"]); df_p["acum"]=df_p["puntos"].cumsum()
        total=int(df_p["puntos"].sum()); prom=int(df_p["puntos"].mean()) if not df_p.empty else 0
        mejor=df_p.loc[df_p["puntos"].idxmax()] if not df_p.empty else None
        peor=df_p.loc[df_p["puntos"].idxmin()]  if not df_p.empty else None
        def _sc(col,lbl,val,sub="",c="#ffdd7a"):
            with col: st.markdown(f'<div class="card fade-up" style="text-align:center;padding:12px 8px;"><div style="font-size:10px;letter-spacing:.12em;text-transform:uppercase;color:rgba(169,178,214,.80);margin-bottom:4px;">{lbl}</div><div style="font-size:24px;font-weight:900;color:{c};">{val}</div><div style="font-size:10px;color:rgba(169,178,214,.55);">{sub}</div></div>',unsafe_allow_html=True)
        c1,c2,c3,c4=st.columns(4)
        _sc(c1,"Total Pts",total,f"{len(df_p)} GPs",cp)
        _sc(c2,"Promedio",prom,"pts/GP")
        _sc(c3,"Mejor GP",int(mejor["puntos"]) if mejor is not None else "-",mejor["gp_s"] if mejor is not None else "","#22c55e")
        _sc(c4,"Peor GP", int(peor["puntos"])  if peor  is not None else "-",peor["gp_s"]  if peor  is not None else "","#ef4444")
        if not df_p.empty and _PLOTLY_OK:
            fig3 = go.Figure()
            fig3.add_trace(go.Scatter(
                x=df_p["gp_s"].tolist(), y=df_p["puntos"].tolist(),
                mode="lines+markers", name="GP",
                line=dict(color=cp, width=2.5),
                marker=dict(color=cp, size=7),
            ))
            fig3.add_trace(go.Scatter(
                x=df_p["gp_s"].tolist(), y=df_p["acum"].tolist(),
                mode="lines+markers", name="Acum.",
                line=dict(color="#ffdd7a", width=2.5, dash="dot"),
                marker=dict(color="#ffdd7a", size=6),
            ))
            fig3.update_layout(
                height=300, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=10,r=10,t=10,b=60),
                xaxis=dict(tickfont=dict(color="#a9b2d6",size=11), tickangle=-42,
                           showgrid=False, zeroline=False),
                yaxis=dict(tickfont=dict(color="#a9b2d6",size=11),
                           title=dict(text="Puntos", font=dict(color="#ffdd7a")),
                           showgrid=True, gridcolor="rgba(246,195,73,0.08)", zeroline=False),
                legend=dict(font=dict(color="#e8ecff"), bgcolor="rgba(0,0,0,0.3)"),
            )
            st.plotly_chart(fig3, use_container_width=True, config={"displayModeBar": False})

    with tab_stats:
        mn=df_hist.groupby("piloto")["puntos"].mean().reset_index().sort_values("puntos",ascending=False)
        mx=df_hist.groupby("piloto")["puntos"].max().reset_index().sort_values("puntos",ascending=False)
        wr_l=[grp.loc[grp["puntos"].idxmax(),"piloto"] for _,grp in df_hist.groupby("gp")]
        wr=pd.Series(wr_l).value_counts().reset_index(); wr.columns=["Piloto","Victorias"]
        def _sl(col,ttl,df_in,vc,fmt):
            with col:
                st.markdown(f"#### {ttl}")
                for _,row in df_in.iterrows():
                    pk="piloto" if "piloto" in row else "Piloto"; c=PILOTO_COLORS.get(row[pk],"#a855f7"); v=row[vc]
                    vf=f"{v:.1f}" if isinstance(v,float) and fmt=="f" else str(int(v) if isinstance(v,float) else v)
                    st.markdown(f'<div style="display:flex;justify-content:space-between;padding:8px 12px;border-radius:10px;margin:4px 0;background:rgba(255,255,255,.04);border:1px solid {c}33;"><span style="color:{c};font-weight:700;">{row[pk]}</span><span style="color:#ffdd7a;font-weight:900;">{vf}</span></div>',unsafe_allow_html=True)
        cA,cB,cC=st.columns(3)
        _sl(cA,"🎯 Promedio/GP",mn,"puntos","f")
        _sl(cB,"🚀 Máximo GP",  mx,"puntos","i")
        with cC:
            st.markdown("#### 🏆 GPs ganados")
            for _,row in wr.iterrows():
                c=PILOTO_COLORS.get(row["Piloto"],"#a855f7")
                st.markdown(f'<div style="display:flex;justify-content:space-between;padding:8px 12px;border-radius:10px;margin:4px 0;background:rgba(255,255,255,.04);border:1px solid {c}33;"><span style="color:{c};font-weight:700;">{row["Piloto"]}</span><span style="color:#ffdd7a;font-weight:900;">{row["Victorias"]} 🏆</span></div>',unsafe_allow_html=True)
                st.markdown('<a href="#top" class="flecha_subir_dorada">↑</a>', unsafe_allow_html=True)


def pantalla_cargar_predicciones():
    mdb  = _mod_db()
    mcore= _mod_core()
    mauth= _mod_auth()
    if "_error" in mdb or "_error" in mcore or "_error" in mauth:
        st.error("⚠️ Módulos no disponibles."); return

    st.title("🔒 SISTEMA DE PREDICCIÓN 2026")
    usr_log=(st.session_state.get("perfil") or {}).get("usuario","")
    c1,c2=st.columns(2)
    usuario=c1.selectbox("Piloto Participante",PILOTOS_TORNEO,
                         index=PILOTOS_TORNEO.index(usr_log) if usr_log in PILOTOS_TORNEO else 0,key="pred_u")
    gp_actual=c2.selectbox("Seleccionar Gran Premio",GPS_OFICIALES,key="pred_gp")

    estado=_safe_call(mcore["obtener_estado_gp"],gp_actual,HORARIOS_CARRERA,TZ,timeout_sec=4,
                      default={"habilitado":True,"mensaje":"(sin datos)"})
    if not (estado or {}).get("habilitado",True):
        st.error(f"🔴 **PREDICCIONES CERRADAS: {gp_actual}**")
        st.warning((estado or {}).get("mensaje","")); return
    else:
        st.success(f"🟢 **HABILITADO** | {(estado or {}).get('mensaje','')}")

    es_sprint = gp_actual in GPS_SPRINT
    if es_sprint: st.info("⚡ **¡FIN DE SEMANA SPRINT!** ⚡")

    st.subheader("🔐 Validación PIN")
    pin=st.text_input("Ingresá tu PIN (4 dígitos):",type="password",max_chars=4,key="pred_pin")

    nn = mcore.get("normalizar_nombre", lambda x:x)
    drivers_all=[d for t in GRILLA_2026.values() for d in t]
    drivers_sprint=[d for d in drivers_all if nn(d)!=nn("Franco Colapinto")]
    teams_all=list(GRILLA_2026.keys())

    def _has(d): return isinstance(d,dict) and any(str(v).strip() for v in d.values())
    def ya_envio(u,gp,etapa):
        try:
            res=_safe_call(mdb["recuperar_predicciones_piloto"],u,gp,timeout_sec=6,default=(None,None,(None,None)))
            dq,ds,(dr,dc)=res
            e=(etapa or "").upper()
            if e=="QUALY":   return _has(dq)
            if e=="SPRINT":  return _has(ds)
            if e=="CARRERA": return _has(dr) or _has(dc)
        except: pass
        return False

    def usel(options,count,kp,lp):
        sel={}
        for i in range(1,count+1):
            used=[v for k,v in sel.items() if k<i and v]
            cur=st.session_state.get(f"{kp}_{i}","")
            avail=[o for o in options if o not in used or o==cur]
            sel[i]=st.selectbox(f"{lp} {i}°",[""]+ avail,key=f"{kp}_{i}",
                                format_func=lambda x:"— Seleccionar —" if x=="" else x)
        return sel

    if es_sprint: tab_q,tab_s,tab_r=st.tabs(["⏱️ CLASIFICACIÓN","⚡ SPRINT","🏁 CARRERA"])
    else:         tab_q,tab_r=st.tabs(["⏱️ CLASIFICACIÓN","🏁 CARRERA"]); tab_s=None

    with tab_q:
        st.subheader(f"Qualy — {gp_actual}")
        st.info("1°(15) 2°(10) 3°(7) 4°(5) 5°(3) | Pleno +5 Pts")
        q_data=usel(drivers_all,5,f"q_{gp_actual}_{usuario}","Posición")
        q_data["colapinto_q"]=st.number_input("Posición Franco Colapinto",1,22,10,key=f"cq-{gp_actual}-{usuario}")
        cd=None
        if gp_actual=="01. Gran Premio de Australia":
            st.markdown("---"); st.error("🚨 **EDICIÓN ESPECIAL AUSTRALIA**")
            cc1,cc2=st.columns(2)
            cp_=cc1.selectbox("🏆 Piloto campeón",[""]+drivers_all,key=f"camp_p_{gp_actual}_{usuario}",format_func=lambda x:"— Seleccionar —" if x=="" else x)
            ce_=cc2.selectbox("🏗️ Constructor campeón",[""]+teams_all,key=f"camp_e_{gp_actual}_{usuario}",format_func=lambda x:"— Seleccionar —" if x=="" else x)
            cd={"piloto":(cp_ or "").strip(),"equipo":(ce_ or "").strip()}
        ya_q=ya_envio(usuario,gp_actual,"QUALY")
        if ya_q: st.success("✅ Ya enviaste la predicción de **QUALY** para este GP.")
        if st.button("🚀 ENVIAR QUALY",use_container_width=True,key=f"btn_q-{gp_actual}-{usuario}",disabled=ya_q):
            if not _safe_call(mauth["verify_pin"],usuario,pin,timeout_sec=30,default=False):
                st.error("⛔ PIN INCORRECTO")
            elif any(not q_data.get(i) for i in range(1,6)):
                st.error("⚠️ Completá las 5 posiciones.")
            elif gp_actual=="01. Gran Premio de Australia" and (not cd or not cd["piloto"] or not cd["equipo"]):
                st.error("⚠️ Completá piloto y constructor campeón.")
            else:
                args=(usuario,gp_actual,"QUALY",q_data,cd) if cd else (usuario,gp_actual,"QUALY",q_data)
                ok,msg=_safe_call(mdb["guardar_etapa"],*args,timeout_sec=10,default=(False,"Timeout"))
                (st.success(msg) or st.balloons() or st.rerun()) if ok else st.error(msg)

    if tab_s is not None:
        with tab_s:
            st.subheader(f"Sprint — {gp_actual}")
            st.info("1°(8) 2°(7) 3°(6) 4°(5) 5°(4) 6°(3) 7°(2) 8°(1) | Pleno +3")
            s_data=usel(drivers_sprint,8,f"s_{gp_actual}_{usuario}","Posición")
            ya_s=ya_envio(usuario,gp_actual,"SPRINT")
            if ya_s: st.success("✅ Ya enviaste la predicción de **SPRINT**.")
            if st.button("🚀 ENVIAR SPRINT",use_container_width=True,key=f"btn_s-{gp_actual}-{usuario}",disabled=ya_s):
                if not _safe_call(mauth["verify_pin"],usuario,pin,timeout_sec=30,default=False): st.error("⛔ PIN INCORRECTO")
                elif any(not s_data.get(i) for i in range(1,9)): st.error("⚠️ Completá las 8 posiciones.")
                else:
                    ok,msg=_safe_call(mdb["guardar_etapa"],usuario,gp_actual,"SPRINT",s_data,timeout_sec=10,default=(False,"Timeout"))
                    (st.success(msg) or st.balloons() or st.rerun()) if ok else st.error(msg)

    with tab_r:
        cr,cc2=st.columns(2)
        with cr:
            st.subheader(f"Carrera — {gp_actual}")
            st.info("1°(25) 2°(18) 3°(15) 4°(12) 5°(10) 6°(8) 7°(6) 8°(4) 9°(2) 10°(1) | Pleno +5")
            r_top=usel(drivers_all,10,f"r_{gp_actual}_{usuario}","Posición")
            col_r=st.number_input("Posición Franco Colapinto",1,22,10,key=f"cr-{gp_actual}-{usuario}")
        with cc2:
            st.subheader("Constructores")
            st.info("1°(10) 2°(5) 3°(2) | Pleno +3")
            c_top=usel(teams_all,3,f"c_{gp_actual}_{usuario}","Equipo")
        r_data=dict(r_top); r_data["colapinto_r"]=col_r
        r_data["c1"],r_data["c2"],r_data["c3"]=c_top[1],c_top[2],c_top[3]
        ya_r=ya_envio(usuario,gp_actual,"CARRERA")
        if ya_r: st.success("✅ Ya enviaste la predicción de **CARRERA/CONSTRUCTORES**.")
        if st.button("🚀 ENVIAR CARRERA Y CONSTRUCTORES",use_container_width=True,key=f"btn_r-{gp_actual}-{usuario}",disabled=ya_r):
            if not _safe_call(mauth["verify_pin"],usuario,pin,timeout_sec=30,default=False): st.error("⛔ PIN INCORRECTO")
            elif any(not r_data.get(i) for i in range(1,11)): st.error("⚠️ Completá las 10 posiciones.")
            elif not r_data["c1"] or not r_data["c2"] or not r_data["c3"]: st.error("⚠️ Completá top 3 Constructores.")
            else:
                ok,msg=_safe_call(mdb["guardar_etapa"],usuario,gp_actual,"CARRERA",r_data,timeout_sec=10,default=(False,"Timeout"))
                (st.success(msg) or st.balloons() or st.rerun()) if ok else st.error(msg)


def pantalla_calculadora_puntos():
    mdb=_mod_db(); madm=_mod_admin(); mcore=_mod_core(); mauth=_mod_auth()
    if any("_error" in x for x in [mdb,madm,mcore,mauth]):
        st.error("⚠️ Módulos no disponibles."); return
    st.title("🧮 CENTRO DE CÓMPUTOS")
    st.info("🔒 ÁREA RESTRINGIDA")
    pwd=st.text_input("🔑 Clave de Comisario:",type="password")
    if pwd!="2022": st.stop()
    st.success("✅ ACCESO AUTORIZADO — MODO COMISARIO"); st.divider()
    gp_calc=st.selectbox("Gran Premio:",GPS_OFICIALES,key="gp_calc_main")
    estado=_safe_call(mcore["obtener_estado_gp"],gp_calc,HORARIOS_CARRERA,TZ,timeout_sec=4,default={"habilitado":False,"mensaje":"OK"})
    if (estado or {}).get("habilitado",True): st.error("⛔ El GP sigue habilitado."); st.stop()
    st.success(f"✅ OK para calcular: {(estado or {}).get('mensaje','')}")
    st.subheader("1) RESULTADOS OFICIALES (FIA)")
    oficial={}; c1,c2,c3=st.columns(3)
    with c1:
        st.markdown("**🏁 Carrera (1–10)**")
        for i in range(1,11): oficial[f"r{i}"]=st.text_input(f"Carrera {i}°",key=f"of_r{i}-{gp_calc}")
        oficial["col_r"]=st.number_input("Colapinto (Carrera)",1,22,10,key=f"of_cr-{gp_calc}")
    with c2:
        st.markdown("**⏱️ Qualy (1–5)**")
        for i in range(1,6): oficial[f"q{i}"]=st.text_input(f"Qualy {i}°",key=f"of_q{i}-{gp_calc}")
        oficial["col_q"]=st.number_input("Colapinto (Qualy)",1,22,10,key=f"of_cq-{gp_calc}")
    with c3:
        st.markdown("**🛠️ Constructores (AUTO)**")
        of_r_auto={i:oficial.get(f"r{i}","") for i in ESCALA_CARRERA_JUEGO.keys()}
        top3,tp=calcular_constructores_auto(of_r_auto,GRILLA_2026,ESCALA_CARRERA_JUEGO)
        if len(top3)>=3: oficial["c1"],oficial["c2"],oficial["c3"]=top3[0],top3[1],top3[2]; st.success(f"Top 3: {' / '.join(top3)}")
        else: oficial["c1"]=oficial["c2"]=oficial["c3"]=""; st.warning("Cargá primero los 10 de carrera.")
        if tp: st.write("Pts por equipo:",tp)
    if gp_calc in GPS_SPRINT:
        st.markdown("### ⚡ Sprint (1–8)"); cs1,cs2=st.columns(2)
        with cs1:
            for i in range(1,5): oficial[f"s{i}"]=st.text_input(f"Sprint {i}°",key=f"of_s{i}-{gp_calc}")
        with cs2:
            for i in range(5,9): oficial[f"s{i}"]=st.text_input(f"Sprint {i}°",key=f"of_s{i}-{gp_calc}")
    gp_done_key=f"GP_DONE::{gp_calc}"
    gp_done=_safe_call(mdb["lock_exists"],gp_done_key,timeout_sec=4,default=False)
    st.divider(); st.subheader("⚡ Calcular y actualizar todo el GP")
    if gp_done: st.warning("🔒 Ya calculado.")
    if st.button("⚡ CALCULAR Y ACTUALIZAR TODOS",use_container_width=True,key=f"btn_auto_{gp_calc}",disabled=gp_done):
        try:
            df_res=madm["calcular"](gp_calc=gp_calc,oficial=oficial,pilotos_torneo=PILOTOS_TORNEO,gps_sprint=GPS_SPRINT)
            st.success("✅ GP calculado."); st.dataframe(df_res,use_container_width=True)
        except Exception as e: st.error(f"❌ {e}"); st.exception(e)
    st.divider(); st.subheader("🧾 Generar historial (sin sumar puntos)")
    if st.button("🧾 GENERAR HISTORIAL",use_container_width=True,key=f"btn_hist_{gp_calc}"):
        try:
            df_h=madm["historial"](gp_calc=gp_calc,oficial=oficial,pilotos_torneo=PILOTOS_TORNEO,gps_sprint=GPS_SPRINT)
            st.success("✅ Historial generado."); st.dataframe(df_h,use_container_width=True)
        except Exception as e: st.error(f"Error: {e}")
    st.divider(); st.subheader("⛔ SANCIONES D.N.S.")
    dns_key=f"DNS_DONE::{gp_calc}"; dns_done=_safe_call(mdb["lock_exists"],dns_key,timeout_sec=4,default=False)
    if dns_done: st.warning("🔒 Sanciones ya aplicadas.")
    else:
        if st.button("Aplicar sanciones D.N.S. (−5 pts)",use_container_width=True,key=f"btn_dns_{gp_calc}"):
            df_d=mdb["aplicar_sanciones_dns"](gp_calc,PILOTOS_TORNEO,GPS_SPRINT)
            _safe_call(mdb["set_lock"],dns_key,timeout_sec=4)
            st.success("✅ Sanciones aplicadas."); st.dataframe(df_d,use_container_width=True)
    st.divider(); st.subheader("2) Preview de puntos (piloto individual)")
    pil_calc=st.selectbox("Piloto:",PILOTOS_TORNEO,key=f"pil_calc_{gp_calc}")
    res_pred=_safe_call(mdb["recuperar_predicciones_piloto"],pil_calc,gp_calc,timeout_sec=6,default=(None,None,(None,None)))
    db_q,db_s,(db_r,db_c)=res_pred
    if db_q or db_r or db_s: st.success(f"✅ Predicciones de {pil_calc} encontradas.")
    else: st.warning(f"⚠️ {pil_calc} sin predicciones para {gp_calc}.")
    if st.button("CALCULAR PREVIEW",use_container_width=True,key=f"btn_calc_{gp_calc}_{pil_calc}"):
        vr=normalizar_keys_num(db_r or {}); vq=normalizar_keys_num(db_q or {})
        vc=normalizar_keys_num(db_c or {}); vs=normalizar_keys_num(db_s or {})
        of_r={i:oficial.get(f"r{i}","") for i in range(1,11)}
        of_q={i:oficial.get(f"q{i}","") for i in range(1,6)}
        of_c={i:oficial.get(f"c{i}","") for i in range(1,4)}
        of_s={i:oficial.get(f"s{i}","") for i in range(1,9)}
        cp=mcore["calcular_puntos"]
        pts_r=cp("CARRERA",vr,of_r,vr.get("colapinto_r"),oficial.get("col_r"))
        pts_q=cp("QUALY",  vq,of_q,vq.get("colapinto_q"),oficial.get("col_q"))
        pts_c=cp("CONSTRUCTORES",vc,of_c)
        pts_s=cp("SPRINT",vs,of_s) if (gp_calc in GPS_SPRINT and db_s) else 0
        total=pts_r+pts_q+pts_c+pts_s
        st.success(f"💰 PUNTOS TOTALES: **{total}**")
        st.info(f"Carrera({pts_r}) + Constructores({pts_c}) + Qualy({pts_q}) + Sprint({pts_s})")
        st.caption("⚠️ Solo preview — no guarda nada.")
    st.divider(); st.subheader("🏆 Bonus Campeones (Final temporada)")
    gp_final=next((g for g in GPS_OFICIALES if g.startswith("24.")),GPS_OFICIALES[-1])
    if gp_calc!=gp_final: st.info(f"Solo en: **{gp_final}**"); return
    if _safe_call(mdb["lock_exists"],f"CHAMP_DONE::{gp_final}",timeout_sec=4,default=False):
        st.warning("🔒 Bonus ya aplicado."); return
    pil_r=st.text_input("Piloto campeón:",key="rcp")
    con_r=st.text_input("Constructor campeón:",key="rcc")
    if st.button("✅ APLICAR BONUS (1 sola vez)",use_container_width=True,key="btn_champ"):
        ok,out=mdb["aplicar_bonus_campeones_final"](gp_final,pil_r,con_r,"01. Gran Premio de Australia",PILOTOS_TORNEO)
        (st.success("✅ Bonus aplicado.") or st.dataframe(out,use_container_width=True)) if ok else st.warning(out)


def pantalla_mesa_chica():
    m = _mod_mesa()
    if "_error" in m:
        st.error(f"⚠️ Mesa Chica no disponible: {m['_error']}")
        return

    perfil = st.session_state.get("perfil") or {}
    usuario = perfil.get("usuario", "")
    if not usuario:
        st.warning("Tenés que iniciar sesión.")
        st.stop()

    is_mod = _mc_is_mod(usuario)

    if "mc_editing_id" not in st.session_state:
        st.session_state["mc_editing_id"] = None

    st.markdown("""
    <style>
    div[data-testid="stForm"] button[kind="primary"] {
        background: linear-gradient(90deg, #e10600, #ff3b3b) !important;
        border: none !important;
        color: white !important;
        font-weight: 700 !important;
        border-radius: 12px !important;
    }

    div[data-testid="stForm"] button[kind="primary"]:hover {
        background: linear-gradient(90deg, #ff3b3b, #ff6a6a) !important;
        transform: scale(1.03);
    }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div class="mc-wrap"><div class="mc-hero">
      <div class="mc-f1-bar">
        <span class="mc-f1-stripe" style="background:#e10600;"></span>
        <span class="mc-f1-stripe" style="background:#fff;"></span>
        <span class="mc-f1-text">F1 · 2026</span>
      </div>
      <div class="mc-title">MESA CHICA — FEFE WOLF</div>
      <div class="mc-sub">El paddock privado. <b>Solo se habla de F1</b> 🏁</div>
      <div class="mc-stats-bar">
        <span class="mc-stat-pill">🏎️ Temporada 2026</span>
        <span class="mc-stat-pill">📡 En vivo</span>
        <span class="mc-stat-pill">🏁 24 GPs</span>
      </div>
    </div></div>
    """, unsafe_allow_html=True)

    with st.form("mc_form", clear_on_submit=True):
        msg = st.text_area(
            "Escribí tu mensaje",
            key="mc_msg",
            height=80,
            placeholder="Hablá de F1…"
        )
        enviar = st.form_submit_button(
            "🏎️ Enviar a la Mesa Chica",
            use_container_width=True
        )

    cA, cB, cC = st.columns(3)

    with cA:
        if st.button("🔄 Actualizar", use_container_width=True):
            st.rerun()

    with cB:
        st.caption("🔵 FIPF = Mod · ⚫ Formulero = Participante")

    with cC:
        if is_mod and st.button("🧹 Limpiar rotos", use_container_width=True):
            m["mc_purge_html_messages"]()
            st.rerun()

    if enviar:
        txt = (msg or "").strip()
        if not txt:
            st.warning("Escribí algo.")
        elif m["mc_is_spam"](usuario):
            st.error("⚠️ Muy rápido.")
        else:
            m["mc_add_message"](usuario, txt)
            st.rerun()

    rows = m["mc_list_messages"](limit=250)

    for row in rows:
        msg_id, u, texto, ts, edited_ts = row[:5]
        tipo, label, stars = _mc_badge(u)
        gc = "mc-fipf" if _mc_is_mod(u) else "mc-formulero"
        badge_html = f'<span class="mc-badge {tipo}">{label} {f"<span class=mc-stars>{stars}</span>" if stars else ""}</span>'
        can_edit = is_mod or (u == usuario)
        can_delete = is_mod
        editando = (st.session_state["mc_editing_id"] == msg_id)
        edit_tag = f" · Editado: {edited_ts.replace('T', ' ')}" if edited_ts else ""
        safe_ts = _mc_safe(ts.replace("T", " ") + edit_tag)

        liked = m["mc_user_liked"](msg_id, usuario)
        lc = m["mc_like_count"](msg_id)
        lbl = f"{'🏁' if liked else '🚩'} {lc}"

        st.markdown(
            f'<div class="mc-card {gc} fade-up"><div class="mc-head"><div class="mc-userwrap"><div class="mc-name">{_mc_safe(u)}</div><div class="mc-badges">{badge_html}</div></div><div class="mc-time">{safe_ts}</div></div>',
            unsafe_allow_html=True
        )

        if editando:
            st.markdown("</div>", unsafe_allow_html=True)
            nuevo = st.text_area("Editar", value=texto, key=f"mc_et_{msg_id}", height=80)

            ec1, ec2, ec3, ec4 = st.columns(4)

            with ec1:
                if st.button("Guardar", key=f"mc_sv_{msg_id}", use_container_width=True):
                    nt = (nuevo or "").strip()
                    if nt:
                        m["mc_update_message"](msg_id, nt)
                        st.session_state["mc_editing_id"] = None
                        st.rerun()

            with ec2:
                if st.button("Cancelar", key=f"mc_ca_{msg_id}", use_container_width=True):
                    st.session_state["mc_editing_id"] = None
                    st.rerun()

            with ec3:
                if can_delete and st.button("Eliminar", key=f"mc_de_{msg_id}", use_container_width=True):
                    m["mc_soft_delete_message"](msg_id, deleted_by=usuario)
                    st.session_state["mc_editing_id"] = None
                    st.rerun()

            with ec4:
                if st.button(lbl, key=f"mc_le_{msg_id}", use_container_width=True):
                    m["mc_toggle_like"](msg_id, usuario)
                    st.rerun()

        else:
            st.markdown(f'<div class="mc-text">{_mc_safe(texto)}</div></div>', unsafe_allow_html=True)

            a1, a2, a3, a4 = st.columns([1, 1, 1, 3])

            with a1:
                if st.button(lbl, key=f"mc_lk_{msg_id}", use_container_width=True):
                    m["mc_toggle_like"](msg_id, usuario)
                    st.rerun()

            with a2:
                if can_edit and st.button("Editar", key=f"mc_ed_{msg_id}", use_container_width=True):
                    st.session_state["mc_editing_id"] = msg_id
                    st.rerun()

            with a3:
                if can_delete and st.button("Eliminar", key=f"mc_dl_{msg_id}", use_container_width=True):
                    m["mc_soft_delete_message"](msg_id, deleted_by=usuario)
                    st.rerun()

        st.divider()


def pantalla_api_test():
    st.title("🧪 Test API F1")
    year=st.number_input("Año",2000,2100,2026,step=1)
    if st.button("Probar constructors"):
        try:
            data=requests.get(f"{API_BASE}/f1/constructors",params={"year":int(year)},timeout=8).json()
            st.success("API OK ✅"); st.json(data)
        except Exception as e: st.error(f"Falló: {e}")


# ─────────────────────────────────────────────────────────
# 10. MAIN
# ─────────────────────────────────────────────────────────
def main():
    sidebar_login_block()

    st.sidebar.markdown("""
    <div style="text-align:center;font-family:monospace;font-size:11px;font-weight:900;
    letter-spacing:.18em;color:rgba(246,195,73,.70);margin-bottom:8px;">MENÚ PRINCIPAL</div>
    """, unsafe_allow_html=True)

    opciones = [
        "🏠  Inicio & Historia",
        "📅  Calendario 2026",
        "🔒  Cargar Predicciones",
        "📊  Tabla de Posiciones",
        "📈  Historial GP",
        "🧮  Calculadora de Puntos",
        "🏎️  Pilotos y Escuderías",
        "📜  Reglamento Oficial",
        "🏆  Muro de Campeones",
        "💬  Mesa Chica",
    ]
    if is_admin(): opciones.append("🧪  Test API F1")

    opcion = st.sidebar.radio("", opciones, index=0, label_visibility="collapsed")
    st.sidebar.markdown("---")
    st.sidebar.markdown('<div style="text-align:center;font-size:11px;color:rgba(169,178,214,.40);">🏁 Torneo Fefe Wolf 2026<br>© Formuleros</div>',unsafe_allow_html=True)

    if   "Inicio"       in opcion: pantalla_inicio()
    elif "Calendario"   in opcion: pantalla_calendario()
    elif "Predicciones" in opcion: pantalla_cargar_predicciones()
    elif "Posiciones"   in opcion: pantalla_tabla_posiciones()
    elif "Historial"    in opcion: pantalla_historial_gp()
    elif "Calculadora"  in opcion: pantalla_calculadora_puntos()
    elif "Pilotos"      in opcion: pantalla_pilotos_y_escuderias()
    elif "Reglamento"   in opcion: pantalla_reglamento()
    elif "Campeones"    in opcion: pantalla_muro()
    elif "Mesa"         in opcion: pantalla_mesa_chica()
    elif "Test"         in opcion: pantalla_api_test()

    flecha_arriba()


main()
