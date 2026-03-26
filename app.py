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

load_css()

# CSS global — tablas centradas + mejoras mobile
st.markdown("""
<style>
/* ── Tablas historial — centradas y scroll horizontal en mobile ── */
.tabla_historial_dark {
    width: 100%; max-width: 900px;
    margin: 10px auto 12px auto !important;
    border-collapse: collapse;
    background: rgba(7,10,25,.96); color: #e8ecff;
    border: 1px solid rgba(212,175,55,.25);
    border-radius: 14px; overflow-x: auto; display: block; font-size: 14px;
}
.tabla_historial_dark th {
    background: linear-gradient(90deg, rgba(212,175,55,.18), rgba(255,255,255,.03));
    color: #ffdd7a; text-align: center; padding: 12px 14px;
    border-bottom: 1px solid rgba(212,175,55,.22); font-weight: 800;
}
.tabla_historial_dark td {
    padding: 11px 14px; color: #e8ecff; text-align: center;
    border-bottom: 1px solid rgba(255,255,255,.06); background: rgba(255,255,255,.02);
}
.tabla_historial_dark tr:hover td { background: rgba(255,221,122,.05); }
/* fw-table también centrada */
.fw-table-wrap { overflow-x: auto; }
/* Selectbox golden arrow — desktop + mobile */
[data-baseweb="select"] > div:first-child {
  border-color: rgba(212,175,55,.4) !important;
  background: rgba(5,7,18,.95) !important;
}
[data-baseweb="select"] > div:first-child:hover,
[data-baseweb="select"] > div:first-child:focus-within {
  border-color: rgba(212,175,55,.85) !important;
  box-shadow: 0 0 8px rgba(212,175,55,.18) !important;
}
[data-baseweb="select"] svg { fill: #d4af37 !important; }
[data-testid="stSelectbox"] label { color: rgba(246,195,73,.8) !important; }
/* Mobile responsive */
@media (max-width:640px) {
  .mc-bubble { max-width:96% !important; }
  .mc-bubble-text { font-size:12px !important; }
  [data-testid="stSidebar"] { min-width:200px !important; }
}
/* Mesa Chica MOD badge glow */
.mc-badge.fipf { background:linear-gradient(90deg,#0d2a6e,#1565c0,#0d2a6e)!important;
  color:#90caf9!important; border:1px solid rgba(100,180,255,.5)!important;
  box-shadow:0 0 10px rgba(21,101,192,.4)!important;
  text-shadow:0 0 8px rgba(100,180,255,.6); }
.mc-stars { color:#d4af37!important; text-shadow:0 0 6px #d4af37; }
.mc-badge.formulero { background:linear-gradient(90deg,#4a0080,#7b2ff7,#4a0080)!important;
  color:#e0b0ff!important; border:1px solid rgba(180,100,255,.6)!important;
  box-shadow:0 0 10px rgba(123,47,247,.4)!important;
  text-shadow:0 0 8px rgba(200,140,255,.7); font-weight:800!important; }
.fw-table { width: 100%; max-width: 860px; margin: 0 auto; }
/* Hide native sidebar collapse arrow - all possible selectors */
[data-testid="stSidebarCollapsedControl"],
button[data-testid="collapsedControl"],
.st-emotion-cache-1cyp2mc,
.st-emotion-cache-czk5ss,
.st-emotion-cache-vk3wp9,
button[aria-label="Collapse sidebar"],
button[aria-label="Expand sidebar"],
[data-testid="baseButton-headerNoPadding"] { display: none !important; }
/* Mobile */
@media (max-width: 768px) {
    .tabla_historial_dark { font-size: 12px; }
    .tabla_historial_dark th, .tabla_historial_dark td { padding: 8px 6px; }
    .mc-title { font-size: 20px !important; }
}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────
# 3. LAZY MODULE LOADERS
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
    "MCLAREN":      ["Oscar Piastri","Lando Norris"],
    "RED BULL":     ["Max Verstappen","Isack Hadjar"],
    "MERCEDES":     ["George Russell","Kimi Antonelli"],
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

# ── Logos oficiales de equipos (CDN Formula1.com) ──────────────
TEAM_LOGOS_CDN = {
    # Logos oficiales F1 — URLs directas y verificadas
    "MCLAREN":      "https://media.formula1.com/image/upload/c_fit,h_64/q_auto/v1740000000/common/f1/2026/mclaren/2026mclarenlogowhite.webp",
    "RED BULL":     "https://media.formula1.com/image/upload/c_fit,h_64/q_auto/v1740000000/common/f1/2025/redbullracing/2025redbullracinglogowhite.webp",
    "MERCEDES":     "https://media.formula1.com/image/upload/c_fit,h_64/q_auto/v1740000000/common/f1/2026/mercedes/2026mercedeslogowhite.webp",
    "FERRARI":      "https://media.formula1.com/image/upload/c_fit,h_64/q_auto/v1740000000/common/f1/2026/ferrari/2026ferrarilogowhite.webp",
    "WILLIAMS":     "https://media.formula1.com/image/upload/c_fit,h_64/q_auto/v1740000000/common/f1/2026/williams/2026williamslogowhite.webp",
    "ASTON MARTIN": "https://media.formula1.com/image/upload/c_fit,h_64/q_auto/v1740000000/common/f1/2026/astonmartin/2026astonmartinlogowhite.webp",
    "RACING BULLS": "https://media.formula1.com/image/upload/c_fit,h_64/q_auto/v1740000000/common/f1/2026/racingbulls/2026racingbullslogowhite.webp",
    "HAAS":         "https://media.formula1.com/image/upload/c_fit,h_64/q_auto/v1740000000/common/f1/2026/haasf1team/2026haasf1teamlogowhite.webp",
    "AUDI":         "https://media.formula1.com/image/upload/c_fit,h_64/q_auto/v1740000000/common/f1/2026/audi/2026audilogowhite.webp",
    "ALPINE":       "https://media.formula1.com/image/upload/c_fit,h_64/q_auto/v1740000000/common/f1/2026/alpine/2026alpinelogowhite.webp",
    "CADILLAC":     "https://media.formula1.com/image/upload/c_fit,h_64/q_auto/v1740000000/common/f1/2026/cadillac/2026cadillaclogowhite.webp",
}
# ── Imágenes de autos para constructores (módulo global) ──
TEAM_CARS_MODULE = {
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
DRIVER_PHOTOS = {
    # Cuerpo completo — usado en sección Pilotos y Escuderías
    "Lando Norris":      "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/mclaren/lannor01/2026mclarenlannor01right.webp",
    "Oscar Piastri":     "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/mclaren/oscpia01/2026mclarenoscpia01right.webp",
    "Max Verstappen":    "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/redbullracing/maxver01/2026redbullracingmaxver01right.webp",
    "Isack Hadjar":      "https://media.formula1.com/image/upload/c_lfill,w_440/q_auto/v1740000000/common/f1/2026/redbullracing/isahad01/2026redbullracingisahad01right.webp",
    "George Russell":    "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/mercedes/georus01/2026mercedesgeorus01right.webp",
    "Kimi Antonelli":    "https://media.formula1.com/image/upload/c_lfill,w_440/q_auto/v1740000000/common/f1/2026/mercedes/andant01/2026mercedesandant01right.webp",
    "Charles Leclerc":   "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/ferrari/chalec01/2026ferrarichalec01right.webp",
    "Lewis Hamilton":    "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/ferrari/lewham01/2026ferrarilewham01right.webp",
    "Alex Albon":        "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/williams/alealb01/2026williamsalealb01right.webp",
    "Carlos Sainz":      "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/williams/carsai01/2026williamscarsai01right.webp",
    "Lance Stroll":      "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/astonmartin/lanstr01/2026astonmartinlanstr01right.webp",
    "Fernando Alonso":   "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/astonmartin/feralo01/2026astonmartinferalo01right.webp",
    "Liam Lawson":       "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/racingbulls/lialaw01/2026racingbullslialaw01right.webp",
    "Arvid Lindblad":    "https://media.formula1.com/image/upload/c_lfill,w_440/q_auto/v1740000000/common/f1/2026/racingbulls/arvlin01/2026racingbullsarvlin01right.webp",
    "Oliver Bearman":    "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/haasf1team/olibea01/2026haasf1teamolibea01right.webp",
    "Esteban Ocon":      "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/haasf1team/estoco01/2026haasf1teamestoco01right.webp",
    "Nico Hulkenberg":   "https://media.formula1.com/image/upload/c_lfill,w_440/q_auto/v1740000000/common/f1/2026/audi/nichul01/2026audinichul01right.webp",
    "Gabriel Bortoleto": "https://media.formula1.com/image/upload/c_lfill,w_440/q_auto/v1740000000/common/f1/2026/audi/gabbor01/2026audigabbor01right.webp",
    "Pierre Gasly":      "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/alpine/piegas01/2026alpinepiegas01right.webp",
    "Franco Colapinto":  "https://media.formula1.com/image/upload/c_fill,w_720/q_auto/v1740000000/common/f1/2026/alpine/fracol01/2026alpinefracol01right.webp",
    "Checo Perez":       "https://media.formula1.com/image/upload/c_lfill,w_440/q_auto/v1740000000/common/f1/2026/cadillac/serper01/2026cadillacserper01right.webp",
    "Valteri Bottas":    "https://media.formula1.com/image/upload/c_lfill,w_440/q_auto/v1740000000/common/f1/2026/cadillac/valbot01/2026cadillacvalbot01right.webp",
}

# Headshots (cara) — crop desde arriba con Cloudinary g_north — usado en Predicciones
DRIVER_HEADSHOTS = {
    "Lando Norris":      "https://media.formula1.com/image/upload/c_fill,w_272,h_272,g_north/q_auto/v1740000000/common/f1/2026/mclaren/lannor01/2026mclarenlannor01right.webp",
    "Oscar Piastri":     "https://media.formula1.com/image/upload/c_fill,w_272,h_272,g_north/q_auto/v1740000000/common/f1/2026/mclaren/oscpia01/2026mclarenoscpia01right.webp",
    "Max Verstappen":    "https://media.formula1.com/image/upload/c_fill,w_272,h_272,g_north/q_auto/v1740000000/common/f1/2026/redbullracing/maxver01/2026redbullracingmaxver01right.webp",
    "Isack Hadjar":      "https://media.formula1.com/image/upload/c_fill,w_272,h_272,g_north/q_auto/v1740000000/common/f1/2026/redbullracing/isahad01/2026redbullracingisahad01right.webp",
    "George Russell":    "https://media.formula1.com/image/upload/c_fill,w_272,h_272,g_north/q_auto/v1740000000/common/f1/2026/mercedes/georus01/2026mercedesgeorus01right.webp",
    "Kimi Antonelli":    "https://media.formula1.com/image/upload/c_fill,w_272,h_272,g_north/q_auto/v1740000000/common/f1/2026/mercedes/andant01/2026mercedesandant01right.webp",
    "Charles Leclerc":   "https://media.formula1.com/image/upload/c_fill,w_272,h_272,g_north/q_auto/v1740000000/common/f1/2026/ferrari/chalec01/2026ferrarichalec01right.webp",
    "Lewis Hamilton":    "https://media.formula1.com/image/upload/c_fill,w_272,h_272,g_north/q_auto/v1740000000/common/f1/2026/ferrari/lewham01/2026ferrarilewham01right.webp",
    "Alex Albon":        "https://media.formula1.com/image/upload/c_fill,w_272,h_272,g_north/q_auto/v1740000000/common/f1/2026/williams/alealb01/2026williamsalealb01right.webp",
    "Carlos Sainz":      "https://media.formula1.com/image/upload/c_fill,w_272,h_272,g_north/q_auto/v1740000000/common/f1/2026/williams/carsai01/2026williamscarsai01right.webp",
    "Lance Stroll":      "https://media.formula1.com/image/upload/c_fill,w_272,h_272,g_north/q_auto/v1740000000/common/f1/2026/astonmartin/lanstr01/2026astonmartinlanstr01right.webp",
    "Fernando Alonso":   "https://media.formula1.com/image/upload/c_fill,w_272,h_272,g_north/q_auto/v1740000000/common/f1/2026/astonmartin/feralo01/2026astonmartinferalo01right.webp",
    "Liam Lawson":       "https://media.formula1.com/image/upload/c_fill,w_272,h_272,g_north/q_auto/v1740000000/common/f1/2026/racingbulls/lialaw01/2026racingbullslialaw01right.webp",
    "Arvid Lindblad":    "https://media.formula1.com/image/upload/c_fill,w_272,h_272,g_north/q_auto/v1740000000/common/f1/2026/racingbulls/arvlin01/2026racingbullsarvlin01right.webp",
    "Oliver Bearman":    "https://media.formula1.com/image/upload/c_fill,w_272,h_272,g_north/q_auto/v1740000000/common/f1/2026/haasf1team/olibea01/2026haasf1teamolibea01right.webp",
    "Esteban Ocon":      "https://media.formula1.com/image/upload/c_fill,w_272,h_272,g_north/q_auto/v1740000000/common/f1/2026/haasf1team/estoco01/2026haasf1teamestoco01right.webp",
    "Nico Hulkenberg":   "https://media.formula1.com/image/upload/c_fill,w_272,h_272,g_north/q_auto/v1740000000/common/f1/2026/audi/nichul01/2026audinichul01right.webp",
    "Gabriel Bortoleto": "https://media.formula1.com/image/upload/c_fill,w_272,h_272,g_north/q_auto/v1740000000/common/f1/2026/audi/gabbor01/2026audigabbor01right.webp",
    "Pierre Gasly":      "https://media.formula1.com/image/upload/c_fill,w_272,h_272,g_north/q_auto/v1740000000/common/f1/2026/alpine/piegas01/2026alpinepiegas01right.webp",
    "Franco Colapinto":  "https://media.formula1.com/image/upload/c_fill,w_272,h_272,g_north/q_auto/v1740000000/common/f1/2026/alpine/fracol01/2026alpinefracol01right.webp",
    "Checo Perez":       "https://media.formula1.com/image/upload/c_fill,w_272,h_272,g_north/q_auto/v1740000000/common/f1/2026/cadillac/serper01/2026cadillacserper01right.webp",
    "Valteri Bottas":    "https://media.formula1.com/image/upload/c_fill,w_272,h_272,g_north/q_auto/v1740000000/common/f1/2026/cadillac/valbot01/2026cadillacvalbot01right.webp",
    # Nicki Lauda — foto real incrustada en base64
    "Nicki Lauda":       "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSgd3Dq9OHc2zK46t1QQLXbN_49nouLYDYv1w&s",
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
# 5. AUTH TOKENS
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
# 6. PERFIL CACHEADO
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

# ─────────────────────────────────────────────────────────
# 7b. NUEVOS HELPERS VISUALES PARA PREDICCIONES (estilo VueltaRápida)
# ─────────────────────────────────────────────────────────
def _make_lineup_preview(kp, count):
    """Genera HTML con cards estilo VueltaRápida leyendo session_state."""
    POS_COL = {1:"#FFD700", 2:"#C0C0C0", 3:"#CD7F32"}
    rows = []
    for i in range(1, count + 1):
        nombre = st.session_state.get(f"{kp}_{i}", "")
        pc = POS_COL.get(i, "#6366f1")

        if not nombre:
            rows.append(
                f'<div style="display:flex;align-items:center;gap:10px;padding:8px 12px;'
                f'background:rgba(255,255,255,.015);border:1px dashed rgba(255,255,255,.09);'
                f'border-radius:10px;min-height:56px;">'
                f'<span style="color:{pc};font-weight:900;font-size:15px;min-width:24px;'
                f'text-align:center;">{i}</span>'
                f'<div style="width:42px;height:42px;border-radius:50%;background:rgba(255,255,255,.04);'
                f'border:1.5px dashed rgba(255,255,255,.15);flex-shrink:0;"></div>'
                f'<span style="color:rgba(169,178,214,.35);font-size:11px;font-style:italic;">'
                f'Sin seleccionar</span></div>'
            )
        else:
            eq   = next((t for t, ds in GRILLA_2026.items() if nombre in ds), "")
            tc   = TEAM_COLORS.get(eq, "#a855f7")
            # ← Headshot (cara) para predicciones, fallback a full-body
            ph   = DRIVER_HEADSHOTS.get(nombre, DRIVER_PHOTOS.get(nombre, ""))
            ini  = "".join(w[0] for w in nombre.split()[:2]).upper()
            last  = nombre.split()[-1].upper()
            first = " ".join(nombre.split()[:-1])
            sz = "18px" if i <= 3 else "15px"

            img_html = (
                f'<img src="{ph}" '
                f'style="width:100%;height:100%;object-fit:cover;object-position:top center;display:block;" '
                f'onerror="this.style.display=\'none\';this.nextSibling.style.display=\'flex\'">'
            ) if ph else ""
            fallback_html = (
                f'<div style="display:{"none" if ph else "flex"};width:100%;height:100%;'
                f'align-items:center;justify-content:center;font-weight:900;font-size:11px;color:{tc};">'
                f'{ini}</div>'
            )
            rows.append(
                f'<div style="display:flex;align-items:center;gap:10px;padding:7px 12px;'
                f'background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.07);'
                f'border-left:3px solid {tc};border-radius:10px;min-height:56px;">'
                f'<span style="color:{pc};font-weight:900;font-size:{sz};min-width:24px;text-align:center;">{i}</span>'
                f'<div style="width:42px;height:42px;border-radius:50%;overflow:hidden;'
                f'border:2px solid {tc};flex-shrink:0;background:#080b1a;">'
                f'{img_html}{fallback_html}</div>'
                f'<div style="flex:1;min-width:0;overflow:hidden;">'
                f'<div style="font-size:9px;font-weight:700;letter-spacing:.1em;color:{tc};'
                f'text-transform:uppercase;opacity:.85;white-space:nowrap;overflow:hidden;">{eq}</div>'
                f'<div style="font-size:11px;font-weight:400;color:rgba(169,178,214,.65);'
                f'white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{first}</div>'
                f'<div style="font-size:13px;font-weight:900;color:#e8ecff;letter-spacing:.02em;'
                f'white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{last}</div>'
                f'</div></div>'
            )
    return '<div style="display:flex;flex-direction:column;gap:4px;">' + "".join(rows) + '</div>'


def _make_teams_preview(kp, count):
    """Genera HTML con cards de constructores leyendo session_state."""
    MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}
    rows = []
    for i in range(1, count + 1):
        team = st.session_state.get(f"{kp}_{i}", "")
        if not team:
            rows.append(
                f'<div style="display:flex;align-items:center;gap:10px;padding:11px 13px;'
                f'background:rgba(255,255,255,.015);border:1px dashed rgba(255,255,255,.09);'
                f'border-radius:9px;min-height:54px;">'
                f'<span style="font-size:22px;">{MEDALS.get(i, str(i)+"°")}</span>'
                f'<span style="color:rgba(169,178,214,.35);font-size:11px;font-style:italic;">'
                f'Sin seleccionar</span></div>'
            )
        else:
            color = TEAM_COLORS.get(team, "#a855f7")
            abbr  = TEAM_LOGOS_SVG.get(team, team[:3])
            car   = TEAM_CARS_MODULE.get(team, "")
            car_html = (
                f'<img src="{car}" style="height:28px;max-width:76px;object-fit:contain;" '
                f'onerror="this.style.display=\'none\'">'
            ) if car else f'<span style="font-size:11px;font-weight:900;color:{color};">{abbr}</span>'
            rows.append(
                f'<div style="display:flex;align-items:center;gap:10px;padding:9px 13px;'
                f'background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.07);'
                f'border-left:3px solid {color};border-radius:9px;min-height:54px;">'
                f'<span style="font-size:22px;">{MEDALS.get(i, str(i)+"°")}</span>'
                f'<div style="background:rgba(255,255,255,.04);border-radius:6px;padding:3px 7px;'
                f'border:1px solid {color}33;display:flex;align-items:center;justify-content:center;'
                f'min-width:64px;height:34px;flex-shrink:0;">{car_html}</div>'
                f'<div style="flex:1;min-width:0;">'
                f'<div style="font-size:13px;font-weight:800;color:#e8ecff;">{team}</div>'
                f'<div style="font-size:9px;color:{color};font-weight:700;letter-spacing:.08em;'
                f'text-transform:uppercase;opacity:.8;">{abbr}</div>'
                f'</div></div>'
            )
    return '<div style="display:flex;flex-direction:column;gap:5px;">' + "".join(rows) + '</div>'


def _pred_section_label(text):
    return (f'<div style="font-size:10px;font-weight:700;letter-spacing:.13em;'
            f'color:rgba(246,195,73,.65);text-transform:uppercase;margin-bottom:6px;'
            f'margin-top:4px;">{text}</div>')


# ─────────────────────────────────────────────────────────
# 8. FLECHA DORADA MEJORADA
# ─────────────────────────────────────────────────────────
def flecha_arriba():
    components.html("""
    <script>
    (function() {
        try {
            var p = window.parent || window;
            var d = p.document;

            // Create or update style
            if (!d.getElementById('fw-top-style')) {
                var s = d.createElement('style');
                s.id = 'fw-top-style';
                s.textContent = [
                    '#fw-top-btn{position:fixed!important;right:22px;bottom:30px;',
                    'width:54px;height:54px;border-radius:50%;',
                    'background:linear-gradient(145deg,#ffe896 0%,#d4af37 55%,#9a7a10 100%);',
                    'border:2px solid rgba(255,238,150,.75);',
                    'box-shadow:0 0 0 5px rgba(212,175,55,.12),0 8px 32px rgba(0,0,0,.65);',
                    'display:flex!important;align-items:center;justify-content:center;',
                    'cursor:pointer;z-index:2147483647!important;',
                    'font-size:28px;font-weight:900;color:#1a1000;line-height:1;',
                    'outline:none;border-style:solid;',
                    'transition:transform .2s cubic-bezier(.34,1.56,.64,1);}',
                    '#fw-top-btn:hover{transform:translateY(-4px) scale(1.08);}',
                    '#fw-top-btn:active{transform:scale(.92);}'
                ].join('');
                d.head.appendChild(s);
            }

            // Remove stale button from previous rerun and recreate
            var old = d.getElementById('fw-top-btn');
            if (old) old.remove();

            var btn = d.createElement('button');
            btn.id = 'fw-top-btn';
            btn.title = 'Volver al inicio';
            btn.innerHTML = '&#8679;';
            btn.addEventListener('click', function() {
                // Try every known Streamlit scroll container
                [
                    d.querySelector('[data-testid="stMain"]'),
                    d.querySelector('[data-testid="stAppViewContainer"]'),
                    d.querySelector('.main > div'),
                    d.querySelector('section.main'),
                    d.querySelector('.block-container'),
                    d.documentElement,
                    d.body
                ].forEach(function(el) {
                    if (el) { try { el.scrollTop = 0; } catch(e){} }
                });
                try { p.scrollTo(0, 0); } catch(e){}
            });
            d.body.appendChild(btn);
        } catch(e) { console.warn('fw-top:', e); }
    })();
    </script>
    """, height=0)


# ─────────────────────────────────────────────────────────
# 8b. MINI BARRA — botón flotante para ocultar/mostrar el menú
# ─────────────────────────────────────────────────────────
def mini_bar():
    components.html("""
    <script>
    (function() {
        try {
            var p = window.parent || window;
            var d = p.document;
            var KEY = 'fw_sb_v4';

            // ── Kill native sidebar toggle forever ──
            if (!d.getElementById('fw-kill-sb-arrow')) {
                var ka=d.createElement('style');ka.id='fw-kill-sb-arrow';
                ka.textContent='[data-testid="stSidebarCollapsedControl"],button[data-testid="collapsedControl"],[data-testid="baseButton-headerNoPadding"],button[aria-label="Collapse sidebar"],button[aria-label="Expand sidebar"],.st-emotion-cache-1cyp2mc,.st-emotion-cache-czk5ss,.st-emotion-cache-vk3wp9{display:none!important;}';
                d.head.appendChild(ka);
            }
            // ── Inject style once ──
            if (!d.getElementById('fw-mb-style')) {
                var s = d.createElement('style');
                s.id = 'fw-mb-style';
                s.textContent = [
                    '#fw-mb-wrap{position:fixed!important;left:0;top:50%;',
                    'transform:translateY(-50%);z-index:2147483646!important;}',
                    '#fw-mb-btn{background:rgba(7,9,20,.96);',
                    'border:1.5px solid rgba(246,195,73,.6);border-left:none;',
                    'border-radius:0 14px 14px 0;padding:16px 10px 16px 6px;',
                    'cursor:pointer;color:#f6c349;display:flex;flex-direction:column;',
                    'align-items:center;gap:5px;min-width:36px;',
                    'box-shadow:5px 0 24px rgba(0,0,0,.6);',
                    'transition:all .2s ease;outline:none;}',
                    '#fw-mb-btn:hover{background:rgba(246,195,73,.15);',
                    'border-color:rgba(246,195,73,.95);}',
                    '#fw-mb-icon{font-size:18px;line-height:1;}',
                    '#fw-mb-lbl{font-size:7px;letter-spacing:.14em;font-weight:900;',
                    'writing-mode:vertical-rl;opacity:.75;text-transform:uppercase;}'
                ].join('');
                d.head.appendChild(s);
            }

            // ── Inject sidebar dynamic style ──
            var dynStyle = d.getElementById('fw-sb-dyn');
            if (!dynStyle) {
                dynStyle = d.createElement('style');
                dynStyle.id = 'fw-sb-dyn';
                d.head.appendChild(dynStyle);
            }

            // ── Create/recreate button ──
            var wrap = d.getElementById('fw-mb-wrap');
            if (!wrap) {
                wrap = d.createElement('div');
                wrap.id = 'fw-mb-wrap';
                wrap.innerHTML =
                    '<button id="fw-mb-btn">' +
                    '<span id="fw-mb-icon">&#9776;</span>' +
                    '<span id="fw-mb-lbl">MEN&Uacute;</span>' +
                    '</button>';
                d.body.appendChild(wrap);
            }

            var hidden = (p.localStorage.getItem(KEY) === '1');

            function applyState() {
                var icon = d.getElementById('fw-mb-icon');
                var lbl  = d.getElementById('fw-mb-lbl');
                if (hidden) {
                    dynStyle.textContent =
                        'section[data-testid="stSidebar"]{' +
                        'transform:translateX(-120%)!important;' +
                        'transition:transform .35s ease!important;}' +
                        '[data-testid="stSidebarCollapsedControl"]{display:none!important;}' +
                        '.block-container{margin-left:0!important;' +
                        'max-width:100%!important;padding-left:1rem!important;}';
                    if (icon) icon.innerHTML = '&#9776;';
                    if (lbl)  lbl.textContent = 'MEN\u00DA';
                } else {
                    dynStyle.textContent =
                        'section[data-testid="stSidebar"]{' +
                        'transform:translateX(0)!important;' +
                        'transition:transform .35s ease!important;}';
                    if (icon) icon.innerHTML = '&times;';
                    if (lbl)  lbl.textContent = 'CERRAR';
                }
            }

            // Always reattach click handler (button may have been recreated)
            var btn = d.getElementById('fw-mb-btn');
            btn.onclick = function() {
                hidden = !hidden;
                p.localStorage.setItem(KEY, hidden ? '1' : '0');
                applyState();
            };

            applyState();
        } catch(e) { console.warn('fw-mb:', e); }
    })();
    </script>
    """, height=0)


# ─────────────────────────────────────────────────────────
# 9. LOGIN + SIDEBAR
# ─────────────────────────────────────────────────────────
def sidebar_login_block():
    for k,d in [("perfil",None),("usuario",None),("_tok_done",False),("fw_force_nav",None)]:
        if k not in st.session_state: st.session_state[k] = d

    token = qp_get("t")
    if not is_logged_in() and token and not st.session_state["_tok_done"]:
        st.session_state["_tok_done"] = True
        u = auth_user_from_token(token)
        if u:
            with st.spinner("Restaurando sesión..."):
                perfil = _get_perfil(u)
            if perfil:
                st.session_state["perfil"] = perfil
                st.session_state["usuario"] = perfil["usuario"]
                st.rerun()
            else:
                try: st.query_params.clear()
                except: pass

    if is_logged_in() and not qp_get("t"):
        u2 = (st.session_state.get("perfil") or {}).get("usuario","")
        if u2: qp_set("t", auth_create_token(u2))

    if not is_logged_in():
        st.markdown("""
    <style>
    .tabla_historial_dark {
        width: 100%;
        max-width: 900px;
        margin: 10px auto 12px auto;
        border-collapse: collapse;
        background: rgba(7, 10, 25, 0.96);
        color: #e8ecff;
        border: 1px solid rgba(212, 175, 55, 0.25);
        border-radius: 14px;
        overflow: hidden;
        font-size: 14px;
        display: block;
        overflow-x: auto;
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
    .tabla_historial_dark tr:hover td { background: rgba(255,221,122,0.05); }
    .hist_car_track {
        position: relative; width: 100%; height: 30px; margin: 4px 0 12px 0;
        overflow: hidden; border-radius: 999px;
        background: linear-gradient(90deg, rgba(255,255,255,0.02), rgba(212,175,55,0.08), rgba(255,255,255,0.02));
        border: 1px solid rgba(212,175,55,0.18);
    }
    .hist_car { position: absolute; left: -60px; top: 3px; font-size: 21px; animation: histCarMove 6s linear infinite; }
    @keyframes histCarMove { 0% { left: -60px; } 100% { left: calc(100% + 60px); } }
    .flecha_subir_dorada {
        position: fixed; right: 24px; bottom: 22px; width: 46px; height: 46px;
        border-radius: 50%; display: flex; align-items: center; justify-content: center;
        text-decoration: none; font-size: 22px; font-weight: 900; color: #1a1200;
        background: linear-gradient(180deg, #ffe38a 0%, #d4af37 100%);
        border: 1px solid rgba(255,240,180,0.65); box-shadow: 0 0 18px rgba(212,175,55,0.40); z-index: 9999;
    }
    .flecha_subir_dorada:hover { transform: scale(1.06); box-shadow: 0 0 24px rgba(212,175,55,0.58); }
    section[data-testid="stSidebar"],
    [data-testid="stSidebarCollapsedControl"] { display: none !important; }
    button[kind="primary"]{
        background:linear-gradient(90deg,#e10600,#ff3b3b); border:none; color:white; font-weight:700;
    }
    button[kind="primary"]:hover{ background:linear-gradient(90deg,#ff3b3b,#ff6a6a); transform:scale(1.03); }
    </style>
    """, unsafe_allow_html=True)

        st.markdown("""
        <div class="fw-login-page">
          <div class="fw-login-shell">
            <div class="fw-login-card">
              <div class="fw-login-overlay"></div>
              <div class="fw-login-inner">
                <div class="fw-login-title">🏆</div>
              </div>
            </div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        _,c2,_ = st.columns([1.1,1,1.1])
        with c2:
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
                        with st.spinner("🔄 Verificando credenciales..."):
                            ok, res = _safe_call(
                                m["login"], u_in, p_in,
                                timeout_sec=45,
                                default=(False, "⏱️ El servidor tardó demasiado. Reintentá.")
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
                            st.success("✅ Contraseña actualizada.") if ok2 else st.error(msg2)

        st.stop()

    # ── LOGUEADO — SIDEBAR ───────────────────────────────
    perfil = st.session_state["perfil"] or {}
    usr   = perfil.get("usuario","")
    rol   = perfil.get("rol","Piloto")
    copas = int(perfil.get("copas",0) or 0)
    color = PILOTO_COLORS.get(usr,"#a855f7")
    trofeos = "🏆"*copas if copas else "—"

    # ── Build rich per-pilot profile tags ──────────────────
    _PILOT_PROFILE_TAGS = {
        "Checo Perez":    [("🏆 Comisario","rgba(212,175,55,.25)","#ffdd7a"),
                           ("👑 Administrador","rgba(212,175,55,.15)","#d4af37"),
                           ("🏎️ Piloto","rgba(255,255,255,.08)","#e8ecff")],
        "Fernando Alonso":[("🏎️ Piloto","rgba(255,68,68,.15)","#FF4444"),
                           ("🔵 FIPF","rgba(21,101,192,.25)","#90caf9")],
        "Lando Norris":   [("🥈 Sub Comisario","rgba(255,165,0,.2)","#FFA500"),
                           ("🏎️ Piloto","rgba(255,255,255,.08)","#e8ecff"),
                           ("🔵 FIPF","rgba(21,101,192,.25)","#90caf9")],
        "Valteri Bottas": [("🏎️ Piloto","rgba(0,207,255,.12)","#00CFFF"),
                           ("🔵 FIPF","rgba(21,101,192,.25)","#90caf9")],
        "Nicki Lauda":    [("🏎️ Piloto","rgba(30,144,255,.15)","#1E90FF"),
                           ("⚫ Formulero","rgba(255,255,255,.08)","#a9b2d6")],
    }
    # Lauda photo for sidebar
    _LAUDA_PHOTO_B64 = "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSgd3Dq9OHc2zK46t1QQLXbN_49nouLYDYv1w&s"
    _sidebar_photo = _LAUDA_PHOTO_B64 if usr == "Nicki Lauda" else ""
    _tags  = _PILOT_PROFILE_TAGS.get(usr, [(rol,"rgba(255,255,255,.08)","#e8ecff")])
    _tags_html = "".join(
        f"<div style='display:block;padding:4px 10px;margin:3px auto;border-radius:10px;"
        f"background:{bg};color:{fc};font-size:11px;font-weight:800;"
        f"letter-spacing:.07em;text-align:center;'>{lbl}</div>"
        for lbl,bg,fc in _tags
    )
    _trophy_label = ""
    if copas > 0:
        _cups_anim = "".join(
            f"<span style='animation:trophy-bounce {2+k*0.3}s ease-in-out {k*0.2}s infinite;"
            f"display:inline-block;font-size:28px;filter:drop-shadow(0 0 8px {color}cc);'>🏆</span>"
            for k in range(copas)
        )
        _trophy_label = f"<div style='margin-top:6px;'>{_cups_anim}</div>"\
            f"<div style='font-size:13px;font-weight:900;color:{color};letter-spacing:.05em;"\
            f"margin-top:4px;'>{copas} Título{'s' if copas!=1 else ''}</div>"

    st.sidebar.markdown(f"""
    <style>
    @keyframes profile-glow {{
      0%,100%{{ box-shadow:0 0 14px {color}44,0 0 0 2px {color}22; }}
      50%{{ box-shadow:0 0 32px {color}88,0 0 0 3px {color}55; }}
    }}
    @keyframes trophy-bounce {{
      0%,100%{{ transform:translateY(0) scale(1); }}
      50%{{ transform:translateY(-5px) scale(1.12); }}
    }}
    @keyframes menu-pulse {{
      0%,100%{{ box-shadow:5px 0 24px rgba(0,0,0,.6); }}
      50%{{ box-shadow:5px 0 28px rgba(246,195,73,.35),0 0 0 2px rgba(246,195,73,.15); }}
    }}
    #fw-mb-btn{{ animation:menu-pulse 3s ease-in-out infinite !important; }}
    </style>
    <div class="sidebar-profile-card" style="border-color:{color}66;
      animation:profile-glow 3s ease-in-out infinite;
      position:relative;overflow:hidden;padding:14px 12px 12px;text-align:center;">
      <div style="position:absolute;top:-20px;right:-20px;width:80px;height:80px;
        border-radius:50%;background:radial-gradient({color}22,transparent 70%);pointer-events:none;"></div>
      {f'<img src="{_sidebar_photo}" style="width:52px;height:52px;border-radius:50%;object-fit:cover;object-position:top;border:2px solid {color};margin-bottom:6px;" />' if _sidebar_photo else ''}
      <div style="font-size:14px;font-weight:900;color:{color};
        letter-spacing:.08em;margin-bottom:6px;">{usr}</div>
      <div style="text-align:center;margin-bottom:4px;">{_tags_html}</div>
      {_trophy_label}
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
# 10. PANTALLAS
# ─────────────────────────────────────────────────────────
def pantalla_inicio():
    st.markdown("""
    <div class="hero">
      <div class="hero-title">🏆 TORNEO DE PREDICCIONES</div>
      <div class="hero-subtitle">FEFE WOLF 2026</div>
      <div class="hero-foot">© 2026 Derechos Reservados — Fundado por <b>Checo Perez</b></div>
    </div>""", unsafe_allow_html=True)

    # ── PRÓXIMO GP COUNTDOWN ─────────────────────────────────────
    # Carrera Dom 29 Mar 05:00 UTC (02:00 ARG)
    # Predicciones abren 72h antes → Jue 26 Mar 05:00 UTC (02:00 ARG)  
    # Predicciones cierran → Sáb 28 Mar 06:00 UTC (03:00 ARG)
    _t_open  = "2026-03-26 02:00"
    _t_close = "2026-03-29 01:00"
    _t_race  = "2026-03-29 02:00"

    _chtml = f"""
    <style>
    @keyframes ngpG{{0%,100%{{box-shadow:0 0 26px rgba(212,175,55,.13);}}
      50%{{box-shadow:0 0 48px rgba(212,175,55,.28);}}}}
    .ngp-box{{background:linear-gradient(145deg,rgba(8,11,28,.99),rgba(13,17,42,.99));
      border:1.5px solid rgba(212,175,55,.45);border-radius:20px;
      padding:22px 20px 18px;text-align:center;
      animation:ngpG 3s ease-in-out infinite;position:relative;overflow:hidden;}}
    .ngp-box::before{{content:'';position:absolute;top:0;left:0;right:0;height:2px;
      background:linear-gradient(90deg,transparent,#d4af37,rgba(255,220,100,.9),#d4af37,transparent);}}
    .ngp-flag{{font-size:26px;display:block;margin-bottom:4px;}}
    .ngp-name{{font-size:38px;font-weight:900;color:#ffdd7a;letter-spacing:.05em;
      text-transform:uppercase;line-height:1.1;text-shadow:0 0 22px rgba(255,221,122,.28);}}
    .ngp-venue{{font-size:11px;color:rgba(169,178,214,.55);margin:4px 0 14px;
      letter-spacing:.1em;text-transform:uppercase;}}
    .ngp-lbl{{font-size:9px;font-weight:900;letter-spacing:.22em;color:rgba(246,195,73,.7);
      text-transform:uppercase;margin-bottom:9px;}}
    .ngp-clock{{display:flex;gap:12px;justify-content:center;flex-wrap:wrap;}}
    .ngp-unit{{text-align:center;min-width:62px;}}
    .ngp-num{{font-size:36px;font-weight:900;color:#ffdd7a;
      background:rgba(246,195,73,.09);border:1.5px solid rgba(246,195,73,.38);
      border-radius:12px;padding:7px 13px;display:block;
      font-variant-numeric:tabular-nums;font-family:'Inter',monospace;
      box-shadow:0 0 14px rgba(212,175,55,.14);}}
    .ngp-ul{{font-size:9px;font-weight:700;letter-spacing:.14em;
      color:rgba(169,178,214,.5);text-transform:uppercase;margin-top:5px;}}
    .ngp-sub{{margin-top:12px;font-size:10px;color:rgba(232,236,255,.45);letter-spacing:.05em;}}
    .ngp-sub b{{color:#ffdd7a;}}
    @media(max-width:640px){{.ngp-name{{font-size:26px;}}
      .ngp-num{{font-size:26px;padding:5px 9px;}} .ngp-clock{{gap:8px;}} .ngp-unit{{min-width:52px;}}}}
    </style>
    <div class="ngp-box">
      <span class="ngp-flag">🇯🇵</span>
      <div class="ngp-name">GP JAPÓN</div>
      <div class="ngp-venue">📍 Suzuka &nbsp;·&nbsp; 27-29 Mar 2026</div>
      <div class="ngp-lbl" id="ngp-top-lbl">⏳ CARGANDO…</div>
      <div class="ngp-clock" id="fw-countdown">
        <div class="ngp-unit"><span class="ngp-num" id="cd-d">--</span><div class="ngp-ul">Días</div></div>
        <div class="ngp-unit"><span class="ngp-num" id="cd-h">--</span><div class="ngp-ul">Horas</div></div>
        <div class="ngp-unit"><span class="ngp-num" id="cd-m">--</span><div class="ngp-ul">Minutos</div></div>
        <div class="ngp-unit"><span class="ngp-num" id="cd-s">--</span><div class="ngp-ul">Segundos</div></div>
      </div>
      <div class="ngp-sub" id="ngp-sub"></div>
    </div>
    <script>
    (function(){{
      var tOpen=new Date("{_t_open} UTC").getTime();
      var tClose=new Date("{_t_close} UTC").getTime();
      var tRace=new Date("{_t_race} UTC").getTime();
      function pad(n){{return String(n).padStart(2,"0");}}
      function upd(ms){{
        var d=Math.floor(ms/86400000),h=Math.floor((ms%86400000)/3600000);
        var m=Math.floor((ms%3600000)/60000),s=Math.floor((ms%60000)/1000);
        ["cd-d","cd-h","cd-m","cd-s"].forEach(function(id,i){{
          var el=document.getElementById(id); if(el)el.textContent=pad([d,h,m,s][i]);
        }});
      }}
      function tick(){{
        var now=Date.now();
        var lbl=document.getElementById("ngp-top-lbl");
        var sub=document.getElementById("ngp-sub");
        if(now<tOpen){{
          upd(tOpen-now);
          if(lbl)lbl.textContent="⏳ FALTAN PARA ABRIR PREDICCIONES";
          if(sub)sub.innerHTML="Apertura: <b>Jue 26 Mar · 02:00 ARG</b> (72hs antes de la carrera)";
        }} else if(now<tClose){{
          upd(tClose-now);
          if(lbl)lbl.textContent="⚡ PREDICCIONES ABIERTAS — CIERRAN EN";
          if(sub)sub.innerHTML="Cierre: <b>Domingo 29 Mar · 01:00 ARG</b>";
        }} else if(now<tRace){{
          document.getElementById("fw-countdown").innerHTML=
            "<b style='color:#ff6644;font-size:15px;'>🔴 PREDICCIONES CERRADAS</b>";
          if(lbl)lbl.textContent="CARRERA EL DOMINGO 29 MAR";
          if(sub)sub.innerHTML="Largada: <b>02:00 ARG</b>";
        }} else {{
          document.getElementById("fw-countdown").innerHTML=
            "<b style='color:#4ade80;font-size:15px;'>🏁 GP JAPÓN 2026 FINALIZADO</b>";
          if(lbl)lbl.textContent="GP JAPÓN 2026"; if(sub)sub.innerHTML="";
        }}
      }}
      tick(); setInterval(tick,1000);
    }})();
    </script>
    """
    components.html(_chtml, height=248, scrolling=False)

    # ── QUICK ACCESS — botones Streamlit reales ───────────────────
    st.markdown("""<style>
    div[data-testid="stHorizontalBlock"] .fw-qb>button{
      background:linear-gradient(145deg,rgba(12,16,38,.98),rgba(7,9,22,.98))!important;
      border:1.5px solid rgba(255,255,255,.1)!important;border-radius:14px!important;
      padding:14px 8px 12px!important;color:#e8ecff!important;
      font-size:11px!important;font-weight:800!important;letter-spacing:.06em!important;
      text-transform:uppercase!important;transition:all .12s!important;
      min-height:68px!important;width:100%!important;
      white-space:pre-line!important;line-height:1.5!important;}
    div[data-testid="stHorizontalBlock"] .fw-qb>button:hover{
      background:linear-gradient(145deg,rgba(212,175,55,.18),rgba(90,60,0,.2))!important;
      border-color:rgba(212,175,55,.55)!important;transform:translateY(-2px)!important;
      box-shadow:0 5px 16px rgba(212,175,55,.22)!important;color:#ffdd7a!important;}
    div[data-testid="stHorizontalBlock"] .fw-qb>button:active{transform:scale(.97)!important;}
    </style>""", unsafe_allow_html=True)
    _q1,_q2,_q3,_q4 = st.columns(4)
    for _col,_lbl,_nav in [(_q1,"⚡\nHacer Predicción","Predicciones"),
                            (_q2,"📊\nVer Tabla","Posiciones"),
                            (_q3,"💬\nMesa Chica","Mesa"),
                            (_q4,"🏆\nSalón de la Fama","Campeones")]:
        with _col:
            st.markdown('<div class="fw-qb">', unsafe_allow_html=True)
            if st.button(_lbl, use_container_width=True, key=f"qb_{_nav}"):
                st.session_state["fw_force_nav"] = _nav; st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)

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
    # Inject all pilot card CSS at once
    st.markdown("""<style>
@keyframes cardFloat{0%,100%{transform:translateY(0) scale(1);}50%{transform:translateY(-7px) scale(1.03);}}
@keyframes engineRev{0%{opacity:.25;width:8%;}60%{opacity:1;width:88%;}100%{opacity:.25;width:8%;}}
.fw-pilot-card{border-radius:18px;padding:20px 8px 16px;text-align:center;position:relative;overflow:hidden;cursor:default;transition:transform .25s ease,box-shadow .3s ease;margin-bottom:4px;}
.fw-pilot-card:hover{transform:translateY(-10px) scale(1.05)!important;animation:none!important;}
.fw-pilot-card::before{content:"";position:absolute;top:0;left:0;right:0;height:2px;border-radius:2px 2px 0 0;}
.fw-pilot-engine{position:absolute;bottom:7px;left:50%;transform:translateX(-50%);height:3px;border-radius:999px;animation:engineRev 2s ease-in-out infinite;}
.fw-pilot-icon{font-size:30px;margin-bottom:8px;display:block;}
.fw-pilot-name{font-size:11px;font-weight:800;letter-spacing:.04em;margin-bottom:3px;line-height:1.3;}
.fw-pilot-role{font-size:8px;font-weight:700;letter-spacing:.14em;text-transform:uppercase;opacity:.55;}
.fw-pilot-pos{position:absolute;top:8px;left:10px;font-size:11px;font-weight:900;opacity:.65;}
.fw-pilot-rank{position:absolute;top:8px;right:10px;font-size:14px;}
.fwpc-0{background:linear-gradient(145deg,#D4AF371A 0%,#D4AF370A 100%);border:1.5px solid #D4AF3766;box-shadow:0 4px 22px #D4AF3722,inset 0 1px 0 #D4AF3733;animation:cardFloat 2.4s ease-in-out 0.0s infinite;}
.fwpc-0::before{background:linear-gradient(90deg,transparent,#D4AF37,transparent);}
.fwpc-0 .fw-pilot-engine{background:#D4AF37;box-shadow:0 0 8px #D4AF3788;}
.fwpc-1{background:linear-gradient(145deg,#1E90FF1A 0%,#1E90FF0A 100%);border:1.5px solid #1E90FF66;box-shadow:0 4px 22px #1E90FF22,inset 0 1px 0 #1E90FF33;animation:cardFloat 2.75s ease-in-out 0.45s infinite;}
.fwpc-1::before{background:linear-gradient(90deg,transparent,#1E90FF,transparent);}
.fwpc-1 .fw-pilot-engine{background:#1E90FF;box-shadow:0 0 8px #1E90FF88;}
.fwpc-2{background:linear-gradient(145deg,#FF44441A 0%,#FF44440A 100%);border:1.5px solid #FF444466;box-shadow:0 4px 22px #FF444422,inset 0 1px 0 #FF444433;animation:cardFloat 3.0999999999999996s ease-in-out 0.9s infinite;}
.fwpc-2::before{background:linear-gradient(90deg,transparent,#FF4444,transparent);}
.fwpc-2 .fw-pilot-engine{background:#FF4444;box-shadow:0 0 8px #FF444488;}
.fwpc-3{background:linear-gradient(145deg,#FF8C001A 0%,#FF8C000A 100%);border:1.5px solid #FF8C0066;box-shadow:0 4px 22px #FF8C0022,inset 0 1px 0 #FF8C0033;animation:cardFloat 3.4499999999999997s ease-in-out 1.35s infinite;}
.fwpc-3::before{background:linear-gradient(90deg,transparent,#FF8C00,transparent);}
.fwpc-3 .fw-pilot-engine{background:#FF8C00;box-shadow:0 0 8px #FF8C0088;}
.fwpc-4{background:linear-gradient(145deg,#00CED11A 0%,#00CED10A 100%);border:1.5px solid #00CED166;box-shadow:0 4px 22px #00CED122,inset 0 1px 0 #00CED133;animation:cardFloat 3.8s ease-in-out 1.8s infinite;}
.fwpc-4::before{background:linear-gradient(90deg,transparent,#00CED1,transparent);}
.fwpc-4 .fw-pilot-engine{background:#00CED1;box-shadow:0 0 8px #00CED188;}
</style>""", unsafe_allow_html=True)
    parrilla_cols = st.columns(5)
    _pilots_grid = [
        ("Checo Perez",     "fwpc-0", "#D4AF37", "P1", "🥇", "Comisario"),
        ("Nicki Lauda",     "fwpc-1", "#1E90FF", "P2", "🥈", "Formulero"),
        ("Fernando Alonso", "fwpc-2", "#FF4444", "P3", "🥉", "FIPF"),
        ("Lando Norris",    "fwpc-3", "#FFA500", "P4", "4°",  "Sub Com."),
        ("Valteri Bottas",  "fwpc-4", "#00CFFF", "P5", "5°",  "FIPF"),
    ]
    for _pg_col, (_pg_n, _pg_c, _pg_cl, _pg_p, _pg_r, _pg_rl) in zip(parrilla_cols, _pilots_grid):
        _pg_ph = DRIVER_HEADSHOTS.get(_pg_n, '')
        if _pg_n == 'Nicki Lauda' and not _pg_ph:
            _pg_ph = 'data:image/png;base64,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'
        with _pg_col:
            if _pg_ph:
                _pg_img = (
                    f'<img src="{_pg_ph}" style="width:68px;height:68px;'
                    f'border-radius:50%;object-fit:cover;object-position:top;'
                    f'border:2px solid {_pg_cl};margin:0 auto 6px;display:block;">'
                )
            else:
                _pg_img = '<span class="fw-pilot-icon">🏎️</span>'
            st.markdown(
                f'<div class="fw-pilot-card {_pg_c}">'
                f'<span class="fw-pilot-pos">{_pg_p}</span>'
                f'<span class="fw-pilot-rank">{_pg_r}</span>'
                + _pg_img +
                f'<div class="fw-pilot-name" style="color:{_pg_cl};">{_pg_n}</div>'
                f'<div class="fw-pilot-role">{_pg_rl}</div>'
                '<div class="fw-pilot-engine"></div></div>',
                unsafe_allow_html=True
            )


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
        all_drivers = []
        for equipo, pilotos in GRILLA_2026.items():
            color = TEAM_COLORS.get(equipo, "#A855F7")
            abbr  = TEAM_LOGOS_SVG.get(equipo, equipo[:3])
            for num_idx, pil in enumerate(pilotos):
                all_drivers.append((pil, equipo, color, abbr, num_idx + 1))

        # Inject improved grid CSS
        st.markdown("""
        <style>
        .f1-drivers-grid {
          display: grid;
          grid-template-columns: repeat(auto-fill, minmax(150px, 1fr));
          gap: 14px;
          padding: 8px 0 16px;
        }
        .f1d-card {
          background: linear-gradient(145deg, rgba(7,10,25,.97) 0%, rgba(14,18,42,.95) 100%);
          border: 1.5px solid var(--tc, #a855f7);
          border-radius: 16px;
          overflow: hidden;
          position: relative;
          transition: transform .25s ease, box-shadow .25s ease;
          box-shadow: 0 4px 18px rgba(0,0,0,.5);
        }
        .f1d-card:hover {
          transform: translateY(-6px) scale(1.02);
          box-shadow: 0 10px 32px rgba(0,0,0,.7), 0 0 20px var(--tc, #a855f7)33;
        }
        .f1d-top-stripe {
          height: 3px;
          background: var(--tc, #a855f7);
          width: 100%;
        }
        .f1d-logo {
          position:absolute;top:10px;left:8px;
          height:18px;max-width:54px;object-fit:contain;
          opacity:.85;filter:brightness(1.2);z-index:3;
        }
        .f1d-team-badge {
          position: absolute;
          top: 10px;
          right: 10px;
          font-size: 8px;
          font-weight: 900;
          color: var(--tc, #a855f7);
          background: rgba(0,0,0,.6);
          border: 1px solid var(--tc, #a855f7)55;
          border-radius: 6px;
          padding: 2px 6px;
          letter-spacing: .1em;
          z-index: 2;
        }
        .f1d-photo-wrap {
          width: 100%;
          aspect-ratio: 3/4;
          overflow: hidden;
          background: linear-gradient(180deg, rgba(0,0,0,.1), rgba(0,0,0,.5));
          position: relative;
        }
        .f1d-photo {
          width: 100%;
          height: 100%;
          object-fit: cover;
          object-position: top;
          display: block;
          transition: transform .3s ease;
        }
        .f1d-card:hover .f1d-photo { transform: scale(1.04); }
        .f1d-fallback {
          width: 100%;
          height: 100%;
          display: flex;
          align-items: center;
          justify-content: center;
          font-size: 36px;
          font-weight: 900;
          background: rgba(0,0,0,.4);
        }
        .f1d-info {
          padding: 10px 10px 12px;
          background: linear-gradient(0deg, rgba(0,0,0,.85), rgba(0,0,0,.4));
        }
        .f1d-flag { font-size: 14px; margin-bottom: 3px; }
        .f1d-firstname {
          font-size: 9px;
          font-weight: 500;
          color: rgba(232,236,255,.6);
          text-transform: uppercase;
          letter-spacing: .08em;
          line-height: 1.2;
        }
        .f1d-lastname {
          font-size: 13px;
          font-weight: 900;
          color: var(--tc, #e8ecff);
          letter-spacing: .04em;
          line-height: 1.2;
          margin-bottom: 4px;
        }
        .f1d-team-name {
          font-size: 8px;
          color: var(--tc, #a855f7);
          font-weight: 700;
          letter-spacing: .12em;
          text-transform: uppercase;
          opacity: .8;
        }
        </style>
        """, unsafe_allow_html=True)
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
            logo_url = TEAM_LOGOS_CDN.get(equipo, "")
            flag = nacionalidades.get(pil, "🌍")
            last_name = pil.split()[-1].upper()
            first_name = " ".join(pil.split()[:-1])
            img_html = f'<img src="{photo}" class="f1d-photo" onerror="this.style.display=\'none\';this.nextElementSibling.style.display=\'flex\';" loading="lazy">'
            fallback = f'<div class="f1d-fallback" style="display:none;color:{color};">{initials}</div>'
            cards_html += f"""
            <div class="f1d-card fade-up" style="--tc:{color}">
              <div class="f1d-top-stripe"></div>
              <div class="f1d-team-badge" style="display:flex;align-items:center;gap:4px;"><span>{abbr}</span></div>
              <img src="{logo_url}" class="f1d-logo" onerror="this.style.display='none';" loading="lazy">
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
        teams_list = list(GRILLA_2026.items())
        for i in range(0, len(teams_list), 2):
            cols = st.columns(2, gap="large")
            for j, (equipo, pilotos) in enumerate(teams_list[i:i+2]):
                color = TEAM_COLORS.get(equipo, "#A855F7")
                abbr  = TEAM_LOGOS_SVG.get(equipo, equipo[:3])
                desc  = TEAM_DESCRIPTIONS.get(equipo, "")
                car   = TEAM_CARS_MODULE.get(equipo, "")
                logo  = TEAM_LOGOS_CDN.get(equipo, "")
                p1, p2 = pilotos[0], pilotos[1]
                car_html  = f'<img src="{car}" class="tfc-car-img" loading="lazy" onerror="this.style.display=\'none\'">' if car else ""
                logo_html = f'<img src="{logo}" style="position:absolute;top:10px;right:12px;height:22px;max-width:72px;object-fit:contain;opacity:.85;z-index:3;" loading="lazy" onerror="this.style.display=\'none\'">' if logo else ""
                with cols[j]:
                    st.markdown(f"""
                    <div class="team-full-card fade-up" style="--tc:{color};position:relative;">
                      {logo_html}
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
    st.markdown("""
    <style>
    @keyframes regGlow{0%,100%{box-shadow:0 0 22px rgba(212,175,55,.12);}
      50%{box-shadow:0 0 42px rgba(212,175,55,.26);}}
    @keyframes goldShimmerReg{0%{background-position:200% center;}100%{background-position:-200% center;}}
    .reg-hero{background:linear-gradient(145deg,rgba(7,9,22,.99),rgba(13,17,42,.99));
      border:1.5px solid rgba(212,175,55,.45);border-radius:22px;
      padding:28px 24px 22px;text-align:center;margin-bottom:20px;
      position:relative;overflow:hidden;animation:regGlow 3.5s ease-in-out infinite;}
    .reg-hero::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;
      background:linear-gradient(90deg,transparent,#d4af37,rgba(255,220,100,.9),#d4af37,transparent);}
    .reg-hero-title{font-size:30px;font-weight:900;letter-spacing:.1em;
      background:linear-gradient(90deg,#9a7a10,#d4af37,#ffe896,#d4af37,#9a7a10);
      background-size:200% auto;
      -webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;
      animation:goldShimmerReg 4s linear infinite;margin-bottom:6px;}
    .reg-hero-sub{font-size:12px;color:rgba(169,178,214,.5);letter-spacing:.18em;text-transform:uppercase;}
    .reg-section{background:linear-gradient(145deg,rgba(10,13,32,.98),rgba(7,9,22,.98));
      border:1.5px solid rgba(255,255,255,.08);border-radius:18px;
      padding:22px 22px 18px;margin-bottom:14px;position:relative;overflow:hidden;
      transition:border-color .2s,box-shadow .2s;}
    .reg-section:hover{border-color:rgba(212,175,55,.3);box-shadow:0 4px 22px rgba(212,175,55,.1);}
    .reg-section-alert{border-color:rgba(217,70,239,.4)!important;}
    .reg-section-alert:hover{border-color:rgba(217,70,239,.7)!important;
      box-shadow:0 4px 22px rgba(217,70,239,.12)!important;}
    .reg-section-red{border-color:rgba(255,64,64,.3)!important;}
    .reg-section-red:hover{border-color:rgba(255,64,64,.6)!important;
      box-shadow:0 4px 22px rgba(255,64,64,.1)!important;}
    .reg-section::before{content:'';position:absolute;top:0;left:0;width:4px;height:100%;
      background:rgba(212,175,55,.5);border-radius:4px 0 0 4px;}
    .reg-section-alert::before{background:rgba(217,70,239,.6)!important;}
    .reg-section-red::before{background:rgba(255,64,64,.6)!important;}
    .reg-tag{display:inline-flex;align-items:center;gap:6px;
      background:rgba(212,175,55,.1);border:1px solid rgba(212,175,55,.3);
      border-radius:20px;padding:3px 12px;font-size:10px;font-weight:800;
      letter-spacing:.1em;color:#ffdd7a;text-transform:uppercase;margin-bottom:12px;}
    .reg-row{display:flex;align-items:center;gap:12px;padding:10px 0;
      border-bottom:1px solid rgba(255,255,255,.05);}
    .reg-row:last-child{border-bottom:none;padding-bottom:0;}
    .reg-cat{font-size:13px;font-weight:900;color:#ffdd7a;min-width:130px;flex-shrink:0;}
    .reg-pts{display:flex;flex-wrap:wrap;gap:6px;flex:1;}
    .reg-chip{background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.1);
      border-radius:8px;padding:3px 8px;font-size:11px;color:#e8ecff;font-weight:600;
      white-space:nowrap;}
    .reg-chip-gold{background:rgba(212,175,55,.12)!important;
      border-color:rgba(212,175,55,.35)!important;color:#ffdd7a!important;font-weight:800!important;}
    .reg-bonus{background:rgba(212,175,55,.08);border:1px solid rgba(212,175,55,.25);
      border-radius:10px;padding:8px 14px;margin-top:8px;font-size:12px;color:#ffdd7a;
      font-weight:700;}
    .reg-dns-row{display:flex;align-items:center;gap:10px;padding:8px 0;
      border-bottom:1px solid rgba(255,255,255,.05);}
    .reg-dns-row:last-child{border-bottom:none;}
    .reg-dns-pill{background:rgba(255,64,64,.12);border:1px solid rgba(255,64,64,.3);
      border-radius:8px;padding:3px 10px;font-size:11px;color:#fca5a5;font-weight:700;}
    .reg-dns-pts{font-size:14px;font-weight:900;color:#ef4444;margin-left:auto;}
    </style>
    <div class="reg-hero">
      <div style="font-size:32px;margin-bottom:6px;">📜</div>
      <div class="reg-hero-title">REGLAMENTO OFICIAL 2026</div>
      <div class="reg-hero-sub">Torneo Fefe Wolf · Temporada 2026</div>
    </div>

    <!-- AVISO -->
    <div class="reg-section reg-section-alert">
      <div class="reg-tag">⚠️ Aviso importante</div>
      <div style="font-size:13px;color:rgba(232,236,255,.85);line-height:1.6;">
        <b style="color:#e879f9;">REGLA DE ESCRITURA ELIMINADA</b><br>
        Los nombres ya no deben escribirse exactamente igual a la sección 'Pilotos y Escuderías'.<br>
        <span style="color:rgba(232,236,255,.55);">El sistema lo detecta automáticamente, aunque siempre deben verificar que esté correcto.</span>
      </div>
    </div>

    <!-- PUNTUACIÓN -->
    <div class="reg-section">
      <div class="reg-tag">⚔️ Sistema de puntuación</div>
      <div class="reg-row">
        <div class="reg-cat">🏁 Carrera</div>
        <div class="reg-pts">
          <span class="reg-chip">1°→25</span><span class="reg-chip">2°→18</span>
          <span class="reg-chip">3°→15</span><span class="reg-chip">4°→12</span>
          <span class="reg-chip">5°→10</span><span class="reg-chip">6°→8</span>
          <span class="reg-chip">7°→6</span><span class="reg-chip">8°→4</span>
          <span class="reg-chip">9°→2</span><span class="reg-chip">10°→1</span>
          <span class="reg-chip reg-chip-gold">Pleno +5 Pts</span>
        </div>
      </div>
      <div class="reg-row">
        <div class="reg-cat">⏱️ Clasificación</div>
        <div class="reg-pts">
          <span class="reg-chip">1°→15</span><span class="reg-chip">2°→10</span>
          <span class="reg-chip">3°→7</span><span class="reg-chip">4°→5</span>
          <span class="reg-chip">5°→3</span>
          <span class="reg-chip reg-chip-gold">Pleno +5 Pts</span>
        </div>
      </div>
      <div class="reg-row">
        <div class="reg-cat">⚡ Sprint</div>
        <div class="reg-pts">
          <span class="reg-chip">1°→8</span><span class="reg-chip">2°→7</span>
          <span class="reg-chip">3°→6</span><span class="reg-chip">4°→5</span>
          <span class="reg-chip">5°→4</span><span class="reg-chip">6°→3</span>
          <span class="reg-chip">7°→2</span><span class="reg-chip">8°→1</span>
          <span class="reg-chip reg-chip-gold">Pleno +3 Pts</span>
        </div>
      </div>
      <div class="reg-row">
        <div class="reg-cat">🛠️ Constructores</div>
        <div class="reg-pts">
          <span class="reg-chip">1°→10</span><span class="reg-chip">2°→5</span>
          <span class="reg-chip">3°→2</span>
          <span class="reg-chip reg-chip-gold">Pleno +3 Pts</span>
        </div>
      </div>
      <div class="reg-bonus">
        🧉 <b>Regla Colapinto:</b> Acierto exacto Qualy <b>+10 Pts</b> &nbsp;·&nbsp; Acierto exacto Carrera <b>+20 Pts</b>
      </div>
      <div class="reg-bonus" style="margin-top:6px;background:rgba(212,175,55,.14);border-color:rgba(212,175,55,.4);">
        🏆 <b>Campeones:</b> Piloto campeón <b>+50 Pts</b> &nbsp;·&nbsp; Constructor campeón <b>+25 Pts</b>
      </div>
    </div>

    <!-- DNS -->
    <div class="reg-section reg-section-red">
      <div class="reg-tag" style="background:rgba(255,64,64,.1);border-color:rgba(255,64,64,.3);color:#fca5a5;">⛔ Sanciones D.N.S.</div>
      <div class="reg-dns-row">
        <span class="reg-dns-pill">Falta en Clasificación</span>
        <span class="reg-dns-pts">−5 Pts</span>
      </div>
      <div class="reg-dns-row">
        <span class="reg-dns-pill">Falta en Sprint</span>
        <span class="reg-dns-pts">−5 Pts</span>
      </div>
      <div class="reg-dns-row">
        <span class="reg-dns-pill">Falta en Carrera / Constructores</span>
        <span class="reg-dns-pts">−5 Pts</span>
      </div>
      <div style="margin-top:10px;font-size:12px;color:rgba(232,236,255,.55);
        background:rgba(255,255,255,.03);border-radius:8px;padding:8px 12px;">
        ⚖️ <b style="color:rgba(232,236,255,.8);">Desempate:</b> En caso de igualdad de puntos, 
        ganará la predicción el Formulero que haya enviado primero.
      </div>
    </div>
    """, unsafe_allow_html=True)


def pantalla_muro():
    # Fotos de los campeones
    _checo_ph  = DRIVER_HEADSHOTS.get("Checo Perez",  DRIVER_PHOTOS.get("Checo Perez",""))
    _bottas_ph = DRIVER_HEADSHOTS.get("Valteri Bottas",DRIVER_PHOTOS.get("Valteri Bottas",""))
    _lauda_ph  = DRIVER_HEADSHOTS.get("Nicki Lauda",  DRIVER_PHOTOS.get("Nicki Lauda",""))
    # Fefe Wolf — usa IMAGENFEFE.jfif del repo si existe
    _fefe_ph = ""
    try:
        import base64 as _b64, os as _os2
        _fefe_path = "IMAGENFEFE.jfif"
        if _os2.path.exists(_fefe_path):
            with open(_fefe_path,"rb") as _ff: _fefe_ph = "data:image/jpeg;base64,"+_b64.b64encode(_ff.read()).decode()
    except: pass

    def _champ_av(ph, ini, clr, sz=70):
        if ph:
            return (f'<img src="{ph}" style="width:{sz}px;height:{sz}px;border-radius:50%;'
                    f'object-fit:cover;object-position:top;border:3px solid {clr};'
                    f'flex-shrink:0;box-shadow:0 0 18px {clr}55;">')
        return (f'<div style="width:{sz}px;height:{sz}px;border-radius:50%;background:{clr}22;'
                f'border:3px solid {clr};display:flex;align-items:center;justify-content:center;'
                f'font-weight:900;font-size:{sz//3}px;color:{clr};flex-shrink:0;">{ini}</div>')

    st.markdown("""
    <style>
    @keyframes hofGlow{0%,100%{box-shadow:0 0 24px rgba(212,175,55,.2),0 4px 28px rgba(0,0,0,.6);}
      50%{box-shadow:0 0 48px rgba(212,175,55,.45),0 4px 36px rgba(0,0,0,.8);}}
    @keyframes starSpin{0%{transform:rotate(0deg) scale(1);}50%{transform:rotate(12deg) scale(1.12);}
      100%{transform:rotate(0deg) scale(1);}}
    @keyframes hofEntry{from{opacity:0;transform:translateY(28px) scale(.97);}
      to{opacity:1;transform:translateY(0) scale(1);}}
    @keyframes goldShimmer2{0%{background-position:200% center;}100%{background-position:-200% center;}}
    @keyframes avGlow{0%,100%{filter:drop-shadow(0 0 8px rgba(212,175,55,.3));}
      50%{filter:drop-shadow(0 0 18px rgba(212,175,55,.6));}}
    .hof-wrap{
      background:linear-gradient(145deg,rgba(7,9,22,.99),rgba(13,18,42,.99));
      border:1.5px solid rgba(212,175,55,.45);border-radius:24px;
      padding:30px 24px 26px;margin:0 auto 20px;position:relative;overflow:hidden;
      max-width:860px;animation:hofGlow 4s ease-in-out infinite;
    }
    .hof-wrap::before{content:'';position:absolute;top:0;left:0;right:0;height:3px;
      background:linear-gradient(90deg,transparent,#9a7a10,#d4af37,#ffe896,#d4af37,#9a7a10,transparent);}
    .hof-wrap::after{content:'';position:absolute;bottom:0;left:0;right:0;height:1px;
      background:linear-gradient(90deg,transparent,rgba(212,175,55,.3),transparent);}
    .hof-title{font-size:32px;font-weight:900;letter-spacing:.12em;text-align:center;
      background:linear-gradient(90deg,#9a7a10,#d4af37,#ffe896,#d4af37,#9a7a10);
      background-size:200% auto;
      -webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;
      animation:goldShimmer2 4s linear infinite;margin-bottom:4px;}
    .hof-sub{text-align:center;font-size:12px;color:rgba(246,195,73,.5);
      letter-spacing:.22em;text-transform:uppercase;margin-bottom:26px;}
    .hof-entry{
      background:linear-gradient(135deg,rgba(212,175,55,.09),rgba(255,255,255,.02));
      border:1px solid rgba(212,175,55,.28);border-radius:18px;
      padding:16px 20px;margin-bottom:12px;
      display:flex;align-items:center;gap:16px;
      animation:hofEntry .6s ease both;
      transition:transform .25s ease,box-shadow .25s ease,border-color .25s;
    }
    .hof-entry:hover{transform:translateX(8px) scale(1.015);
      box-shadow:0 6px 28px rgba(212,175,55,.18);border-color:rgba(212,175,55,.55);}
    .hof-entry.tri{border-color:rgba(212,175,55,.5)!important;
      background:linear-gradient(135deg,rgba(212,175,55,.14),rgba(255,255,255,.04))!important;}
    .hof-entry:nth-child(3){animation-delay:.05s;}
    .hof-entry:nth-child(4){animation-delay:.1s;}
    .hof-entry:nth-child(5){animation-delay:.15s;}
    .hof-entry:nth-child(6){animation-delay:.2s;}
    .hof-crown{font-size:36px;flex-shrink:0;animation:starSpin 6s ease-in-out infinite;}
    .hof-av{flex-shrink:0;animation:avGlow 3s ease-in-out infinite;}
    .hof-info{flex:1;display:flex;flex-direction:column;gap:2px;}
    .hof-name{font-size:17px;font-weight:900;letter-spacing:.06em;color:#ffdd7a;}
    .hof-meta{font-size:11px;color:rgba(232,236,255,.55);letter-spacing:.04em;}
    .hof-stars{font-size:18px;letter-spacing:3px;filter:drop-shadow(0 0 5px #d4af37);margin-top:2px;}
    .hof-badge{background:linear-gradient(90deg,rgba(212,175,55,.25),rgba(212,175,55,.1));
      border:1px solid rgba(212,175,55,.5);border-radius:20px;
      padding:5px 16px;font-size:12px;font-weight:900;color:#ffdd7a;
      white-space:nowrap;flex-shrink:0;text-align:center;
      box-shadow:0 0 12px rgba(212,175,55,.15);}
    </style>
    """, unsafe_allow_html=True)

    # Build champion entries with photos
    entries = [
        (_checo_ph,  "CP", "#D4AF37", "tri",  "🏆", "CHECO PEREZ",    "TriCampeón: 2022 · 2023 · 2025", "★★★", "3 TÍTULOS"),
        (_bottas_ph, "VB", "#00CFFF", "",     "🥇", "VALTERI BOTTAS", "Campeón: 2024",                   "★",   "1 TÍTULO"),
        (_fefe_ph,    "FW", "#9B59B6", "",     "🥇", "FEFE WOLF",      "Campeón: 2021",                   "★",   "1 TÍTULO"),
        (_lauda_ph,  "NL", "#1E90FF", "",     "🥇", "NICKI LAUDA",    "Campeón: 2021",                   "★",   "1 TÍTULO"),
    ]

    html = '<div class="hof-wrap"><div class="hof-title">🏆 MURO DE CAMPEONES</div>'
    html += '<div class="hof-sub">👑 Hall of Fame · Torneo Fefe Wolf</div>'
    for ph, ini, clr, extra_cls, crown, name, meta, stars, badge in entries:
        av = _champ_av(ph, ini, clr)
        html += (f'<div class="hof-entry {extra_cls}">'
                 f'<span class="hof-crown">{crown}</span>'
                 f'<div class="hof-av">{av}</div>'
                 f'<div class="hof-info">'
                 f'<div class="hof-name">{name}</div>'
                 f'<div class="hof-meta">{meta}</div>'
                 f'<div class="hof-stars">{stars}</div>'
                 f'</div>'
                 f'<div class="hof-badge">{badge}</div>'
                 f'</div>')
    html += '</div>'
    st.markdown(html, unsafe_allow_html=True)


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
        st.markdown("""
        <style>
        @keyframes podGlow1{0%,100%{box-shadow:0 0 20px #D4AF3766,0 0 0 2px #D4AF3733;}
          50%{box-shadow:0 0 40px #D4AF3799,0 0 0 4px #D4AF3755;}}
        @keyframes podGlow2{0%,100%{box-shadow:0 0 16px rgba(192,192,192,.5);}
          50%{box-shadow:0 0 30px rgba(192,192,192,.7);}}
        @keyframes podGlow3{0%,100%{box-shadow:0 0 14px rgba(205,127,50,.45);}
          50%{box-shadow:0 0 26px rgba(205,127,50,.65);}}
        @keyframes podBounce{0%,100%{transform:translateY(0);}50%{transform:translateY(-6px);}}
        .pod-1st{animation:podGlow1 2.5s ease-in-out infinite;border-radius:20px;
          background:linear-gradient(145deg,rgba(212,175,55,.18),rgba(212,175,55,.05));
          border:2px solid rgba(212,175,55,.6)!important;padding:24px 12px!important;}
        .pod-2nd{animation:podGlow2 3s ease-in-out infinite;border-radius:18px;
          background:linear-gradient(145deg,rgba(192,192,192,.12),rgba(192,192,192,.03));
          border:1.5px solid rgba(192,192,192,.4)!important;padding:20px 12px!important;}
        .pod-3rd{animation:podGlow3 3.5s ease-in-out infinite;border-radius:18px;
          background:linear-gradient(145deg,rgba(205,127,50,.12),rgba(205,127,50,.03));
          border:1.5px solid rgba(205,127,50,.4)!important;padding:20px 12px!important;}
        .pod-medal{font-size:42px;animation:podBounce 2s ease-in-out infinite;display:block;margin-bottom:8px;}
        .pod-medal-2{font-size:36px;animation:podBounce 2.4s ease-in-out .3s infinite;display:block;margin-bottom:8px;}
        .pod-medal-3{font-size:34px;animation:podBounce 2.8s ease-in-out .6s infinite;display:block;margin-bottom:8px;}
        .pod-pts{font-size:36px!important;font-weight:900!important;line-height:1.1;}
        </style>""", unsafe_allow_html=True)
        def _pod(col,row,medal,pod_class,medal_class):
            c=PILOTO_COLORS.get(row["Piloto"],"#a855f7")
            pts=int(row.get("Puntos",0))
            with col:
                st.markdown(
                    f'<div class="card fade-up {pod_class}" style="text-align:center;">' 
                    f'<span class="{medal_class}">{medal}</span>'
                    f'<div style="font-weight:900;color:{c};font-size:14px;letter-spacing:.04em;">{row["Piloto"]}</div>'
                    f'<div class="pod-pts" style="color:#ffdd7a;">{pts}</div>'
                    f'<div style="font-size:11px;color:rgba(232,236,255,.45);letter-spacing:.1em;">PUNTOS</div>'
                    f'</div>', unsafe_allow_html=True)
        c1,c2,c3=st.columns(3)
        _pod(c1,p2,"🥈","pod-2nd","pod-medal-2")
        _pod(c2,p1,"🥇","pod-1st","pod-medal")
        _pod(c3,p3,"🥉","pod-3rd","pod-medal-3")
        st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)
        render_dark_table(df)
    if "Puntos" in df.columns and not df.empty and _PLOTLY_OK:
        try:
            colors = [PILOTO_COLORS.get(p, "#a855f7") for p in df["Piloto"]]
            _ymax_pos = int(df["Puntos"].max()) if not df.empty else 10
            fig = go.Figure(go.Bar(
                x=df["Piloto"], y=df["Puntos"],
                marker_color=colors,
                marker_line_width=0,
                text=df["Puntos"], textposition="outside",
                textfont=dict(color="#ffdd7a", size=14, family="Inter"),
                cliponaxis=False,
            ))
            fig.update_layout(
                height=300, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=10, r=10, t=48, b=10),
                xaxis=dict(tickfont=dict(color="#e8ecff", size=12), showgrid=False, zeroline=False),
                yaxis=dict(tickfont=dict(color="#a9b2d6", size=11), showgrid=True,
                           gridcolor="rgba(246,195,73,0.08)", zeroline=False,
                           range=[0, _ymax_pos * 1.25]),
                showlegend=False,
            )
            fig.update_traces(marker_cornerradius="5%")
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "staticPlot": True})
        except Exception:
            pass


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

    _hcol1, _hcol2 = st.columns([4,1])
    with _hcol2:
        if st.button("🔄 Actualizar", key="hist_refresh", use_container_width=True):
            _get.clear(); st.rerun()
    tab_evo,tab_gp,tab_pers,tab_stats=st.tabs(["📉 Evolución","🏁 Por GP","👤 Personal","🏅 Stats"])

    with tab_evo:
        pivot=df_hist.pivot_table(index="gp",columns="piloto",values="puntos",fill_value=0)
        pivot=pivot.reindex([g for g in GPS_OFICIALES if g in pivot.index])
        cumdf = pivot.cumsum()
        short_idx = [short.get(g,g) for g in cumdf.index]

        # ── SIMULACIÓN DE CARRERA ANIMADA ─────────────────────
        import json as _json

        gps_disp = short_idx
        if not gps_disp:
            st.info("Sin datos de evolución aún.")
        else:
            gp_sel_evo = st.selectbox(
                "📍 Ver acumulado hasta:",
                gps_disp, index=len(gps_disp)-1, key="evo_gp_selector"
            )
            idx_fin    = gps_disp.index(gp_sel_evo)
            race_data  = {}
            for pil in cumdf.columns:
                race_data[pil] = [int(v) for v in cumdf[pil].values[:idx_fin+1]]
            race_rounds = gps_disp[:idx_fin+1]
            race_colors = {p: PILOTO_COLORS.get(p, "#a855f7") for p in cumdf.columns}

            # ── LINE CHART — evolución acumulada por piloto ────────────
            if _PLOTLY_OK:
                try:
                    import plotly.graph_objects as _pgo
                    _fig_line = _pgo.Figure()
                    _all_rounds_disp = gps_disp[:idx_fin+1]  # full list up to selection
                    for pil in cumdf.columns:
                        _yvals = [int(v) for v in cumdf[pil].values[:idx_fin+1]]
                        _color = PILOTO_COLORS.get(pil, "#a855f7")
                        _fig_line.add_trace(_pgo.Scatter(
                            x=_all_rounds_disp,
                            y=_yvals,
                            mode="markers+text" if len(_all_rounds_disp)==1 else "lines+markers+text",
                            name=pil,
                            line=dict(color=_color, width=3, shape="linear"),
                            marker=dict(color=_color, size=9, symbol="circle",
                                        line=dict(color="#070918", width=1.5)),
                            text=[None]*(len(_yvals)-1) + [f"  <b>{_yvals[-1]}</b>"],
                            textposition="middle right",
                            textfont=dict(color=_color, size=11),
                        ))
                    _fig_line.update_layout(
                        height=max(320, 300),
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(5,7,18,.97)",
                        margin=dict(l=10, r=90, t=46, b=60),
                        title=dict(
                            text=f"📈 Evolución acumulada hasta {gp_sel_evo}",
                            font=dict(color="#ffdd7a", size=13, family="Inter"),
                            x=0.5, xanchor="center"
                        ),
                        xaxis=dict(
                            tickfont=dict(color="#a9b2d6", size=9),
                            tickangle=-30,
                            showgrid=True,
                            gridcolor="rgba(246,195,73,.06)",
                            zeroline=False,
                            showline=True,
                            linecolor="rgba(255,255,255,.1)",
                        ),
                        yaxis=dict(
                            tickfont=dict(color="#a9b2d6", size=10),
                            showgrid=True,
                            gridcolor="rgba(255,255,255,.05)",
                            zeroline=False,
                        ),
                        legend=dict(
                            font=dict(color="#e8ecff", size=10),
                            bgcolor="rgba(5,7,18,.8)",
                            bordercolor="rgba(246,195,73,.2)",
                            borderwidth=1,
                            x=1.01, y=1, xanchor="left",
                        ),
                        hovermode="x unified",
                    )
                    st.plotly_chart(_fig_line, use_container_width=True,
                        config={"displayModeBar": False, "staticPlot": True})
                except Exception as _ex:
                    st.warning(f"Gráfico: {_ex}")
            else:
                st.info("Gráfico no disponible.")


            cd=pivot.cumsum().copy(); cd.index=cd.index.map(short); cd.index.name="GP"
            st.markdown(cd.to_html(classes="tabla_historial_dark", border=0), unsafe_allow_html=True)

    with tab_gp:
        _short_cap = short.copy()  # capture for lambda
        gp_sel=st.selectbox("GP:",gps_j,key="hist_gp_sel",format_func=lambda x,s=_short_cap:s.get(x,x))
        df_gp=df_hist[df_hist["gp"]==gp_sel].sort_values("puntos",ascending=False).reset_index(drop=True)
        if df_gp.empty: st.warning("Sin datos.")
        else:
            gan=df_gp.iloc[0]; cg=PILOTO_COLORS.get(gan["piloto"],"#D4AF37")
            st.markdown(f'<div class="card fade-up" style="text-align:center;border-color:{cg}55;padding:22px;"><div style="font-size:38px;">🏆</div><div style="font-weight:900;font-size:18px;color:{cg};">Ganador: {gan["piloto"]}</div><div style="font-size:32px;font-weight:900;color:#ffdd7a;">{gan["puntos"]} pts</div></div>',unsafe_allow_html=True)
            ds=df_gp[["piloto","puntos"]].rename(columns={"piloto":"Piloto","puntos":"Puntos"}); ds.index=range(1,len(ds)+1)
            st.markdown(ds.to_html(classes="tabla_historial_dark", border=0), unsafe_allow_html=True)
            if df_det is not None and not (hasattr(df_det,"empty") and df_det.empty):
                ddt=df_det.copy(); ddt.columns=[c.lower().strip() for c in ddt.columns]
                ddt=ddt[ddt["gp"]==gp_sel]
                if not ddt.empty:
                    # Pivot and clean columns
                    pv=ddt.pivot_table(index="piloto",columns="etapa",values="puntos",fill_value=0,aggfunc="sum").reset_index()
                    pv.columns.name = None
                    pv.columns = [str(c).upper() if c != "piloto" else "Piloto" for c in pv.columns]
                    # Define standard columns (exclude CARRERA_CONST)
                    _cols_want = ["Piloto","QUALY","SPRINT","CARRERA","CONSTRUCTORES"]
                    for _c in _cols_want:
                        if _c not in pv.columns: pv[_c] = 0
                    # Filter only the wanted cols that exist
                    _cols_show = [c for c in _cols_want if c in pv.columns]
                    pv = pv[_cols_show].copy()
                    # Sprint: if all zeros, show "No hubo"
                    _es_sprint_gp = gp_sel in GPS_SPRINT
                    if not _es_sprint_gp:
                        pv["SPRINT"] = "No hubo"
                    # Total column (includes DNS)
                    _num_cols = [c for c in _cols_show if c != "Piloto" and pv[c].dtype != object]
                    pv["TOTAL"] = pv[_num_cols].sum(axis=1)
                    # Subtract DNS if exists in detalle
                    if "DNS" in [str(x).upper() for x in ddt["etapa"].unique()]:
                        _dns_sub = ddt[ddt["etapa"].str.upper()=="DNS"][["piloto","puntos"]].copy()
                        _dns_sub = _dns_sub.groupby("piloto")["puntos"].sum().reset_index()
                        _dns_sub.columns = ["Piloto","_dns_pen"]
                        pv = pv.merge(_dns_sub, on="Piloto", how="left")
                        pv["_dns_pen"] = pv["_dns_pen"].fillna(0).astype(int)
                        pv["DNS"] = pv["_dns_pen"].apply(lambda x: str(x) if x!=0 else "-")
                        pv["TOTAL"] = (pv["TOTAL"] + pv["_dns_pen"]).astype(int)
                        pv = pv.drop(columns=["_dns_pen"])
                    # Style: highlight top scorer
                    _col_map = {"Piloto":"Piloto","QUALY":"Qualy","SPRINT":"Sprint","CARRERA":"Carrera","CONSTRUCTORES":"Constructores","TOTAL":"⚡ TOTAL"}
                    pv = pv.rename(columns=_col_map)
                    # Sort by total desc
                    if "⚡ TOTAL" in pv.columns:
                        _num_mask = pd.to_numeric(pv["⚡ TOTAL"], errors="coerce").notna()
                        pv = pv.sort_values("⚡ TOTAL", ascending=False).reset_index(drop=True)
                    st.markdown(pv.to_html(classes="tabla_historial_dark", border=0, index=False), unsafe_allow_html=True)
            df_gp["Color"]=df_gp["piloto"].map(PILOTO_COLORS).fillna("#a855f7")
            if _PLOTLY_OK:
                try:
                    fig2 = go.Figure(go.Bar(
                        x=df_gp["piloto"], y=df_gp["puntos"],
                        marker_color=df_gp["Color"].tolist(),
                        text=df_gp["puntos"], textposition="outside",
                        textfont=dict(color="#ffdd7a", size=13),
                        cliponaxis=False,
                    ))
                    _ymax_gp = int(df_gp["puntos"].max()) if not df_gp.empty else 10
                    fig2.update_layout(
                        height=270, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                        margin=dict(l=10,r=10,t=48,b=10), showlegend=False,
                        xaxis=dict(tickfont=dict(color="#e8ecff",size=12), showgrid=False),
                        yaxis=dict(tickfont=dict(color="#a9b2d6",size=11), showgrid=True,
                                   gridcolor="rgba(246,195,73,0.08)", zeroline=False,
                                   range=[0, _ymax_gp * 1.25]),
                    )
                    st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False, "staticPlot": True})
                except Exception:
                    pass

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
            try:
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
                st.plotly_chart(fig3, use_container_width=True, config={"displayModeBar": False, "staticPlot": True})
            except Exception:
                pass

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


# ─────────────────────────────────────────────────────────
# PANTALLA PREDICCIONES — Rediseñada con cards estilo VueltaRápida
# ─────────────────────────────────────────────────────────



# ─────────────────────────────────────────────────────────
# UNIFIED MODAL PILOT SELECTOR — click to pick from photo grid
# ─────────────────────────────────────────────────────────
def _init_slots(kp, count):
    for i in range(1, count+1):
        sk = f"{kp}_{i}"
        if sk not in st.session_state:
            st.session_state[sk] = ""

def _get_sel(kp, count):
    return {i: st.session_state.get(f"{kp}_{i}", "") for i in range(1, count+1)}

def modal_pilot_selector(options, count, kp):
    """Hybrid: photo preview row + native selectbox + X button."""
    _init_slots(kp, count)
    medals = {1:"🥇",2:"🥈",3:"🥉"}

    st.markdown("""
    <style>
    /* Pilot row base (inline styles override these) */
    .qrow{display:flex;align-items:center;gap:7px;padding:5px 8px;
      border-radius:9px;margin-bottom:3px;
      border:1px solid rgba(255,255,255,.07);background:rgba(255,255,255,.02);}
    .qrow.qfill{background:rgba(255,255,255,.04);}
    </style>""", unsafe_allow_html=True)

    for i in range(1, count+1):
        cur_d = st.session_state.get(f"{kp}_{i}", "")
        photo = DRIVER_HEADSHOTS.get(cur_d,"") if cur_d else ""
        team  = next((t for t,ds in GRILLA_2026.items() if cur_d in ds),"") if cur_d else ""
        tc    = TEAM_COLORS.get(team,"#a855f7") if team else "#666"
        medal = medals.get(i, f"P{i}")
        logo  = TEAM_LOGOS_CDN.get(team,"") if team else ""

        c_row, c_del = st.columns([9, 1])
        with c_row:
            if cur_d and photo:
                st.markdown(
                    f"<div style='display:flex;align-items:center;justify-content:center;"
                    f"gap:10px;padding:8px 14px;"
                    f"border-radius:12px;margin-bottom:3px;position:relative;"
                    f"border:1.5px solid {tc}55;"
                    f"background:linear-gradient(135deg,{tc}12,rgba(0,0,0,.45));'>"
                    f"<div style='width:28px;height:28px;border-radius:50%;display:flex;"
                    f"align-items:center;justify-content:center;font-size:12px;font-weight:900;"
                    f"flex-shrink:0;background:rgba(246,195,73,.1);border:1px solid {tc}44;color:{tc};'>{medal}</div>"
                    f"<img src='{photo}' style='width:44px;height:44px;border-radius:8px;"
                    f"object-fit:cover;object-position:top;border:1.5px solid {tc}55;flex-shrink:0;'>"
                    f"<div style='text-align:center;min-width:0;'>"
                    f"<div style='font-size:12px;font-weight:800;color:{tc};"
                    f"overflow:hidden;white-space:nowrap;text-overflow:ellipsis;'>{cur_d}</div>"
                    f"<div style='font-size:8px;opacity:.5;letter-spacing:.08em;text-transform:uppercase;margin-top:1px;'>{team}</div>"
                    f"</div>"
                    + (f"<img src='{logo}' style='height:20px;max-width:54px;"
                       f"object-fit:contain;opacity:.8;flex-shrink:0;' loading='lazy'>" if logo else "")
                    + "</div>",
                    unsafe_allow_html=True
                )
            else:
                st.markdown(
                    f"<div style='display:flex;align-items:center;justify-content:center;"
                    f"gap:10px;padding:8px 14px;"
                    f"border-radius:9px;margin-bottom:3px;"
                    f"border:1px solid rgba(255,255,255,.07);background:rgba(255,255,255,.02);'>"
                    f"<div style='width:26px;height:26px;border-radius:50%;display:flex;"
                    f"align-items:center;justify-content:center;font-size:11px;font-weight:900;"
                    f"flex-shrink:0;background:rgba(246,195,73,.07);border:1px solid rgba(246,195,73,.2);"
                    f"color:#ffdd7a;'>{medal}</div>"
                    f"<div style='width:36px;height:36px;border-radius:6px;flex-shrink:0;"
                    f"background:rgba(255,255,255,.04);border:1px dashed rgba(255,255,255,.13);"
                    f"display:flex;align-items:center;justify-content:center;"
                    f"font-size:15px;color:rgba(255,255,255,.2);'>?</div>"
                    f"<div style='font-size:11px;font-weight:700;text-align:center;"
                    f"color:rgba(169,178,214,.38);'>Sin piloto seleccionado</div>"
                    f"</div>",
                    unsafe_allow_html=True
                )
        with c_del:
            if cur_d:
                if st.button("✖", key=f"{kp}_x_{i}", use_container_width=True, help="Quitar"):
                    st.session_state[f"{kp}_{i}"] = ""
                    st.rerun()
            else:
                st.write("")

        taken = {st.session_state.get(f"{kp}_{j}","") for j in range(1,count+1) if j!=i}
        avail = [""] + [o for o in options if o not in taken]
        st.selectbox(
            f"",
            avail,
            index=avail.index(cur_d) if cur_d in avail else 0,
            key=f"{kp}_{i}",
            format_func=lambda x: "— Elegí un piloto —" if x=="" else x,
            label_visibility="collapsed"
        )

    return {i: st.session_state.get(f"{kp}_{i}","") for i in range(1,count+1)}



def modal_constructor_selector(teams, count, kp):
    """Constructor selector: centered car image + full name + selectbox."""
    _init_slots(kp, count)
    medals_lbl = {1:"🥇 1° Lugar", 2:"🥈 2° Lugar", 3:"🥉 3° Lugar"}

    st.markdown("""
    <style>
    .crow{display:flex;align-items:center;gap:10px;padding:8px 12px;
      border-radius:12px;margin-bottom:4px;
      border:1px solid rgba(255,255,255,.08);background:rgba(255,255,255,.02);}
    .crow.cfill{background:rgba(255,255,255,.04);}
    .crow-medal{font-size:16px;flex-shrink:0;}
    .crow-car-wrap{flex:1;display:flex;
      flex-direction:column;align-items:center;justify-content:center;gap:3px;}
    .crow-car{width:100%;max-width:200px;height:58px;object-fit:contain;display:block;}
    .crow-logo{height:22px;max-width:80px;object-fit:contain;opacity:.85;}
    .crow-ph{width:120px;height:46px;border-radius:8px;flex-shrink:0;
      background:rgba(255,255,255,.05);border:1.5px dashed rgba(255,255,255,.15);
      display:flex;align-items:center;justify-content:center;font-size:22px;}
    .crow-info{flex:1;min-width:0;}
    .crow-name{font-size:12px;font-weight:800;overflow:hidden;
      white-space:nowrap;text-overflow:ellipsis;}
    .crow-sub{font-size:9px;opacity:.42;letter-spacing:.06em;text-transform:uppercase;}
    </style>""", unsafe_allow_html=True)

    for i in range(1, count+1):
        cur_t = st.session_state.get(f"{kp}_{i}", "")
        car   = TEAM_CARS_MODULE.get(cur_t,"") if cur_t else ""
        tc    = TEAM_COLORS.get(cur_t,"#a855f7") if cur_t else "rgba(255,255,255,.15)"
        mlbl  = medals_lbl.get(i, f"P{i}")
        medal = ["🥇","🥈","🥉"][i-1]

        c_row, c_del = st.columns([9, 1])
        with c_row:
            if cur_t:
                st.markdown(
                    f"<div style='display:flex;align-items:center;gap:8px;padding:8px 12px;"
                    f"border-radius:12px;margin-bottom:3px;"
                    f"border:1.5px solid {tc}55;"
                    f"background:linear-gradient(135deg,{tc}12,rgba(0,0,0,.5));'>"
                    f"<div style='font-size:15px;flex-shrink:0;'>{medal}</div>"
                    + "<div style='flex:1;display:flex;align-items:center;justify-content:center;min-width:0;'>"
                    + (f"<img style='max-width:190px;height:52px;object-fit:contain;' src='{car}'>" if car else "")
                    + "</div>"
                    + "<div style='flex-shrink:0;min-width:90px;display:flex;flex-direction:column;align-items:flex-end;gap:2px;padding-right:4px;'>"
                    + (f"<img src='" + TEAM_LOGOS_CDN.get(cur_t,"") + "' style='height:18px;max-width:65px;object-fit:contain;opacity:.85;' loading='lazy'>" if TEAM_LOGOS_CDN.get(cur_t,"") else "")
                    + f"<div style='font-size:11px;font-weight:900;color:{tc};white-space:nowrap;text-align:right;'>{cur_t}</div>"
                    + f"<div style='font-size:8px;font-weight:700;color:{tc};opacity:.6;letter-spacing:.06em;text-align:right;'>{mlbl}</div>"
                    + "</div></div>",
                    unsafe_allow_html=True
                )
            else:
                st.markdown(
                    f"<div class='crow'>"
                    f"<div class='crow-medal' style='opacity:.5;'>{medal}</div>"
                    f"<div class='crow-ph'>🏎️</div>"
                    f"<div class='crow-info' style='text-align:center;'>"
                    f"<div class='crow-name' style='color:rgba(169,178,214,.38);'>Sin equipo seleccionado</div>"
                    f"<div class='crow-sub'>{mlbl} — Elegí abajo ↓</div></div></div>",
                    unsafe_allow_html=True
                )
        with c_del:
            if cur_t:
                if st.button("✖", key=f"{kp}_x_{i}", use_container_width=True, help="Quitar"):
                    st.session_state[f"{kp}_{i}"] = ""
                    st.rerun()
            else:
                st.write("")

        taken = {st.session_state.get(f"{kp}_{j}","") for j in range(1,count+1) if j!=i}
        avail = [""] + [t for t in teams if t not in taken]
        st.selectbox(
            "",
            avail,
            index=avail.index(cur_t) if cur_t in avail else 0,
            key=f"{kp}_{i}",
            format_func=lambda x: "— Sin equipo seleccionado —" if x=="" else x,
            label_visibility="collapsed"
        )

    return {i: st.session_state.get(f"{kp}_{i}","") for i in range(1,count+1)}



def pantalla_cargar_predicciones():
    mdb   = _mod_db()
    mcore = _mod_core()
    mauth = _mod_auth()
    if "_error" in mdb or "_error" in mcore or "_error" in mauth:
        st.error("⚠️ Módulos no disponibles."); return

    st.title("🔒 SISTEMA DE PREDICCIÓN 2026")

    # CSS exclusivo para la sección de predicciones
    st.markdown("""
    <style>
    .pred-preview-title {
        font-size: 10px;
        font-weight: 700;
        letter-spacing: .14em;
        color: rgba(246,195,73,.65);
        text-transform: uppercase;
        margin-bottom: 7px;
        margin-top: 2px;
        display: flex;
        align-items: center;
        gap: 6px;
    }
    .pred-preview-title::before {
        content: '';
        display: inline-block;
        width: 18px;
        height: 2px;
        background: rgba(246,195,73,.45);
        border-radius: 2px;
    }
    </style>
    """, unsafe_allow_html=True)

    usr_log = (st.session_state.get("perfil") or {}).get("usuario", "")
    c1, c2  = st.columns(2)
    usuario = c1.selectbox(
        "Piloto Participante", PILOTOS_TORNEO,
        index=PILOTOS_TORNEO.index(usr_log) if usr_log in PILOTOS_TORNEO else 0,
        key="pred_u"
    )
    gp_actual = c2.selectbox("Seleccionar Gran Premio", GPS_OFICIALES, key="pred_gp")

    estado = _safe_call(
        mcore["obtener_estado_gp"], gp_actual, HORARIOS_CARRERA, TZ,
        timeout_sec=4, default={"habilitado": True, "mensaje": "(sin datos)"}
    )
    if not (estado or {}).get("habilitado", True):
        st.error(f"🔴 **PREDICCIONES CERRADAS: {gp_actual}**")
        st.warning((estado or {}).get("mensaje", ""))
        return
    else:
        st.success(f"🟢 **HABILITADO** | {(estado or {}).get('mensaje', '')}")

    es_sprint = gp_actual in GPS_SPRINT
    if es_sprint:
        st.info("⚡ **¡FIN DE SEMANA SPRINT!** ⚡")

    st.subheader("🔐 Validación PIN")
    pin = st.text_input("Ingresá tu PIN (4 dígitos):", type="password", max_chars=4, key="pred_pin")

    nn           = mcore.get("normalizar_nombre", lambda x: x)
    drivers_all  = [d for t in GRILLA_2026.values() for d in t]
    drivers_sprint = [d for d in drivers_all if nn(d) != nn("Franco Colapinto")]
    teams_all    = list(GRILLA_2026.keys())

    def _has(d): return isinstance(d, dict) and any(str(v).strip() for v in d.values())

    def ya_envio(u, gp, etapa):
        try:
            res = _safe_call(mdb["recuperar_predicciones_piloto"], u, gp,
                             timeout_sec=6, default=(None, None, (None, None)))
            dq, ds, (dr, dc) = res
            e = (etapa or "").upper()
            if e == "QUALY":   return _has(dq)
            if e == "SPRINT":  return _has(ds)
            if e == "CARRERA": return _has(dr) or _has(dc)
        except: pass
        return False

    def usel(options, count, kp, lp):
        sel = {}
        for i in range(1, count + 1):
            used  = [v for k, v in sel.items() if k < i and v]
            cur   = st.session_state.get(f"{kp}_{i}", "")
            avail = [o for o in options if o not in used or o == cur]
            sel[i] = st.selectbox(
                f"{lp} {i}°", [""] + avail, key=f"{kp}_{i}",
                format_func=lambda x: "— Seleccionar —" if x == "" else x
            )
        return sel

    # ── Prefijos de clave para session_state ──────────────
    kp_q = f"q_{gp_actual}_{usuario}"
    kp_s = f"s_{gp_actual}_{usuario}"
    kp_r = f"r_{gp_actual}_{usuario}"
    kp_c = f"c_{gp_actual}_{usuario}"

    if es_sprint:
        tab_q, tab_s, tab_r = st.tabs(["⏱️ CLASIFICACIÓN", "⚡ SPRINT", "🏁 CARRERA"])
    else:
        tab_q, tab_r = st.tabs(["⏱️ CLASIFICACIÓN", "🏁 CARRERA"])
        tab_s = None

    # ═══════════════════════════════════════════════════════
    # TAB QUALY
    # ═══════════════════════════════════════════════════════
    with tab_q:
        st.subheader(f"⏱️ Qualy — {gp_actual}")
        st.info("1°(15)  2°(10)  3°(7)  4°(5)  5°(3)  |  Pleno +5 Pts")

        cd = None
        if True:
            q_data = modal_pilot_selector(drivers_all, 5, kp_q)
            st.markdown("---")
            # ── Regla Colapinto ──────────────────────
            _col_photo = DRIVER_HEADSHOTS.get("Franco Colapinto","")
            _col_tc    = TEAM_COLORS.get("ALPINE","#FF4FD8")
            _col_logo = TEAM_LOGOS_CDN.get("ALPINE","")
            st.markdown(
                f"<div style='display:flex;align-items:center;justify-content:center;"
                f"gap:12px;margin:8px 0;position:relative;"
                f"background:rgba(255,79,216,.08);border:1px solid rgba(255,79,216,.35);"
                f"border-radius:12px;padding:10px 14px;'>"
                f"<img src='{_col_photo}' style='width:48px;height:48px;border-radius:50%;object-fit:cover;border:2px solid {_col_tc};flex-shrink:0;'>"
                f"<div style='text-align:center;'><div style='font-size:12px;font-weight:800;color:#FF4FD8;'>🇦🇷 Franco Colapinto</div>"
                f"<div style='font-size:9px;color:rgba(232,236,255,.6);'>Regla especial</div></div>"
                + (f"<img src='{_col_logo}' style='height:20px;max-width:54px;object-fit:contain;opacity:.85;flex-shrink:0;'>" if _col_logo else "")
                + f"</div>",
                unsafe_allow_html=True
            )
            # ── Colapinto selector ─────────────────────────
            _col_pos_q = st.selectbox(
                "🇦🇷 Posición de Franco Colapinto:",
                list(range(1, 23)),
                index=9,
                key=f"cq-{gp_actual}-{usuario}",
                format_func=lambda x: f"P{x}"
            )
            q_data["colapinto_q"] = _col_pos_q
            if gp_actual == "01. Gran Premio de Australia":
                st.markdown("---")
                st.error("🚨 **EDICIÓN ESPECIAL AUSTRALIA — Campeones 2026**")
                cc1, cc2_a = st.columns(2)
                cp_ = cc1.selectbox(
                    "🏆 Piloto campeón", [""] + drivers_all,
                    key=f"camp_p_{gp_actual}_{usuario}",
                    format_func=lambda x: "— Seleccionar —" if x == "" else x
                )
                ce_ = cc2_a.selectbox(
                    "🏗️ Constructor campeón", [""] + teams_all,
                    key=f"camp_e_{gp_actual}_{usuario}",
                    format_func=lambda x: "— Seleccionar —" if x == "" else x
                )
                cd = {"piloto": (cp_ or "").strip(), "equipo": (ce_ or "").strip()}


        ya_q = ya_envio(usuario, gp_actual, "QUALY")
        if ya_q:
            st.success("✅ Ya enviaste la predicción de **QUALY** para este GP.")
        # ── Resumen visual pre-envío ──────────────────────
        if not ya_q:
            _q_filled = [q_data.get(i,"") for i in range(1,6)]
            if all(_q_filled):
                _q_names = " · ".join([f"**P{i}** {q_data[i]}" for i in range(1,6)])
                st.markdown(
                    f"<div style='background:rgba(0,255,100,.06);border:1px solid rgba(0,255,100,.25);"
                    f"border-radius:10px;padding:8px 12px;font-size:11px;margin-bottom:6px;'>"
                    f"✅ <b>Resumen Qualy:</b> {_q_names}</div>",
                    unsafe_allow_html=True
                )
        if st.button("🚀 ENVIAR QUALY", use_container_width=True,
                     key=f"btn_q-{gp_actual}-{usuario}", disabled=ya_q):
            if not pin or len(str(pin).strip()) < 4:
                st.error("⛔ Ingresá tu PIN de 4 dígitos.")
            elif not _safe_call(mauth["verify_pin"], usuario, pin, timeout_sec=30, default=False):
                st.error("⛔ PIN INCORRECTO — verificá y reintentá.")
            elif any(not q_data.get(i) for i in range(1, 6)):
                st.error("⚠️ Completá las 5 posiciones.")
            elif gp_actual == "01. Gran Premio de Australia" and (
                    not cd or not cd["piloto"] or not cd["equipo"]):
                st.error("⚠️ Completá piloto y constructor campeón.")
            else:
                args = (usuario, gp_actual, "QUALY", q_data, cd) if cd else (usuario, gp_actual, "QUALY", q_data)
                try:
                    ok, msg = mdb["guardar_etapa"](*args)
                except Exception as _ge: ok, msg = False, f"Error al guardar: {_ge}"
                if ok:
                    st.success(msg); st.balloons(); st.rerun()
                else:
                    st.error(msg)

    # ═══════════════════════════════════════════════════════
    # TAB SPRINT
    # ═══════════════════════════════════════════════════════
    if tab_s is not None:
        with tab_s:
            st.subheader(f"⚡ Sprint — {gp_actual}")
            st.info("1°(8)  2°(7)  3°(6)  4°(5)  5°(4)  6°(3)  7°(2)  8°(1)  |  Pleno +3 Pts")

            if True:
                s_data = modal_pilot_selector(drivers_sprint, 8, kp_s)


            ya_s = ya_envio(usuario, gp_actual, "SPRINT")
            if ya_s:
                st.success("✅ Ya enviaste la predicción de **SPRINT**.")
            if not ya_s:
                _s_filled = [s_data.get(i,"") for i in range(1,9)]
                if all(_s_filled):
                    _s_names = " · ".join([f"P{i} {s_data[i]}" for i in range(1,9)])
                    st.markdown(
                        f"<div style='background:rgba(0,255,100,.06);border:1px solid rgba(0,255,100,.25);"
                        f"border-radius:10px;padding:8px 12px;font-size:11px;margin-bottom:6px;'>"
                        f"✅ <b>Resumen Sprint:</b> {_s_names}</div>",
                        unsafe_allow_html=True
                    )
            if st.button("🚀 ENVIAR SPRINT", use_container_width=True,
                         key=f"btn_s-{gp_actual}-{usuario}", disabled=ya_s):
                if not _safe_call(mauth["verify_pin"], usuario, pin, timeout_sec=30, default=False):
                    st.error("⛔ PIN INCORRECTO")
                elif any(not s_data.get(i) for i in range(1, 9)):
                    st.error("⚠️ Completá las 8 posiciones.")
                else:
                    try:
                        ok, msg = mdb["guardar_etapa"](usuario, gp_actual, "SPRINT", s_data)
                    except Exception as _ge: ok, msg = False, f"Error al guardar: {_ge}"
                    if ok:
                        st.success(msg); st.balloons(); st.rerun()
                    else:
                        st.error(msg)

    # ═══════════════════════════════════════════════════════
    # TAB CARRERA
    # ═══════════════════════════════════════════════════════
    with tab_r:
        # ── CARRERA ─────────────────────────────────────────
        st.subheader(f"🏁 Carrera — {gp_actual}")
        st.info("1°(25)  2°(18)  3°(15)  4°(12)  5°(10)  6°(8)  7°(6)  8°(4)  9°(2)  10°(1)  |  Pleno +5 Pts")

        if True:
            r_top = modal_pilot_selector(drivers_all, 10, kp_r)
            st.markdown("---")
            # ── Regla Colapinto ──────────────────────
            _col_photo2 = DRIVER_HEADSHOTS.get("Franco Colapinto","")
            _col_tc2    = TEAM_COLORS.get("ALPINE","#FF4FD8")
            _col_logo2 = TEAM_LOGOS_CDN.get("ALPINE","")
            st.markdown(
                f"<div style='display:flex;align-items:center;justify-content:center;"
                f"gap:12px;margin:8px 0;position:relative;"
                f"background:rgba(255,79,216,.08);border:1px solid rgba(255,79,216,.35);"
                f"border-radius:12px;padding:10px 14px;'>"
                f"<img src='{_col_photo2}' style='width:48px;height:48px;border-radius:50%;object-fit:cover;border:2px solid {_col_tc2};flex-shrink:0;'>"
                f"<div style='text-align:center;'><div style='font-size:12px;font-weight:800;color:#FF4FD8;'>🇦🇷 Franco Colapinto</div>"
                f"<div style='font-size:9px;color:rgba(232,236,255,.6);'>Regla especial — posición aparte</div></div>"
                + (f"<img src='{_col_logo2}' style='height:20px;max-width:54px;object-fit:contain;opacity:.85;flex-shrink:0;'>" if _col_logo2 else "")
                + f"</div>",
                unsafe_allow_html=True
            )
            # ── Colapinto selector ─────────────────────────
            _col_pos_r = st.selectbox(
                "🇦🇷 Posición de Franco Colapinto:",
                list(range(1, 23)),
                index=9,
                key=f"cr-{gp_actual}-{usuario}",
                format_func=lambda x: f"P{x}"
            )
            col_r = _col_pos_r


        # ── CONSTRUCTORES ────────────────────────────────────
        st.markdown(
            '<hr style="border:none;border-top:1px solid rgba(246,195,73,.18);margin:18px 0;">',
            unsafe_allow_html=True
        )
        st.subheader("🏗️ Constructores")
        st.info("1°(10)  2°(5)  3°(2)  |  Pleno +3 Pts")

        if True:
            c_top = modal_constructor_selector(teams_all, 3, kp_c)


        # ── Combinar datos y enviar ──────────────────────────
        r_data = dict(r_top)
        r_data["colapinto_r"] = col_r
        r_data["c1"], r_data["c2"], r_data["c3"] = c_top[1], c_top[2], c_top[3]

        ya_r = ya_envio(usuario, gp_actual, "CARRERA")
        if ya_r:
            st.success("✅ Ya enviaste la predicción de **CARRERA/CONSTRUCTORES**.")
        if not ya_r:
            _r_filled = [r_top.get(i,"") for i in range(1,11)]
            _c_filled = [c_top.get(j,"") for j in range(1,4)]
            if all(_r_filled) and all(_c_filled):
                _r_names = ", ".join([f"P{i} {r_top[i]}" for i in range(1,11)])
                _c_names = f"1° {c_top.get(1,'?')} · 2° {c_top.get(2,'?')} · 3° {c_top.get(3,'?')}"
                st.markdown(
                    f"<div style='background:rgba(0,255,100,.06);border:1px solid rgba(0,255,100,.25);"
                    f"border-radius:10px;padding:8px 12px;font-size:11px;margin-bottom:6px;'>"
                    f"✅ <b>Carrera:</b> {_r_names}<br>"
                    f"🏗️ <b>Constructores:</b> {_c_names}</div>",
                    unsafe_allow_html=True
                )
        if st.button("🚀 ENVIAR CARRERA Y CONSTRUCTORES", use_container_width=True,
                     key=f"btn_r-{gp_actual}-{usuario}", disabled=ya_r):
            if not _safe_call(mauth["verify_pin"], usuario, pin, timeout_sec=30, default=False):
                st.error("⛔ PIN INCORRECTO")
            elif any(not r_data.get(i) for i in range(1, 11)):
                st.error("⚠️ Completá las 10 posiciones.")
            elif not r_data["c1"] or not r_data["c2"] or not r_data["c3"]:
                st.error("⚠️ Completá top 3 Constructores.")
            else:
                try:
                    ok, msg = mdb["guardar_etapa"](usuario, gp_actual, "CARRERA", r_data)
                except Exception as _ge: ok, msg = False, f"Error al guardar: {_ge}"
                if ok:
                    st.success(msg); st.balloons(); st.rerun()
                else:
                    st.error(msg)


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
        st.markdown("**🛠️ Constructores**")
        of_r_auto={i:oficial.get(f"r{i}","") for i in ESCALA_CARRERA_JUEGO.keys()}
        top3,tp=calcular_constructores_auto(of_r_auto,GRILLA_2026,ESCALA_CARRERA_JUEGO)
        if len(top3)>=3:
            oficial["c1"],oficial["c2"],oficial["c3"]=top3[0],top3[1],top3[2]
            st.success(f"Auto: {top3[0]} / {top3[1]} / {top3[2]}")
        else:
            oficial["c1"]=oficial["c2"]=oficial["c3"]=""
            st.warning("Cargá primero los 10 de carrera.")
        if tp: st.caption(str(tp))
        # Manual override — in case auto differs from official F1 result
        st.markdown("**✏️ Ajuste manual** (si el auto difiere del FIA):")
        _teams_calc = list(GRILLA_2026.keys())
        _c1_ov = st.selectbox("1° Constructor", ["(Automático)"] + _teams_calc, key=f"c1_ov-{gp_calc}")
        _c2_ov = st.selectbox("2° Constructor", ["(Automático)"] + _teams_calc, key=f"c2_ov-{gp_calc}")
        _c3_ov = st.selectbox("3° Constructor", ["(Automático)"] + _teams_calc, key=f"c3_ov-{gp_calc}")
        if _c1_ov != "(Automático)": oficial["c1"] = _c1_ov
        if _c2_ov != "(Automático)": oficial["c2"] = _c2_ov
        if _c3_ov != "(Automático)": oficial["c3"] = _c3_ov
        st.caption(f"✅ Final: **{oficial.get('c1','?')}** / **{oficial.get('c2','?')}** / **{oficial.get('c3','?')}**")
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
    st.caption("Úsalo para reconstruir el historial local sin volver a sumar a Posiciones. "  
               "Requiere que ya hayas cargado los resultados oficiales arriba.")
    hist_done_key = f"HIST_DONE::{gp_calc}"
    hist_done = _safe_call(mdb["lock_exists"], hist_done_key, timeout_sec=4, default=False)
    # Validate that at least carrera P1 is filled
    _of_r1 = (oficial.get("r1","") or "").strip()
    _of_q1 = (oficial.get("q1","") or "").strip()
    _hist_ok = bool(_of_r1 and _of_q1)
    if hist_done:
        st.warning("🔒 Historial ya generado para este GP. Si necesitás rehacerlo contactá al admin.")
        if st.button("🔓 Desbloquear historial (emergencia)", key=f"hist_unlock_{gp_calc}",
                     use_container_width=True):
            try:
                import sqlite3 as _sq2, os as _os3
                for _d in [_os3.getenv("DB_PATH","torneo.db"),"torneo.db","database.db","data.db","fw.db"]:
                    if _os3.path.exists(_d):
                        _c = _sq2.connect(_d)
                        _c.execute("DELETE FROM locks WHERE key=?", (hist_done_key,))
                        _c.commit(); _c.close(); break
                st.success("✅ Lock eliminado. Completá los resultados oficiales antes de regenerar.")
                st.rerun()
            except Exception as _he: st.error(str(_he))
    else:
        if not _hist_ok:
            st.error("⛔ Completá primero los resultados oficiales (al menos Carrera 1° y Qualy 1°). "
                     "Sin datos, el historial se guardaría con todos en 0.")
        if st.button("🧾 GENERAR HISTORIAL", use_container_width=True,
                     key=f"btn_hist_{gp_calc}", disabled=not _hist_ok):
            try:
                df_h = madm["historial"](
                    gp_calc=gp_calc, oficial=oficial,
                    pilotos_torneo=PILOTOS_TORNEO, gps_sprint=GPS_SPRINT
                )
                _safe_call(mdb["set_lock"], hist_done_key, timeout_sec=4)
                st.success("✅ Historial generado y bloqueado.")
                st.dataframe(df_h, use_container_width=True)
            except Exception as e: st.error(f"Error: {e}")
    st.divider(); st.subheader("⛔ SANCIONES D.N.S.")
    st.info("**Regla**: −5 pts por cada etapa no enviada (QUALY · SPRINT si aplica · CARRERA+CONSTRUCTORES). El sistema detecta automáticamente quién no envió.")
    dns_key=f"DNS_DONE::{gp_calc}"; dns_done=_safe_call(mdb["lock_exists"],dns_key,timeout_sec=4,default=False)

    # — Preview: show who sent what BEFORE applying
    if st.button("🔍 Ver quién envió predicciones", key=f"btn_dns_preview_{gp_calc}", use_container_width=True):
        try:
            from core.database import detectar_faltantes_por_gp
            falt_prev = detectar_faltantes_por_gp(gp_calc, PILOTOS_TORNEO, GPS_SPRINT)
            want_sprint = gp_calc in GPS_SPRINT
            rows_prev = []
            for p in PILOTOS_TORNEO:
                f = falt_prev[p]
                miss = []
                if not f["QUALY"]: miss.append("❌ QUALY")
                if want_sprint and not f["SPRINT"]: miss.append("❌ SPRINT")
                if not f["CARRERA"]: miss.append("❌ CARRERA")
                pen = -5 * len(miss)
                rows_prev.append({
                    "Piloto": p,
                    "QUALY": "✅" if f["QUALY"] else "❌",
                    "SPRINT": ("✅" if f["SPRINT"] else "❌") if want_sprint else "—",
                    "CARRERA": "✅" if f["CARRERA"] else "❌",
                    "Faltantes": ", ".join(miss) if miss else "✅ Todo enviado",
                    "Penalización": f"{pen} pts" if pen != 0 else "Sin sanción"
                })
            import pandas as _pd_dns
            st.dataframe(_pd_dns.DataFrame(rows_prev), use_container_width=True)
        except Exception as _e:
            st.error(f"Error al verificar: {_e}")

    if dns_done:
        st.warning("🔒 Sanciones ya aplicadas para este GP.")
        st.markdown("---")
        st.markdown("**🚨 Zona de emergencia** — Solo usar si las sanciones fueron aplicadas por error:")
        if st.button("🔓 DESHACER SANCIONES DNS (EMERGENCIA)", key=f"btn_dns_undo_{gp_calc}",
                     use_container_width=True):
            try:
                import sqlite3 as _sq, os as _os2
                st.warning("⚠️ Para revertir puntos: borrá las filas DNS del GP en la hoja 'historial_detalle' de Google Sheets, luego recalculá la tabla general.")
                _DBN = _os2.getenv("DB_PATH", "torneo.db")
                _deleted = False
                for _dbn2 in [_DBN, "torneo.db","database.db","data.db","fw.db"]:
                    if _os2.path.exists(_dbn2):
                        try:
                            _conn2 = _sq.connect(_dbn2)
                            _conn2.execute("DELETE FROM locks WHERE key=?", (dns_key,))
                            _conn2.commit(); _conn2.close()
                            _deleted = True; break
                        except: pass
                if _deleted:
                    st.success("✅ Lock DNS eliminado. Podés volver a aplicar sanciones correctas.")
                    st.rerun()
                else:
                    st.error("No se encontró la base de datos. Contactá al admin técnico.")
            except Exception as _ue:
                st.error(f"Error al deshacer: {_ue}")
    else:
        st.warning("⚠️ Asegurate de haber revisado el preview antes de aplicar.")
        if st.button("⛔ APLICAR SANCIONES D.N.S. (−5 pts por etapa faltante)", use_container_width=True, key=f"btn_dns_{gp_calc}", type="primary"):
            df_d=mdb["aplicar_sanciones_dns"](gp_calc,PILOTOS_TORNEO,GPS_SPRINT)
            _safe_call(mdb["set_lock"],dns_key,timeout_sec=4)
            st.success("✅ Sanciones D.N.S. aplicadas correctamente.")
            st.dataframe(df_d, use_container_width=True)
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
    /* ── MESA CHICA MODERNA ─────────────────── */
    .mc-modern-header {
      background: linear-gradient(135deg,
        rgba(7,9,22,.98) 0%,
        rgba(13,18,42,.98) 50%,
        rgba(7,9,22,.98) 100%);
      border: 1px solid rgba(212,175,55,.35);
      border-radius: 20px;
      padding: 24px 20px 20px;
      text-align: center;
      position: relative;
      overflow: hidden;
      margin-bottom: 16px;
    }
    .mc-modern-header::before {
      content:'';position:absolute;top:0;left:0;right:0;height:2px;
      background: linear-gradient(90deg, transparent, #d4af37, #1565c0, #d4af37, transparent);
    }
    .mc-modern-header::after {
      content:'';position:absolute;bottom:0;left:0;right:0;height:1px;
      background: linear-gradient(90deg, transparent, rgba(212,175,55,.4), transparent);
    }
    .mc-header-f1tag {
      display:inline-flex;align-items:center;gap:6px;
      background:rgba(21,101,192,.25);border:1px solid rgba(21,101,192,.55);
      border-radius:20px;padding:3px 12px;font-size:10px;font-weight:800;
      letter-spacing:.14em;color:#90caf9;margin-bottom:12px;
    }
    .mc-header-title {
      font-size:24px;font-weight:900;letter-spacing:.08em;
      background:linear-gradient(90deg, #d4af37, #ffe896, #d4af37);
      -webkit-background-clip:text;-webkit-text-fill-color:transparent;
      background-clip:text;margin-bottom:6px;
    }
    .mc-header-sub {
      font-size:12px;color:rgba(232,236,255,.6);margin-bottom:14px;
    }
    .mc-header-pills {
      display:flex;flex-wrap:wrap;justify-content:center;gap:6px;
    }
    .mc-header-pill {
      background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.1);
      border-radius:20px;padding:4px 12px;font-size:10px;color:rgba(232,236,255,.7);
      font-weight:600;
    }
    /* Message bubbles — modern chat style */
    .mc-chat-feed { display:flex;flex-direction:column;gap:8px;padding:4px 0; }
    .mc-bubble {
      max-width:88%;display:flex;flex-direction:column;
      animation:fadeIn .2s ease;
    }
    .mc-bubble.mc-left { align-self:flex-start; }
    .mc-bubble.mc-right { align-self:flex-end; }
    .mc-bubble-inner {
      padding:9px 13px 8px;border-radius:16px;position:relative;
      word-break:break-word;line-height:1.55;font-size:13px;
    }
    .mc-left .mc-bubble-inner {
      background:linear-gradient(135deg,rgba(21,101,192,.18),rgba(8,12,30,.97));
      border:1px solid rgba(100,180,255,.35);border-bottom-left-radius:4px;
      box-shadow:0 2px 10px rgba(21,101,192,.12);
    }
    .mc-right .mc-bubble-inner {
      background:linear-gradient(135deg,rgba(90,30,180,.18),rgba(8,12,30,.97));
      border:1px solid rgba(180,100,255,.3);border-bottom-right-radius:4px;
      box-shadow:0 2px 10px rgba(123,47,247,.10);
    }
    .mc-bubble-name {
      font-size:10px;font-weight:900;letter-spacing:.04em;margin-bottom:3px;
      display:flex;align-items:center;gap:6px;flex-wrap:wrap;
    }
    .mc-bubble-text { color:rgba(232,236,255,.9);font-size:13px; }
    .mc-bubble-time {
      font-size:9px;opacity:.38;letter-spacing:.03em;margin-top:5px;
      text-align:right;
    }
    @keyframes fadeIn{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:none}}
    /* Hide old card classes */
    .mc-msg-card, .mc-card, .mc-head, .mc-text { display:none !important; }
    /* Form button override */
    div[data-testid="stForm"] button[kind="primary"] {
      background: linear-gradient(90deg, #9a7a10, #d4af37, #9a7a10) !important;
      border: 1px solid rgba(255,238,150,.6) !important;
      color: #1a1000 !important;
      font-weight: 900 !important;
      border-radius: 12px !important;
      letter-spacing: .07em;
      box-shadow: 0 0 14px rgba(212,175,55,.25) !important;
    }
    div[data-testid="stForm"] button[kind="primary"]:hover {
      background: linear-gradient(90deg, #d4af37, #ffe896, #d4af37) !important;
      box-shadow: 0 0 22px rgba(212,175,55,.45) !important;
    }
    </style>
    <div class="mc-modern-header">
      <div class="mc-header-f1tag">
        <span style="width:8px;height:8px;border-radius:50%;
          background:#1565c0;display:inline-block;"></span>
        F1 · TEMPORADA 2026
        <span style="width:8px;height:8px;border-radius:50%;
          background:#d4af37;display:inline-block;"></span>
      </div>
      <div class="mc-header-title">MESA CHICA — FEFE WOLF</div>
      <div class="mc-header-sub">El paddock privado &nbsp;·&nbsp; <b style="color:rgba(232,236,255,.8);">Solo se habla de F1</b> 🏁</div>
      <div class="mc-header-pills">
        <span class="mc-header-pill">🏎️ Temporada 2026</span>
        <span class="mc-header-pill">📡 En vivo</span>
        <span class="mc-header-pill">🏁 24 GPs</span>
        <span class="mc-header-pill">🔵 Mod = FIPF</span>
        <span class="mc-header-pill">⚫ Participante = Formulero</span>
      </div>
    </div>
    """, unsafe_allow_html=True)

    with st.form("mc_form", clear_on_submit=True):
        _ci, _cb = st.columns([10, 1])
        with _ci:
            msg = st.text_input("", key="mc_msg",
                placeholder="✍️ Escribí un mensaje en la Mesa Chica…",
                label_visibility="collapsed")
        with _cb:
            enviar = st.form_submit_button("🏎️", use_container_width=True)

    st.markdown("""<style>
    div[data-testid="stForm"]{
      background:rgba(7,9,22,.97)!important;
      border:1px solid rgba(212,175,55,.2)!important;
      border-radius:13px!important;padding:7px 10px!important;margin-bottom:6px!important;
    }
    div[data-testid="stForm"] input[type="text"]{
      background:rgba(255,255,255,.04)!important;
      border:1px solid rgba(255,255,255,.1)!important;
      border-radius:9px!important;color:#e8ecff!important;font-size:13px!important;
    }
    div[data-testid="stForm"] button[kind="primaryFormSubmit"],
    div[data-testid="stForm"] button[kind="primary"]{
      background:linear-gradient(90deg,#8a6c0a,#d4af37)!important;
      border:none!important;border-radius:9px!important;
      color:#1a1000!important;font-weight:900!important;font-size:18px!important;
      min-height:38px!important;
    }
    </style>""", unsafe_allow_html=True)

    _ta, _tb = st.columns([3, 2])
    with _ta:
        if st.button("🔄 Actualizar", use_container_width=True, key="mc_ref"): st.rerun()
    with _tb:
        if is_mod and st.button("🧹 Limpiar", use_container_width=True, key="mc_cln"):
            m["mc_purge_html_messages"](); st.rerun()

    if enviar:
        txt = (msg or "").strip()
        if not txt: st.warning("Escribí algo.")
        elif m["mc_is_spam"](usuario): st.error("⚠️ Muy rápido.")
        else: m["mc_add_message"](usuario, txt); st.rerun()

    # Pagination: single DB call, slice for display
    _mc_limit = st.session_state.get("mc_show_limit", 50)
    _all_rows = m["mc_list_messages"](limit=999) or []
    _total_count = len(_all_rows)
    rows = _all_rows[:_mc_limit]

    for row in rows:
        msg_id, u, texto, ts, edited_ts = row[:5]
        tipo, label, stars = _mc_badge(u)
        badge_html = f'<span class="mc-badge {tipo}">{label} {f"<span class=mc-stars>{stars}</span>" if stars else ""}</span>'
        can_edit   = is_mod or (u == usuario)
        can_delete = is_mod
        editando   = (st.session_state["mc_editing_id"] == msg_id)

        # Avatar del usuario — foto de cabeza con borde de color
        _av_ph  = DRIVER_HEADSHOTS.get(u, DRIVER_PHOTOS.get(u, ""))
        _av_clr = PILOTO_COLORS.get(u, "#a855f7")
        _av_ini = "".join(w[0] for w in u.split()[:2]).upper()
        if _av_ph:
            _av_html = (f'<img src="{_av_ph}" style="width:38px;height:38px;border-radius:50%;'
                        f'object-fit:cover;object-position:top;border:2.5px solid {_av_clr};'
                        f'flex-shrink:0;box-shadow:0 0 10px {_av_clr}55;">')
        else:
            _av_html = (f'<div style="width:38px;height:38px;border-radius:50%;background:{_av_clr}22;'
                        f'border:2.5px solid {_av_clr};display:flex;align-items:center;justify-content:center;'
                        f'font-weight:900;font-size:11px;color:{_av_clr};flex-shrink:0;">{_av_ini}</div>')

        # Convert timestamps to Argentina time
        def _ts_arg(t):
            if not t: return ""
            try:
                from datetime import datetime as _dt
                import pytz as _ptz
                _utc = _ptz.utc
                _arg = _ptz.timezone("America/Argentina/Buenos_Aires")
                _s = t.replace("T"," ").strip().split(".")[0]
                _d = _dt.strptime(_s, "%Y-%m-%d %H:%M:%S")
                _d = _utc.localize(_d).astimezone(_arg)
                return _d.strftime("%d/%m %H:%M")
            except: return t.replace("T"," ")[:16]
        edit_tag   = f" · ✏️{_ts_arg(edited_ts)}" if edited_ts else ""
        safe_ts    = _mc_safe(_ts_arg(ts) + edit_tag)

        liked = m["mc_user_liked"](msg_id, usuario)
        lc    = m["mc_like_count"](msg_id)
        lbl   = f"{'🏁' if liked else '🚩'} {lc}"

        _is_own = (u == usuario)
        _side   = "mc-right" if _is_own else "mc-left"
        _nc     = "#90caf9" if _mc_is_mod(u) else ("#e0b0ff" if _is_own else "#c8b8ff")

        if editando:
            # Edit mode
            st.markdown(
                f'<div class="mc-bubble {_side}"><div class="mc-bubble-inner">'
                f'<div class="mc-bubble-name" style="color:{_nc};">{_mc_safe(u)} {badge_html}</div>',
                unsafe_allow_html=True
            )
            nuevo = st.text_area("Editar", value=texto, key=f"mc_et_{msg_id}", height=80)
            st.markdown("</div></div>", unsafe_allow_html=True)
            ec1, ec2, ec3, ec4 = st.columns(4)
            with ec1:
                if st.button("💾 Guardar", key=f"mc_sv_{msg_id}", use_container_width=True):
                    nt = (nuevo or "").strip()
                    if nt:
                        m["mc_update_message"](msg_id, nt)
                        st.session_state["mc_editing_id"] = None; st.rerun()
            with ec2:
                if st.button("❌ Cancelar", key=f"mc_ca_{msg_id}", use_container_width=True):
                    st.session_state["mc_editing_id"] = None; st.rerun()
            with ec3:
                if can_delete and st.button("🗑️ Borrar", key=f"mc_de_{msg_id}", use_container_width=True):
                    m["mc_soft_delete_message"](msg_id, deleted_by=usuario)
                    st.session_state["mc_editing_id"] = None; st.rerun()
            with ec4:
                if st.button(lbl, key=f"mc_le_{msg_id}", use_container_width=True):
                    m["mc_toggle_like"](msg_id, usuario); st.rerun()
        else:
            # Normal display — burbuja completa con botones incorporados
            _liked_val = m["mc_user_liked"](msg_id, usuario)
            _lc_val    = m["mc_like_count"](msg_id)
            _lbl_val   = f"{'🏁' if _liked_val else '🚩'} {_lc_val}"

            # Botones compactos como HTML puro dentro de la burbuja
            _btn_like_style = (
                f"cursor:pointer;background:none;border:none;padding:2px 6px;"
                f"font-size:12px;color:rgba(169,178,214,.45);line-height:1;"
                f"transition:color .15s;")
            _btns_html = f'<div style="display:flex;gap:2px;margin-top:6px;justify-content:{"flex-end" if _is_own else "flex-start"};">'
            _btns_html += f'<span style="{_btn_style_base}">{_lbl_val}</span>' if False else ""  # placeholder

            # Build action row as pure HTML — no st.button (avoids misalignment)
            _act_align = "flex-end" if _is_own else "flex-start"
            _bubble_content = (
                f'<div class="mc-bubble-name" style="color:{_nc};{"text-align:right;" if _is_own else ""}">'
                f'{_mc_safe(u)} {badge_html}</div>'
                f'<div class="mc-bubble-text">{_mc_safe(texto)}</div>'
                f'<div class="mc-bubble-time">{safe_ts}</div>'
            )

            if _is_own:
                st.markdown(
                    f'<div style="display:flex;align-items:flex-end;gap:8px;justify-content:flex-end;margin-bottom:2px;">'
                    f'<div style="display:flex;flex-direction:column;align-items:flex-end;max-width:78%;">'
                    f'<div class="mc-bubble mc-right"><div class="mc-bubble-inner">{_bubble_content}</div></div>'
                    f'</div>{_av_html}</div>',
                    unsafe_allow_html=True
                )
            else:
                st.markdown(
                    f'<div style="display:flex;align-items:flex-end;gap:8px;margin-bottom:2px;">'
                    f'{_av_html}'
                    f'<div style="display:flex;flex-direction:column;align-items:flex-start;max-width:78%;">'
                    f'<div class="mc-bubble mc-left"><div class="mc-bubble-inner">{_bubble_content}</div></div>'
                    f'</div></div>',
                    unsafe_allow_html=True
                )
            # Botones debajo, alineados, sin fondo — usando st.columns pero con offset correcto
            if _is_own:
                _, _bcols = st.columns([7, 3])
            else:
                _bcols, _ = st.columns([3, 7])
            with _bcols:
                st.markdown("""<style>
                div[data-testid="stHorizontalBlock"] .mc-mini-btn > button {
                  background:transparent!important;border:none!important;
                  box-shadow:none!important;color:rgba(169,178,214,.38)!important;
                  font-size:13px!important;padding:1px 5px!important;
                  min-height:18px!important;width:auto!important;line-height:1!important;
                }
                div[data-testid="stHorizontalBlock"] .mc-mini-btn > button:hover {
                  color:#ffdd7a!important;
                }
                </style>""", unsafe_allow_html=True)
                _c1, _c2, _c3 = st.columns(3)
                with _c1:
                    st.markdown('<div class="mc-mini-btn">', unsafe_allow_html=True)
                    if st.button(_lbl_val, key=f"mc_lk_{msg_id}", use_container_width=True):
                        m["mc_toggle_like"](msg_id, usuario); st.rerun()
                    st.markdown('</div>', unsafe_allow_html=True)
                with _c2:
                    if can_edit:
                        st.markdown('<div class="mc-mini-btn">', unsafe_allow_html=True)
                        if st.button("✏️", key=f"mc_ed_{msg_id}", use_container_width=True, help="Editar"):
                            st.session_state["mc_editing_id"] = msg_id; st.rerun()
                        st.markdown('</div>', unsafe_allow_html=True)
                with _c3:
                    if can_delete:
                        st.markdown('<div class="mc-mini-btn">', unsafe_allow_html=True)
                        if st.button("🗑️", key=f"mc_dl_{msg_id}", use_container_width=True, help="Borrar"):
                            m["mc_soft_delete_message"](msg_id, deleted_by=usuario); st.rerun()
                        st.markdown('</div>', unsafe_allow_html=True)
    # ── "Ver más mensajes" button ─────────────────────
    _showing = len(rows)
    if _total_count > _showing:
        _remaining = _total_count - _showing
        if st.button(f"📜 Ver {min(_remaining, 50)} mensajes más ({_remaining} restantes)",
                     use_container_width=True, key="mc_load_more"):
            st.session_state["mc_show_limit"] = _mc_limit + 50
            st.rerun()
    elif _total_count > 50:
        if st.button("🔼 Mostrar menos", use_container_width=True, key="mc_show_less"):
            st.session_state["mc_show_limit"] = 50
            st.rerun()


def pantalla_head_to_head():
    for k in ["h2h_a","h2h_b"]:
        if k not in st.session_state: st.session_state[k]=None
    pa=st.session_state["h2h_a"]; pb=st.session_state["h2h_b"]

    st.markdown("""<style>
    @keyframes h2hG{0%,100%{box-shadow:0 0 24px rgba(212,175,55,.13);}
      50%{box-shadow:0 0 46px rgba(212,175,55,.26);}}
    .h2h-hero{background:linear-gradient(145deg,rgba(7,9,22,.99),rgba(13,17,42,.99));
      border:1.5px solid rgba(212,175,55,.45);border-radius:20px;
      padding:20px 18px 16px;text-align:center;margin-bottom:12px;
      animation:h2hG 3.5s ease-in-out infinite;position:relative;overflow:hidden;}
    .h2h-hero::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;
      background:linear-gradient(90deg,transparent,#d4af37,rgba(255,220,100,.9),#d4af37,transparent);}
    .h2h-t{font-size:28px;font-weight:900;letter-spacing:.1em;
      background:linear-gradient(90deg,#d4af37,#ffe896,#d4af37);
      -webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;}
    .h2h-sub{font-size:10px;color:rgba(169,178,214,.5);margin-top:3px;letter-spacing:.07em;}
    .h2h-stat{background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.06);
      border-radius:10px;padding:8px 13px;margin-bottom:6px;}
    .h2h-sl{font-size:9px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;
      color:rgba(246,195,73,.5);text-align:center;margin-bottom:5px;}
    div[data-testid="stHorizontalBlock"] .h2hbw{position:relative;}
    div[data-testid="stHorizontalBlock"] .h2hbw>button{
      position:absolute!important;top:-82px!important;left:0!important;
      width:100%!important;height:82px!important;
      background:transparent!important;border:none!important;
      box-shadow:none!important;opacity:0!important;
      cursor:pointer!important;z-index:10!important;
      padding:0!important;min-height:0!important;}
    div[data-testid="stHorizontalBlock"] .h2hbw>button:hover{opacity:0!important;}
    </style>""", unsafe_allow_html=True)

    st.markdown('<div class="h2h-hero"><div style="font-size:26px;margin-bottom:3px;">⚔️</div>'
                '<div class="h2h-t">HEAD TO HEAD</div>'
                '<div class="h2h-sub">Seleccioná dos participantes para comparar</div></div>',
                unsafe_allow_html=True)

    cols=st.columns(len(PILOTOS_TORNEO))
    for idx,pil in enumerate(PILOTOS_TORNEO):
        with cols[idx]:
            clr=PILOTO_COLORS.get(pil,"#a855f7")
            ph=DRIVER_HEADSHOTS.get(pil,DRIVER_PHOTOS.get(pil,""))
            ini="".join(w[0] for w in pil.split()[:2]).upper()
            is_a=(pa==pil);is_b=(pb==pil)
            bc="#ffdd7a" if is_a else("#3b82f6" if is_b else "rgba(255,255,255,.2)")
            bw="3px" if(is_a or is_b) else "1.5px"
            shd=f"box-shadow:0 0 14px {clr}55;" if(is_a or is_b) else ""
            tag=(f'<div style="position:absolute;top:-2px;right:calc(50% - 32px);width:16px;height:16px;'
                 f'border-radius:50%;background:{"#ffdd7a" if is_a else "#3b82f6"};color:#000;'
                 f'font-size:9px;font-weight:900;display:flex;align-items:center;justify-content:center;'
                 f'z-index:5;">{"A" if is_a else "B"}</div>') if(is_a or is_b) else ""
            if ph:
                img=(f'<img src="{ph}" style="width:58px;height:58px;border-radius:50%;'
                     f'object-fit:cover;object-position:top;border:{bw} solid {bc};'
                     f'{shd}display:block;margin:0 auto 4px;">')
            else:
                img=(f'<div style="width:58px;height:58px;border-radius:50%;background:{clr}22;'
                     f'border:{bw} solid {bc};{shd}display:flex;align-items:center;justify-content:center;'
                     f'font-weight:900;font-size:15px;color:{clr};margin:0 auto 4px;">{ini}</div>')
            st.markdown(f'<div style="text-align:center;position:relative;">{tag}{img}'
                        f'<div style="font-size:9px;font-weight:800;color:{clr};'
                        f'letter-spacing:.05em;text-transform:uppercase;">{pil.split()[0]}</div></div>',
                        unsafe_allow_html=True)
            st.markdown('<div class="h2hbw">', unsafe_allow_html=True)
            if st.button(pil.split()[0], key=f"h2h_{idx}", use_container_width=True, help=f"Sel. {pil}"):
                if pa==pil: st.session_state["h2h_a"]=pb; st.session_state["h2h_b"]=None
                elif pb==pil: st.session_state["h2h_b"]=None
                elif pa is None: st.session_state["h2h_a"]=pil
                else: st.session_state["h2h_b"]=pil
                st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)

    pa=st.session_state["h2h_a"]; pb=st.session_state["h2h_b"]
    if not pa or not pb or pa==pb:
        st.markdown('<div style="text-align:center;padding:26px 0;color:rgba(169,178,214,.38);'
                    'font-size:13px;">Seleccioná dos participantes distintos.</div>', unsafe_allow_html=True)
        return

    @st.cache_data(ttl=60, show_spinner=False)
    def _h2hdf():
        m2=_mod_db()
        if "_error" in m2: return None
        return _safe_call(m2["leer_tabla_posiciones"],PILOTOS_TORNEO,timeout_sec=8,default=None)
    dft=_h2hdf()
    if dft is None or (hasattr(dft,"empty") and dft.empty):
        dft=pd.DataFrame({"Piloto":PILOTOS_TORNEO,"Puntos":[0]*5,"Qualys":[0]*5,"Sprints":[0]*5,"Carreras":[0]*5})
    def _rv(n,c):
        if not(hasattr(dft,"empty") and dft.empty):
            r=dft[dft["Piloto"]==n]
            if not r.empty: return int(r.iloc[0].get(c,0) or 0)
        return 0
    ca=PILOTO_COLORS.get(pa,"#a855f7"); cb=PILOTO_COLORS.get(pb,"#3b82f6")
    ph_a=DRIVER_HEADSHOTS.get(pa,DRIVER_PHOTOS.get(pa,"")); ph_b=DRIVER_HEADSHOTS.get(pb,DRIVER_PHOTOS.get(pb,""))
    ini_a="".join(w[0] for w in pa.split()[:2]).upper(); ini_b="".join(w[0] for w in pb.split()[:2]).upper()
    pts_a=_rv(pa,"Puntos"); pts_b=_rv(pb,"Puntos")
    qua_a=_rv(pa,"Qualys"); qua_b=_rv(pb,"Qualys")
    spr_a=_rv(pa,"Sprints"); spr_b=_rv(pb,"Sprints")
    car_a=_rv(pa,"Carreras"); car_b=_rv(pb,"Carreras")
    gps_a=qua_a+spr_a+car_a; gps_b=qua_b+spr_b+car_b
    win=(pa if pts_a>pts_b else(pb if pts_b>pts_a else None))

    def _av(ph,ini,clr,sz=76):
        if ph:return(f'<img src="{ph}" style="width:{sz}px;height:{sz}px;border-radius:50%;'
                     f'object-fit:cover;object-position:top;border:3px solid {clr};'
                     f'margin:0 auto 6px;display:block;box-shadow:0 0 12px {clr}44;">')
        return(f'<div style="width:{sz}px;height:{sz}px;border-radius:50%;background:{clr}22;'
               f'border:3px solid {clr};display:flex;align-items:center;justify-content:center;'
               f'font-weight:900;font-size:{sz//4}px;color:{clr};margin:0 auto 6px;">{ini}</div>')

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
    c1,cV,c2=st.columns([5,1,5])
    def _pc(col,name,ph,ini,clr,pts,isw):
        ldr=(f'<div style="margin-top:5px;"><span style="background:{clr}33;color:{clr};'
             f'border-radius:20px;padding:3px 10px;font-size:10px;font-weight:900;">👑 LIDERA</span></div>'
             ) if isw else ""
        col.markdown(
            f'<div style="background:{clr}0d;border:1.5px solid {clr}44;border-radius:16px;'
            f'padding:16px 12px;text-align:center;">{_av(ph,ini,clr)}'
            f'<div style="font-weight:900;font-size:14px;color:{clr};">{name}</div>'
            f'<div style="font-size:32px;font-weight:900;color:#ffdd7a;margin:4px 0;">{pts}</div>'
            f'<div style="font-size:9px;color:rgba(232,236,255,.4);letter-spacing:.1em;'
            f'text-transform:uppercase;">PUNTOS TOTALES</div>{ldr}</div>', unsafe_allow_html=True)
    _pc(c1,pa,ph_a,ini_a,ca,pts_a,win==pa)
    cV.markdown('<div style="display:flex;align-items:center;justify-content:center;height:100%;padding:8px 0;">'
                '<div style="width:42px;height:42px;border-radius:50%;background:linear-gradient(145deg,#d4af37,#9a7a10);'
                'display:flex;align-items:center;justify-content:center;font-weight:900;font-size:13px;color:#1a1000;'
                'box-shadow:0 0 14px rgba(212,175,55,.35);">VS</div></div>', unsafe_allow_html=True)
    _pc(c2,pb,ph_b,ini_b,cb,pts_b,win==pb)

    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
    st.markdown('<div style="font-size:10px;font-weight:700;letter-spacing:.14em;'
                'color:rgba(246,195,73,.65);text-transform:uppercase;margin-bottom:6px;">📊 Estadísticas</div>',
                unsafe_allow_html=True)
    def _sb(lbl,va,vb):
        t=max(va+vb,1);pa2=va/t*100;pb2=vb/t*100
        wa="font-weight:900;" if va>vb else"";wb="font-weight:900;" if vb>va else""
        return(f'<div class="h2h-stat"><div class="h2h-sl">{lbl}</div>'
               f'<div style="display:flex;align-items:center;gap:8px;">'
               f'<div style="min-width:32px;text-align:right;font-size:13px;{wa}color:{ca};">{va}</div>'
               f'<div style="flex:1;display:flex;height:9px;border-radius:4px;overflow:hidden;gap:1px;">'
               f'<div style="width:{pa2:.0f}%;background:{ca};border-radius:3px 0 0 3px;box-shadow:0 0 5px {ca}55;"></div>'
               f'<div style="width:{pb2:.0f}%;background:{cb};border-radius:0 3px 3px 0;box-shadow:0 0 5px {cb}55;"></div>'
               f'</div><div style="min-width:32px;font-size:13px;{wb}color:{cb};">{vb}</div>'
               f'</div></div>')
    st.markdown(
        _sb("🏆 Puntos Totales",pts_a,pts_b)+_sb("⏱️ Qualys",qua_a,qua_b)+
        _sb("⚡ Sprints",spr_a,spr_b)+_sb("🏁 Carreras",car_a,car_b)+
        _sb("🔢 GPs ganados (suma)",gps_a,gps_b), unsafe_allow_html=True)

    if _PLOTLY_OK:
        try:
            import plotly.graph_objects as _go
            cats=["Qualys","Sprints","Carreras","Total"]
            fig=_go.Figure()
            fig.add_trace(_go.Bar(name=pa,x=cats,y=[qua_a,spr_a,car_a,pts_a],marker_color=ca,
                marker_line_width=0,text=[qua_a,spr_a,car_a,pts_a],textposition="outside",
                textfont=dict(color=ca,size=12),cliponaxis=False))
            fig.add_trace(_go.Bar(name=pb,x=cats,y=[qua_b,spr_b,car_b,pts_b],marker_color=cb,
                marker_line_width=0,text=[qua_b,spr_b,car_b,pts_b],textposition="outside",
                textfont=dict(color=cb,size=12),cliponaxis=False))
            ymax=max(pts_a,pts_b,qua_a,qua_b,spr_a,spr_b,car_a,car_b,1)*1.35
            fig.update_layout(height=260,barmode="group",
                paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=6,r=6,t=42,b=6),
                title=dict(text="📊 Comparativa por categoría",font=dict(color="#ffdd7a",size=13),x=0),
                legend=dict(font=dict(color="#e8ecff",size=11),bgcolor="rgba(0,0,0,0)",orientation="h",y=1.14,x=0),
                xaxis=dict(tickfont=dict(color="#e8ecff",size=11),showgrid=False),
                yaxis=dict(tickfont=dict(color="#a9b2d6",size=10),gridcolor="rgba(255,255,255,.04)",
                           zeroline=False,range=[0,ymax]))
            st.plotly_chart(fig,use_container_width=True,config={"displayModeBar":False,"staticPlot":True})
        except Exception: pass


def pantalla_api_test():
    st.title("🧪 Test API F1")
    year=st.number_input("Año",2000,2100,2026,step=1)
    if st.button("Probar constructors"):
        try:
            data=requests.get(f"{API_BASE}/f1/constructors",params={"year":int(year)},timeout=8).json()
            st.success("API OK ✅"); st.json(data)
        except Exception as e: st.error(f"Falló: {e}")


# ─────────────────────────────────────────────────────────
# 11. MAIN
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
        "⚔️  Head to Head",
        "💬  Mesa Chica",
    ]
    if is_admin(): opciones.append("🧪  Test API F1")

    # ── Navegación persistente: inicializar si no existe ──
    if "_main_nav" not in st.session_state:
        st.session_state["_main_nav"] = opciones[0]

    # Si un botón rápido forzó navegación, actualizar el estado del radio
    _nav = st.session_state.pop("fw_force_nav", None)
    if _nav:
        for _o in opciones:
            if _nav in _o:
                st.session_state["_main_nav"] = _o
                break

    opcion = st.sidebar.radio("", opciones, key="_main_nav", label_visibility="collapsed")

    st.sidebar.markdown("---")
    st.sidebar.markdown(
        '<div style="text-align:center;font-size:11px;color:rgba(169,178,214,.40);">'
        '🏁 Torneo Fefe Wolf 2026<br>© Formuleros</div>',
        unsafe_allow_html=True
    )

    if   "Inicio"       in opcion: pantalla_inicio()
    elif "Calendario"   in opcion: pantalla_calendario()
    elif "Predicciones" in opcion: pantalla_cargar_predicciones()
    elif "Posiciones"   in opcion: pantalla_tabla_posiciones()
    elif "Historial"    in opcion: pantalla_historial_gp()
    elif "Calculadora"  in opcion: pantalla_calculadora_puntos()
    elif "Pilotos"      in opcion: pantalla_pilotos_y_escuderias()
    elif "Reglamento"   in opcion: pantalla_reglamento()
    elif "Campeones"    in opcion: pantalla_muro()
    elif "Head"         in opcion: pantalla_head_to_head()
    elif "Mesa"         in opcion: pantalla_mesa_chica()
    elif "Test"         in opcion: pantalla_api_test()

    mini_bar()
    flecha_arriba()


main()
