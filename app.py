import os
import streamlit as st
import pandas as pd
import pytz
import streamlit.components.v1 as components
import requests
import csv
import sqlite3
import altair as alt
import html as _html
from core.mesa_chica_db import (
    mc_is_mod, mc_badge_for, mc_is_spam,
    mc_add_message, mc_list_messages,
    mc_update_message, mc_soft_delete_message,
    mc_purge_html_messages, _mc_safe_text,
    mc_toggle_like, mc_like_count, mc_user_liked,
)
from datetime import datetime
from collections import defaultdict
from core.database import (
    leer_historial_df,
    leer_historial_detalle_df,
)
from core.admin_tools import calcular_y_actualizar_todos, generar_historial_solo
from core.database import lock_exists, set_lock
from core.database import aplicar_sanciones_dns
from core.database import aplicar_bonus_campeones_final

# ======================================================================
# Streamlit config (SIEMPRE PRIMERO, antes de cualquier st.*)
# ======================================================================
st.set_page_config(
    page_title="Torneo de Predicciones Fefe Wolf 2026",
    layout="wide",
    page_icon="🏆",
)

# ======================================================================
# CSS (UNA sola vez) + overrides necesarios para centrar + tablas dark
# ======================================================================
def load_css():
    css_path = os.path.join("ui", "styles.css")
    if not os.path.exists(css_path):
        st.warning("⚠️ Falta ui/styles.css (no es grave).")
        return
    with open(css_path, "r", encoding="utf-8") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

    # Overrides mínimos (NO cambian textos, solo layout / look)
    st.markdown(
        """
        <style>
          /* === Layout centrado tipo Lovable (PC + Mobile) === */
          .block-container{
            max-width: 1100px !important;
            padding-left: 24px !important;
            padding-right: 24px !important;
            margin: 0 auto !important;
          }
          @media (max-width: 768px){
            .block-container{
              padding-left: 14px !important;
              padding-right: 14px !important;
            }
          }

          /* === Títulos/Secciones centrados (sin tocar texto) === */
          .hero, .section-title{
            text-align: center !important;
          }
          .section-title{
            margin-left: auto !important;
            margin-right: auto !important;
          }

          /* === Tablas HTML (calendario/posiciones) dark + gold === */
          .fw-table-wrap{
            overflow-x: auto;
            border-radius: 14px;
          }
          .fw-table{
            width: 100%;
            border-collapse: collapse;
            min-width: 520px;
          }
          .fw-table th{
            text-align: left;
            padding: 10px 12px;
            background: rgba(246,195,73,.10);
            color: #ffdd7a;
            border-bottom: 1px solid rgba(246,195,73,.25);
            font-weight: 900;
            letter-spacing: .04em;
            font-size: 13px;
            white-space: nowrap;
          }
          .fw-table td{
            padding: 10px 12px;
            border-bottom: 1px solid rgba(246,195,73,.10);
            color: rgba(232,236,255,.95);
            font-size: 13px;
            white-space: nowrap;
          }
          .fw-table tr{
            background: rgba(12,14,24,.25);
          }
          .fw-table tr:hover{
            background: rgba(168,85,247,.08);
          }

          /* === Cards equipo (Pilotos y Escuderías) === */
          .team-card{
            background: linear-gradient(180deg, rgba(16,18,28,.82), rgba(10,12,18,.72));
            border: 1px solid rgba(246,195,73,.20);
            border-left: 3px solid var(--team);
            border-radius: 18px;
            box-shadow: 0 18px 50px rgba(0,0,0,.55);
            padding: 14px 14px;
            margin-bottom: 14px;
            position: relative;
            overflow: hidden;
          }
          .team-card::before{
            content:"";
            position:absolute; inset:-2px;
            background: radial-gradient(520px 240px at 40% 0%, rgba(168,85,247,.14), transparent 60%);
            pointer-events:none;
          }
          .team-head{
            display:flex;
            align-items:center;
            gap:10px;
            margin-bottom: 10px;
          }
          .team-dot{
            width:10px;
            height:10px;
            border-radius:999px;
            background: var(--team);
            box-shadow: 0 0 18px color-mix(in srgb, var(--team) 60%, transparent);
            flex: 0 0 auto;
          }
          .team-name{
            font-weight: 900;
            letter-spacing: .06em;
            color: #ffdd7a;
          }
          .team-drivers{
            display:grid;
            grid-template-columns: 1fr 1fr;
            gap: 10px;
          }
          @media (max-width: 560px){
            .team-drivers{ grid-template-columns: 1fr; }
          }
          .driver-pill{
            border: 1px solid rgba(246,195,73,.18);
            border-radius: 14px;
            padding: 10px 10px;
            background: rgba(12,14,24,.35);
            display:flex;
            align-items:center;
            gap:10px;
          }
          .driver-name{ font-weight: 800; color: rgba(232,236,255,.98); }
          .car-ic{ opacity: .95; }

          /* === Quita franja/padding raro (refuerzo) === */
          header[data-testid="stHeader"]{
            background: transparent !important;
          }
          section.main > div{
            padding-top: 1.2rem !important;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )

load_css()

# ======================================================================
# API
# ======================================================================
API_BASE = "http://127.0.0.1:8000"

def api_get(path: str, params=None):
    r = requests.get(API_BASE + path, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

# ======================================================================
# Imports internos
# ======================================================================
from core.admin_tools import calcular_y_actualizar_todos
from core.database import (
    guardar_etapa,
    recuperar_predicciones_piloto,
    leer_tabla_posiciones,
    actualizar_tabla_general,
    guardar_historial
    
)
from core.auth import (
    login,
    change_password,
    reset_password_with_mother,
    bootstrap_user,
    get_user_row,
    admin_update_user_fields,
    admin_reset_password,
    verify_pin,
    set_pin,
)
from core.scoring import calcular_puntos
from core.rules import obtener_estado_gp
from core.utils import normalizar_nombre

TZ = pytz.timezone("America/Argentina/Buenos_Aires")

def qp_get_one(key: str):
    qp = st.query_params
    v = qp.get(key, None)
    if isinstance(v, list):
        return v[0] if v else None
    return v

def qp_set(key: str, value: str):
    qp = dict(st.query_params)
    qp[key] = value
    st.query_params.update(qp)

def _perfil_from_usuario(usuario: str):
    found, row = get_user_row(usuario)
    if not found or not row:
        return None
    # row debería contener al menos: usuario, rol, copas, forzar_cambio
    # Ajustá si tu get_user_row devuelve otra estructura (dict/tuple).
    if isinstance(row, dict):
        return row
    # Si devuelve tupla, adaptalo a tu orden real:
    # EJEMPLO (cambialo según tu hoja Usuarios):
    # (usuario, rol, copas, color, forzar_cambio, ...)
    try:
        return {
            "usuario": row[0],
            "rol": row[1],
            "copas": row[2],
            "color": row[3] if len(row) > 3 else "white",
            "forzar_cambio": row[4] if len(row) > 4 else 0,
        }
    except Exception:
        return None

# ======================================================================
# UI helpers
# ======================================================================

def _scroll_top_js():
    # IMPORTANTE: components.html corre en un iframe. Hay que scrollear el parent.
    return """
      <script>
        (function(){
          try{
            const p = window.parent;
            // 1) intento scroll global
            if (p && p.scrollTo) p.scrollTo({top:0, left:0, behavior:'smooth'});
            // 2) intento contenedor Streamlit (main)
            const doc = p.document;
            const main = doc.querySelector('section.main');
            if (main) main.scrollTo({top:0, left:0, behavior:'smooth'});
            // 3) intento contenedor de app
            const app = doc.querySelector('div[data-testid="stAppViewContainer"]');
            if (app) app.scrollTo({top:0, left:0, behavior:'smooth'});
          }catch(e){}
        })();
      </script>
    """

def floating_scroll_top_button():
    components.html(
        f"""
        <style>
          .fw-top {{
            position: fixed;
            right: 18px;
            bottom: 18px;
            width: 54px;
            height: 54px;
            border-radius: 999px;
            border: 2px solid rgba(246,195,73,.9);
            background: radial-gradient(circle at 30% 30%, rgba(246,195,73,.95), rgba(168,85,247,.25));
            box-shadow: 0 0 0 6px rgba(168,85,247,.10), 0 14px 40px rgba(0,0,0,.55);
            display: flex;
            align-items: center;
            justify-content: center;
            cursor: pointer;
            z-index: 999999;
            transition: transform .15s ease, box-shadow .15s ease;
            user-select: none;
          }}
          .fw-top:hover{{
            transform: translateY(-2px);
            box-shadow: 0 0 0 8px rgba(168,85,247,.14), 0 18px 55px rgba(0,0,0,.65);
          }}
          .fw-top span{{
            font-size: 22px;
            font-weight: 900;
            color: #10131d;
            text-shadow: 0 1px 0 rgba(255,255,255,.35);
            line-height: 1;
          }}
        </style>

        <div class="fw-top" onclick="(function(){{ try{{ { _scroll_top_js() } }}catch(e){{}} }})();" title="Ir arriba">
          <span>↑</span>
        </div>
        """,
        height=0,
    )

def scroll_to_top():
    components.html(
        """
        <script>
          // 1) scroll en la página actual
          window.scrollTo({top: 0, behavior: 'smooth'});
          document.documentElement.scrollTo({top:0, behavior:'smooth'});
          document.body.scrollTo({top:0, behavior:'smooth'});

          // 2) scroll dentro del parent (Streamlit usa iframes)
          try{
            const doc = window.parent.document;
            const main = doc.querySelector('section.main');
            if (main) main.scrollTo({top:0, behavior:'smooth'});

            const view = doc.querySelector('[data-testid="stAppViewContainer"]');
            if (view) view.scrollTo({top:0, behavior:'smooth'});
          }catch(e){}
        </script>
        """,
        height=0,
    )

def render_dark_table(df: pd.DataFrame):
    # render HTML controlado para evitar tabla blanca de st.dataframe
    html = df.to_html(index=False, escape=False)
    # forzar class para aplicar estilos
    html = html.replace('<table border="1" class="dataframe">', '<table class="fw-table">')
    st.markdown(
        f"""
        <div class="card fade-up" style="padding:14px;">
          <div class="fw-table-wrap">
            {html}
          </div>
        </div>
        """,
        unsafe_allow_html=True
    )

def is_logged_in():
    return "perfil" in st.session_state and st.session_state["perfil"] is not None

def is_admin_or_comisario():
    perfil = st.session_state.get("perfil") or {}
    rol = str(perfil.get("rol", "")).lower()
    return ("admin" in rol) or ("comisario" in rol)

def logout():
    # 1) intenta borrar token de URL si existe
    try:
        t = qp_get_one("t")
        if t:
            try:
                auth_delete_token(t)
            except Exception:
                pass
    except Exception:
        pass

    # 2) limpia query params (compat + fallback)
    try:
        st.query_params.clear()
    except Exception:
        try:
            st.query_params.update({})
        except Exception:
            pass

    # 3) limpia sesión
    st.session_state["perfil"] = None
    st.session_state["usuario"] = None
    


# ===============================
# AUTH TOKENS (STREAMLIT CLOUD SAFE) - token firmado, sin DB
# ===============================
import base64, hmac, hashlib, secrets
from datetime import timedelta

from streamlit.errors import StreamlitSecretNotFoundError

def _auth_secret() -> str:
    try:
        secret = st.secrets.get("AUTH_SECRET", None)
    except StreamlitSecretNotFoundError:
        secret = None

    return secret or os.getenv("AUTH_SECRET") or "DEV_SECRET_CAMBIAR_EN_PRODUCCION_123456789"

def _b64url(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode("utf-8").rstrip("=")

def _b64url_decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("utf-8"))

def auth_create_token(usuario: str, hours_valid: int = 72) -> str:
    # payload: usuario|exp|nonce
    exp = int((datetime.utcnow() + timedelta(hours=hours_valid)).timestamp())
    nonce = secrets.token_urlsafe(8)
    payload = f"{usuario}|{exp}|{nonce}".encode("utf-8")
    sig = hmac.new(_auth_secret().encode("utf-8"), payload, hashlib.sha256).digest()
    return f"{_b64url(payload)}.{_b64url(sig)}"

def auth_user_from_token(token: str):
    try:
        if not token or "." not in token:
            return None
        p64, s64 = token.split(".", 1)
        payload = _b64url_decode(p64)
        sig = _b64url_decode(s64)

        good = hmac.new(_auth_secret().encode("utf-8"), payload, hashlib.sha256).digest()
        if not hmac.compare_digest(sig, good):
            return None

        usuario, exp_str, _nonce = payload.decode("utf-8").split("|", 2)
        if int(exp_str) < int(datetime.utcnow().timestamp()):
            return None
        return usuario
    except Exception:
        return None

def auth_delete_token(token: str):
    # Stateless: no hay nada para borrar del server.
    return

# Perfiles / roles Mesa Chica
# MODERADORES (FIPF): Bottas, Norris, Alonso (pueden EDITAR y BORRAR cualquier mensaje)
# FORMULEROS: Checo y Lauda (solo pueden EDITAR sus propios mensajes)
MESA_CHICA_PROFILE = {
    "Valteri Bottas":   {"grupo":"FIPF",      "tag":"MIEMBRO DE LA FIPF", "stars":"★",   "mod": True},
    "Lando Norris":     {"grupo":"FIPF",      "tag":"MIEMBRO DE LA FIPF", "stars":"",    "mod": True},
    "Fernando Alonso":  {"grupo":"FIPF",      "tag":"MIEMBRO DE LA FIPF", "stars":"",    "mod": True},
    "Checo Perez":      {"grupo":"FORMULERO", "tag":"FORMULERO",          "stars":"★★★", "mod": False},
    "Nicki Lauda":      {"grupo":"FORMULERO", "tag":"FORMULERO",          "stars":"★",   "mod": False},
}

MESA_CHICA_BADGES = {
    "Valteri Bottas":  {"tipo": "fipf",      "label": "MIEMBRO FIPF", "stars": "★"},
    "Lando Norris":    {"tipo": "fipf",      "label": "MIEMBRO FIPF", "stars": ""},
    "Fernando Alonso": {"tipo": "fipf",      "label": "MIEMBRO FIPF", "stars": ""},
    "Checo Perez":     {"tipo": "formulero", "label": "FORMULERO",    "stars": "★★★"},
    "Nicki Lauda":     {"tipo": "formulero", "label": "FORMULERO",    "stars": "★"},
}

def mc_badge_for(usuario: str):
    b = MESA_CHICA_BADGES.get(usuario, {"tipo":"formulero", "label":"FORMULERO", "stars":""})
    return b["tipo"], b["label"], b["stars"]

def mc_is_mod(usuario: str) -> bool:
    p = MESA_CHICA_PROFILE.get(usuario, {})
    return bool(p.get("mod", False))

# ======================================================================
# Datos del torneo
# ======================================================================
HORARIOS_CARRERA = {
    "01. Gran Premio de Australia": "2026-03-08 01:00",
    "02. Gran Premio de China": "2026-03-15 04:00",
    "03. Gran Premio de Japón": "2026-03-29 02:00",
    "04. Gran Premio de Baréin": "2026-04-12 12:00",
    "05. Gran Premio de Arabia Saudita": "2026-04-19 14:00",
    "06. Gran Premio de Miami": "2026-05-03 17:00",
    "07. Gran Premio de Canadá": "2026-05-24 17:00",
    "08. Gran Premio de Mónaco": "2026-06-07 10:00",
    "09. Gran Premio de Barcelona": "2026-06-14 10:00",
    "10. Gran Premio de Austria": "2026-06-28 10:00",
    "11. Gran Premio de Gran Bretaña": "2026-07-05 11:00",
    "12. Gran Premio de Bélgica": "2026-07-19 10:00",
    "13. Gran Premio de Hungría": "2026-07-26 10:00",
    "14. Gran Premio de los Países Bajos": "2026-08-23 10:00",
    "15. Gran Premio de Italia": "2026-09-06 10:00",
    "16. Gran Premio de Madrid": "2026-09-13 10:00",
    "17. Gran Premio de Azerbaiyán": "2026-09-27 08:00",
    "18. Gran Premio de Singapur": "2026-10-11 09:00",
    "19. Gran Premio de los Estados Unidos": "2026-10-25 17:00",
    "20. Gran Premio de México": "2026-11-01 17:00",
    "21. Gran Premio de Brasil": "2026-11-08 14:00",
    "22. Gran Premio de Las Vegas": "2026-11-21 01:00",
    "23. Gran Premio de Qatar": "2026-11-29 13:00",
    "24. Gran Premio de Abu Dabi": "2026-12-06 10:00",
}
GPS_OFICIALES = list(HORARIOS_CARRERA.keys())
GPS_2026 = GPS_OFICIALES

GPS_SPRINT = [
    "02. Gran Premio de China",
    "06. Gran Premio de Miami",
    "07. Gran Premio de Canadá",
    "11. Gran Premio de Gran Bretaña",
    "14. Gran Premio de los Países Bajos",
    "18. Gran Premio de Singapur",
]

PILOTOS_TORNEO = ["Checo Perez", "Nicki Lauda", "Valteri Bottas", "Lando Norris", "Fernando Alonso"]

GRILLA_2026 = {
    "MCLAREN": ["Lando Norris", "Oscar Piastri"],
    "RED BULL": ["Max Verstappen", "Isack Hadjar"],
    "MERCEDES": ["Kimi Antonelli", "George Russell"],
    "FERRARI": ["Charles Leclerc", "Lewis Hamilton"],
    "WILLIAMS": ["Alex Albon", "Carlos Sainz"],
    "ASTON MARTIN": ["Lance Stroll", "Fernando Alonso"],
    "RACING BULLS": ["Liam Lawson", "Arvid Lindblad"],
    "HAAS": ["Oliver Bearman", "Esteban Ocon"],
    "AUDI": ["Nico Hulkenberg", "Gabriel Bortoleto"],
    "ALPINE": ["Pierre Gasly", "Franco Colapinto"],
    "CADILLAC": ["Checo Perez", "Valteri Bottas"],
}

ESCALA_CARRERA_JUEGO = {1: 25, 2: 18, 3: 15, 4: 12, 5: 10, 6: 8, 7: 6, 8: 4, 9: 2, 10: 1}

CALENDARIO_VISUAL = [
    {"Fecha": "06-08 Mar", "Gran Premio": "GP Australia", "Circuito": "Melbourne", "Formato": "Clásico"},
    {"Fecha": "13-15 Mar", "Gran Premio": "GP China", "Circuito": "Shanghai", "Formato": "⚡ SPRINT"},
    {"Fecha": "27-29 Mar", "Gran Premio": "GP Japón", "Circuito": "Suzuka", "Formato": "Clásico"},
    {"Fecha": "10-12 Abr", "Gran Premio": "GP Bahréin", "Circuito": "Sakhir", "Formato": "Clásico"},
    {"Fecha": "17-19 Abr", "Gran Premio": "GP Arabia Saudita", "Circuito": "Jeddah", "Formato": "Clásico"},
    {"Fecha": "01-03 May", "Gran Premio": "GP Miami", "Circuito": "Miami", "Formato": "⚡ SPRINT"},
    {"Fecha": "22-24 May", "Gran Premio": "GP Canadá", "Circuito": "Montreal", "Formato": "⚡ SPRINT"},
    {"Fecha": "05-07 Jun", "Gran Premio": "GP Mónaco", "Circuito": "Montecarlo", "Formato": "Clásico"},
    {"Fecha": "12-14 Jun", "Gran Premio": "GP España", "Circuito": "Barcelona", "Formato": "Clásico"},
    {"Fecha": "26-28 Jun", "Gran Premio": "GP Austria", "Circuito": "Spielberg", "Formato": "Clásico"},
    {"Fecha": "03-05 Jul", "Gran Premio": "GP Reino Unido", "Circuito": "Silverstone", "Formato": "⚡ SPRINT"},
    {"Fecha": "17-19 Jul", "Gran Premio": "GP Bélgica", "Circuito": "Spa", "Formato": "Clásico"},
    {"Fecha": "24-26 Jul", "Gran Premio": "GP Hungría", "Circuito": "Budapest", "Formato": "Clásico"},
    {"Fecha": "21-23 Ago", "Gran Premio": "GP Países Bajos", "Circuito": "Zandvoort", "Formato": "⚡ SPRINT"},
    {"Fecha": "04-06 Sep", "Gran Premio": "GP Italia", "Circuito": "Monza", "Formato": "Clásico"},
    {"Fecha": "11-13 Sep", "Gran Premio": "GP Madrid", "Circuito": "Madrid", "Formato": "Clásico"},
    {"Fecha": "25-27 Sep", "Gran Premio": "GP Azerbaiyán", "Circuito": "Bakú", "Formato": "Clásico"},
    {"Fecha": "09-11 Oct", "Gran Premio": "GP Singapur", "Circuito": "Marina Bay", "Formato": "⚡ SPRINT"},
    {"Fecha": "23-25 Oct", "Gran Premio": "GP Estados Unidos", "Circuito": "Austin", "Formato": "Clásico"},
    {"Fecha": "30-01 Nov", "Gran Premio": "GP México", "Circuito": "Hermanos Rodríguez", "Formato": "Clásico"},
    {"Fecha": "06-08 Nov", "Gran Premio": "GP Brasil", "Circuito": "Interlagos", "Formato": "Clásico"},
    {"Fecha": "19-21 Nov", "Gran Premio": "GP Las Vegas", "Circuito": "Las Vegas", "Formato": "Clásico"},
    {"Fecha": "27-29 Nov", "Gran Premio": "GP Qatar", "Circuito": "Lusail", "Formato": "Clásico"},
    {"Fecha": "04-06 Dic", "Gran Premio": "GP Abu Dabi", "Circuito": "Yas Marina", "Formato": "Clásico"},
]

# ======================================================================
# Helpers
# ======================================================================
def normalizar_keys_numericas(d):
    if not isinstance(d, dict):
        return {}
    out = {}
    for k, v in d.items():
        if isinstance(k, str) and k.isdigit():
            out[int(k)] = v
        else:
            out[k] = v
    return out

def _driver_to_team_map(grilla: dict) -> dict:
    m = {}
    for team, drivers in grilla.items():
        for d in drivers:
            m[normalizar_nombre(d)] = team
    return m

def calcular_constructores_auto(of_r: dict, grilla: dict, escala_carrera: dict, top_n: int = 3):
    d2t = _driver_to_team_map(grilla)
    team_pts = defaultdict(int)

    for pos, puntos in escala_carrera.items():
        piloto = normalizar_nombre(of_r.get(pos, ""))
        if not piloto:
            continue
        team = d2t.get(piloto)
        if team:
            team_pts[team] += int(puntos)

    ranking = sorted(team_pts.items(), key=lambda x: (-x[1], x[0]))
    top = [t for (t, _) in ranking[:top_n]]
    return top, dict(team_pts)

# ======================================================================
# Auth / UI blocks
# ======================================================================
def sidebar_login_block():
    st.sidebar.markdown("## 🔐 Login")

    if "perfil" not in st.session_state:
        st.session_state["perfil"] = None
    if "usuario" not in st.session_state:
        st.session_state["usuario"] = None

    # =========================
    # AUTO-LOGIN POR TOKEN URL
    # =========================
    token = qp_get_one("t")
    if (not is_logged_in()) and token:
        u = auth_user_from_token(token)
        if u:
            perfil_auto = _perfil_from_usuario(u)
            if perfil_auto:
                st.session_state["perfil"] = perfil_auto
                st.session_state["usuario"] = perfil_auto["usuario"]
                st.rerun()

    # =========================
    # LOGIN NORMAL
    # =========================
    if not is_logged_in():
        u = st.sidebar.text_input("Usuario", key="login_user")
        p = st.sidebar.text_input("Contraseña", type="password", key="login_pass")

        if st.sidebar.button("Ingresar"):
            ok, res = login(u, p)
            if ok:
                st.session_state["perfil"] = res
                st.session_state["usuario"] = res["usuario"]

                # token ANTES del rerun (para que no se "desloguee" al refrescar)
                t = auth_create_token(res["usuario"])
                qp_set("t", t)

                st.sidebar.success(f"OK: {res['usuario']}")
                st.rerun()
            else:
                st.sidebar.error(res)

        # ✅ VOLVIÓ "OLVIDÉ MI CONTRASEÑA"
        with st.sidebar.expander("¿Olvidaste tu contraseña?", expanded=False):
            u2 = st.text_input("Usuario a recuperar", key="rp_user")
            mother = st.text_input("Mother code", type="password", key="rp_mother")
            newp1 = st.text_input("Nueva contraseña", type="password", key="rp_new1")
            newp2 = st.text_input("Repetir nueva contraseña", type="password", key="rp_new2")

            if st.button("Resetear contraseña", key="rp_btn"):
                if newp1 != newp2:
                    st.error("No coinciden.")
                elif not newp1 or len(newp1.strip()) < 4:
                    st.error("Muy corta (mín 4).")
                else:
                    ok, msg = reset_password_with_mother(u2, mother, newp1)
                    if ok:
                        st.success("Listo ✅ Ahora ingresá con tu nueva contraseña.")
                    else:
                        st.error(msg)

        st.stop()

    # Ya logueado
    perfil = st.session_state["perfil"] or {}
    st.sidebar.success(f"✅ {perfil.get('usuario','')}")

    copas = int(perfil.get("copas", 0) or 0)
    st.sidebar.caption(f"{perfil.get('rol','')} | Títulos: {copas} {'🏆'*copas}")

    if st.sidebar.button("Cerrar sesión"):
        logout()
        st.rerun()

def bootstrap_admin_ui():
    if st.session_state.get("perfil"):
        return

    with st.sidebar.expander("🛠️ Inicializar Admin (1 vez)", expanded=False):
        st.caption("Usar solo si la hoja Usuarios está vacía.")
        usuario_admin = "Checo Perez"
        rol_admin = "Comisario | Administrador"

        pw_inicial = st.text_input("Password inicial (Checo)", type="password", key="boot_pw")
        mother_code = st.text_input("Mother code", type="password", key="boot_mother")

        if st.button("✅ Crear Admin Checo", key="boot_btn"):
            if not pw_inicial or not mother_code:
                st.error("Te falta completar Password inicial y Mother code.")
                return

            found, _ = get_user_row(usuario_admin)
            if found:
                st.warning("Checo Perez ya existe en Usuarios.")
                return

            ok, msg = bootstrap_user(
                usuario=usuario_admin,
                rol=rol_admin,
                password_inicial=pw_inicial,
                mother_code=mother_code,
                copas=3,
                color="gold",
            )
            if ok:
                st.success(msg)
                st.info("Ahora logueate con Checo Perez.")
                st.rerun()
            else:
                st.error(msg)

def admin_crear_usuario_ui():
    perfil = st.session_state.get("perfil") or {}
    rol = str(perfil.get("rol", "")).lower()
    if not ("admin" in rol or "comisario" in rol):
        return

    with st.sidebar.expander("👤 Crear usuario (Admin)", expanded=False):
        u = st.text_input("Usuario (exacto)", key="new_u")
        r = st.selectbox("Rol", ["Piloto", "Comisario | Administrador"], key="new_r")
        p = st.text_input("Password inicial", type="password", key="new_p")
        m = st.text_input("Mother code", type="password", key="new_m")
        c = st.number_input("Títulos", 0, 99, 0, key="new_c")
        col = st.text_input("Color", value="white", key="new_col")

        if st.button("Crear usuario", key="btn_new_user"):
            ok, msg = bootstrap_user(u, r, p, m, copas=int(c), color=col)
            if ok:
                st.success(msg)
                st.rerun()
            else:
                st.error(msg)

def admin_set_pin_ui():
    perfil = st.session_state.get("perfil") or {}
    rol = str(perfil.get("rol", "")).lower()
    if not ("admin" in rol or "comisario" in rol):
        return

    with st.sidebar.expander("🔐 Setear PIN (Admin)", expanded=False):
        u = st.selectbox("Usuario", PILOTOS_TORNEO, key="pin_user")
        pin = st.text_input("Nuevo PIN (4 dígitos)", type="password", max_chars=4, key="pin_new")

        if st.button("Guardar PIN", key="btn_set_pin"):
            ok, msg = set_pin(u, pin)
            if ok:
                st.success(msg)
            else:
                st.error(msg)

# ======================================================================
# PANTALLAS
# ======================================================================

def leer_historial_df():
    con = sqlite3.connect(HIST_DB_PATH, check_same_thread=False)
    try:
        df = pd.read_sql_query(
            "SELECT gp, piloto, puntos, ts FROM tabla_historial ORDER BY ts ASC",
            con
        )
        return df
    finally:
        con.close()



def _mc_safe_text(s: str) -> str:
    return _html.escape(s or "").replace("\n", "<br>")

def pantalla_mesa_chica():
    perfil = st.session_state.get("perfil") or {}
    usuario = perfil.get("usuario", "")

    if not usuario:
        st.warning("Tenés que iniciar sesión para entrar a la Mesa Chica.")
        st.stop()

    is_mod = mc_is_mod(usuario)

    if "mc_editing_id" not in st.session_state:
        st.session_state["mc_editing_id"] = None

    st.markdown("""
    <div class="mc-wrap">
      <div class="mc-hero">
        <div class="mc-title">BIENVENIDOS FORMULEROS A LA MESA CHICA</div>
        <div class="mc-sub">
          Un lugar para comentar y hablar de la Fórmula 1. <b>Únicamente de F1</b> 🏁<br>
          Respeten la mesa. El show lo hacen los motores.
        </div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # ===== Composer (UNO SOLO) =====
    st.markdown('<div class="mc-wrap"><div class="mc-compose">', unsafe_allow_html=True)

    with st.form("mc_form", clear_on_submit=True):
        msg = st.text_area(
            "Escribí tu mensaje",
            key="mc_msg",
            height=90,
            placeholder="Hablá de F1: carreras, pilotos, equipos, predicciones…"
        )
        enviar = st.form_submit_button("Enviar a la Mesa Chica", use_container_width=True)

    st.markdown('</div></div>', unsafe_allow_html=True)

    colA, colB, colC = st.columns([1, 1, 1])
    with colA:
        if st.button("Actualizar", use_container_width=True):
            st.rerun()
    with colB:
        st.caption("Moderadores (FIPF) pueden editar/borrar si infringe normas.")
    with colC:
        if is_mod:
            if st.button("🧹 Limpiar mensajes rotos", use_container_width=True):
                mc_purge_html_messages()
                st.success("Listo ✅ (se ocultaron mensajes rotos)")
                st.rerun()

    if enviar:
        txt = (msg or "").strip()
        if not txt:
            st.warning("Escribí algo antes de enviar.")
        elif mc_is_spam(usuario):
            st.error("⚠️ Estás enviando mensajes muy rápido. Esperá unos segundos.")
        else:
            mc_add_message(usuario, txt)
            st.success("Mensaje enviado ✅")
            st.rerun()

    # ===== Feed =====
    rows = mc_list_messages(limit=250)

    st.markdown('<div class="mc-wrap"><div class="mc-feed">', unsafe_allow_html=True)

    for row in rows:
        msg_id, u, texto, ts, edited_ts = row[:5]
        tipo, label, stars = mc_badge_for(u)
        group_class = "mc-fipf" if mc_is_mod(u) else "mc-formulero"

        badge_html = f"""
        <span class="mc-badge {tipo}">
          {label} {f'<span class="mc-stars">{stars}</span>' if stars else ''}
        </span>
        """

        can_edit = is_mod or (u == usuario)
        can_delete = is_mod
        editando = (st.session_state["mc_editing_id"] == msg_id)

        edit_tag = f" · Editado: {edited_ts.replace('T',' ')}" if edited_ts else ""
        safe_time = _mc_safe_text(ts.replace('T',' ') + edit_tag)

        liked = mc_user_liked(msg_id, usuario)
        likes_count = mc_like_count(msg_id)
        like_label = f"{'❤️' if liked else '🤍'} {likes_count}"

        st.markdown(
            f"""
            <div class="mc-card {group_class} fade-up">
              <div class="mc-head">
                <div class="mc-userwrap">
                  <div class="mc-name">{_mc_safe_text(u)}</div>
                  <div class="mc-badges">{badge_html}</div>
                </div>
                <div class="mc-time">{safe_time}</div>
              </div>
            """,
            unsafe_allow_html=True
        )

        if editando:
            st.markdown("</div>", unsafe_allow_html=True)

            nuevo = st.text_area("Editar mensaje", value=texto, key=f"mc_edit_txt_{msg_id}", height=90)
            c1, c2, c3, c4 = st.columns([1, 1, 1, 1])

            with c1:
                if st.button("Guardar edición", key=f"mc_save_{msg_id}", use_container_width=True):
                    nt = (nuevo or "").strip()
                    if not nt:
                        st.warning("No podés guardar vacío.")
                    else:
                        mc_update_message(msg_id, nt)
                        st.session_state["mc_editing_id"] = None
                        st.rerun()

            with c2:
                if st.button("Cancelar", key=f"mc_cancel_{msg_id}", use_container_width=True):
                    st.session_state["mc_editing_id"] = None
                    st.rerun()

            with c3:
                if can_delete:
                    if st.button("Eliminar (mod)", key=f"mc_del2_{msg_id}", use_container_width=True):
                        mc_soft_delete_message(msg_id, deleted_by=usuario)
                        st.session_state["mc_editing_id"] = None
                        st.rerun()

            with c4:
                if st.button(like_label, key=f"mc_like_edit_{msg_id}", use_container_width=True):
                    mc_toggle_like(msg_id, usuario)
                    st.rerun()

        else:
            st.markdown(
                f"""
                <div class="mc-text">{_mc_safe_text(texto)}</div>
                </div>
                """,
                unsafe_allow_html=True
            )

            a1, a2, a3, a4 = st.columns([1, 1, 1, 3])

            with a1:
                if st.button(like_label, key=f"mc_like_{msg_id}", use_container_width=True):
                    mc_toggle_like(msg_id, usuario)
                    st.rerun()

            with a2:
                if can_edit:
                    if st.button("Editar", key=f"mc_edit_{msg_id}", use_container_width=True):
                        st.session_state["mc_editing_id"] = msg_id
                        st.rerun()

            with a3:
                if can_delete:
                    if st.button("Eliminar (mod)", key=f"mc_del_{msg_id}", use_container_width=True):
                        mc_soft_delete_message(msg_id, deleted_by=usuario)
                        st.rerun()

        st.divider()

    st.markdown('</div></div>', unsafe_allow_html=True)
        
def pantalla_api_test():
    st.title("🧪 Test API F1")

    col1, col2 = st.columns(2)
    with col1:
        year = st.number_input("Año", 2000, 2100, 2026, step=1)

    if st.button("Probar constructors", use_container_width=True):
        try:
            data = api_get("/f1/constructors", {"year": int(year)})
            st.success("API OK ✅")
            st.json(data)
        except Exception as e:
            st.error(f"Falló la API: {e}")
            st.info("Tip: asegurate de tener Uvicorn corriendo (api en :8000).")

def pantalla_inicio():
    st.markdown("""
    <div class="hero">
      <div class="hero-title">🏆 TORNEO DE PREDICCIONES</div>
      <div class="hero-subtitle">FEFE WOLF 2026</div>
      <div class="hero-foot">© 2026 Derechos Reservados — Fundado por <b>Checo Perez</b></div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div class="section-title">📜 EL LEGADO DE FEFE WOLF</div>
    <div class="card gold-left">
      <div class="card-title">EN EL PRINCIPIO, HUBO RUIDO DE MOTORES...</div>
      <div class="card-text">
        Corría el año 2021. El mundo estaba cambiando, y la Fórmula 1 vivía una de sus batallas más feroces.
        En ese caos, cinco amigos decidieron que ser espectadores no era suficiente. Necesitaban ser protagonistas.<br><br>
        Bajo la visión fundacional de <b>Checo Perez</b>, se creó este santuario: un lugar donde la amistad se mide en puntos
        y el honor se juega en cada curva.<br><br>
        Pero este torneo no sería posible sin nuestra guía eterna: <b>Fefe Wolf</b>. Aunque no esté físicamente en el paddock,
        su espíritu competitivo impregna cada decisión. Él es el líder espiritual que nos recuerda que nunca hay que levantar
        el pie del acelerador.<br><br>
        <b>LOS CINCO ELEGIDOS:</b> Checo, Lauda, Bottas, Lando y Alonso.<br>
        No corremos por dinero. Corremos por el derecho sagrado de decir “te lo dije” el domingo por la tarde.<br><br>
        Hemos visto campeones ascender y caer. Vimos a Lauda y a Fefe Wolf compartir la gloria del 21. Vimos el dominio implacable de Checo,
        actual Tri Campeón. Vimos la sorpresa táctica y caída de Bottas.
        Ahora, en 2026, Audi ruge, Cadillac desafía al sistema y Colapinto lleva la bandera argentina.
        <i>¿Quién tendrá la audacia para reclamar el trono este año?</i>
        </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<div class='section-title'>👑 EN MEMORIA DEL REY FEFE WOLF</div>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        try:
            st.image("IMAGENFEFE.jfif", use_container_width=True)
        except Exception:
            st.info("Subí una imagen local llamada 'IMAGENFEFE.jfif' para mostrarla acá.")

    st.markdown("<div class='section-title'>🏛️ LA CÚPULA OFICIAL DEL TORNEO 2026</div>", unsafe_allow_html=True)
    d1, d2, d3 = st.columns([1, 2, 1])
    with d2:
        try:
            st.image("IMAGENCUPULA.jfif", use_container_width=True)
        except Exception:
            st.info("Subí una imagen local llamada 'IMAGENCUPULA.jfif' para mostrarla acá.")

    st.markdown("<div class='section-title'>🏎️ PILOTOS EN PARRILLA</div>", unsafe_allow_html=True)

    cols = st.columns(5)
    for i, p in enumerate(PILOTOS_TORNEO):
        with cols[i % 5]:
            st.markdown(f"""
            <div class="pilot-chip">
              <div class="pilot-name">{p}</div>
              <div class="pilot-role">Piloto</div>
            </div>
            """, unsafe_allow_html=True)

def pantalla_calendario():
    st.title("📅 CALENDARIO TEMPORADA 2026")
    x1, x2, x3 = st.columns([1, 2, 1])
    with x2:
        try:
            st.image("IMAGENCALENDARIO.jfif", caption="Mapa de la Temporada")
        except Exception:
            st.info("ℹ️ Si querés una imagen acá, subí un archivo llamado 'IMAGENCALENDARIO.jfif'")
    st.divider()
    render_dark_table(pd.DataFrame(CALENDARIO_VISUAL))

def pantalla_pilotos_y_escuderias():
    st.markdown('<div class="section-title">🏎️ PARRILLA OFICIAL F1 2026</div>', unsafe_allow_html=True)

    TEAM_COLORS = {
        "MCLAREN": "#FF8000",
        "RED BULL": "#3671C6",
        "MERCEDES": "#00D2BE",
        "FERRARI": "#DC0000",
        "WILLIAMS": "#005AFF",
        "ASTON MARTIN": "#006F62",
        "RACING BULLS": "#2B4562",
        "HAAS": "#B6BABD",
        "AUDI": "#00E676",
        "ALPINE": "#FF4FD8",
        "CADILLAC": "#E6C200",
    }

    teams = list(GRILLA_2026.items())
    colL, colR = st.columns(2, gap="large")

    for idx, (equipo, pilotos) in enumerate(teams):
        color = TEAM_COLORS.get(equipo, "#A855F7")
        card_html = f"""
        <div class="team-card fade-up" style="--team:{color}">
          <div class="team-head">
            <span class="team-dot"></span>
            <div class="team-name">{equipo}</div>
          </div>

          <div class="team-drivers">
            <div class="driver-pill">
              <span class="car-ic">🏎️</span>
              <span class="driver-name">{pilotos[0]}</span>
            </div>
            <div class="driver-pill">
              <span class="car-ic">🏎️</span>
              <span class="driver-name">{pilotos[1]}</span>
            </div>
          </div>
        </div>
        """

        if idx % 2 == 0:
            with colL:
                st.markdown(card_html, unsafe_allow_html=True)
        else:
            with colR:
                st.markdown(card_html, unsafe_allow_html=True)

# ===== Colores PRO por piloto (los que me diste) =====
PILOTO_COLORS = {
    "Checo Perez": "#D4AF37",      # dorado
    "Valteri Bottas": "#00CFFF",   # cyan
    "Nicki Lauda": "#1E90FF",      # azul
    "Lando Norris": "#FFA500",     # naranja
    "Fernando Alonso": "#FF0000",  # rojo
}

# Ruta default del historial (ajustá si tu DB está en otro lado)
HIST_DB_PATH = "tabla_historial.db"

def leer_historial_df(db_path: str = HIST_DB_PATH):
    """
    Lee historial desde SQLite.
    Espera una tabla con columnas tipo: gp, piloto, puntos, ts
    (y opcional: etapa)
    """
    try:
        con = sqlite3.connect(db_path, check_same_thread=False)
        # Intento 1: tabla común "historial"
        try:
            df = pd.read_sql_query("SELECT * FROM historial", con)
        except Exception:
            # Intento 2: si la tabla se llama distinto, probamos "puntos_gp"
            df = pd.read_sql_query("SELECT * FROM puntos_gp", con)

        con.close()

        if df is None or df.empty:
            return pd.DataFrame()

        # normalizar nombres de columnas (por si vienen con mayúsculas)
        df.columns = [c.strip().lower() for c in df.columns]

        # asegurar columnas mínimas
        for col in ["gp", "piloto", "puntos"]:
            if col not in df.columns:
                # si faltan, devolvemos vacío para no romper la app
                return pd.DataFrame()

        # completar ts si no existe
        if "ts" not in df.columns:
            df["ts"] = ""

        # convertir puntos a int seguro
        df["puntos"] = pd.to_numeric(df["puntos"], errors="coerce").fillna(0).astype(int)

        # limpiar strings
        df["gp"] = df["gp"].astype(str).str.strip()
        df["piloto"] = df["piloto"].astype(str).str.strip()

        return df

    except Exception:
        # ante cualquier problema, NO rompemos la app
        return pd.DataFrame()

def pantalla_reglamento():
    # NO cambio tu texto. Solo lo dejo con mejor look (cards)
    st.title("📜 REGLAMENTO OFICIAL 2026")
    st.markdown(
        """
<div class="card fade-up" style="border-color: rgba(217,70,239,.35);">
  <div class="card-text">

### ⚠️ REGLA DE ESCRITURA
**LOS NOMBRES DE PILOTOS Y EQUIPOS DEBEN ESCRIBIRSE EXACTAMENTE IGUAL A LA SECCIÓN 'PILOTOS Y ESCUDERÍAS'.**  
Si un nombre está mal escrito, el sistema no detectará el acierto y **NO SUMARÁ PUNTOS**.

  </div>
</div>

<div style="height:14px;"></div>

<div class="card fade-up">
  <div class="card-text">

### ⚔️ SISTEMA DE PUNTUACIÓN OFICIAL ⚔️

**🏁 CARRERA PRINCIPAL:**
- 1° (25), 2° (18), 3° (15), 4° (12), 5° (10), 6° (8), 7° (6), 8° (4), 9° (2), 10° (1).
- **Pleno:** +5 Puntos.

**⏱️ CLASIFICACIÓN:**
- 1° (15), 2° (10), 3° (7), 4° (5), 5° (3).
- **Pleno:** +5 Puntos.

**⚡ SPRINT:**
- 1° (8), 2° (7), 3° (6), 4° (5), 5° (4), 6° (3), 7° (2), 8° (1).
- **Pleno:** +3 Puntos.

**🛠️ CONSTRUCTORES:**
- 1° (10), 2° (5), 3° (2).
- **Pleno:** +3 Puntos.

**🧉 REGLA COLAPINTO:**
- Acierto Exacto en Qualy: **+10 Puntos**
- Acierto Exacto en Carrera: **+20 Puntos**

**🏎️PILOTO Y CONSTRUCTORES CAMPEÓN:**
- Acierto a Piloto campeón: **+50 puntos**
- Acierto a Constructor campeón: **+25 puntos**

  </div>
</div>

<div style="height:14px;"></div>

<div class="card fade-up" style="border-color: rgba(255,64,64,.35);">
  <div class="card-text">

### ⛔ SANCIONES POR NO ENVÍO (D.N.S.)
- Falta en Clasificación: -5 Puntos.
- Falta en Sprint (si hay): -5 Puntos.
- Falta en Carrera + Constructores: -5 Puntos.

**Desempate:** En caso de igualdad de puntos, gana quien envió primero la predicción.

  </div>
</div>
        """,
        unsafe_allow_html=True
    )

def pantalla_muro():
    # Título principal como en el screen
    st.markdown("""
    <div class="hof-wrap">
      <div class="hof-title">🏆 MURO DE CAMPEONES</div>
      <div class="hof-subtitle">👑 HALL OF FAME</div>
    </div>
    """, unsafe_allow_html=True)

    # Cards exactamente estilo Lovable (azul + dorado)
    st.markdown("""
    <div class="hof-wrap">

      <div class="hof-card fade-up">
        <div class="hof-row">
          <span class="hof-medal">🏅</span>
          <div class="hof-name">CHECO PEREZ</div>
        </div>
        <div class="hof-stars">★★★</div>
        <div class="hof-meta"> TriCampeón: 2022, 2023, 2025</div>
      </div>

      <div class="hof-card fade-up">
        <div class="hof-row">
          <span class="hof-medal">🏅</span>
          <div class="hof-name">VALTERI BOTTAS</div>
        </div>
        <div class="hof-stars">★</div>
        <div class="hof-meta">Campeón: 2024</div>
      </div>

      <div class="hof-card fade-up">
        <div class="hof-row">
          <span class="hof-medal">🏅</span>
          <div class="hof-name">FEFE WOLF</div>
        </div>
        <div class="hof-stars">★</div>
        <div class="hof-meta">Campeón: 2021</div>
      </div>

      <div class="hof-card fade-up">
        <div class="hof-row">
          <span class="hof-medal">🏅</span>
          <div class="hof-name">NICKI LAUDA</div>
        </div>
        <div class="hof-stars">★</div>
        <div class="hof-meta">Campeón: 2021</div>
      </div>

    </div>
    """, unsafe_allow_html=True)

def pantalla_tabla_posiciones():
    st.title("📊 TABLA GENERAL 2026")
    df_pos = leer_tabla_posiciones(PILOTOS_TORNEO)
    if df_pos is None or df_pos.empty:
        df_pos = pd.DataFrame({
            "Piloto": PILOTOS_TORNEO,
            "Puntos": [0] * len(PILOTOS_TORNEO),
            "Qualys": [0] * len(PILOTOS_TORNEO),
            "Sprints": [0] * len(PILOTOS_TORNEO),
            "Carreras": [0] * len(PILOTOS_TORNEO),
        })
    if "Puntos" in df_pos.columns:
        df_pos = df_pos.sort_values(by="Puntos", ascending=False)

    render_dark_table(df_pos)

def pantalla_cargar_predicciones():
    st.title("🔒 SISTEMA DE PREDICCIÓN 2026")

    # --- Usuario logueado (si existe) ---
    usuario_logueado = (st.session_state.get("perfil") or {}).get("usuario", "")

    c1, c2 = st.columns(2)
    usuario = c1.selectbox(
        "Piloto Participante",
        PILOTOS_TORNEO,
        index=PILOTOS_TORNEO.index(usuario_logueado) if usuario_logueado in PILOTOS_TORNEO else 0,
        key="pred_usuario_select",
    )
    gp_actual = c2.selectbox("Seleccionar Gran Premio", GPS_OFICIALES, key="pred_gp_select")

    # --- Estado GP (ventana de predicción) ---
    estado_gp = obtener_estado_gp(gp_actual, HORARIOS_CARRERA, TZ)
    if not estado_gp["habilitado"]:
        st.error(f"🔴 **PREDICCIONES CERRADAS PARA: {gp_actual}**")
        st.warning(f"ESTADO: {estado_gp['mensaje']}")
        st.stop()
    else:
        st.success(f"🟢 **HABILITADO** | {estado_gp['mensaje']}")

    es_sprint = gp_actual in GPS_SPRINT
    if es_sprint:
        st.info("⚡ **¡ATENCIÓN! ESTE ES UN FIN DE SEMANA SPRINT** ⚡")

    # --- PIN ---
    st.subheader("🔐 Validación")
    pin = st.text_input("Ingresa tu PIN (4 dígitos):", type="password", max_chars=4, key="pred_pin")
    st.caption("Si tu PIN está mal, no se guarda nada.")

    # ---------------------------------------------------------------------
    # Helpers locales (evitan NameError y no dependen de Google Sheets)
    # ---------------------------------------------------------------------
    def _tiene_algo(d: dict) -> bool:
        if not isinstance(d, dict) or not d:
            return False
        for k, v in d.items():
            if v is None:
                continue
            if isinstance(v, str) and v.strip() == "":
                continue
            return True
        return False

    def ya_envio_etapa(usuario_: str, gp_: str, etapa_: str) -> bool:
        """
        Usa tu storage actual via recuperar_predicciones_piloto().
        Retorna True si ya hay data cargada para esa etapa.
        """
        try:
            db_qualy, db_sprint, (db_race, db_const) = recuperar_predicciones_piloto(usuario_, gp_)
        except Exception:
            # si falla lectura, no bloqueamos (mejor permitir)
            return False

        etapa_ = (etapa_ or "").upper().strip()
        if etapa_ == "QUALY":
            return _tiene_algo(db_qualy)
        if etapa_ == "SPRINT":
            return _tiene_algo(db_sprint)
        if etapa_ == "CARRERA":
            # carrera+constructores suelen venir separados; si hay cualquiera, consideramos enviado
            return _tiene_algo(db_race) or _tiene_algo(db_const)
        return False

    # ---------------------------------------------------------------------
    # Tabs
    # ---------------------------------------------------------------------
    if es_sprint:
        tab_qualy, tab_sprint, tab_race = st.tabs(["⏱️ CLASIFICACIÓN", "⚡ SPRINT", "🏁 CARRERA Y CONSTRUCTORES"])
    else:
        tab_qualy, tab_race = st.tabs(["⏱️ CLASIFICACIÓN", "🏁 CARRERA Y CONSTRUCTORES"])
        tab_sprint = None

    # =========================
    # QUALY
    # =========================
    with tab_qualy:
        st.subheader(f"Qualy - {gp_actual}")
        st.info("1°(15) - 2°(10) - 3°(7) - 4°(5) - 5°(3) | Pleno: +5 Pts")

        q_data = {}
        cq1, cq2 = st.columns(2)

        with cq1:
            for i in range(1, 6):
                q_data[i] = st.text_input(
                    f"Posición {i}° (Qualy)",
                    key=f"q{i}-{gp_actual}-{usuario}",
                )

        with cq2:
            st.markdown("#### 🇦🇷 Regla Colapinto (Qualy)")
            st.write("**Acierto Exacto:** +10 Puntos")
            q_data["colapinto_q"] = st.number_input(
                "Posición Franco Colapinto",
                1, 22, 10,
                key=f"cq-{gp_actual}-{usuario}",
            )

        # ✅ SOLO AUSTRALIA + SOLO QUALY (OBLIGATORIO)
        cd = None
        if gp_actual == "01. Gran Premio de Australia":
            st.markdown("---")
            st.error("🚨 **EDICIÓN ESPECIAL AUSTRALIA (solo Qualy)**")
            st.write("**Puntos:** Piloto Campeón (50 pts) | Constructor Campeón (25 pts)")

            col_c1, col_c2 = st.columns(2)
            camp_piloto = col_c1.text_input(
                "🏆 Piloto campeón (OBLIGATORIO - solo Australia)",
                key=f"camp_piloto_{gp_actual}_{usuario}",
            )
            camp_equipo = col_c2.text_input(
                "🏗️ Constructor campeón (OBLIGATORIO - solo Australia)",
                key=f"camp_equipo_{gp_actual}_{usuario}",
            )

            cd = {
                "piloto": (camp_piloto or "").strip(),
                "equipo": (camp_equipo or "").strip(),
            }

        # Bloqueo si ya envió QUALY
        ya_qualy = ya_envio_etapa(usuario, gp_actual, "QUALY")
        if ya_qualy:
            st.success("✅ Ya enviaste la predicción de **QUALY** para este GP. No podés volver a enviarla.")
            st.caption("Mucha suerte Formuleros...")

        btn_q = st.button(
            "🚀 ENVIAR SOLO QUALY",
            use_container_width=True,
            key=f"btn_q-{gp_actual}-{usuario}",
            disabled=ya_qualy,
        )

        if btn_q and not ya_qualy:
            if not verify_pin(usuario, pin):
                st.error("⛔ PIN INCORRECTO (4 dígitos)")
            else:
                # Validación campeones SOLO para Australia Qualy
                if gp_actual == "01. Gran Premio de Australia":
                    if not cd or not cd["piloto"] or not cd["equipo"]:
                        st.error("⚠️ En Australia (Qualy) debés completar **Piloto Campeón** y **Constructor Campeón**.")
                        st.stop()

                with st.spinner("Enviando..."):
                    # Si tu guardar_etapa acepta extra (cd), se lo pasamos solo en QUALY Australia.
                    try:
                        if cd is not None:
                            exito, msg = guardar_etapa(usuario, gp_actual, "QUALY", q_data, cd)
                        else:
                            exito, msg = guardar_etapa(usuario, gp_actual, "QUALY", q_data)
                    except TypeError:
                        # Por si tu guardar_etapa NO acepta 5to parámetro, mandamos sin cd.
                        exito, msg = guardar_etapa(usuario, gp_actual, "QUALY", q_data)

                if exito:
                    st.success(msg)
                    st.balloons()
                    st.rerun()
                else:
                    st.error(msg)

    # =========================
    # SPRINT (si aplica)
    # =========================
    if tab_sprint is not None:
        with tab_sprint:
            st.subheader(f"Sprint - {gp_actual}")
            st.info("TOP 8 real: 1°(8) 2°(7) 3°(6) 4°(5) 5°(4) 6°(3) 7°(2) 8°(1) | Pleno: +3")

            s_data = {}
            for i in range(1, 9):
                s_data[i] = st.text_input(
                    f"Posición {i}° (Sprint)",
                    key=f"s{i}-{gp_actual}-{usuario}",
                )

            ya_sprint = ya_envio_etapa(usuario, gp_actual, "SPRINT")
            if ya_sprint:
                st.success("✅ Ya enviaste la predicción de **SPRINT** para este GP. No podés volver a enviarla.")

            btn_s = st.button(
                "🚀 ENVIAR SOLO SPRINT",
                use_container_width=True,
                key=f"btn_s-{gp_actual}-{usuario}",
                disabled=ya_sprint,
            )

            if btn_s and not ya_sprint:
                if not verify_pin(usuario, pin):
                    st.error("⛔ PIN INCORRECTO (4 dígitos)")
                else:
                    with st.spinner("Enviando..."):
                        exito, msg = guardar_etapa(usuario, gp_actual, "SPRINT", s_data)

                    if exito:
                        st.success(msg)
                        st.balloons()
                        st.rerun()
                    else:
                        st.error(msg)

    # =========================
    # CARRERA + CONSTRUCTORES
    # =========================
    with tab_race:
        cr, cc = st.columns(2)
        r_data = {}

        with cr:
            st.subheader(f"Carrera - {gp_actual}")
            st.info("1°(25) - 2°(18) - 3°(15) - 4°(12) - 5°(10) | Pleno: +5 Pts")
            for i in range(1, 11):
                r_data[i] = st.text_input(
                    f"Posición {i}° (Carrera)",
                    key=f"r{i}-{gp_actual}-{usuario}",
                )

            st.markdown("#### 🇦🇷 Regla Colapinto (Carrera)")
            st.write("**Acierto Exacto:** +20 Puntos")
            r_data["colapinto_r"] = st.number_input(
                "Posición Franco Colapinto",
                1, 22, 10,
                key=f"cr-{gp_actual}-{usuario}",
            )

        with cc:
            st.subheader("Constructores")
            st.info("1°(10) - 2°(5) - 3°(2) | Pleno: +3 Pts")
            r_data["c1"] = st.text_input("Equipo 1°", key=f"c1-{gp_actual}-{usuario}")
            r_data["c2"] = st.text_input("Equipo 2°", key=f"c2-{gp_actual}-{usuario}")
            r_data["c3"] = st.text_input("Equipo 3°", key=f"c3-{gp_actual}-{usuario}")

        # Bloqueo si ya envió CARRERA/CONSTRUCTORES
        ya_carrera = ya_envio_etapa(usuario, gp_actual, "CARRERA")
        if ya_carrera:
            st.success("✅ Ya enviaste la predicción de **CARRERA/CONSTRUCTORES** para este GP. No podés volver a enviarla.")

        btn_r = st.button(
            "🚀 ENVIAR CARRERA Y CONSTRUCTORES",
            use_container_width=True,
            key=f"btn_r-{gp_actual}-{usuario}",
            disabled=ya_carrera,
        )

        if btn_r and not ya_carrera:
            if not verify_pin(usuario, pin):
                st.error("⛔ PIN INCORRECTO (4 dígitos)")
            else:
                # IMPORTANTE: NO bloquear por campeones acá (como pediste)
                with st.spinner("Enviando..."):
                    exito, msg = guardar_etapa(usuario, gp_actual, "CARRERA", r_data)

                if exito:
                    st.success(msg)
                    st.balloons()
                    st.rerun()
                else:
                    st.error(msg)

def pantalla_calculadora_puntos():
    st.title("🧮 CENTRO DE CÓMPUTOS")
    st.info("🔒 ÁREA RESTRINGIDA: Para evitar espionaje, se requiere autorización.")

    pwd = st.text_input("🔑 Ingrese Clave de Comisario:", type="password")
    if pwd != "2022":
        st.stop()

    st.success("✅ ACCESO AUTORIZADO - MODO COMISARIO ACTIVO")
    st.divider()

    gp_calc = st.selectbox("Gran Premio a Calcular:", GPS_OFICIALES, key="gp_calc_main")

    # ✅ Solo calcular cuando el GP ya cerró predicciones
    estado_gp = obtener_estado_gp(gp_calc, HORARIOS_CARRERA, TZ)
    if estado_gp["habilitado"]:
        st.error("⛔ Todavía NO podés calcular puntos: el GP sigue habilitado para predicciones.")
        st.caption("Regla: solo se calcula cuando la predicción está cerrada (post-carrera).")
        st.stop()
    else:
        st.success(f"✅ OK para calcular: {estado_gp['mensaje']}")

    st.subheader("1) RESULTADOS OFICIALES (FIA)")
    oficial = {}

    col_res1, col_res2, col_res3 = st.columns(3)

    with col_res1:
        st.markdown("**🏁 Carrera (1–10)**")
        for i in range(1, 11):
            oficial[f"r{i}"] = st.text_input(f"Oficial Carrera {i}°", key=f"of_r{i}-{gp_calc}")
        oficial["col_r"] = st.number_input("Oficial Colapinto (Carrera)", 1, 22, 10, key=f"of_cr-{gp_calc}")

    with col_res2:
        st.markdown("**⏱️ Qualy (1–5)**")
        for i in range(1, 6):
            oficial[f"q{i}"] = st.text_input(f"Oficial Qualy {i}°", key=f"of_q{i}-{gp_calc}")
        oficial["col_q"] = st.number_input("Oficial Colapinto (Qualy)", 1, 22, 10, key=f"of_cq-{gp_calc}")

    with col_res3:
        st.markdown("**🛠️ Constructores (AUTO)**")
        st.caption("Se calculan automáticamente en base al resultado oficial de la carrera (1–10).")
        of_r_auto = {i: oficial.get(f"r{i}", "") for i in ESCALA_CARRERA_JUEGO.keys()}
        top3, team_pts = calcular_constructores_auto(
            of_r=of_r_auto,
            grilla=GRILLA_2026,
            escala_carrera=ESCALA_CARRERA_JUEGO,
            top_n=3
        )

        if len(top3) >= 3:
            oficial["c1"], oficial["c2"], oficial["c3"] = top3[0], top3[1], top3[2]
            st.success(f"Top 3: 1) {oficial['c1']} 2) {oficial['c2']} 3) {oficial['c3']}")
        else:
            oficial["c1"], oficial["c2"], oficial["c3"] = "", "", ""
            st.warning("Cargá al menos los primeros 10 de carrera para calcular constructores.")

        if team_pts:
            st.write("Puntos por equipo:", team_pts)

    if gp_calc in GPS_SPRINT:
        st.markdown("### ⚡ Sprint (Oficial 1–8)")
        cs1, cs2 = st.columns(2)
        with cs1:
            for i in range(1, 5):
                oficial[f"s{i}"] = st.text_input(f"Oficial Sprint {i}°", key=f"of_s{i}-{gp_calc}")
        with cs2:
            for i in range(5, 9):
                oficial[f"s{i}"] = st.text_input(f"Oficial Sprint {i}°", key=f"of_s{i}-{gp_calc}")

    # =========================
    # 🔒 LOCK 1 vez por GP (SUMA REAL)
    # =========================
    gp_done_key = f"GP_DONE::{gp_calc}"
    gp_done = lock_exists(gp_done_key)

    st.divider()
    st.subheader("⚡ Auto-calcular y actualizar TODO el GP (todos los pilotos)")

    if gp_done:
        st.warning("🔒 Este GP ya fue calculado. Botón bloqueado para evitar doble suma.")

    if st.button(
        "⚡ CALCULAR Y ACTUALIZAR TODOS (GP)",
        use_container_width=True,
        key=f"btn_auto_{gp_calc}",
        disabled=gp_done
    ):
        df_res = calcular_y_actualizar_todos(
            gp_calc=gp_calc,
            oficial=oficial,
            pilotos_torneo=PILOTOS_TORNEO,
            gps_sprint=GPS_SPRINT
        )
        set_lock(gp_done_key)
        st.success("✅ GP calculado y cerrado (lock aplicado).")
        st.dataframe(df_res, use_container_width=True)

    # =========================
    # ✅ HISTORIAL (SIN SUMAR PUNTOS)
    # =========================
    st.divider()
    st.subheader("🧾 Historial por GP (sin sumar puntos)")

    st.caption(
        "Usá este botón si el GP ya está cerrado (lock aplicado) y el Historial por GP quedó vacío.\n"
        "Esto REGENERA tabla_historial.db y tabla_historial_detalle.db para ese GP, sin tocar Posiciones."
    )

    if st.button(
        "🧾 GENERAR HISTORIAL (sin sumar puntos)",
        use_container_width=True,
        key=f"btn_hist_{gp_calc}",
    ):
        try:
            df_hist = generar_historial_solo(
                gp_calc=gp_calc,
                oficial=oficial,
                pilotos_torneo=PILOTOS_TORNEO,
                gps_sprint=GPS_SPRINT
            )
            st.success("✅ Historial generado. Ahora debería aparecer en 'Historial por GP'.")
            st.dataframe(df_hist, use_container_width=True)
        except Exception as e:
            st.error(f"Error generando historial: {e}")

    # =========================
    # ⛔ SANCIONES D.N.S. (-5)
    # =========================
    st.divider()
    st.subheader("⛔ SANCIONES POR NO ENVÍO (D.N.S.)")
    st.caption("Falta en Qualy: -5 | Falta en Sprint (si hay): -5 | Falta en Carrera+Constructores: -5")

    dns_key = f"DNS_DONE::{gp_calc}"
    dns_done = lock_exists(dns_key)
    if dns_done:
        st.warning("🔒 Sanciones D.N.S. ya aplicadas para este GP. Bloqueado.")
    else:
        if st.button("Aplicar sanciones D.N.S. (-5)", use_container_width=True, key=f"btn_dns_{gp_calc}"):
            df_dns = aplicar_sanciones_dns(gp_calc, PILOTOS_TORNEO, GPS_SPRINT)
            set_lock(dns_key)
            st.success("✅ Sanciones aplicadas.")
            st.dataframe(df_dns, use_container_width=True)

    # =========================
    # PREVIEW POR PILOTO (NO GUARDA)
    # =========================
    st.divider()
    st.subheader("2) CALCULAR PUNTOS DE PILOTO (PREVIEW)")

    piloto_calc = st.selectbox("Seleccionar Piloto:", PILOTOS_TORNEO, key=f"piloto_calc_{gp_calc}")
    db_qualy, db_sprint, (db_race, db_const) = recuperar_predicciones_piloto(piloto_calc, gp_calc)

    val_r = normalizar_keys_numericas(db_race if db_race else {})
    val_q = normalizar_keys_numericas(db_qualy if db_qualy else {})
    val_c = normalizar_keys_numericas(db_const if db_const else {})
    val_s = normalizar_keys_numericas(db_sprint if db_sprint else {})

    if db_qualy or db_race or db_sprint:
        st.success(f"✅ Predicciones encontradas de {piloto_calc}")
    else:
        st.warning(f"⚠️ {piloto_calc} NO ha enviado predicciones para {gp_calc} (o no se pudieron leer).")

    if st.button("CALCULAR TOTAL AUTOMÁTICO (PREVIEW)", use_container_width=True, key=f"btn_calc_{gp_calc}_{piloto_calc}"):
        of_r = {i: oficial.get(f"r{i}", "") for i in range(1, 11)}
        of_q = {i: oficial.get(f"q{i}", "") for i in range(1, 6)}
        of_c = {i: oficial.get(f"c{i}", "") for i in range(1, 4)}
        of_s = {i: oficial.get(f"s{i}", "") for i in range(1, 9)}

        pts_carrera = calcular_puntos("CARRERA", val_r, of_r, val_r.get("colapinto_r"), oficial.get("col_r"))
        pts_qualy = calcular_puntos("QUALY", val_q, of_q, val_q.get("colapinto_q"), oficial.get("col_q"))
        pts_const = calcular_puntos("CONSTRUCTORES", val_c, of_c)

        pts_sprint = 0
        if gp_calc in GPS_SPRINT and db_sprint:
            pts_sprint = calcular_puntos("SPRINT", val_s, of_s)

        total = pts_carrera + pts_qualy + pts_const + pts_sprint
        st.success(f"💰 PUNTOS TOTALES DE {piloto_calc}: **{total}**")
        st.info(f"Desglose: Carrera ({pts_carrera}) + Const ({pts_const}) + Qualy ({pts_qualy}) + Sprint ({pts_sprint})")
        st.caption("⚠️ PREVIEW: no guarda nada (para evitar doble suma).")

    # =========================
    # 🏆 BONUS CAMPEONES (solo GP final 24.*)
    # =========================
    st.divider()
    st.subheader("🏆 Bonus Campeones (Final de temporada)")

    gp_final = None
    for g in GPS_OFICIALES:
        if str(g).strip().startswith("24."):
            gp_final = g
            break
    if gp_final is None:
        gp_final = GPS_OFICIALES[-1]

    if gp_calc != gp_final:
        st.info(f"Este bonus solo se aplica en el GP final: **{gp_final}**")
        return

    lock_ch_key = f"CHAMP_DONE::{gp_final}"
    if lock_exists(lock_ch_key):
        st.warning("🔒 Bonus campeones ya aplicado. Bloqueado.")
        return

    GP_AUSTRALIA = "01. Gran Premio de Australia"
    piloto_real = st.text_input("Piloto campeón REAL (exacto):", key="real_camp_pil")
    constructor_real = st.text_input("Constructor campeón REAL (exacto):", key="real_camp_team")
    st.caption("Suma: +50 (piloto campeón) +25 (constructor campeón) según predicción hecha en Australia.")

    if st.button("✅ APLICAR BONUS CAMPEONES (1 sola vez)", use_container_width=True, key="btn_apply_champ"):
        ok, out = aplicar_bonus_campeones_final(
            gp_final=gp_final,
            piloto_campeon_real=piloto_real,
            constructor_campeon_real=constructor_real,
            gp_prediccion_campeones=GP_AUSTRALIA,
            pilotos_torneo=PILOTOS_TORNEO
        )
        if ok:
            st.success("✅ Bonus aplicado.")
            st.dataframe(out, use_container_width=True)
        else:
            st.warning(out)

# ======================================================================
# MAIN
# ======================================================================
def main():
    st.sidebar.title("🏁 MENU PRINCIPAL")

    # Icono F1 (solo si existe el archivo)
    f1_logo = os.path.join("ui", "f1.png")
    if os.path.exists(f1_logo):
        st.sidebar.image(f1_logo, use_container_width=True)

    # Mostrar inicializador SOLO si FW_SETUP=1 (así queda pro)
    if os.getenv("FW_SETUP", "0") == "1":
        bootstrap_admin_ui()

    sidebar_login_block()
    admin_crear_usuario_ui()
    admin_set_pin_ui()

    # Botón flotante (funciona en PC y mobile)
    floating_scroll_top_button()

    opciones = [
        "🏠 Inicio & Historia",
        "📅 Calendario Oficial 2026",
        "🔒 Cargar Predicciones",
        "📊 Tabla de Posiciones",
        "🧮 Calculadora de Puntos",
        "🏎️ Pilotos y Escuderías 2026",
        "📜 Reglamento Oficial",
        "🏆 Muro de Campeones",
        "💬 Mesa Chica",
    ]

    if is_admin_or_comisario():
        opciones.append("🧪 Test API F1")

    opcion = st.sidebar.radio("Navegación:", opciones, index=0)

    # Flecha del menú (ahora sí funciona)
    if st.sidebar.button("⬆️ Ir arriba"):
        scroll_to_top()

    st.sidebar.markdown("---")

    if opcion == "🏠 Inicio & Historia":
        pantalla_inicio()
    elif opcion == "📅 Calendario Oficial 2026":
        pantalla_calendario()
    elif opcion == "🔒 Cargar Predicciones":
        pantalla_cargar_predicciones()
    elif opcion == "📊 Tabla de Posiciones":
        pantalla_tabla_posiciones()
    elif opcion == "🧮 Calculadora de Puntos":
        pantalla_calculadora_puntos()
    elif opcion == "🏎️ Pilotos y Escuderías 2026":
        pantalla_pilotos_y_escuderias()

    elif opcion == "📜 Reglamento Oficial":
        pantalla_reglamento()
    elif opcion == "🏆 Muro de Campeones":
        pantalla_muro()
    elif opcion == "💬 Mesa Chica":
        pantalla_mesa_chica()    
    elif opcion == "🧪 Test API F1":
        pantalla_api_test()
        
        
if __name__ == "__main__":
        main()   