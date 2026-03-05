import sqlite3
from datetime import datetime
import html

DB_PATH = "mesa_chica.db"

MESA_CHICA_PROFILE = {
    "Valteri Bottas":   {"grupo": "FIPF",      "tag": "MIEMBRO DE LA FIPF", "stars": "★",   "mod": True},
    "Lando Norris":     {"grupo": "FIPF",      "tag": "MIEMBRO DE LA FIPF", "stars": "",    "mod": True},
    "Fernando Alonso":  {"grupo": "FIPF",      "tag": "MIEMBRO DE LA FIPF", "stars": "",    "mod": True},
    "Checo Perez":      {"grupo": "FORMULERO", "tag": "FORMULERO",          "stars": "★★★", "mod": False},
    "Nicki Lauda":      {"grupo": "FORMULERO", "tag": "FORMULERO",          "stars": "★",   "mod": False},
}

MESA_CHICA_BADGES = {
    "Valteri Bottas":  {"tipo": "fipf",      "label": "MIEMBRO FIPF", "stars": "★"},
    "Lando Norris":    {"tipo": "fipf",      "label": "MIEMBRO FIPF", "stars": ""},
    "Fernando Alonso": {"tipo": "fipf",      "label": "MIEMBRO FIPF", "stars": ""},
    "Checo Perez":     {"tipo": "formulero", "label": "FORMULERO",    "stars": "★★★"},
    "Nicki Lauda":     {"tipo": "formulero", "label": "FORMULERO",    "stars": "★"},
}

def _now_iso_local() -> str:
    return datetime.now().replace(microsecond=0).isoformat()

def _mc_safe_text(s: str) -> str:
    return html.escape(str(s or ""))

def mc_badge_for(usuario: str):
    b = MESA_CHICA_BADGES.get(usuario, {"tipo": "formulero", "label": "FORMULERO", "stars": ""})
    return b["tipo"], b["label"], b["stars"]

def mc_is_mod(usuario: str) -> bool:
    p = MESA_CHICA_PROFILE.get(usuario, {})
    return bool(p.get("mod", False))

def _mc_db():
    con = sqlite3.connect(DB_PATH, check_same_thread=False)

    con.execute("""
        CREATE TABLE IF NOT EXISTS mesa_chica (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario TEXT NOT NULL,
            mensaje TEXT NOT NULL,
            ts TEXT NOT NULL,
            edited_ts TEXT,
            deleted INTEGER NOT NULL DEFAULT 0,
            deleted_ts TEXT,
            deleted_by TEXT
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS mesa_chica_likes (
            msg_id INTEGER NOT NULL,
            usuario TEXT NOT NULL,
            ts TEXT NOT NULL,
            PRIMARY KEY (msg_id, usuario)
        )
    """)

    con.commit()
    return con

# -----------------------
# SPAM
# -----------------------
def mc_is_spam(usuario: str, seconds: int = 15, max_msgs: int = 3) -> bool:
    con = _mc_db()
    try:
        cur = con.execute("""
            SELECT ts FROM mesa_chica
            WHERE usuario = ? AND deleted = 0
            ORDER BY id DESC
            LIMIT ?
        """, (str(usuario).strip(), int(max_msgs)))
        rows = cur.fetchall()
        if len(rows) < max_msgs:
            return False

        last_times = [datetime.fromisoformat(r[0]) for r in rows]
        now = datetime.now()
        return all((now - t).total_seconds() < seconds for t in last_times)
    finally:
        con.close()

# -----------------------
# MENSAJES
# -----------------------
def mc_add_message(usuario: str, mensaje: str):
    txt = (mensaje or "").strip()
    if not txt:
        return
    con = _mc_db()
    try:
        con.execute("""
            INSERT INTO mesa_chica (usuario, mensaje, ts, edited_ts, deleted, deleted_ts, deleted_by)
            VALUES (?,?,?,?,?,?,?)
        """, (str(usuario).strip(), txt, _now_iso_local(), None, 0, None, None))
        con.commit()
    finally:
        con.close()

def mc_list_messages(limit: int = 250):
    con = _mc_db()
    try:
        cur = con.execute("""
            SELECT id, usuario, mensaje, ts, edited_ts
            FROM mesa_chica
            WHERE deleted = 0
            ORDER BY id DESC
            LIMIT ?
        """, (int(limit),))
        return cur.fetchall()
    finally:
        con.close()

def mc_update_message(msg_id: int, nuevo_texto: str):
    txt = (nuevo_texto or "").strip()
    if not txt:
        return
    con = _mc_db()
    try:
        con.execute("""
            UPDATE mesa_chica
            SET mensaje = ?, edited_ts = ?
            WHERE id = ? AND deleted = 0
        """, (txt, _now_iso_local(), int(msg_id)))
        con.commit()
    finally:
        con.close()

def mc_soft_delete_message(msg_id: int, deleted_by: str = ""):
    con = _mc_db()
    try:
        con.execute("""
            UPDATE mesa_chica
            SET deleted = 1, deleted_ts = ?, deleted_by = ?
            WHERE id = ?
        """, (_now_iso_local(), str(deleted_by or "").strip(), int(msg_id)))
        con.commit()
    finally:
        con.close()

def mc_purge_html_messages():
    """
    Oculta mensajes que probablemente rompen el layout (HTML/JS pegado).
    No borra: marca deleted=1.
    """
    con = _mc_db()
    try:
        cur = con.execute("SELECT id, mensaje FROM mesa_chica WHERE deleted = 0")
        rows = cur.fetchall()

        bad_ids = []
        for mid, msg in rows:
            m = (msg or "").lower()
            if "<script" in m or "</" in m or "<div" in m or "<span" in m or "<style" in m:
                bad_ids.append(int(mid))
            elif (msg or "").count("<") >= 2:
                bad_ids.append(int(mid))

        if bad_ids:
            con.executemany(
                "UPDATE mesa_chica SET deleted = 1, deleted_ts = ? WHERE id = ?",
                [(_now_iso_local(), mid) for mid in bad_ids],
            )
            con.commit()
    finally:
        con.close()

# -----------------------
# LIKES (toggle)
# -----------------------
def mc_toggle_like(msg_id: int, usuario: str) -> bool:
    """
    Devuelve True si quedó liked, False si quedó unliked.
    """
    con = _mc_db()
    try:
        msg_id = int(msg_id)
        usuario = str(usuario).strip()

        cur = con.execute(
            "SELECT 1 FROM mesa_chica_likes WHERE msg_id = ? AND usuario = ?",
            (msg_id, usuario),
        )
        exists = cur.fetchone() is not None

        if exists:
            con.execute(
                "DELETE FROM mesa_chica_likes WHERE msg_id = ? AND usuario = ?",
                (msg_id, usuario),
            )
            con.commit()
            return False
        else:
            con.execute(
                "INSERT INTO mesa_chica_likes (msg_id, usuario, ts) VALUES (?,?,?)",
                (msg_id, usuario, _now_iso_local()),
            )
            con.commit()
            return True
    finally:
        con.close()

def mc_like_count(msg_id: int) -> int:
    con = _mc_db()
    try:
        cur = con.execute(
            "SELECT COUNT(*) FROM mesa_chica_likes WHERE msg_id = ?",
            (int(msg_id),),
        )
        return int(cur.fetchone()[0])
    finally:
        con.close()

def mc_user_liked(msg_id: int, usuario: str) -> bool:
    con = _mc_db()
    try:
        cur = con.execute(
            "SELECT 1 FROM mesa_chica_likes WHERE msg_id = ? AND usuario = ?",
            (int(msg_id), str(usuario).strip()),
        )
        return cur.fetchone() is not None
    finally:
        con.close()