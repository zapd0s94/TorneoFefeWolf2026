# core/utils.py
import re
import unicodedata

_ACCENT_MAP = str.maketrans({
    "á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ü": "u", "ñ": "n",
    "Á": "a", "É": "e", "Í": "i", "Ó": "o", "Ú": "u", "Ü": "u", "Ñ": "n",
})

def normalizar_nombre(nombre: str) -> str:
    if nombre is None:
        return ""
    s = str(nombre).strip()
    if not s:
        return ""

    s = unicodedata.normalize("NFC", s)
    s = s.translate(_ACCENT_MAP)
    s = s.lower()
    s = re.sub(r"[^a-z\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s
