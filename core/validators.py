# core/validators.py
from datetime import datetime, timedelta
from typing import Tuple, Optional
import sys

def validar_pin(pin_ingresado: str, pin_correcto: str) -> bool:
    if not pin_ingresado:
        return False
    return pin_ingresado.strip() == str(pin_correcto).strip()


def validar_envio_permitido(
    ahora: datetime,
    fecha_carrera: datetime,
    abre_horas_antes: int = 72,
    cierra_horas_antes: int = 1
) -> Tuple[bool, Optional[str]]:
    """
    Devuelve:
      (True, None) si puede enviar
      (False, motivo) si no puede
    """
    if ahora.tzinfo is None or fecha_carrera.tzinfo is None:
        return False, "Error interno: las fechas deben tener timezone."

    limite_apertura = fecha_carrera - timedelta(hours=abre_horas_antes)
    limite_cierre = fecha_carrera - timedelta(hours=cierra_horas_antes)

    if ahora < limite_apertura:
        return False, f"PRÓXIMAMENTE (Abre {abre_horas_antes}hs antes del evento)"
    if ahora > limite_cierre:
        return False, f"TIEMPO AGOTADO (Cierra {cierra_horas_antes}h antes del evento)"

    return True, None
