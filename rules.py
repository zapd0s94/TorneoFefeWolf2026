from datetime import datetime, timedelta
import pytz

def verificar_estado_gp(gp_seleccionado, horarios_carrera, tz):
    """
    Devuelve (mensaje_estado, habilitado)
    Lógica pura. No depende de variables globales.
    """
    if gp_seleccionado not in horarios_carrera:
        return "ABIERTO (SIN FECHA)", True

    fecha_carrera = tz.localize(
        datetime.strptime(horarios_carrera[gp_seleccionado], "%Y-%m-%d %H:%M")
    )

    ahora = datetime.now(tz)
    limite_apertura = fecha_carrera - timedelta(hours=72)
    limite_cierre = fecha_carrera - timedelta(hours=1)

    if ahora < limite_apertura:
        return "PRÓXIMAMENTE (Abre 72hs antes del evento)", False
    elif ahora > limite_cierre:
        return "TIEMPO AGOTADO (Cerrado 1 hora antes)", False
    else:
        return f"ABIERTO (Cierra: {limite_cierre.strftime('%d/%m %H:%M')})", True


def obtener_estado_gp(gp_seleccionado, horarios_carrera, tz):
    """
    Source of truth del estado del GP.
    Devuelve todo lo que la UI necesita.
    """
    mensaje, habilitado = verificar_estado_gp(gp_seleccionado, horarios_carrera, tz)

    if "ABIERTO" in mensaje:
        estado = "ABIERTO"
        color = "green"
    elif "PRÓXIMAMENTE" in mensaje:
        estado = "PRÓXIMAMENTE"
        color = "orange"
    else:
        estado = "CERRADO"
        color = "red"

    return {
        "estado": estado,
        "habilitado": habilitado,
        "mensaje": mensaje,
        "color": color,
    }
