from .utils import normalizar_nombre

def calcular_puntos(tipo, prediccion, oficial, colapinto_pred=None, colapinto_real=None):
    puntos = 0
    aciertos = 0

    if tipo == "SPRINT":
        # F1 real: top 8
        escala = {1: 8, 2: 7, 3: 6, 4: 5, 5: 4, 6: 3, 7: 2, 8: 1}
        bonus_perfecto = 3
        max_pos = 8

    elif tipo == "CARRERA":
        # F1 real: top 10
        escala = {1: 25, 2: 18, 3: 15, 4: 12, 5: 10, 6: 8, 7: 6, 8: 4, 9: 2, 10: 1}
        bonus_perfecto = 5
        max_pos = 10

    elif tipo == "QUALY":
        # tu sistema
        escala = {1: 15, 2: 10, 3: 7, 4: 5, 5: 3}
        bonus_perfecto = 5
        max_pos = 5

    elif tipo == "CONSTRUCTORES":
        # tu sistema
        escala = {1: 10, 2: 5, 3: 2}
        bonus_perfecto = 3
        max_pos = 3

    else:
        return 0

    for i in range(1, max_pos + 1):
        user = normalizar_nombre(prediccion.get(i, ""))
        real = normalizar_nombre(oficial.get(i, ""))

        if user and user == real:
            puntos += escala.get(i, 0)
            aciertos += 1

    # bonus por pleno
    if aciertos == max_pos:
        puntos += bonus_perfecto

    # regla colapinto (solo qualy/carrera)
    if tipo in ("QUALY", "CARRERA"):
        try:
            if colapinto_pred is not None and colapinto_real is not None:
                if int(colapinto_pred) == int(colapinto_real):
                    puntos += 10 if tipo == "QUALY" else 20
        except Exception:
            pass

    return puntos