from fastapi import APIRouter, HTTPException
import requests

router = APIRouter()

ERGAST = "https://api.jolpi.ca/ergast/f1"

def _get_json(url: str):
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Error llamando F1 API: {e}")

@router.get("/calendar")
def calendar(year: int = 2026):
    """
    Devuelve calendario (rondas, nombre GP, circuito, fecha, hora si está).
    """
    data = _get_json(f"{ERGAST}/{year}.json")
    races = data["MRData"]["RaceTable"].get("Races", [])
    out = []
    for x in races:
        out.append({
            "round": int(x.get("round")),
            "raceName": x.get("raceName"),
            "circuit": x.get("Circuit", {}).get("circuitName"),
            "locality": x.get("Circuit", {}).get("Location", {}).get("locality"),
            "country": x.get("Circuit", {}).get("Location", {}).get("country"),
            "date": x.get("date"),
            "time": x.get("time"),  # UTC si viene
        })
    return {"year": year, "count": len(out), "races": out}

@router.get("/constructors")
def constructors(year: int = 2026, round: int | None = None):
    """
    Standings de constructores.
    - Si round no viene: standings actuales del año.
    - Si round viene: standings hasta esa ronda.
    """
    if round is None:
        url = f"{ERGAST}/{year}/constructorStandings.json"
    else:
        url = f"{ERGAST}/{year}/{round}/constructorStandings.json"

    data = _get_json(url)
    lists = data["MRData"]["StandingsTable"].get("StandingsLists", [])
    if not lists:
        return {"year": year, "round": round, "constructors": []}

    standings = lists[0].get("ConstructorStandings", [])
    out = []
    for s in standings:
        c = s.get("Constructor", {})
        out.append({
            "position": int(s.get("position", 0)),
            "points": float(s.get("points", 0)),
            "wins": int(s.get("wins", 0)),
            "constructorId": c.get("constructorId"),
            "name": c.get("name"),
            "nationality": c.get("nationality"),
        })
    return {"year": year, "round": round, "count": len(out), "constructors": out}

@router.get("/race/{round}/results")
def race_results(round: int, year: int = 2026, type: str = "race"):
    """
    type: race | qualy | sprint
    """
    type = (type or "").lower().strip()
    if type not in ("race", "qualy", "sprint"):
        raise HTTPException(status_code=400, detail="type debe ser race|qualy|sprint")

    if type == "race":
        url = f"{ERGAST}/{year}/{round}/results.json"
        data = _get_json(url)
        races = data["MRData"]["RaceTable"].get("Races", [])
        if not races:
            return {"year": year, "round": round, "type": type, "results": []}
        results = races[0].get("Results", [])
        out = []
        for r in results:
            d = r.get("Driver", {})
            c = r.get("Constructor", {})
            out.append({
                "position": int(r.get("position", 0)),
                "driver": f'{d.get("givenName","")} {d.get("familyName","")}'.strip(),
                "constructor": c.get("name"),
                "status": r.get("status"),
                "points": float(r.get("points", 0)),
                "grid": int(r.get("grid", 0)),
            })
        return {"year": year, "round": round, "type": type, "count": len(out), "results": out}

    if type == "qualy":
        url = f"{ERGAST}/{year}/{round}/qualifying.json"
        data = _get_json(url)
        races = data["MRData"]["RaceTable"].get("Races", [])
        if not races:
            return {"year": year, "round": round, "type": type, "results": []}
        results = races[0].get("QualifyingResults", [])
        out = []
        for r in results:
            d = r.get("Driver", {})
            c = r.get("Constructor", {})
            out.append({
                "position": int(r.get("position", 0)),
                "driver": f'{d.get("givenName","")} {d.get("familyName","")}'.strip(),
                "constructor": c.get("name"),
                "q1": r.get("Q1"),
                "q2": r.get("Q2"),
                "q3": r.get("Q3"),
            })
        return {"year": year, "round": round, "type": type, "count": len(out), "results": out}

    # sprint
    url = f"{ERGAST}/{year}/{round}/sprint.json"
    data = _get_json(url)
    races = data["MRData"]["RaceTable"].get("Races", [])
    if not races:
        return {"year": year, "round": round, "type": type, "results": []}
    results = races[0].get("SprintResults", [])
    out = []
    for r in results:
        d = r.get("Driver", {})
        c = r.get("Constructor", {})
        out.append({
            "position": int(r.get("position", 0)),
            "driver": f'{d.get("givenName","")} {d.get("familyName","")}'.strip(),
            "constructor": c.get("name"),
            "status": r.get("status"),
            "points": float(r.get("points", 0)),
        })
    return {"year": year, "round": round, "type": type, "count": len(out), "results": out}