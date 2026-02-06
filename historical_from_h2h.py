import re
import requests
from bs4 import BeautifulSoup
from typing import Dict, List, Tuple
from statistics import mean

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/91.0.4472.124 Safari/537.36'
    )
}


def obtener_historial_desde_h2h(partido_id: str, limite: int = 5) -> Tuple[List[Dict], List[Dict], str, str]:
    """
    Extrae los últimos `limite` partidos de cada equipo desde la pestaña H2H
    de la versión móvil de Flashscore, usando el mid del partido actual.
    Devuelve:
      - hist_local: lista de dicts con goles_favor/goles_contra del local
      - hist_visita: idem para el visitante
      - nombre_local_h2h, nombre_visita_h2h: como los muestra la página
    """
    url_h2h = f"https://m.flashscore.cl/detalle-del-partido/{partido_id}/?s=2&t=h2h"
    print(f"[H2H] Cargando URL: {url_h2h}")

    resp = requests.get(url_h2h, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    html = resp.text
    print(f"[H2H] HTML recibido, longitud={len(html)}")

    soup = BeautifulSoup(html, "html.parser")

    # Buscar encabezados "Últimos partidos: X"
    h4s = soup.find_all("h4")
    print(f"[H2H] Encontrados {len(h4s)} elementos <h4>")

    bloques_ult = [h for h in h4s if "Últimos partidos:" in h.get_text()]
    print(f"[H2H] Bloques 'Últimos partidos:' encontrados = {len(bloques_ult)}")

    for i, h in enumerate(bloques_ult):
        print(f"   [{i}] texto bloque: {h.get_text(strip=True)}")

    if len(bloques_ult) < 2:
        print(f"[H2H] ⚠️ No hay dos bloques de 'Últimos partidos' para {partido_id}")
        return [], [], "", ""

    # Primer bloque = local, segundo = visita
    h_local = bloques_ult[0]
    h_visita = bloques_ult[1]

    nombre_local = h_local.get_text(strip=True).replace("Últimos partidos:", "").strip()
    nombre_visita = h_visita.get_text(strip=True).replace("Últimos partidos:", "").strip()

    print(f"[H2H] Nombre local detectado: {nombre_local}")
    print(f"[H2H] Nombre visita detectado: {nombre_visita}")

    # Cada h4 va seguido de una tabla class="h2h" con las filas de partidos
    tabla_local = h_local.find_next("table", class_="h2h")
    tabla_visita = h_visita.find_next("table", class_="h2h")

    hist_local = extraer_partidos_tabla(tabla_local, nombre_local, limite) if tabla_local else []
    hist_visita = extraer_partidos_tabla(tabla_visita, nombre_visita, limite) if tabla_visita else []

    print(f"[H2H] Partidos extraídos local={len(hist_local)}, visita={len(hist_visita)}")

    return hist_local, hist_visita, nombre_local, nombre_visita


def extraer_partidos_tabla(tabla, nombre_equipo: str, limite: int) -> List[Dict]:
    """
    A partir de una tabla como:

      <table class="h2h">
         <tr>
           <td class="data">
             <span>29.11.2025</span>
             <span>Aris Limassol - Paralimni</span>
             <a ...><b>4:0</b></a>
           </td>
         </tr>
         ...

    Extrae una lista de diccionarios con goles_favor/goles_contra
    para el equipo 'nombre_equipo' (Paralimni, Krasava, etc).
    """
    if not tabla:
        print(f"[H2H] Tabla vacía para {nombre_equipo}")
        return []

    partidos: List[Dict] = []
    filas = tabla.find_all("tr")
    print(f"[H2H] {nombre_equipo}: {len(filas)} filas <tr> encontradas en tabla")

    for fila in filas:
        if len(partidos) >= limite:
            break

        td = fila.find("td", class_="data")
        if not td:
            continue

        spans = td.find_all("span")
        if len(spans) < 2:
            continue

        # spans[0] = fecha, spans[1] = "EquipoA - EquipoB"
        texto_partidos = spans[1].get_text(" ", strip=True)
        marcador_tag = td.find("b")
        if not marcador_tag:
            continue

        marcador = marcador_tag.get_text(strip=True).replace(" ", "")
        if ":" not in marcador:
            continue

        try:
            goles_a_str, goles_b_str = marcador.split(":", 1)
            goles_a = int(goles_a_str)
            goles_b = int(goles_b_str)
        except ValueError:
            continue

        # Parsear "EquipoA - EquipoB"
        m = re.match(r'(.+?)\s*-\s*(.+)', texto_partidos)
        if not m:
            continue

        eq_a = m.group(1).strip()
        eq_b = m.group(2).strip()

        # Determinar si nuestro equipo es local (eq_a) o visita (eq_b)
        if _team_matches(nombre_equipo, eq_a):
            gf, gc = goles_a, goles_b
        elif _team_matches(nombre_equipo, eq_b):
            gf, gc = goles_b, goles_a
        else:
        # Debug útil: ver por qué no matchea
        # print(f"[H2H] Skip (no match) target='{nombre_equipo}' vs '{eq_a}' / '{eq_b}' | marcador={marcador}")
            continue

        partidos.append({
            "equipo": nombre_equipo,
            "goles_favor": gf,
            "goles_contra": gc,
        })

    print(f"[H2H] {nombre_equipo}: partidos parseados = {len(partidos)}")
    return partidos



def _norm_team_name(s: str) -> str:
    s = (s or "").strip().lower()

    # quitar tildes básico
    for a, b in (("á","a"),("é","e"),("í","i"),("ó","o"),("ú","u"),
                 ("ä","a"),("ë","e"),("ï","i"),("ö","o"),("ü","u"),
                 ("ñ","n")):
        s = s.replace(a, b)

    # unificar separadores
    s = s.replace(".", " ").replace(",", " ").replace("_", " ")
    s = s.replace("–", "-").replace("—", "-")

    # normalizar u20/sub20/sub-20/sub 20
    s = re.sub(r"\bsub[\s\-]*20\b", "sub20", s)
    s = re.sub(r"\bu[\s\-]*20\b", "sub20", s)
    s = re.sub(r"\bsub[\s\-]*19\b", "sub19", s)
    s = re.sub(r"\bu[\s\-]*19\b", "sub19", s)

    # colapsar espacios
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _team_matches(target: str, candidate: str) -> bool:
    """
    Matching tolerante:
    - igualdad normalizada
    - uno contenido en el otro (para casos "AC Milan Sub" vs "AC Milan Sub-20")
    - fallback por tokens principales
    """
    t = _norm_team_name(target)
    c = _norm_team_name(candidate)
    if not t or not c:
        return False

    if t == c:
        return True

    # contiene
    if t in c or c in t:
        return True

    # tokens: exigir que la mayoría de tokens largos estén presentes
    t_tokens = [x for x in t.split() if len(x) >= 3]
    if not t_tokens:
        return False
    hits = sum(1 for tok in t_tokens if tok in c)
    return hits / len(t_tokens) >= 0.6

def analizar_patrones_simple(historial: List[Dict]) -> Dict:
    """
    Calcula forma básica (G-E-P), índice de forma, medias de GF/GC
    y % de partidos con 3+ goles (Over 2.5).
    """
    g = e = p = 0
    gf_list, gc_list = [], []

    for h in historial:
        gf = h["goles_favor"]
        gc = h["goles_contra"]
        gf_list.append(gf)
        gc_list.append(gc)

        if gf == gc:
            e += 1
        elif gf > gc:
            g += 1
        else:
            p += 1

    total = len(historial)
    if total == 0:
        return {
            "forma_resumen": "0-0-0",
            "indice_forma": 0,
            "media_ga": 0.0,
            "media_gc": 0.0,
            "partidos": 0,
            "over25_perc": 0.0,
        }

    over25 = sum(1 for h in historial if h["goles_favor"] + h["goles_contra"] >= 3)
    over25_perc = over25 / total * 100

    return {
        "forma_resumen": f"{g}-{e}-{p}",
        "indice_forma": g * 3 + e,
        "media_ga": round(mean(gf_list), 2),
        "media_gc": round(mean(gc_list), 2),
        "partidos": total,
        "over25_perc": round(over25_perc, 1),
    }


def estimar_probabilidades_por_forma(pl: Dict, pv: Dict) -> Dict:
    """
    Probabilidades empíricas muy simples basadas en:
      - Índice de forma (G-E-P)
      - Fuerza ofensiva/defensiva (GA/GC)
    """
    pL, pE, pV = 0.45, 0.28, 0.27

    diff = pl.get("indice_forma", 0) - pv.get("indice_forma", 0)
    if diff > 0:
        pL += min(0.10, diff * 0.02)
    elif diff < 0:
        pV += min(0.10, abs(diff) * 0.02)

    gaL = pl.get("media_ga", 1.0)
    gcL = pl.get("media_gc", 1.0)
    gaV = pv.get("media_ga", 1.0)
    gcV = pv.get("media_gc", 1.0)

    fuerza_local = gaL - gcV
    fuerza_visita = gaV - gcL

    if fuerza_local - fuerza_visita > 0.4:
        pL += 0.05
    elif fuerza_visita - fuerza_local > 0.4:
        pV += 0.05

    # Normalizar y acotar
    pL = max(0.05, pL)
    pE = max(0.05, pE)
    pV = max(0.05, pV)

    s = pL + pE + pV
    pL /= s
    pE /= s
    pV /= s

    return {
        "p_local": round(pL * 100, 1),
        "p_empate": round(pE * 100, 1),
        "p_visita": round(pV * 100, 1),
    }