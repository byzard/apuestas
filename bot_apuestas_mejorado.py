import json
import os
import re
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from scoring_system import ScoringEngine, integrar_scoring_en_partido
from data_logger import ImprovedDataLogger, integrar_logger_en_main
from historical_from_h2h import (
    obtener_historial_desde_h2h,
    analizar_patrones_simple,
    estimar_probabilidades_por_forma,
)



# Para que esto funcione sin detener tu análisis de Flashscore, 
# deberías ejecutar 'bot.polling()' en un hilo (Thread) separado.


# --- Configuración ---
# Recomendación: configura estas variables mediante variables de entorno:
# TELEGRAM_BOT_TOKEN y TELEGRAM_CHAT_ID
idBot = os.getenv('TELEGRAM_BOT_TOKEN', '6868920020:AAHZ_cbnWQhyi3Rjp4JkFLgzHF95AnVPLxM')
idGrupo = os.getenv('TELEGRAM_CHAT_ID', '-1003331928750')
INTERVALO_ACTUALIZACION = 60
INDICE_FORMA_UMBRAL_ALTO = 15
INDICE_FORMA_UMBRAL_BAJO = 0

URL_LIVESCORE = 'https://m.flashscore.cl/?s=2'
URL_ESTADISTICAS_BASE = 'https://m.flashscore.cl/detalle-del-partido/{}/?s=2&t=estadisticas'

PARTIDOS_EN_SEGUIMIENTO: Dict[str, 'Partido'] = {}
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

DEBUG_ALERTAS = False

def debug(msg: str):
    if DEBUG_ALERTAS:
        print(f"[DEBUG] {msg}")


# --- Modelos de datos ---

class EstadisticasPartido:
    def __init__(self, minuto: int, g_local: int, g_visita: int, rojas_local: int, rojas_visita: int,
                remates_totales: int = 0, tiros_puerta: int = 0, corners: int = 0,
                posesion_local: int = 0, posesion_visita: int = 0, ataques_peligrosos: int = 0,
                remates_local: int = 0, remates_visita: int = 0,
                tiros_puerta_local: int = 0, tiros_puerta_visita: int = 0,
                corners_local: int = 0, corners_visita: int = 0,
                ataques_peligrosos_local: int = 0, ataques_peligrosos_visita: int = 0,
                faltas_local: int = 0, faltas_visita: int = 0,
                amarillas_local: int = 0, amarillas_visita: int = 0,
                grandes_ocasiones_local: int = 0, grandes_ocasiones_visita: int = 0,
                xgot_local: float = 0.0, xgot_visita: float = 0.0):
        self.minuto = minuto
        self.goles_local = g_local
        self.goles_visita = g_visita
        self.tarjetas_rojas_local = rojas_local
        self.tarjetas_rojas_visita = rojas_visita
        self.tarjetas_rojas_totales = rojas_local + rojas_visita
        self.grandes_ocasiones_local = grandes_ocasiones_local
        self.grandes_ocasiones_visita = grandes_ocasiones_visita
        self.xgot_local = xgot_local
        self.xgot_visita = xgot_visita

        # Totales agregados
        self.remates_totales = remates_totales
        self.tiros_puerta = tiros_puerta
        self.corners = corners
        self.posesion_local = posesion_local
        self.posesion_visita = posesion_visita
        self.ataques_peligrosos = ataques_peligrosos

        # Desglose por equipo
        self.remates_local = remates_local
        self.remates_visita = remates_visita
        self.tiros_puerta_local = tiros_puerta_local
        self.tiros_puerta_visita = tiros_puerta_visita
        self.corners_local = corners_local
        self.corners_visita = corners_visita
        self.ataques_peligrosos_local = ataques_peligrosos_local
        self.ataques_peligrosos_visita = ataques_peligrosos_visita
        self.faltas_local = faltas_local
        self.faltas_visita = faltas_visita
        self.amarillas_local = amarillas_local
        self.amarillas_visita = amarillas_visita

        # Derivadas
        self.diferencia_goles = abs(g_local - g_visita)
        self.posesion_equilibrada = (40 <= posesion_local <= 60 and posesion_local > 0)
        self.dominio_ofensivo_abs = abs(tiros_puerta_local - tiros_puerta_visita)
        self.dominio_ofensivo_rel = self.dominio_ofensivo_abs / (tiros_puerta if tiros_puerta > 0 else 1) * 100
        self.equipo_dominante = None
        if self.dominio_ofensivo_abs > 0:
            self.equipo_dominante = 'Local' if tiros_puerta_local > tiros_puerta_visita else 'Visita'


class Partido:
    def __init__(self, partido_id: str, equipos: str, liga: str):
        self.id = partido_id
        self.equipos = equipos
        self.liga = liga
        self.historial: List[EstadisticasPartido] = []
        self.estadisticas_actuales: Optional[EstadisticasPartido] = None
        self.perfil_local: Optional[Dict] = None
        self.perfil_visita: Optional[Dict] = None        
        self.probs_prematch: Optional[Dict] = None
        self.alerta_resumen_prematch_enviada = False
        self.tiene_estadisticas = False
        self.tiene_apuestas = False    
        self.clasificacion_local: Optional[Dict] = None
        self.clasificacion_visita: Optional[Dict] = None

        # Banderas anti-duplicado
        self.alerta_dominio_enviada = False
        self.alerta_roja_enviada = False
        self.alerta_descanso_enviada = False
        self.alerta_doble_roja_local_enviada = False
        self.alerta_doble_roja_visita_enviada = False
        self.alerta_doble_roja_ambos_enviada = False
        self.alerta_over_corners_final_enviada = False
        self.alerta_over15_abierto_enviada = False
        self.alerta_over_amarillas_enviada = False
        self.alerta_over_amarillas_ext_enviada = False
        self.alerta_corners_tempranos_enviada = False
        self.alerta_presion_sostenida_enviada = False
        self.alerta_empate_frictivo_enviada = False
        self.alerta_dominio_pose_ataques_enviada = False
        self.alerta_wave_local_enviada = False
        self.alerta_wave_visita_enviada = False
        self.alerta_rebote_local_enviada = False
        self.alerta_rebote_visita_enviada = False
        self.alerta_friccion_presion_gol_enviada = False
        self.alerta_dominio_silencioso_local_enviada = False
        self.alerta_dominio_silencioso_visita_enviada = False
        self.alerta_scoring_enviada = False

        # Minuto de primera roja por equipo
        self.minuto_primera_roja_local: Optional[int] = None
        self.minuto_primera_roja_visita: Optional[int] = None

    def actualizar_stats(self, nueva_stats: EstadisticasPartido):
        prev_l = self.estadisticas_actuales.tarjetas_rojas_local if self.estadisticas_actuales else 0
        prev_v = self.estadisticas_actuales.tarjetas_rojas_visita if self.estadisticas_actuales else 0

        if not self.historial or self.historial[-1].minuto != nueva_stats.minuto:
            self.historial.append(nueva_stats)
        self.estadisticas_actuales = nueva_stats
        if len(self.historial) > 30:
            self.historial = self.historial[-30:]

        if nueva_stats.tarjetas_rojas_local > prev_l and self.minuto_primera_roja_local is None:
            self.minuto_primera_roja_local = nueva_stats.minuto
        if nueva_stats.tarjetas_rojas_visita > prev_v and self.minuto_primera_roja_visita is None:
            self.minuto_primera_roja_visita = nueva_stats.minuto

    def calcular_momentum(self, minutos: int) -> Dict[str, Dict[str, int]]:
        if not self.estadisticas_actuales:
            return {'local': {}, 'visita': {}}

        stats_actual = self.estadisticas_actuales
        minuto_inicio = stats_actual.minuto - minutos

        stats_inicio = next(
            (s for s in reversed(self.historial) if s.minuto <= minuto_inicio),
            self.historial[0] if self.historial else stats_actual
        )

        if stats_inicio is stats_actual and len(self.historial) <= 1:
            return {'local': {}, 'visita': {}}

        momentum_local = {
            'remates': max(0, stats_actual.remates_local - stats_inicio.remates_local),
            'tiros_puerta': max(0, stats_actual.tiros_puerta_local - stats_inicio.tiros_puerta_local),
            'corners': max(0, stats_actual.corners_local - stats_inicio.corners_local),
        }
        momentum_visita = {
            'remates': max(0, stats_actual.remates_visita - stats_inicio.remates_visita),
            'tiros_puerta': max(0, stats_actual.tiros_puerta_visita - stats_inicio.tiros_puerta_visita),
            'corners': max(0, stats_actual.corners_visita - stats_inicio.corners_visita),
        }

        return {'local': momentum_local, 'visita': momentum_visita}


# --- Funciones auxiliares ---
class PerfilCornersLiga:
    """Almacena promedios históricos de córners por liga"""
    
    # Promedios de córners por partido según liga (ajusta según tus observaciones)
    PROMEDIOS_LIGA = {
        # Ligas Top (Tendencia al alza por tiempos de descuento extendidos)
        'premier league': 11.5,      # Subió ligeramente
        'championship': 11.0,        
        'bundesliga': 10.8,
        'serie a': 10.2,             # Mayor verticalidad recientemente
        'la liga': 9.6,
        'ligue 1': 9.4,
        
        # Ligas secundarias europeas (Muy ofensivas)
        'eredivisie': 10.8,          # Sigue siendo de las más altas
        'liga portugal': 10.1,       # Incremento en competitividad
        'super lig': 10.2,           # Partidos muy rotos en Turquía
        'jupiler pro league': 10.5,  # Alta intensidad
        'scottish premiership': 10.8, # Nueva adición sugerida (muy alta en corners)
        
        # Ligas americanas
        'mls': 10.4,                 # Liga con mucho "over"
        'brasileirao': 10.2,         # Ojo: Ha subido mucho, ya no es tan "under"
        'liga profesional': 9.2,     # Argentina (subió de 8.0)
        'primera division chile': 9.4, 
        'liga mx': 9.8,              # México suele ser estable en estos rangos
        
        # Ligas asiáticas y otras
        'j1 league': 9.5,
        'k league': 9.2,
        'a-league': 10.9,            # Australia es excelente para overs
        
        # Default balanceado
        'default': 9.8               # El promedio global ha subido de 9.5 a casi 10
    }
    
    @staticmethod
    def obtener_promedio(nombre_liga: str) -> float:
        """Obtiene el promedio de córners para una liga"""
        nombre_norm = _normalizar_nombre_equipo(nombre_liga)
        return PerfilCornersLiga.PROMEDIOS_LIGA.get(nombre_norm, 
                                                     PerfilCornersLiga.PROMEDIOS_LIGA['default'])

def construir_snapshot(stats: EstadisticasPartido) -> Dict:
    """Convierte EstadisticasPartido a formato dict para scoring"""
    return {
        'minute': stats.minuto,
        'score_home': stats.goles_local,
        'score_away': stats.goles_visita,
        'home': {
            'xg': 0.0,
            'shots': stats.remates_local,
            'shots_on_target': stats.tiros_puerta_local,
            'dangerous_attacks': stats.ataques_peligrosos_local,
            'possession': stats.posesion_local / 100.0,
            'corners': stats.corners_local,
            'fouls': stats.faltas_local,
            'yellow_cards': stats.amarillas_local,
            'red_cards': stats.tarjetas_rojas_local,
            'last5_momentum': 0.0,
            'big_chances': stats.grandes_ocasiones_local,
            'xgot': stats.xgot_local,
            'avg_shot_quality': None
        },
        'away': {
            'xg': 0.0,
            'shots': stats.remates_visita,
            'shots_on_target': stats.tiros_puerta_visita,
            'dangerous_attacks': stats.ataques_peligrosos_visita,
            'possession': stats.posesion_visita / 100.0,
            'corners': stats.corners_visita,
            'fouls': stats.faltas_visita,
            'yellow_cards': stats.amarillas_visita,
            'red_cards': stats.tarjetas_rojas_visita,
            'big_chances': stats.grandes_ocasiones_visita,
            'xgot': stats.xgot_visita,
            'last5_momentum': 0.0,
            'avg_shot_quality': None
        },
        'league_weight': 1.0
    }


def _normalizar_nombre_stat(nombre: str) -> str:
    n = nombre.lower()
    for a, b in (('á', 'a'), ('é', 'e'), ('í', 'i'), ('ó', 'o'), ('ú', 'u'),
                ('ä', 'a'), ('ë', 'e'), ('ï', 'i'), ('ö', 'o'), ('ü', 'u')):
        n = n.replace(a, b)
    return n.strip()


def _normalizar_nombre_equipo(nombre: str) -> str:
    nombre = nombre.lower().strip()
    transl = str.maketrans("áéíóúäëïöüñ", "aeiouaeioun")
    nombre = nombre.translate(transl)
    nombre = re.sub(r'\s+', ' ', nombre)
    nombre = nombre.replace(".", "")
    return nombre


def iniciar_seguimiento_partido(partido_id: str, equipos_str: str, liga: str, info_basica: dict):
    """Inicializa un objeto Partido y carga sus perfiles históricos usando H2H"""
    if partido_id in PARTIDOS_EN_SEGUIMIENTO:
        return PARTIDOS_EN_SEGUIMIENTO[partido_id]

    partido = Partido(partido_id, equipos_str, liga)
    
    partido.equipo_local = info_basica.get('equipo_local', equipos_str.split(" - ")[0])
    partido.equipo_visita = info_basica.get('equipo_visita', equipos_str.split(" - ")[1])

    # Clasificación de la liga
    try:
        print(f"⚙️ Cargando clasificación de liga para: {equipos_str}...")
        tabla = _obtener_clasificacion_liga(partido_id)

        def _buscar_equipo(tabla, nombre_objetivo):
            objetivo_norm = _normalizar_nombre_equipo(nombre_objetivo)
            for fila in tabla:
                if _normalizar_nombre_equipo(fila["equipo"]) == objetivo_norm:
                    return fila
            return None

        if tabla:
            partido.clasificacion_local = _buscar_equipo(tabla, partido.equipo_local)
            partido.clasificacion_visita = _buscar_equipo(tabla, partido.equipo_visita)

            if partido.clasificacion_local and partido.clasificacion_visita:
                cl = partido.clasificacion_local
                cv = partido.clasificacion_visita
                print(
                    f"📈 CLASIFICACIÓN:\n"
                    f"  Local ({partido.equipo_local})  Pos:{cl['pos']}  DG:{cl['dg']}  Pts:{cl['pts']}\n"
                    f"  Visita ({partido.equipo_visita}) Pos:{cv['pos']}  DG:{cv['dg']}  Pts:{cv['pts']}"
                )
        else:
            print(f"⚠️ No se pudo obtener clasificación para {equipos_str}")

    except Exception as e:
        print(f"⚠️ ERROR al cargar clasificación de liga para {equipos_str}: {e}")
    
    partido.tiene_estadisticas = info_basica.get('tiene_estadisticas', False)
    partido.tiene_apuestas = info_basica.get('tiene_apuestas', False)

    # Obtener historiales desde H2H
    print(f"\n⚙️ Iniciando análisis histórico (H2H) para: {equipos_str}...")
    try:
        hist_local, hist_visita, nombre_local_h2h, nombre_visita_h2h = \
            obtener_historial_desde_h2h(partido_id)

        perfil_local = analizar_patrones_simple(hist_local)
        perfil_visita = analizar_patrones_simple(hist_visita)

        partido.perfil_local = perfil_local
        partido.perfil_visita = perfil_visita
        partido.probs_prematch = estimar_probabilidades_por_forma(perfil_local, perfil_visita)

        print("✅ Análisis histórico (H2H) completado.")
        print("--- 📋 PERFIL HISTÓRICO (FORMA) CARGADO ---")
        print(f"🏠 LOCAL ({nombre_local_h2h}): Forma {perfil_local.get('forma_resumen','N/D')}, "
              f"GA/GC: {perfil_local.get('media_ga', 0.0):.2f}/{perfil_local.get('media_gc', 0.0):.2f}")
        print(f"✈️ VISITA ({nombre_visita_h2h}): Forma {perfil_visita.get('forma_resumen','N/D')}, "
              f"GA/GC: {perfil_visita.get('media_ga', 0.0):.2f}/{perfil_visita.get('media_gc', 0.0):.2f}")
        print("-------------------------------------------")

    except Exception as e:
        print(f"⚠️ ERROR al cargar historial H2H para {equipos_str}. Continuará sin análisis histórico. Error: {e}")

    PARTIDOS_EN_SEGUIMIENTO[partido_id] = partido
    return partido


# --- Estrategias de análisis ---

class EstrategiaAnalisis:
    
    @staticmethod
    def alerta_corners_ritmo_alto(partido: Partido) -> Optional[str]:
        s = partido.estadisticas_actuales
        # Validación: evitar alertas si no hay estadísticas reales
        if not s or not partido.tiene_estadisticas:
            return None

        if s.minuto < 15 or s.minuto > 75:
            return None

        if s.corners < 4:
            return None

        ritmo_actual = s.corners / s.minuto
        corners_proyectados = ritmo_actual * 90

        promedio_liga = PerfilCornersLiga.obtener_promedio(partido.liga)
        std_liga = promedio_liga * 0.20  # desviación estándar estimada

        z_score = (corners_proyectados - promedio_liga) / std_liga if std_liga > 0 else 0

        if z_score < 1.5:  # solo alertar si es estadísticamente significativo
            return None

        mom10 = partido.calcular_momentum(10)
        c10 = mom10['local'].get('corners', 0) + mom10['visita'].get('corners', 0)
        if s.minuto >= 30 and c10 < 2:
            return None

        if getattr(partido, 'alerta_corners_ritmo_alto_enviada', False):
            return None
        partido.alerta_corners_ritmo_alto_enviada = True

        explicacion = f"Ritmo actual {ritmo_actual:.2f} corners/min, Z-score {z_score:.2f} indica ritmo inusual alto."

        return (
            f'📊 ALERTA RITMO ALTO DE CORNERS {s.minuto}\'\n'
            f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
            f'🎯 Corners actuales: {s.corners} ({s.corners_local}-{s.corners_visita})\n'
            f'📈 Proyección 90\': {corners_proyectados:.1f} corners\n'
            f'📊 Promedio liga: {promedio_liga:.1f} corners\n'
            f'🔍 Z-score: {z_score:.2f} (indicador de rareza)\n\n'
            f'⚡ U10: {c10} corners\n'
            f'💡 Explicación: {explicacion}\n'
            f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
        )
    

    @staticmethod
    def alerta_corners_ritmo_bajo(partido: Partido) -> Optional[str]:
        """Detecta cuando el ritmo de córners proyectado para 90' está muy por debajo del promedio de la liga."""
        s = partido.estadisticas_actuales

        # Validaciones básicas
        if not s or not partido.tiene_estadisticas:
            return None
        if s.minuto < 25 or s.minuto > 75:
            return None

        # Evitar falsos positivos con muy pocos córners
        if s.corners < 1:
            return None

        # Ritmo lineal actual (corners por minuto)
        ritmo_actual = s.corners / s.minuto if s.minuto > 0 else 0.0
        proyeccion_lineal = ritmo_actual * 90.0  # proyección simple

        # Factor para 2º tiempo (los segundos tiempos tienden a ser algo más activos en corners)
        if s.minuto <= 45:
            factor_st = 1.20  # asume +20% en promedio en 2T respecto a ritmo PT
        elif s.minuto <= 60:
            factor_st = 1.10
        else:
            factor_st = 1.00

        # Estimación alternativa usando regresión a la media:
        promedio_liga = PerfilCornersLiga.obtener_promedio(partido.liga)
        # Si estamos en el descanso o muy pronto, aplicamos más peso a la media de liga
        if s.minuto <= 45:
            peso_lineal = 0.55
            peso_liga = 0.40
            peso_mom = 0.05
        else:
            peso_lineal = 0.70
            peso_liga = 0.20
            peso_mom = 0.10

        # Momentum reciente (U10) — si hay actividad reciente, la usamos para ajustar
        mom10 = partido.calcular_momentum(10)
        c10 = mom10['local'].get('corners', 0) + mom10['visita'].get('corners', 0)

        # Escalar c10 a una contribución estimada (regla heurística)
        contrib_momentum = 0.0
        if c10 >= 3:
            contrib_momentum = 1.5
        elif c10 == 2:
            contrib_momentum = 1.0
        elif c10 == 1:
            contrib_momentum = 0.5

        # Proyección ajustada combinando lineal, liga y momentum
        proyeccion_ajustada = (
            peso_lineal * (proyeccion_lineal * factor_st) +
            peso_liga * promedio_liga +
            peso_mom * (contrib_momentum + proyeccion_lineal * 0.05)
        )

        # Asegurar que la variable esté definida y sea float
        corners_proyectados = float(proyeccion_ajustada)

        # Cálculo diferencia porcentual respecto al promedio de la liga
        diferencia_pct = ((promedio_liga - corners_proyectados) / promedio_liga) * 100 if promedio_liga > 0 else 0.0

        # Umbral para alertar: al menos -30% respecto al promedio (ajusta según prefieras)
        if diferencia_pct < 30:
            return None

        # Evitar alertas duplicadas
        if getattr(partido, 'alerta_corners_ritmo_bajo_enviada', False):
            return None
        partido.alerta_corners_ritmo_bajo_enviada = True

        # Probabilidades heurísticas (mantén la lógica que te guste)
        under_95 = "✅ Alta" if corners_proyectados <= 8.5 else "⚠️ Media" if corners_proyectados <= 9.5 else "❌ Baja"
        under_85 = "✅ Alta" if corners_proyectados <= 7.5 else "⚠️ Media" if corners_proyectados <= 8.5 else "❌ Baja"
        under_75 = "✅ Alta" if corners_proyectados <= 6.5 else "⚠️ Media" if corners_proyectados <= 7.5 else "❌ Baja"

        return (
            f'📉 ALERTA RITMO BAJO DE CORNERS {s.minuto}\'\n'
            f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
            f'🎯 Corners actuales: {s.corners} ({s.corners_local}-{s.corners_visita})\n'
            f'📉 Proyección 90\': {corners_proyectados:.1f} corners\n'
            f'📊 Promedio liga: {promedio_liga:.1f} corners\n'
            f'❄️ Diferencia: -{diferencia_pct:.0f}%\n\n'
            f'Probabilidades:\n'
            f'• Under 9.5: {under_95}\n'
            f'• Under 8.5: {under_85}\n'
            f'• Under 7.5: {under_75}\n'
            f'⚡ U10: {c10} corners\n'
            f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
        )   
    
    @staticmethod
    def alerta_corners_desequilibrio(partido: Partido) -> Optional[str]:
        """Detecta cuando un equipo tiene muchos más córners que el otro"""
        s = partido.estadisticas_actuales
        # Validación: evitar alertas si no hay estadísticas reales
        if not s or not partido.tiene_estadisticas:
            return None

        if s.minuto < 20 or s.minuto > 80:
            return None
        
        if s.corners < 5:  # Mínimo de córners totales
            return None
        
        diferencia = abs(s.corners_local - s.corners_visita)
        
        # Solo alertar si hay desequilibrio significativo
        if diferencia < 4:
            return None
        
        # Identificar equipo dominante
        if s.corners_local > s.corners_visita:
            equipo_dominante = partido.equipos.split(' - ')[0]
            corners_dom = s.corners_local
            corners_otro = s.corners_visita
            lado = 'local'
        else:
            equipo_dominante = partido.equipos.split(' - ')[1]
            corners_dom = s.corners_visita
            corners_otro = s.corners_local
            lado = 'visita'
        
        # Verificar momentum del equipo dominante
        mom10 = partido.calcular_momentum(10)
        c10_dom = mom10[lado].get('corners', 0)
        
        # Si el dominante no tiene momentum reciente, no alertar
        if c10_dom < 1:
            return None
        
        flag = f'alerta_corners_desequilibrio_{lado}_enviada'
        if getattr(partido, flag, False):
            return None
        setattr(partido, flag, True)
        
        # ✅ CORRECCIÓN: Proyección realista con regresión a la media
        ritmo_actual = corners_dom / s.minuto if s.minuto > 0 else 0
        proyeccion_lineal = ritmo_actual * 90
        
        # Aplicar regresión a la media según el minuto
        # Cuanto más temprano, más regresión aplicamos
        if s.minuto <= 30:
            factor_regresion = 0.5  # 50% regresión
        elif s.minuto <= 45:
            factor_regresion = 0.65
        elif s.minuto <= 60:
            factor_regresion = 0.80
        else:
            factor_regresion = 0.90
        
        # Promedio realista para un equipo dominante en corners (8-10)
        promedio_realista = 9.0
        proyeccion_dom = (proyeccion_lineal * factor_regresion) + (promedio_realista * (1 - factor_regresion))
        
        # Límite máximo realista: 15 corners individuales
        proyeccion_dom = min(proyeccion_dom, 15.0)
        
        # ✅ Calcular líneas de mercado alcanzables
        lineas_individuales = [4.5, 5.5, 6.5, 7.5, 8.5, 9.5]
        linea_sugerida = None
        for linea in lineas_individuales:
            if proyeccion_dom >= linea + 1.5:  # Margen de seguridad
                linea_sugerida = linea
        
        mercado_sugerido = f"• {equipo_dominante} Over {linea_sugerida} córners individuales" if linea_sugerida else f"• Siguiente córner: {equipo_dominante}"
        
        return (
            f'⚖️ ALERTA DESEQUILIBRIO DE CORNERS {s.minuto}\'\n'
            f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
            f'🔥 {equipo_dominante} domina en córners:\n'
            f'📊 Corners: {equipo_dominante}:{corners_dom} vs {corners_otro} (Δ{diferencia})\n'
            f'📈 Proyección {equipo_dominante}: {proyeccion_dom:.1f} corners\n'
            f'⚡ U10 {equipo_dominante}: {c10_dom} corners\n\n'
            f'💡 Mercado sugerido:\n'
            f'{mercado_sugerido}\n'
            f'• Siguiente córner: {equipo_dominante}\n'
            f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
        )
    
    @staticmethod
    def alerta_corners_segundo_tiempo(partido: Partido) -> Optional[str]:
        """Alerta específica para Over córners en segundo tiempo"""
        s = partido.estadisticas_actuales
        if not s or not partido.tiene_estadisticas:
            return None

        if s.minuto < 50 or s.minuto > 70:
            return None
        
        # Estimar córners del primer tiempo (asumiendo que min 45 = HT)
        if s.minuto <= 50:
            corners_1t = s.corners
        else:
            # Estimar proporcionalmente
            corners_1t = int(s.corners * (45 / s.minuto))
        
        # Calcular ritmo del segundo tiempo
        if s.minuto > 45:
            corners_2t = s.corners - corners_1t
            minutos_2t = s.minuto - 45
            ritmo_2t = corners_2t / minutos_2t if minutos_2t > 0 else 0
            proyeccion_2t = ritmo_2t * 45
        else:
            return None
        
        # Solo alertar si el segundo tiempo va más rápido que el primero
        if corners_2t < 3 or proyeccion_2t <= corners_1t:
            return None
        
        # Verificar momentum reciente
        mom10 = partido.calcular_momentum(10)
        c10 = mom10['local'].get('corners', 0) + mom10['visita'].get('corners', 0)
        
        if c10 < 2:
            return None
        
        if getattr(partido, 'alerta_corners_segundo_tiempo_enviada', False):
            return None
        partido.alerta_corners_segundo_tiempo_enviada = True
        
        diferencia_pct = ((proyeccion_2t - corners_1t) / corners_1t * 100) if corners_1t > 0 else 0
        
        return (
            f'⏱️ ALERTA CORNERS 2T ACELERADO {s.minuto}\'\n'
            f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
            f'📊 Corners 1T: ~{corners_1t}\n'
            f'📊 Corners 2T (actual): {corners_2t}\n'
            f'📈 Proyección 2T: {proyeccion_2t:.1f} corners\n'
            f'🔥 Ritmo 2T: +{diferencia_pct:.0f}% vs 1T\n'
            f'⚡ U10: {c10} corners\n\n'
            f'💡 El segundo tiempo está más movido que el primero\n'
            f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
        )
    
    @staticmethod
    def alerta_corners_tramo_final_live(partido: Partido) -> Optional[str]:
        """Alerta para Over córners en los últimos 15 minutos"""
        s = partido.estadisticas_actuales
        if not s or not partido.tiene_estadisticas:
            return None

        if s.minuto < 75 or s.minuto > 88:
            return None
        
        # Verificar si está cerca de una línea de Over común
        lineas_over = [8.5, 9.5, 10.5, 11.5, 12.5]
        linea_objetivo = None
        
        for linea in lineas_over:
            if s.corners == int(linea) or s.corners == int(linea) - 1:
                linea_objetivo = linea
                break
        
        if not linea_objetivo:
            return None
        
        corners_necesarios = int(linea_objetivo) + 1 - s.corners
        
        # Verificar momentum reciente
        mom10 = partido.calcular_momentum(10)
        c10 = mom10['local'].get('corners', 0) + mom10['visita'].get('corners', 0)
        
        # Calcular ritmo reciente
        ritmo_reciente = c10 / 10
        
        # Solo alertar si el ritmo reciente sugiere que se puede alcanzar
        minutos_restantes = 90 - s.minuto
        corners_proyectados_restantes = ritmo_reciente * minutos_restantes
        
        if corners_proyectados_restantes < corners_necesarios - 0.5:
            return None
        
        # Verificar que el partido esté abierto (no muy desequilibrado en goles)
        if s.diferencia_goles > 2:
            return None
        
        flag = f'alerta_corners_tramo_final_live_{int(linea_objetivo)}_enviada'
        if getattr(partido, flag, False):
            return None
        setattr(partido, flag, True)
        
        probabilidad = "Alta" if corners_proyectados_restantes >= corners_necesarios + 0.5 else "Media"
        
        return (
            f'🎯 ALERTA OVER {linea_objetivo} CORNERS ALCANZABLE {s.minuto}\'\n'
            f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
            f'📊 Corners actuales: {s.corners} ({s.corners_local}-{s.corners_visita})\n'
            f'🎯 Necesarios para Over {linea_objetivo}: {corners_necesarios}\n'
            f'⏱️ Minutos restantes: ~{minutos_restantes}\n'
            f'📈 Ritmo U10: {ritmo_reciente:.2f} corners/min\n'
            f'🔮 Proyección restante: {corners_proyectados_restantes:.1f} corners\n'
            f'✅ Probabilidad: {probabilidad}\n'
            f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
        )
    
    
    @staticmethod
    def alerta_brecha_clasificacion(partido: 'Partido') -> Optional[str]:
        """Alerta cuando hay una brecha fuerte en diferencia de goles (DG) y/o posición"""
        if getattr(partido, 'alerta_brecha_clasificacion_enviada', False):
            return None

        cl = getattr(partido, 'clasificacion_local', None)
        cv = getattr(partido, 'clasificacion_visita', None)
        if not cl or not cv:
            return None

        dg_L = cl["dg"]
        dg_V = cv["dg"]
        diff_dg = abs(dg_L - dg_V)

        pos_L = cl["pos"]
        pos_V = cv["pos"]
        diff_pos = abs(pos_L - pos_V)

        UMBRAL_DG = 15
        UMBRAL_POS = 3

        if diff_dg < UMBRAL_DG and diff_pos < UMBRAL_POS:
            return None

        partido.alerta_brecha_clasificacion_enviada = True

        if dg_L > dg_V:
            favorito = partido.equipo_local
            dg_fav = dg_L
            dg_und = dg_V
            pos_fav = pos_L
            pos_und = pos_V
            lado_fav = "LOCAL"
        else:
            favorito = partido.equipo_visita
            dg_fav = dg_V
            dg_und = dg_L
            pos_fav = pos_V
            pos_und = pos_L
            lado_fav = "VISITA"

        return (
            f"📊 ALERTA BRECHA CLASIFICACIÓN\n"
            f"{partido.equipos} (DG Liga)\n\n"
            f"{lado_fav} ({favorito}) con clara ventaja:\n"
            f"• Posición: {pos_fav} vs {pos_und}\n"
            f"• DG: {dg_fav:+d} vs {dg_und:+d} (ΔDG = {diff_dg})\n"
            f"Esto sugiere una diferencia importante de nivel en la liga.\n"
            f"🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}"
        )
    
    @staticmethod
    def alerta_over25_con_edge(partido: 'Partido') -> Optional[str]:
        """Alerta Over 2.5 solo si hay edge positivo"""
        s = partido.estadisticas_actuales
        if not s or s.minuto < 60 or s.minuto > 85:
            return None
        
        goles_actuales = s.goles_local + s.goles_visita
        
        if goles_actuales >= 3:
            return None
        
        if goles_actuales == 2:
            prob_base = 0.55
        elif goles_actuales == 1:
            prob_base = 0.35
        else:
            prob_base = 0.20
        
        if s.remates_totales >= 20:
            prob_base += 0.10
        if s.tiros_puerta >= 8:
            prob_base += 0.08
        if s.corners >= 10:
            prob_base += 0.05
        
        mom10 = partido.calcular_momentum(10)
        tp_10 = mom10['local'].get('tiros_puerta', 0) + mom10['visita'].get('tiros_puerta', 0)
        if tp_10 >= 3:
            prob_base += 0.12
        elif tp_10 >= 2:
            prob_base += 0.07
        
        minutos_restantes = 90 - s.minuto
        if minutos_restantes <= 15:
            prob_base *= 1.15
        elif minutos_restantes <= 25:
            prob_base *= 1.08
        
        if s.posesion_equilibrada and s.diferencia_goles <= 1:
            prob_base += 0.05
        
        prob_over25 = max(0.05, min(0.85, prob_base))
        
        from edge_calculator import EdgeCalculator
        tiene_valor, edge, explicacion = EdgeCalculator.tiene_valor(prob_over25, 'over_2.5')
        
        if not tiene_valor:
            return None
        
        if getattr(partido, 'alerta_over25_edge_enviada', False):
            return None
        partido.alerta_over25_edge_enviada = True
        
        if edge >= 20: 
            estrellas = "⭐⭐⭐⭐⭐"
        elif edge >= 15: 
            estrellas = "⭐⭐⭐⭐"
        elif edge >= 10: 
            estrellas = "⭐⭐⭐"
        else: 
            estrellas = "⭐⭐"
        
        return (
            f'💰 ALERTA OVER 2.5 (VALOR) {s.minuto}\' {estrellas}\n'
            f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
            f'{explicacion}\n'
            f'📈 Stats: R:{s.remates_totales}, TP:{s.tiros_puerta}, C:{s.corners}\n'
            f'⚡ U10: TP:{tp_10}\n'
            f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
        )
    
    @staticmethod
    def alerta_resumen_prematch(partido: 'Partido') -> Optional[str]:
        """Envía un resumen de los últimos 5 partidos de cada equipo (forma reciente)"""
        if partido.alerta_resumen_prematch_enviada:
            return None

        perfil_local = partido.perfil_local or {}
        perfil_visita = partido.perfil_visita or {}
        probs = partido.probs_prematch or {}
        
        indice_L = perfil_local.get("indice_forma", 0)
        indice_V = perfil_visita.get("indice_forma", 0)

        cumple_condicion = False
        
        if indice_L >= INDICE_FORMA_UMBRAL_ALTO or indice_L == INDICE_FORMA_UMBRAL_BAJO:
            cumple_condicion = True

        if indice_V >= INDICE_FORMA_UMBRAL_ALTO or indice_V == INDICE_FORMA_UMBRAL_BAJO:
            cumple_condicion = True

        if not cumple_condicion:
            return None
        
        forma_L = perfil_local.get("forma_resumen", "0-0-0")
        forma_V = perfil_visita.get("forma_resumen", "0-0-0")

        gaL = perfil_local.get("media_ga", 0.0)
        gcL = perfil_local.get("media_gc", 0.0)
        overL = perfil_local.get("over25_perc", 0.0)

        gaV = perfil_visita.get("media_ga", 0.0)
        gcV = perfil_visita.get("media_gc", 0.0)
        overV = perfil_visita.get("over25_perc", 0.0)

        pL = probs.get("p_local", 0.0)
        pE = probs.get("p_empate", 0.0)
        pV = probs.get("p_visita", 0.0)
        
        partido.alerta_resumen_prematch_enviada = True
        
        return (
            f"📊 RESUMEN PRE-PARTIDO (últimos 5 partidos)\n"
            f"{partido.equipo_local} - {partido.equipo_visita}\n\n"
            f"🏠 {partido.equipo_local}:\n"
            f"Forma: {forma_L}  (Índice: {indice_L})\n"
            f"GF/GC: {gaL:.2f}/{gcL:.2f}  | Over2.5: {overL:.1f}%\n\n"
            f"✈️ {partido.equipo_visita}:\n"
            f"Forma: {forma_V}  (Índice: {indice_V})\n"
            f"GF/GC: {gaV:.2f}/{gcV:.2f}  | Over2.5: {overV:.1f}%\n\n"
            f"📈 Probabilidades estimadas por forma reciente:\n"
            f"Local: {pL:.1f}%  |  Empate: {pE:.1f}%  |  Visita: {pV:.1f}%\n"
            f"🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}"
        )
    
    @staticmethod
    def alerta_dominio_prematch(partido: 'Partido') -> Optional[str]:
        """Detecta un dominio histórico claro de un equipo sobre el otro"""
        p_local = partido.perfil_local
        p_visita = partido.perfil_visita
        s = partido.estadisticas_actuales

        if not p_local or not p_visita or s.minuto > 1:
            return None
        
        if getattr(partido, 'alerta_dominio_prematch_enviada', False):
            return None

        diff_forma = p_local['indice_forma'] - p_visita['indice_forma']
        
        score_local = p_local['media_ga'] - p_visita['media_gc']
        score_visita = p_visita['media_ga'] - p_local['media_gc']
        
        if diff_forma >= 5 and score_local >= 0.5:
            equipo_dominante = partido.equipos.split(' - ')[0]
            partido.alerta_dominio_prematch_enviada = True
            return (
                f'👑 ALERTA DOMINIO HISTÓRICO ({s.minuto}\')\n'
                f'{partido.equipos} (0-0)\n'
                f'El **LOCAL ({equipo_dominante})** domina claramente en forma:\n'
                f'Local Forma: {p_local["forma_resumen"]} | Goles Esperados: {p_local["media_ga"]:.2f} vs {p_visita["media_gc"]:.2f}\n'
                f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
            )
        
        if diff_forma <= -5 and score_visita >= 0.5:
            equipo_dominante = partido.equipos.split(' - ')[1]
            partido.alerta_dominio_prematch_enviada = True
            return (
                f'👑 ALERTA DOMINIO HISTÓRICO ({s.minuto}\')\n'
                f'{partido.equipos} (0-0)\n'
                f'La **VISITA ({equipo_dominante})** domina claramente en forma:\n'
                f'Visita Forma: {p_visita["forma_resumen"]} | Goles Esperados: {p_visita["media_ga"]:.2f} vs {p_local["media_gc"]:.2f}\n'
                f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
            )

        return None

    # Helpers
    @staticmethod
    def _equipo_dominante_por_tp(stats: EstadisticasPartido) -> int:
        if stats.tiros_puerta_local >= stats.tiros_puerta_visita + 2:
            return 1
        if stats.tiros_puerta_visita >= stats.tiros_puerta_local + 2:
            return -1
        return 0

    @staticmethod
    def _ratio_amarillas_por_falta(s: EstadisticasPartido) -> float:
        faltas_tot = s.faltas_local + s.faltas_visita
        amarillas_tot = s.amarillas_local + s.amarillas_visita
        if faltas_tot <= 0:
            return 0.0
        return amarillas_tot / float(faltas_tot)

    # Alertas en vivo

    @staticmethod
    def alerta_goleada_temprana(partido: Partido) -> Optional[str]:
        s = partido.estadisticas_actuales
        if not s or s.minuto > 20:
            return None

        if s.goles_local >= 4 or s.goles_visita >= 4:
            if getattr(partido, 'alerta_goleada_temprana_enviada', False):
                return None
            partido.alerta_goleada_temprana_enviada = True

            equipo_goleador = (
                partido.equipos.split(' - ')[0] if s.goles_local >= 4 else partido.equipos.split(' - ')[1]
            )
            return (
                f'⚡⚽ ALERTA GOLEADA TEMPRANA ({s.minuto}\')\n'
                f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
                f'{equipo_goleador} ya suma 4+ goles antes del min 20.\n'
                f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
            )
        return None
    
    @staticmethod
    def alerta_corners_tempranos(partido: Partido) -> Optional[str]:
        s = partido.estadisticas_actuales
        if not s or not partido.tiene_estadisticas:
            return None
        if s.minuto >= 30 or s.corners <= 5:
            return None
        if partido.alerta_corners_tempranos_enviada:
            return None
        partido.alerta_corners_tempranos_enviada = True
        return (
            f'⛳🔥 ALERTA CORNERS TEMPRANOS {s.minuto}\'\n'
            f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
            f'🎯 ¡{s.corners} córners en solo {s.minuto} minutos!\n'
            f'📊 Local {s.corners_local} - Visita {s.corners_visita}\n'
            f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
        )

    @staticmethod
    def alerta_over_amarillas_pro(partido: Partido) -> Optional[str]:
        s = partido.estadisticas_actuales
        if not s:
            return None
        if s.minuto < 60 or s.minuto > 88 or s.diferencia_goles > 1:
            return None

        faltas_tot = s.faltas_local + s.faltas_visita
        amarillas_tot = s.amarillas_local + s.amarillas_visita
        amarillas_local = s.amarillas_local
        amarillas_visita = s.amarillas_visita
        ratio_af = EstrategiaAnalisis._ratio_amarillas_por_falta(s)

        if s.minuto >= 70 and faltas_tot >= 24 and amarillas_tot <= 2 and ratio_af < 0.11:
            return None

        mom10 = partido.calcular_momentum(10)
        tp10 = mom10['local'].get('tiros_puerta', 0) + mom10['visita'].get('tiros_puerta', 0)
        r10 = mom10['local'].get('remates', 0) + mom10['visita'].get('remates', 0)
        c10 = mom10['local'].get('corners', 0) + mom10['visita'].get('corners', 0)

        cond_calidad_alta = (
            faltas_tot >= 22 and
            (amarillas_tot >= 4 or (amarillas_tot >= 3 and (tp10 >= 3 or r10 >= 6 or c10 >= 3))) and
            ratio_af >= 0.14
        )
        cond_oportunidad = (
            s.minuto >= 70 and
            s.diferencia_goles == 0 and
            faltas_tot >= 26 and
            amarillas_tot >= 3 and
            (tp10 >= 1 or (r10 >= 2 and c10 >= 1))
        )
        if not (cond_calidad_alta or cond_oportunidad):
            return None

        if getattr(partido, 'alerta_over_amarillas_ext_enviada', False):
            return None
        partido.alerta_over_amarillas_ext_enviada = True

        motivo = "CALIDAD ALTA" if cond_calidad_alta else "OPORTUNIDAD"
        return (
            f'🟨 ALERTA OVER TARJETAS ({motivo}) {s.minuto}\'\n'
            f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
            f'Faltas totales: {s.faltas_local}-{s.faltas_visita} '
            f'({faltas_tot}), Amarillas: {amarillas_tot} ({amarillas_local}-{amarillas_visita}), Ratio A/F: {ratio_af:.2f}\n'
            f'U10: TP:{tp10}, R:{r10}, C:{c10}\n'
            f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
        )

    @staticmethod
    def alerta_over15_abierto(partido: Partido) -> Optional[str]:
        s = partido.estadisticas_actuales
        if not s or s.minuto < 25 or s.minuto > 60:
            return None
        if s.goles_local + s.goles_visita > 1:
            return None
        if s.remates_totales < 16 or s.tiros_puerta < 6:
            return None
        if not s.posesion_equilibrada:
            return None
        mom10 = partido.calcular_momentum(10)
        rem_10 = mom10['local'].get('remates', 0) + mom10['visita'].get('remates', 0)
        tp_10 = mom10['local'].get('tiros_puerta', 0) + mom10['visita'].get('tiros_puerta', 0)
        if rem_10 < 5 or tp_10 < 2:
            return None
        if getattr(partido, 'alerta_over15_abierto_enviada', False):
            return None
        partido.alerta_over15_abierto_enviada = True
        return (
            f'✅ ALERTA OVER 1.5 (PARTIDO ABIERTO) {s.minuto}\'\n'
            f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
            f'Totales: R:{s.remates_totales}, TP:{s.tiros_puerta}. U10: R:{rem_10}, TP:{tp_10}.\n'
            f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
        )

    @staticmethod
    def alerta_over_corners_tramo_final(partido: Partido) -> Optional[str]:
        s = partido.estadisticas_actuales
        if not s or not partido.tiene_estadisticas:
            return None
        if s.minuto < 65 or s.minuto > 85:
            return None
        if s.diferencia_goles > 1 or s.corners < 8:
            return None
        mom10 = partido.calcular_momentum(10)
        corners_10 = mom10['local'].get('corners', 0) + mom10['visita'].get('corners', 0)
        if corners_10 < 3:
            return None
        if getattr(partido, 'alerta_over_corners_final_enviada', False):
            return None
        partido.alerta_over_corners_final_enviada = True
        return (
            f'⛳ ALERTA OVER CÓRNERS (TRAMO FINAL) {s.minuto}\'\n'
            f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
            f'Totales Córners: {s.corners} | U10: {corners_10}\n'
            f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
        )

    @staticmethod
    def alerta_ritmo_lento_under(partido: Partido) -> Optional[str]:
        """Detecta partidos trabados con tendencia a 0-0 al descanso"""
        s = partido.estadisticas_actuales
        if not s:
            return None
        
        if not (20 <= s.minuto <= 35):
            return None
        
        if s.goles_local != 0 or s.goles_visita != 0:
            return None
        
        faltas_tot = s.faltas_local + s.faltas_visita
        
        if s.remates_totales <= 6 and s.tiros_puerta <= 2 and faltas_tot >= 14:
            if getattr(partido, 'alerta_ritmo_lento_enviada', False):
                return None
            partido.alerta_ritmo_lento_enviada = True
            
            return (
                f'💤 ALERTA RITMO LENTO / UNDER LIVE {s.minuto}\'\n'
                f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
                f'Partido trabado: R:{s.remates_totales}, TP:{s.tiros_puerta}, Faltas:{faltas_tot}\n'
                f'Tendencia a 0-0 al descanso.\n'
                f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
            )
        return None

    @staticmethod
    def alerta_remontada_potencial(partido: Partido) -> Optional[str]:
        """Detecta cuando un equipo va perdiendo pero domina claramente"""
        s = partido.estadisticas_actuales
        if not s or s.minuto < 40 or s.minuto > 75:
            return None
        
        if abs(s.goles_local - s.goles_visita) != 1:
            return None
        
        equipo_perdedor = None
        dominio_tp = 0
        dominio_remates = 0
        
        if s.goles_local < s.goles_visita:
            dominio_tp = s.tiros_puerta_local - s.tiros_puerta_visita
            dominio_remates = s.remates_local - s.remates_visita
            if dominio_tp >= 3 or dominio_remates >= 6:
                equipo_perdedor = partido.equipos.split(' - ')[0]
        
        elif s.goles_visita < s.goles_local:
            dominio_tp = s.tiros_puerta_visita - s.tiros_puerta_local
            dominio_remates = s.remates_visita - s.remates_local
            if dominio_tp >= 3 or dominio_remates >= 6:
                equipo_perdedor = partido.equipos.split(' - ')[1]
        
        if not equipo_perdedor:
            return None
        
        if getattr(partido, 'alerta_remontada_enviada', False):
            return None
        partido.alerta_remontada_enviada = True
        
        return (
            f'⚔️ ALERTA REMONTADA POTENCIAL {s.minuto}\'\n'
            f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
            f'{equipo_perdedor} va perdiendo pero domina claramente:\n'
            f'ΔTP: +{dominio_tp}, ΔRemates: +{dominio_remates}\n'
            f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
        )

    @staticmethod
    def alerta_gol_tardio(partido: Partido) -> Optional[str]:
        """Detecta partidos con mucha acción pero pocos goles después del 70'"""
        s = partido.estadisticas_actuales
        if not s or s.minuto < 70:
            return None
        
        goles_totales = s.goles_local + s.goles_visita
        
        if goles_totales <= 1 and s.remates_totales >= 20 and s.tiros_puerta >= 7 and s.corners >= 8:
            if getattr(partido, 'alerta_gol_tardio_enviada', False):
                return None
            partido.alerta_gol_tardio_enviada = True
            
            return (
                f'⏳🔥 ALERTA GOL TARDÍO {s.minuto}\'\n'
                f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
                f'Alta actividad sin goles: R:{s.remates_totales}, TP:{s.tiros_puerta}, C:{s.corners}\n'
                f'Probabilidad de gol tardío alta.\n'
                f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
            )
        return None



    @staticmethod
    def alerta_colapso_defensivo_post_roja(partido: Partido) -> Optional[str]:
        """Detecta colapso defensivo acelerado tras tarjeta roja"""
        s = partido.estadisticas_actuales
        if not s or s.minuto > 75:
            return None
        
        def check_colapso(minuto_roja, lado_rival, nombre_rival):
            if minuto_roja is None or s.minuto - minuto_roja > 15 or s.minuto - minuto_roja < 3:
                return None
            
            mom = partido.calcular_momentum(8)[lado_rival]
            remates = mom.get('remates', 0)
            tp = mom.get('tiros_puerta', 0)
            corners = mom.get('corners', 0)
            
            if (remates >= 5 and tp >= 2) or (tp >= 3) or (remates >= 7):
                return {
                    'equipo': nombre_rival,
                    'remates': remates,
                    'tp': tp,
                    'corners': corners,
                    'minutos_desde_roja': s.minuto - minuto_roja
                }
            return None
        
        nombres_equipos = partido.equipos.split(' - ')
        
        colapso_local = check_colapso(
            getattr(partido, 'minuto_primera_roja_local', None),
            'visita',
            nombres_equipos[1] if len(nombres_equipos) > 1 else 'Visita'
        )
        
        if colapso_local:
            if getattr(partido, 'alerta_colapso_local_enviada', False):
                return None
            partido.alerta_colapso_local_enviada = True
            
            return (
                f'🚨💥 ALERTA COLAPSO DEFENSIVO {s.minuto}\'\n'
                f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
                f'{colapso_local["equipo"]} acelera tras roja del LOCAL:\n'
                f'U8 ➜ R:{colapso_local["remates"]}, TP:{colapso_local["tp"]}, C:{colapso_local["corners"]}\n'
                f'⏱️ {colapso_local["minutos_desde_roja"]} min desde la roja\n'
                f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
            )
        
        colapso_visita = check_colapso(
            getattr(partido, 'minuto_primera_roja_visita', None),
            'local',
            nombres_equipos[0]
        )
        
        if colapso_visita:
            if getattr(partido, 'alerta_colapso_visita_enviada', False):
                return None
            partido.alerta_colapso_visita_enviada = True
            
            return (
                f'🚨💥 ALERTA COLAPSO DEFENSIVO {s.minuto}\'\n'
                f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
                f'{colapso_visita["equipo"]} acelera tras roja de la VISITA:\n'
                f'U8 ➜ R:{colapso_visita["remates"]}, TP:{colapso_visita["tp"]}, C:{colapso_visita["corners"]}\n'
                f'⏱️ {colapso_visita["minutos_desde_roja"]} min desde la roja\n'
                f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
            )
        
        return None

    @staticmethod
    def alerta_partido_roto(partido: Partido) -> Optional[str]:
        """Detecta partidos con alta volatilidad ofensiva de ambos lados"""
        s = partido.estadisticas_actuales
        if not s or s.minuto < 60 or s.diferencia_goles > 1:
            return None
        
        if (s.remates_totales >= 24 and s.tiros_puerta >= 9 and 
            s.corners >= 10 and s.posesion_equilibrada):
            
            equilibrio_remates = abs(s.remates_local - s.remates_visita) <= 6
            equilibrio_tp = abs(s.tiros_puerta_local - s.tiros_puerta_visita) <= 3
            
            if not (equilibrio_remates and equilibrio_tp):
                return None
            
            if getattr(partido, 'alerta_partido_roto_enviada', False):
                return None
            partido.alerta_partido_roto_enviada = True
            
            return (
                f'💫🔥 ALERTA PARTIDO ROTO {s.minuto}\'\n'
                f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
                f'Alta volatilidad ofensiva de ambos lados:\n'
                f'R: {s.remates_local}-{s.remates_visita} (Tot:{s.remates_totales})\n'
                f'TP: {s.tiros_puerta_local}-{s.tiros_puerta_visita} (Tot:{s.tiros_puerta})\n'
                f'C: {s.corners_local}-{s.corners_visita} (Tot:{s.corners})\n'
                f'Posesión equilibrada. Probabilidad Over 3.5 alta.\n'
                f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
            )
        return None

    @staticmethod
    def alerta_doble_roja(partido: Partido) -> Optional[str]:
        s = partido.estadisticas_actuales
        if not s:
            return None
        msgs = []
        nombres_equipos = partido.equipos.split(' - ')
        nombre_local = nombres_equipos[0]
        nombre_visita = nombres_equipos[1] if len(nombres_equipos) > 1 else "Visita"

        if s.tarjetas_rojas_local >= 2 and not partido.alerta_doble_roja_local_enviada:
            partido.alerta_doble_roja_local_enviada = True
            msgs.append(
                f'🟥🟥 ALERTA DOBLE ROJA ({s.minuto}\')\n'
                f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
                f'El LOCAL ({nombre_local}) tiene {s.tarjetas_rojas_local} rojas.\n'
                f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
            )
        if s.tarjetas_rojas_visita >= 2 and not partido.alerta_doble_roja_visita_enviada:
            partido.alerta_doble_roja_visita_enviada = True
            msgs.append(
                f'🟥🟥 ALERTA DOBLE ROJA ({s.minuto}\')\n'
                f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
                f'La VISITA ({nombre_visita}) tiene {s.tarjetas_rojas_visita} rojas.\n'
                f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
            )
        if (s.tarjetas_rojas_local >= 2 and s.tarjetas_rojas_visita >= 2
            and not partido.alerta_doble_roja_ambos_enviada):
            partido.alerta_doble_roja_ambos_enviada = True
            msgs.append(
                f'🟥🟥🟥🟥 ALERTA ROJAS MÚLTIPLES EN AMBOS ({s.minuto}\')\n'
                f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
                f'Ambos equipos con múltiples rojas.\n'
                f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
            )
        return "\n\n".join(msgs) if msgs else None

    @staticmethod
    def alerta_roja_rapida(partido: Partido) -> Optional[str]:
        s = partido.estadisticas_actuales
        if not s or s.minuto > 35:
            return None
        if s.tarjetas_rojas_local + s.tarjetas_rojas_visita <= 0:
            return None

        nombres_equipos = partido.equipos.split(' - ')
        equipo_roja = nombres_equipos[0] if s.tarjetas_rojas_local > 0 else (
            nombres_equipos[1] if s.tarjetas_rojas_visita > 0 else None)
        if not equipo_roja:
            return None

        if not getattr(partido, 'alerta_roja_enviada', False):
            partido.alerta_roja_enviada = True
            return (
                f'🚨 ALERTA ROJA RÁPIDA ({s.minuto}\')\n'
                f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
                f'🛑 Roja para {equipo_roja}. Oportunidad para el rival.\n'
                f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
            )
        return None

    @staticmethod
    def alerta_gol_tras_descanso(partido: Partido) -> Optional[str]:
        s = partido.estadisticas_actuales
        if not s:
            return None
        if not (40 <= s.minuto <= 55):
            return None
        if (s.goles_local + s.goles_visita) > 1:
            return None
        if s.remates_totales >= 12 and s.tiros_puerta >= 5 and s.corners >= 4:
            if getattr(partido, 'alerta_descanso_enviada', False):
                return None
            partido.alerta_descanso_enviada = True

            nombres_equipos = partido.equipos.split(' - ')
            nombre_local = nombres_equipos[0]
            nombre_visita = nombres_equipos[1] if len(nombres_equipos) > 1 else "Visita"

            score_local = (
                s.remates_local * 1.0 +
                s.tiros_puerta_local * 2.5 +
                s.corners_local * 1.5 +
                s.ataques_peligrosos_local * 0.3
            )
            score_visita = (
                s.remates_visita * 1.0 +
                s.tiros_puerta_visita * 2.5 +
                s.corners_visita * 1.5 +
                s.ataques_peligrosos_visita * 0.3
            )

            diff_score = score_local - score_visita
            inclinacion_texto = "Sin inclinación clara para el próximo gol."

            if abs(diff_score) >= 4:
                if diff_score > 0:
                    lado_probable = "LOCAL"
                    equipo_probable = nombre_local
                else:
                    lado_probable = "VISITA"
                    equipo_probable = nombre_visita

                base_prob = 55.0
                extra = min(15.0, abs(diff_score) * 2.0)
                prob_lado = base_prob + extra
                prob_otro = 100.0 - prob_lado

                inclinacion_texto = (
                    f"Inclinación del próximo gol hacia {lado_probable} ({equipo_probable}).\n"
                    f"Estimación: {lado_probable} {prob_lado:.0f}% vs {prob_otro:.0f}% el rival."
                )

            return (
                f'⏳ ALERTA GOL TRAS DESCANSO ({s.minuto}\') ⚽\n'
                f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
                f'Stats 1T: R:{s.remates_totales}, TP:{s.tiros_puerta}, C:{s.corners}\n'
                f'{inclinacion_texto}\n'
                f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
            )
        return None

    @staticmethod
    def alerta_dominio_gol(partido: Partido) -> Optional[str]:
        s = partido.estadisticas_actuales
        if not s or s.minuto < 50 or s.minuto > 80 or s.diferencia_goles > 1:
            return None
        dom_score = EstrategiaAnalisis._equipo_dominante_por_tp(s)
        if dom_score == 0:
            return None

        equipo_dominante = partido.equipos.split(' - ')[0] if dom_score == 1 else partido.equipos.split(' - ')[1]
        momentum = partido.calcular_momentum(10)
        mom = momentum['local'] if dom_score == 1 else momentum['visita']
        req_rem = 3 if abs(s.tiros_puerta_local - s.tiros_puerta_visita) >= 3 else 4

        if mom.get('remates', 0) >= req_rem and mom.get('tiros_puerta', 0) >= 2:
            if not partido.alerta_dominio_enviada:
                partido.alerta_dominio_enviada = True
                return (
                    f'🚨 ALERTA DOMINIO GOL ({s.minuto}\')\n'
                    f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
                    f'{equipo_dominante} domina: U10 R:{mom.get("remates",0)}, TP:{mom.get("tiros_puerta",0)}, ΔTP:{abs(s.tiros_puerta_local - s.tiros_puerta_visita)}\n'
                    f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
                )
        return None

    @staticmethod
    def alerta_presion_sostenida(partido: Partido) -> Optional[str]:
        s = partido.estadisticas_actuales
        if not s or s.minuto < 35 or s.minuto > 85 or s.diferencia_goles > 1:
            return None
        dom = EstrategiaAnalisis._equipo_dominante_por_tp(s)
        if dom == 0:
            return None
        mom10 = partido.calcular_momentum(10)
        mom = mom10['local'] if dom == 1 else mom10['visita']
        if mom.get('tiros_puerta', 0) >= 2 and mom.get('remates', 0) >= 4 and mom.get('corners', 0) >= 2:
            if getattr(partido, 'alerta_presion_sostenida_enviada', False):
                return None
            partido.alerta_presion_sostenida_enviada = True
            eq_dom = partido.equipos.split(' - ')[0] if dom == 1 else partido.equipos.split(' - ')[1]
            eq_op = partido.equipos.split(' - ')[1] if dom == 1 else partido.equipos.split(' - ')[0]
            return (
                f'🔥 ALERTA PRESIÓN SOSTENIDA {s.minuto}\'\n'
                f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
                f'{eq_dom} presiona a {eq_op} U10: R:{mom.get("remates",0)}, TP:{mom.get("tiros_puerta",0)}, C:{mom.get("corners",0)}\n'
                f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
            )
        return None

    @staticmethod
    def alerta_wave_ofensiva(partido: Partido) -> Optional[str]:
        s = partido.estadisticas_actuales
        if not s or s.minuto < 25 or s.minuto > 85 or s.diferencia_goles > 1 or s.tiros_puerta < 5:
            return None
        mom7 = partido.calcular_momentum(7)
        for idx in ['local', 'visita']:
            c = mom7[idx].get('corners', 0)
            tp = mom7[idx].get('tiros_puerta', 0)
            if c >= 1 and tp >= 1:
                flag = f'alerta_wave_{idx}_enviada'
                if getattr(partido, flag, False):
                    continue
                setattr(partido, flag, True)
                eq_dom = partido.equipos.split(' - ')[0] if idx == 'local' else partido.equipos.split(' - ')[1]
                return (
                    f'🌊 ALERTA WAVE OFENSIVA {s.minuto}\'\n'
                    f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
                    f'{eq_dom} encadena córner y TP en U7 (C:{c}, TP:{tp}).\n'
                    f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
                )
        return None

    @staticmethod
    def alerta_dominio_con_posesion_y_ataques(partido: Partido) -> Optional[str]:
        s = partido.estadisticas_actuales
        if not s or s.minuto < 35 or s.minuto > 80 or s.diferencia_goles > 1:
            return None
        dom = 0
        if s.posesion_local >= 60 and s.tiros_puerta_local >= s.tiros_puerta_visita + 2:
            dom = 1
        elif s.posesion_visita >= 60 and s.tiros_puerta_visita >= s.tiros_puerta_local + 2:
            dom = -1
        if dom == 0:
            return None
        mom10 = partido.calcular_momentum(10)
        mom_dom = mom10['local'] if dom == 1 else mom10['visita']
        rem = mom_dom.get('remates', 0)
        if rem < 3 and s.ataques_peligrosos < 12:
            return None
        if getattr(partido, 'alerta_dominio_pose_ataques_enviada', False):
            return None
        partido.alerta_dominio_pose_ataques_enviada = True
        eq_dom = partido.equipos.split(' - ')[0] if dom == 1 else partido.equipos.split(' - ')[1]
        return (
            f'📊 ALERTA DOMINIO+POSESIÓN {s.minuto}\'\n'
            f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
            f'{eq_dom} domina: Posesión≥60% y TP superior. U10 remates:{rem}.\n'
            f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
        )

    @staticmethod
    def alerta_rebote_post_roja(partido: Partido) -> Optional[str]:
        s = partido.estadisticas_actuales
        if not s or s.minuto > 70 or s.diferencia_goles > 1:
            return None

        def check_rebote(minuto_roja, lado_rival):
            if minuto_roja is None or minuto_roja > 40 or s.minuto - minuto_roja > 12:
                return None
            mom = partido.calcular_momentum(12)[lado_rival]
            if mom.get('tiros_puerta', 0) >= 2 or mom.get('remates', 0) >= 4:
                return mom
            return None

        mom_visita = check_rebote(getattr(partido, 'minuto_primera_roja_local', None), 'visita')
        if mom_visita:
            if getattr(partido, 'alerta_rebote_visita_enviada', False):
                return None
            partido.alerta_rebote_visita_enviada = True
            eq = partido.equipos.split(' - ')[1]
            return (
                f'🚨 REBOTE POST ROJA {s.minuto}\'\n'
                f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
                f'{eq} acelera tras roja del LOCAL: U12 R:{mom_visita.get("remates",0)} TP:{mom_visita.get("tiros_puerta",0)}.\n'
                f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
            )

        mom_local = check_rebote(getattr(partido, 'minuto_primera_roja_visita', None), 'local')
        if mom_local:
            if getattr(partido, 'alerta_rebote_local_enviada', False):
                return None
            partido.alerta_rebote_local_enviada = True
            eq = partido.equipos.split(' - ')[0]
            return (
                f'🚨 REBOTE POST ROJA {s.minuto}\'\n'
                f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
                f'{eq} acelera tras roja del VISITA: U12 R:{mom_local.get("remates",0)} TP:{mom_local.get("tiros_puerta",0)}.\n'
                f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
            )
        return None
    
    @staticmethod    
    def alerta_over_corners_con_edge(partido: 'Partido') -> Optional[str]:
        """Alerta Over Corners solo con edge"""
        s = partido.estadisticas_actuales
        
        if not s or not partido.tiene_estadisticas:
            return None
        
        if s.minuto < 60 or s.minuto > 85:
            return None
        
        corners_actuales = s.corners
        
        if corners_actuales >= 10:
            return None
        
        tasa_corners_min = corners_actuales / s.minuto if s.minuto > 0 else 0
        corners_proyectados = tasa_corners_min * 90
        
        mom10 = partido.calcular_momentum(10)
        c10 = mom10['local'].get('corners', 0) + mom10['visita'].get('corners', 0)
        
        if c10 >= 3:
            corners_proyectados += 2
        elif c10 >= 2:
            corners_proyectados += 1
        
        if corners_proyectados >= 11:
            prob_over = 0.70
        elif corners_proyectados >= 10:
            prob_over = 0.55
        elif corners_proyectados >= 9:
            prob_over = 0.40
        else:
            prob_over = 0.25
        
        if s.posesion_equilibrada and s.diferencia_goles <= 1:
            prob_over += 0.10
        
        prob_over = max(0.05, min(0.85, prob_over))
        
        from edge_calculator import EdgeCalculator
        tiene_valor, edge, explicacion = EdgeCalculator.tiene_valor(prob_over, 'over_corners_9.5')
        
        if not tiene_valor:
            return None
        
        if getattr(partido, 'alerta_over_corners_edge_enviada', False):
            return None
        partido.alerta_over_corners_edge_enviada = True
        
        if edge >= 20: 
            estrellas = "⭐⭐⭐⭐⭐"
        elif edge >= 15: 
            estrellas = "⭐⭐⭐⭐"
        elif edge >= 10: 
            estrellas = "⭐⭐⭐"
        else: 
            estrellas = "⭐⭐"
        
        return (
            f'⛳ ALERTA OVER CORNERS 9.5 (VALOR) {s.minuto}\' {estrellas}\n'
            f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
            f'{explicacion}\n'
            f'Corners actuales: {corners_actuales} | Proyección: {corners_proyectados:.1f}\n'
            f'U10: {c10} corners\n'
            f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
        )

    @staticmethod
    def alerta_btts_con_edge(partido: 'Partido') -> Optional[str]:
        """Alerta BTTS (Both Teams To Score) solo con edge"""
        s = partido.estadisticas_actuales
        
        if not s or s.minuto < 30 or s.minuto > 80:
            return None
        
        if s.goles_local > 0 and s.goles_visita > 0:
            return None
        
        tp_local = s.tiros_puerta_local
        tp_visita = s.tiros_puerta_visita
        
        if tp_local < 2 or tp_visita < 2:
            return None
        
        prob_gol_local = min(0.85, tp_local * 0.15)
        prob_gol_visita = min(0.85, tp_visita * 0.15)
        
        prob_btts = prob_gol_local * prob_gol_visita
        
        mom10 = partido.calcular_momentum(10)
        tp10_local = mom10['local'].get('tiros_puerta', 0)
        tp10_visita = mom10['visita'].get('tiros_puerta', 0)
        
        if tp10_local >= 2:
            prob_btts += 0.10
        if tp10_visita >= 2:
            prob_btts += 0.10
        
        if s.posesion_equilibrada and s.diferencia_goles <= 1:
            prob_btts += 0.10
        
        prob_btts = max(0.05, min(0.85, prob_btts))
        
        from edge_calculator import EdgeCalculator
        tiene_valor, edge, explicacion = EdgeCalculator.tiene_valor(prob_btts, 'btts')
        
        if not tiene_valor:
            return None
        
        if getattr(partido, 'alerta_btts_edge_enviada', False):
            return None
        partido.alerta_btts_edge_enviada = True
        
        if edge >= 20: 
            estrellas = "⭐⭐⭐⭐⭐"
        elif edge >= 15: 
            estrellas = "⭐⭐⭐⭐"
        elif edge >= 10: 
            estrellas = "⭐⭐⭐"
        else: 
            estrellas = "⭐⭐"
        
        return (
            f'⚽⚽ ALERTA BTTS (VALOR) {s.minuto}\' {estrellas}\n'
            f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
            f'{explicacion}\n'
            f'TP: {tp_local}-{tp_visita} | U10: {tp10_local}-{tp10_visita}\n'
            f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
        )

    @staticmethod
    def alerta_siguiente_gol_con_edge(partido: 'Partido') -> Optional[str]:
        """Alerta siguiente gol solo si hay edge claro"""
        s = partido.estadisticas_actuales

        if not s or s.minuto < 25 or s.minuto > 85 or s.diferencia_goles > 2:
            return None

        tp_local = s.tiros_puerta_local
        tp_visita = s.tiros_puerta_visita

        # ✅ NUEVA VALIDACIÓN 1: Mínimo de actividad
        if tp_local + tp_visita < 6:  # Cambiado de 4 a 6
            return None  # Muy poca actividad para calcular probabilidades

        # ✅ NUEVA VALIDACIÓN 2: Verificar grandes ocasiones
        grandes_ocasiones_local = getattr(s, 'grandes_ocasiones_local', 0)
        grandes_ocasiones_visita = getattr(s, 'grandes_ocasiones_visita', 0)

        if grandes_ocasiones_local + grandes_ocasiones_visita < 2:
            return None  # Remates de baja calidad

        # ✅ NUEVA VALIDACIÓN 3: Verificar xGOT si está disponible
        xgot_local = getattr(s, 'xgot_local', 0)
        xgot_visita = getattr(s, 'xgot_visita', 0)

        if xgot_local + xgot_visita < 0.6:  # xGOT total muy bajo
            return None  # Los tiros a puerta no son peligrosos

        total_tp = tp_local + tp_visita
        prob_local = tp_local / total_tp if total_tp > 0 else 0.5
        prob_visita = tp_visita / total_tp if total_tp > 0 else 0.5

        # Ajustes por momentum (últimos 10 minutos)
        mom10 = partido.calcular_momentum(10)
        tp10_local = mom10['local'].get('tiros_puerta', 0)
        tp10_visita = mom10['visita'].get('tiros_puerta', 0)

        # ✅ NUEVA VALIDACIÓN 4: Verificar momentum reciente
        if tp10_local + tp10_visita < 2:
            return None  # Sin momentum reciente, partido apagado

        if tp10_local >= 2:
            prob_local += 0.15
        if tp10_visita >= 2:
            prob_visita += 0.15

        # Normalizar las probabilidades ajustadas
        total = prob_local + prob_visita
        prob_local /= total
        prob_visita /= total

        # ✅ AJUSTE: Penalizar si xGOT es muy bajo comparado con TP
        if xgot_local > 0 and tp_local > 0:
            calidad_local = xgot_local / tp_local
            if calidad_local < 0.15:  # Menos de 0.15 xGOT por tiro
                prob_local *= 0.7  # Reducir probabilidad 30%

        if xgot_visita > 0 and tp_visita > 0:
            calidad_visita = xgot_visita / tp_visita
            if calidad_visita < 0.15:
                prob_visita *= 0.7

        # Renormalizar después del ajuste
        total = prob_local + prob_visita
        prob_local /= total
        prob_visita /= total

        # Determinar favorito (ahora con umbral más alto)
        if prob_local >= 0.65:  # Cambiado de 0.60 a 0.65
            mercado = 'next_goal_home'
            prob = prob_local
            equipo = partido.equipos.split(' - ')[0]
        elif prob_visita >= 0.65:  # Cambiado de 0.60 a 0.65
            mercado = 'next_goal_away'
            prob = prob_visita
            equipo = partido.equipos.split(' - ')[1]
        else:
            return None  # No hay favorito claro

        # CALCULAR EDGE
        from edge_calculator import EdgeCalculator
        tiene_valor, edge, explicacion = EdgeCalculator.tiene_valor(prob, mercado)

        if not tiene_valor:
            return None

        # Control de alerta ya enviada
        flag = f'alerta_next_goal_{mercado}_enviada'
        if getattr(partido, flag, False):
            return None
        setattr(partido, flag, True)

        # Estrellas según edge
        if edge >= 20: 
            estrellas = "⭐⭐⭐⭐⭐"
        elif edge >= 15: 
            estrellas = "⭐⭐⭐⭐"
        elif edge >= 10: 
            estrellas = "⭐⭐⭐"
        else: 
            estrellas = "⭐⭐"

        # ✅ NUEVO: Agregar advertencia si calidad es baja
        warning = ""
        if grandes_ocasiones_local + grandes_ocasiones_visita < 3:
            warning = "\n⚠️ Pocas grandes ocasiones - Apostar con precaución"

        return (
            f'🎯 ALERTA SIGUIENTE GOL (VALOR) {s.minuto}\' {estrellas}\n'
            f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
            f'Favorito: {equipo}\n'
            f'{explicacion}{warning}\n'
            f'TP: {tp_local}-{tp_visita} | U10: {tp10_local}-{tp10_visita}\n'
            f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
        )


    @staticmethod
    def alerta_dominio_silencioso(partido: Partido, lado: str) -> Optional[str]:
        """
        Detecta dominio silencioso y estima probabilidad razonable de gol
        (usa xGOT / scoring si está disponible, y señales U10).
        'lado' es 'local' o 'visita'.
        """
        s = partido.estadisticas_actuales
        if not s:
            return None
        # Ventana de análisis
        if s.minuto < 30 or s.minuto > 88 or s.diferencia_goles > 1:
            return None

        # Valores por lado
        rem_lado = s.remates_local if lado == 'local' else s.remates_visita
        rem_opp = s.remates_visita if lado == 'local' else s.remates_local
        cor_lado = s.corners_local if lado == 'local' else s.corners_visita
        cor_opp = s.corners_visita if lado == 'local' else s.corners_local
        tp_lado = s.tiros_puerta_local if lado == 'local' else s.tiros_puerta_visita
        tp_opp = s.tiros_puerta_visita if lado == 'local' else s.tiros_puerta_local
        grandes_ocasiones = s.grandes_ocasiones_local if lado == 'local' else s.grandes_ocasiones_visita

        rem_diff = rem_lado - rem_opp
        cor_diff = cor_lado - cor_opp

        # Momentum U10 por lado
        mom10 = partido.calcular_momentum(10)
        r10 = mom10[lado].get('remates', 0)
        c10 = mom10[lado].get('corners', 0)
        tp10 = mom10[lado].get('tiros_puerta', 0)

        # Condiciones mínimas para considerar "dominio silencioso"
        cond_volumen = (rem_diff >= 4) or (rem_diff >= 3 and cor_diff >= 1) or (cor_diff >= 2)
        cond_momentum = (r10 >= 3 and c10 >= 1) or (r10 >= 4) or (c10 >= 2)

        if not (cond_volumen and cond_momentum):
            return None

        flag = f'alerta_dominio_silencioso_{lado}_enviada'
        if getattr(partido, flag, False):
            return None
        setattr(partido, flag, True)

        # Construir snapshot y tratar de obtener scoring/xg si existe
        snap_dict = construir_snapshot(s)
        xgot_side = getattr(s, 'xgot_local', 0.0) if lado == 'local' else getattr(s, 'xgot_visita', 0.0)

        try:
            scoring_engine = ScoringEngine()
            resultado = integrar_scoring_en_partido(scoring_engine, snap_dict)
            # intentar usar xG del scoring si viene
            xg = resultado.get('xg_home' if lado == 'local' else 'xg_away', None)
            if xg is not None:
                # preferimos xGOT si está disponible; si scoring devuelve xg, lo normalizamos un poco
                xgot_side = max(xgot_side, float(resultado.get('xg_home' if lado == 'local' else 'xg_away', 0.0)))
        except Exception:
            # Si el scoring falla, seguimos con xGOT del scraper (o 0.0)
            xg = None

        # Heurística para estimar probabilidad de gol en próximos ~10 minutos
        # Normalizaciones/ratios
        xgot_norm = min(1.0, xgot_side / 0.6)                  # 0.6 ≈ umbral alto de xGOT
        tp10_norm = min(1.0, tp10 / 3.0)                       # 3 TP en U10 es señal fuerte
        r10_norm = min(1.0, r10 / 6.0)                         # 6 remates en U10 = muy alto
        big_chance_flag = 1.0 if grandes_ocasiones >= 1 else 0.0

        # Puntaje de calidad defensiva/ofensiva local
        quality_score = (
            0.55 * xgot_norm +
            0.20 * tp10_norm +
            0.15 * r10_norm +
            0.10 * big_chance_flag
        )  # en rango aproximado 0..1

        # Ajustes por momentum (si U10 muestra presión sostenida)
        momentum_multiplier = 1.0
        if tp10 >= 2 and r10 >= 4:
            momentum_multiplier = 1.15
        elif tp10 >= 1 and r10 >= 3:
            momentum_multiplier = 1.08

        # Ajuste por urgencia (si va perdiendo aumenta probabilidad de buscar gol)
        urgencia = 1.0
        if (lado == 'local' and s.goles_local < s.goles_visita) or (lado == 'visita' and s.goles_visita < s.goles_local):
            urgencia = 1.20

        # Estimación final (probabilidad en porcentaje de próximo gol en ~10 minutos)
        prob_gol = quality_score * 0.70 * 100.0  # escala base: quality_score->% (0..70%)
        prob_gol *= momentum_multiplier * urgencia

        # Asegurar límites razonables
        prob_gol = max(3.0, min(85.0, prob_gol))

        # Estrellas según probabilidad
        if prob_gol >= 70:
            estrellas = "⭐⭐⭐⭐⭐"
        elif prob_gol >= 55:
            estrellas = "⭐⭐⭐⭐"
        elif prob_gol >= 40:
            estrellas = "⭐⭐⭐"
        elif prob_gol >= 25:
            estrellas = "⭐⭐"
        else:
            estrellas = "⭐"

        # Mensaje de advertencia por calidad si detectamos pocas grandes ocasiones pese al dominio
        warning_text = ""
        if grandes_ocasiones == 0 and tp10 >= 2:
            warning_text = "\n⚠️ ADVERTENCIA: 0 grandes ocasiones (remates de baja calidad)"

        # Construir texto U10 más completo (R, C, TP)
        # Asegurarse de extraer U10 para el rival también para mostrar contexto
        r10_opp = mom10['visita'].get('remates', 0) if lado == 'local' else mom10['local'].get('remates', 0)
        c10_opp = mom10['visita'].get('corners', 0) if lado == 'local' else mom10['local'].get('corners', 0)
        tp10_opp = mom10['visita'].get('tiros_puerta', 0) if lado == 'local' else mom10['local'].get('tiros_puerta', 0)

        lado_texto = "LOCAL" if lado == 'local' else "VISITA"
        equipo = partido.equipos.split(' - ')[0] if lado == 'local' else partido.equipos.split(' - ')[1]

        # Incluir tiros a puerta en el mensaje principal
        tp_display = f"TP: {tp_lado}-{tp_opp}"

        return (
            f'🧭 ALERTA DOMINIO SILENCIOSO {lado_texto} {s.minuto}\'\n'
            f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
            f'📊 Probabilidad gol {lado_texto}: {prob_gol:.1f}% {estrellas}'
            f'{warning_text}\n'
            f'Volumen favorece a {lado_texto}: Remates {rem_lado}-{rem_opp} (Δ{rem_diff}), Corners {cor_lado}-{cor_opp} (Δ{cor_diff}).\n'
            f'U10 {lado_texto} ➜ R:{r10}, C:{c10}, TP:{tp10} | U10 Rival ➜ R:{r10_opp}, C:{c10_opp}, TP:{tp10_opp}\n'
            f'📊 Tiros a puerta ahora: {tp_lado}-{tp_opp} ({tp_display})\n'
            f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
        )
        

    @staticmethod
    def alerta_friccion_mas_presion_gol(partido: Partido) -> Optional[str]:
        s = partido.estadisticas_actuales
        if not s:
            return None
            
        if s.minuto < 60 or s.minuto > 88 or s.diferencia_goles > 1:
            debug(f"Friccion+Presion skip: min={s.minuto}, diff={s.diferencia_goles}")
            return None

        faltas_tot = s.faltas_local + s.faltas_visita
        amarillas_tot = s.amarillas_local + s.amarillas_visita
        
        if faltas_tot < 24 or amarillas_tot < 3:
            debug(f"Friccion insuficiente: faltas={faltas_tot}, amarillas={amarillas_tot}")
            return None

        # ✅ NUEVA VALIDACIÓN 1: Verificar xGOT mínimo
        xgot_total = s.xgot_local + s.xgot_visita
        if xgot_total < 1.5:  # Menos de 1.5 xGOT total = remates de muy baja calidad
            debug(f"xGOT insuficiente: {xgot_total:.2f}")
            return None

        # ✅ NUEVA VALIDACIÓN 2: Verificar grandes ocasiones
        grandes_ocasiones_total = s.grandes_ocasiones_local + s.grandes_ocasiones_visita
        if grandes_ocasiones_total < 2:
            debug(f"Pocas grandes ocasiones: {grandes_ocasiones_total}")
            return None

        mom10 = partido.calcular_momentum(10)
        tp10 = mom10['local'].get('tiros_puerta', 0) + mom10['visita'].get('tiros_puerta', 0)
        r10 = mom10['local'].get('remates', 0) + mom10['visita'].get('remates', 0)
        c10 = mom10['local'].get('corners', 0) + mom10['visita'].get('corners', 0)

        # ✅ NUEVA VALIDACIÓN 3: Momentum U10 más exigente
        if tp10 == 0:  # Si no hay ni un tiro a puerta en U10, no alertar
            debug(f"Sin tiros a puerta en U10")
            return None

        dom_visita_vol = (s.remates_visita - s.remates_local >= 4 and s.corners_visita - s.corners_local >= 1)
        dom_local_vol = (s.remates_local - s.remates_visita >= 4 and s.corners_local - s.corners_visita >= 1)
        hay_dom_vol = dom_visita_vol or dom_local_vol

        # ✅ NUEVA VALIDACIÓN 4: Condiciones de momentum más estrictas
        cond_momentum_fuerte = (tp10 >= 2 and r10 >= 5) or (tp10 >= 3)
        cond_momentum_relajado = (r10 >= 7 and c10 >= 2)

        if not (cond_momentum_fuerte or (hay_dom_vol and cond_momentum_relajado)):
            debug(f"Momentum insuficiente: U10 tp={tp10}, r={r10}, c={c10}, dom_vol={hay_dom_vol}")
            return None

        mom7 = partido.calcular_momentum(7)
        wave = any(mom7[idx].get('corners', 0) >= 1 and mom7[idx].get('tiros_puerta', 0) >= 1 for idx in ['local', 'visita'])
        tp_diff = abs(s.tiros_puerta_local - s.tiros_puerta_visita)
        marcador_bajo_empate = (s.goles_local == s.goles_visita and (s.goles_local in (0, 1)))

        # ✅ NUEVA VALIDACIÓN 5: Verificar calidad de remates bloqueados
        # Nota: Asegúrate de que 'remates_totales' esté definido en tu objeto 's'
        remates_bloqueados_local = s.remates_local - s.tiros_puerta_local - (s.remates_local - s.tiros_puerta_local - getattr(s, 'remates_fuera_local', 0))
        remates_bloqueados_visita = s.remates_visita - s.tiros_puerta_visita - (s.remates_visita - s.tiros_puerta_visita - getattr(s, 'remates_fuera_visita', 0))
        
        if getattr(s, 'remates_totales', 0) > 0:
            tasa_bloqueo = (remates_bloqueados_local + remates_bloqueados_visita) / s.remates_totales
            if tasa_bloqueo > 0.60:
                debug(f"Partido muy trabado: {tasa_bloqueo:.1%} remates bloqueados")
                return None

        if not (wave or tp_diff >= 2 or marcador_bajo_empate):
            debug(f"Sin señal estructural: wave={wave}, tp_diff={tp_diff}, empate_bajo={marcador_bajo_empate}")
            return None

        if getattr(partido, 'alerta_friccion_presion_gol_enviada', False):
            return None
        partido.alerta_friccion_presion_gol_enviada = True

        # ✅ MEJORA: Agregar advertencia de calidad en el mensaje
        warning = ""
        if xgot_total < 2.0:
            warning = f"\n⚠️ xGOT bajo ({xgot_total:.2f}) - Calidad de remates limitada"

        return (
            f'⚔️🔥 ALERTA GOL (FRICCIÓN + PRESIÓN) {s.minuto}\'\n'
            f'{partido.equipos} ({s.goles_local}-{s.goles_visita})\n'
            f'Faltas:{s.faltas_local}-{s.faltas_visita} (Tot:{faltas_tot}), Amarillas:{amarillas_tot}\n'
            f'U10 ➜ TP:{tp10}, R:{r10}, C:{c10} {"(wave)" if wave else ""} | ΔTP Partido:{tp_diff}\n'
            f'📊 xGOT: {xgot_total:.2f} | Grandes ocasiones: {grandes_ocasiones_total}\n'
            f'Contexto: marcador ajustado{" + empate bajo" if marcador_bajo_empate else ""}{warning}\n'
            f'🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}'
        )


# --- Extracción de datos ---

def _obtener_estadisticas_detalladas(partido_id: str) -> Dict:
    stats = {
        'remates_totales': 0, 'tiros_puerta': 0, 'corners': 0,
        'posesion_local': 0, 'posesion_visita': 0, 'ataques_peligrosos': 0,
        'remates_local': 0, 'remates_visita': 0,
        'tiros_puerta_local': 0, 'tiros_puerta_visita': 0,
        'corners_local': 0, 'corners_visita': 0,
        'ataques_peligrosos_local': 0, 'ataques_peligrosos_visita': 0,
        'faltas_local': 0, 'faltas_visita': 0,
        'amarillas_local': 0, 'amarillas_visita': 0,
        'grandes_ocasiones_local': 0, 'grandes_ocasiones_visita': 0,
        'xgot_local': 0.0, 'xgot_visita': 0.0,
        'tiene_estadisticas': False
    }
    
    try:
        url = URL_ESTADISTICAS_BASE.format(partido_id)
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code != 200:
            return stats

        soup = BeautifulSoup(resp.text, "html.parser")

        # Buscar todos los bloques de estadística
        filas = soup.find_all("div", class_=re.compile(r"wcl-row_"))

        for fila in filas:
            # Nombre de la estadística
            nombre_div = fila.find("div", class_=re.compile(r"wcl-category_"))
            if not nombre_div:
                continue
            nombre = nombre_div.get_text(strip=True).lower()

            # Valores local y visitante
            valor_local_div = fila.find("div", class_=re.compile(r"wcl-homeValue_"))
            valor_visita_div = fila.find("div", class_=re.compile(r"wcl-awayValue_"))
            if not valor_local_div or not valor_visita_div:
                continue

            valor_local_text = valor_local_div.get_text(strip=True)
            valor_visita_text = valor_visita_div.get_text(strip=True)

            # Función para limpiar y convertir a número
            def limpiar_valor(texto):
                texto = texto.replace("%", "").split(" ")[0]  # Quitar % y texto extra
                try:
                    if "." in texto:
                        return float(texto)
                    else:
                        return int(texto)
                except:
                    return 0

            val_local = limpiar_valor(valor_local_text)
            val_visita = limpiar_valor(valor_visita_text)

            stats['tiene_estadisticas'] = True

            # Mapear según nombre
            if "remates totales" in nombre or "tiros totales" in nombre or "disparos" in nombre:
                stats['remates_local'] = val_local
                stats['remates_visita'] = val_visita
                stats['remates_totales'] = val_local + val_visita
            elif "remates a puerta" in nombre or "tiros a puerta" in nombre or "shot on target" in nombre:
                stats['tiros_puerta_local'] = val_local
                stats['tiros_puerta_visita'] = val_visita
                stats['tiros_puerta'] = val_local + val_visita
            elif "córneres" in nombre or "corners" in nombre:
                stats['corners_local'] = val_local
                stats['corners_visita'] = val_visita
                stats['corners'] = val_local + val_visita
            elif "posesión" in nombre or "possession" in nombre:
                stats['posesion_local'] = val_local
                stats['posesion_visita'] = val_visita
            elif "grandes ocasiones" in nombre or "big chances" in nombre:
                stats['grandes_ocasiones_local'] = val_local
                stats['grandes_ocasiones_visita'] = val_visita
            elif "faltas" in nombre:
                stats['faltas_local'] = val_local
                stats['faltas_visita'] = val_visita
            elif "amarillas" in nombre or "yellow card" in nombre:
                stats['amarillas_local'] = val_local
                stats['amarillas_visita'] = val_visita
            elif "xgot" in nombre or "xg a puerta" in nombre:
                stats['xgot_local'] = val_local
                stats['xgot_visita'] = val_visita

    except Exception as e:
        print(f"Error extrayendo estadísticas {partido_id}: {e}")

    return stats


def _obtener_clasificacion_liga(partido_id: str) -> List[Dict]:
    """Lee la pestaña 'Clasificación' del partido"""
    url = f"https://m.flashscore.cl/detalle-del-partido/{partido_id}/?s=2&t=clasificacion"
    clasificacion = []
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code != 200:
            print(f"⚠️ No se pudo acceder a CLASIFICACIÓN ({resp.status_code}) para {partido_id}")
            return clasificacion

        soup = BeautifulSoup(resp.text, "html.parser")

        tabla = soup.find("table")
        if not tabla:
            print(f"⚠️ No se encontró tabla de clasificación para {partido_id}")
            return clasificacion

        filas = tabla.find_all("tr")
        for fila in filas[1:]:
            celdas = [c.get_text(strip=True) for c in fila.find_all("td")]
            if len(celdas) < 7:
                continue
            try:
                pos_str = celdas[0].split(".")[0]
                pos = int(pos_str)
                equipo = celdas[1]
                pj = int(celdas[2])
                g = int(celdas[3])
                e = int(celdas[4])
                p = int(celdas[5])

                gf_gc = celdas[6]
                if ":" in gf_gc:
                    gf, gc = gf_gc.split(":")
                    gf = int(gf)
                    gc = int(gc)
                else:
                    gf, gc = 0, 0

                pts = int(celdas[7]) if len(celdas) > 7 else 0
                dg = gf - gc

                clasificacion.append({
                    "pos": pos,
                    "equipo": equipo,
                    "pj": pj,
                    "g": g,
                    "e": e,
                    "p": p,
                    "gf": gf,
                    "gc": gc,
                    "dg": dg,
                    "pts": pts,
                })
            except Exception:
                continue

    except Exception as e:
        print(f"Error extrayendo CLASIFICACIÓN {partido_id}: {e}")

    return clasificacion


def _extraer_info_basica(html_partido: str) -> Optional[Dict]:
    try:
        partido_id = re.search(r'href="/detalle-del-partido/([a-zA-Z0-9]{8})/\?s=2"', html_partido).group(1)
        minuto_str_match = re.search(r'<span class="live">([^<]+)</span>', html_partido)
        if not minuto_str_match:
            return None
        minuto_str = minuto_str_match.group(1).replace("'", "").replace("+", "").strip()
        if minuto_str.lower().startswith("descanso"):
            minuto = 45
        elif minuto_str.isdigit():
            minuto = int(minuto_str)
        else:
            return None

        marcador_match = re.search(r'>([^<>]+)\s*-\s*([^<>]+)\s*<a class="live" href="[^"]+">(\d+):(\d+)</a>', html_partido, re.DOTALL)
        if not marcador_match:
            return None

        equipo_local = marcador_match.group(1).strip()
        equipo_visita = marcador_match.group(2).strip()
        goles_local = int(marcador_match.group(3))
        goles_visita = int(marcador_match.group(4))

        tarjetas = re.findall(r'class="rcard-(\d+)"', html_partido)
        tarjetas_local = 0
        tarjetas_visita = 0
        if len(tarjetas) == 2:
            tarjetas_local = int(tarjetas[0])
            tarjetas_visita = int(tarjetas[1])
        elif len(tarjetas) == 1:
            tarjetas_local = int(tarjetas[0])
            tarjetas_visita = 0
        elif len(tarjetas) > 2:
            try:
                tarjetas_local = sum(int(t) for t in tarjetas[::2])
                tarjetas_visita = sum(int(t) for t in tarjetas[1::2])
            except ValueError:
                tarjetas_local = 0
                tarjetas_visita = 0

        print(f"DEBUG: Tarjetas rojas extraídas: local={tarjetas_local}, visita={tarjetas_visita}")
        
        tiene_estadisticas = 't=estadisticas' in html_partido
        tiene_apuestas = 't=apuestas' in html_partido or 't=betting' in html_partido
        # ✅ NUEVO: Intentar extraer el nombre de la liga
        liga = 'Desconocida'
        liga_match = re.search(r'class="[^"]*league[^"]*">([^<]+)</[^>]+>', html_partido, re.IGNORECASE)
        if liga_match:
            liga = liga_match.group(1).strip()
        
        
        return {
            'partido_id': partido_id, 
            'equipo_local': equipo_local, 
            'equipo_visita': equipo_visita,
            'minuto': minuto, 
            'goles_local': goles_local, 
            'goles_visita': goles_visita,
            'rojas_local': tarjetas_local, 
            'rojas_visita': tarjetas_visita, 
            'liga': liga,
            'tiene_estadisticas': tiene_estadisticas,
            'tiene_apuestas': tiene_apuestas
        }
    except Exception:
        return None


def _formatear_estadisticas_detalladas(stats: EstadisticasPartido) -> str:
    partes = []
    if stats.remates_local > 0 or stats.remates_visita > 0:
        partes.append(f"Remates:{stats.remates_local}-{stats.remates_visita}")
    if stats.tiros_puerta_local > 0 or stats.tiros_puerta_visita > 0:
        partes.append(f"TirosAPuerta:{stats.tiros_puerta_local}-{stats.tiros_puerta_visita}")
    if stats.corners_local > 0 or stats.corners_visita > 0:
        partes.append(f"Corners:{stats.corners_local}-{stats.corners_visita}")
    if stats.posesion_local > 0 or stats.posesion_visita > 0:
        partes.append(f"Posesion:{stats.posesion_local}%-{stats.posesion_visita}%")
    if stats.ataques_peligrosos_local > 0 or stats.ataques_peligrosos_visita > 0:
        partes.append(f"AtaquesPeligrosos:{stats.ataques_peligrosos_local}-{stats.ataques_peligrosos_visita}")
    if stats.amarillas_local > 0 or stats.amarillas_visita > 0:
        partes.append(f"Amarillas:{stats.amarillas_local}-{stats.amarillas_visita}")
    if stats.faltas_local > 0 or stats.faltas_visita > 0:
        partes.append(f"Faltas:{stats.faltas_local}-{stats.faltas_visita}")
    if stats.tarjetas_rojas_local > 0 or stats.tarjetas_rojas_visita > 0:
        partes.append(f"Rojas:{stats.tarjetas_rojas_local}-{stats.tarjetas_rojas_visita}")
    return " | ".join(partes) if partes else "Sin stats detalladas"


def enviar_alerta_telegram(mensaje: str):
    # Permite configurar las credenciales por variables de entorno
    token = os.getenv('TELEGRAM_BOT_TOKEN', idBot)
    chat_id = os.getenv('TELEGRAM_CHAT_ID', idGrupo)

    if not token or 'TU_TOKEN_DE_BOT' in token or not chat_id or 'TU_ID_DE_GRUPO_O_USUARIO' in str(chat_id):
        print("ERROR: Configura 'TELEGRAM_BOT_TOKEN' y 'TELEGRAM_CHAT_ID' (o ajusta idBot/idGrupo) con tus credenciales de Telegram.")
        return
    try:
        requests.post(
            f'https://api.telegram.org/bot{token}/sendMessage',
            data={'chat_id': chat_id, 'text': mensaje}
        )
    except Exception as e:
        print(f"Error enviando Telegram: {e}")


# --- Motor principal ---

def main_mejorado(data_logger: ImprovedDataLogger, scoring_engine: ScoringEngine):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Ejecutando verificación...")

    try:
        response = requests.get(URL_LIVESCORE, headers=HEADERS, timeout=10)
        soup_dos = BeautifulSoup(response.text, 'html.parser')
        
        # Buscamos el contenedor principal
        score_data = soup_dos.find(id='score-data')
        if not score_data:
            print("No se encontró el contenedor de partidos 'score-data'.")
            return

        # --- MEJORA CRÍTICA: Extraer partidos por bloques de liga ---
        # En lugar de split("<​br/>"), buscamos cada bloque que contiene un ID de partido
        # Los partidos suelen estar dentro de etiquetas <a> o seguidos de spans con clase 'live'
        
        # Buscamos todos los fragmentos de HTML que contienen un enlace a un partido
        # Usamos una expresión regular para encontrar los IDs de 8 caracteres
        html_str = str(score_data)
        
        # Dividimos el HTML por la etiqueta de cierre de cada bloque de partido 
        # o por patrones comunes de separación en la versión móvil
        bloques_partidos = re.split(r'<br\s*/?>', html_str)
        
        partidos_activos = 0

        for x in bloques_partidos:
            # Si el bloque no tiene un ID de partido, lo saltamos
            if 'detalle-del-partido' not in x:
                continue
                
            info_basica = _extraer_info_basica(x)
            
            # Si no pudimos extraer info o el minuto es 0 (no ha empezado), saltar
            if not info_basica:
                continue

            partidos_activos += 1
            partido_id = info_basica['partido_id']
            equipos = f"{info_basica['equipo_local']} - {info_basica['equipo_visita']}"
            liga = info_basica.get('liga', 'Desconocida')

            # Iniciar seguimiento si no existe
            if partido_id not in PARTIDOS_EN_SEGUIMIENTO:
                partido = iniciar_seguimiento_partido(partido_id, equipos, liga, info_basica)
                
                nueva_stats = EstadisticasPartido(
                    minuto=0, g_local=0, g_visita=0, rojas_local=0, rojas_visita=0
                )
                partido.actualizar_stats(nueva_stats)
                
                msg = EstrategiaAnalisis.alerta_resumen_prematch(partido)
                if msg:
                    enviar_alerta_telegram(msg)
                
                msg = EstrategiaAnalisis.alerta_dominio_prematch(partido)
                if msg:
                    enviar_alerta_telegram(msg)
                
                msg = EstrategiaAnalisis.alerta_brecha_clasificacion(partido)
                if msg:
                    enviar_alerta_telegram(msg)

            partido = PARTIDOS_EN_SEGUIMIENTO[partido_id]

            # Obtener estadísticas detalladas
            stats_detalle = _obtener_estadisticas_detalladas(partido_id)
            tiene_estadisticas = stats_detalle.pop('tiene_estadisticas', False)

            stats_actual = EstadisticasPartido(
                minuto=info_basica['minuto'],
                g_local=info_basica['goles_local'],
                g_visita=info_basica['goles_visita'],
                rojas_local=info_basica['rojas_local'],
                rojas_visita=info_basica['rojas_visita'],
                **stats_detalle
            )

            # ACTUALIZACIÓN CRÍTICA:
            # Solo marcamos que tiene estadísticas si el scraper detallado encontró datos.
            partido.tiene_estadisticas = tiene_estadisticas

            partido.actualizar_stats(stats_actual)
            
            partido.tiene_apuestas = info_basica.get('tiene_apuestas', False)

            # Mostrar info extra
            info_extra = []
            if partido.tiene_estadisticas:
                info_extra.append("📊 Estadísticas disponibles")
            if partido.tiene_apuestas:
                info_extra.append("🎲 Apuestas disponibles")
            
            if info_extra:
                mensaje_info = f"⚽ Partido en vivo: {partido.equipos} | " + " | ".join(info_extra) + f"\n🔗 {URL_ESTADISTICAS_BASE.format(partido.id)}"
                print(mensaje_info)
                if not getattr(partido, 'alerta_info_extra_enviada', False):
                    partido.alerta_info_extra_enviada = True

            # Integración scoring + logging
            snap_dict = construir_snapshot(stats_actual)

            try:
                integrar_logger_en_main(data_logger, partido_id, stats_actual.minuto, snap_dict)
            except Exception as e:
                print(f"Error en logging para {partido_id}: {e}")

            # Scoring
            try:
                resultado = integrar_scoring_en_partido(scoring_engine, snap_dict)
                
                xg_home = resultado.get('xg_home', 0.3)
                xg_away = resultado.get('xg_away', 0.3)
                quality_warning = resultado.get('quality_warning', None)
                
                minutos_restantes = 90 - stats_actual.minuto
                
                if stats_actual.goles_local < stats_actual.goles_visita:
                    urgencia_home = 1.3
                    urgencia_away = 0.9
                elif stats_actual.goles_visita < stats_actual.goles_local:
                    urgencia_home = 0.9
                    urgencia_away = 1.3
                else:
                    urgencia_home = 1.0
                    urgencia_away = 1.0
                
                if minutos_restantes <= 10:
                    urgencia_home *= 1.2
                    urgencia_away *= 1.2
                
                factor_10min = 0.15
                
                probs = {
                    'p_goal_10': max(0.05, min(0.95, (xg_home * urgencia_home + xg_away * urgencia_away) * factor_10min)),
                    'p_home_goal_10': max(0.03, min(0.80, xg_home * urgencia_home * factor_10min)),
                    'p_away_goal_10': max(0.03, min(0.80, xg_away * urgencia_away * factor_10min))
                }

                if probs['p_goal_10'] >= 0.35 and 15 <= stats_actual.minuto <= 85:
                    if not partido.alerta_scoring_enviada:
                        partido.alerta_scoring_enviada = True
                        
                        warning_text = ""
                        if quality_warning == 'home_low_quality':
                            warning_text = "\n⚠️ ADVERTENCIA: LOCAL con 0 grandes ocasiones (remates de baja calidad)"
                        elif quality_warning == 'away_low_quality':
                            warning_text = "\n⚠️ ADVERTENCIA: VISITA con 0 grandes ocasiones (remates de baja calidad)"
                        elif quality_warning == 'both_low_quality':
                            warning_text = "\n⚠️ ADVERTENCIA: Ambos equipos con 0 grandes ocasiones (remates de baja calidad)"
                        
                        alerta_scoring = (
                            f"⚽🤖 ALERTA SCORING GOL {stats_actual.minuto}\'\n"
                            f"{equipos} ({stats_actual.goles_local}-{stats_actual.goles_visita})\n"
                            f"Probabilidad gol próximos 10 min: {probs['p_goal_10']:.1%}\n"
                            f"P(Local): {probs['p_home_goal_10']:.1%} | P(Visita): {probs['p_away_goal_10']:.1%}\n"
                            f"{warning_text}\n"
                            f"🔗 {URL_ESTADISTICAS_BASE.format(partido_id)}"
                        )
                        enviar_alerta_telegram(alerta_scoring)
            except Exception as e:
                print(f"Error en scoring para {partido_id}: {e}")

            # Todas las alertas
            for alerta_func in [
                EstrategiaAnalisis.alerta_roja_rapida,
                EstrategiaAnalisis.alerta_rebote_post_roja,
                EstrategiaAnalisis.alerta_corners_tempranos,
                EstrategiaAnalisis.alerta_over15_abierto,
                EstrategiaAnalisis.alerta_dominio_gol,
                EstrategiaAnalisis.alerta_presion_sostenida,
                EstrategiaAnalisis.alerta_wave_ofensiva,
                lambda p: EstrategiaAnalisis.alerta_dominio_silencioso(p, lado='local'),
                lambda p: EstrategiaAnalisis.alerta_dominio_silencioso(p, lado='visita'),
                EstrategiaAnalisis.alerta_dominio_con_posesion_y_ataques,
                EstrategiaAnalisis.alerta_gol_tras_descanso,
                EstrategiaAnalisis.alerta_over_corners_tramo_final,
                EstrategiaAnalisis.alerta_over_amarillas_pro,
                EstrategiaAnalisis.alerta_friccion_mas_presion_gol,
                EstrategiaAnalisis.alerta_doble_roja,
                EstrategiaAnalisis.alerta_goleada_temprana,
                EstrategiaAnalisis.alerta_ritmo_lento_under,
                EstrategiaAnalisis.alerta_remontada_potencial,
                EstrategiaAnalisis.alerta_gol_tardio,
                EstrategiaAnalisis.alerta_colapso_defensivo_post_roja,
                EstrategiaAnalisis.alerta_partido_roto,
                EstrategiaAnalisis.alerta_over25_con_edge,
                EstrategiaAnalisis.alerta_siguiente_gol_con_edge,
                EstrategiaAnalisis.alerta_btts_con_edge,
                EstrategiaAnalisis.alerta_over_corners_con_edge,
                EstrategiaAnalisis.alerta_corners_ritmo_alto,
                EstrategiaAnalisis.alerta_corners_ritmo_bajo,
                EstrategiaAnalisis.alerta_corners_desequilibrio,
                EstrategiaAnalisis.alerta_corners_segundo_tiempo,
                EstrategiaAnalisis.alerta_corners_tramo_final_live,
            ]:
                msg = alerta_func(partido)
                if msg:
                    enviar_alerta_telegram(msg)

            # Log en consola
            stats_detalladas_str = _formatear_estadisticas_detalladas(stats_actual) if tiene_estadisticas else "Sin datos"

            print(
                f"|{stats_actual.minuto}\'| {equipos} ({stats_actual.goles_local}-{stats_actual.goles_visita}) "
                f"| {stats_detalladas_str} | ID: {partido_id}"
            )

        print(f'Partidos en vivo procesados: {partidos_activos}')

        # Limpieza de partidos finalizados
        partidos_a_eliminar = [id_p for id_p, p in PARTIDOS_EN_SEGUIMIENTO.items()
                               if p.estadisticas_actuales and p.estadisticas_actuales.minuto > 95]
        for id_p in partidos_a_eliminar:
            del PARTIDOS_EN_SEGUIMIENTO[id_p]

    except Exception as e:
        print(f"Error general en main: {e}")
        import traceback
        traceback.print_exc()


def run_bot_mejorado():
    print("Iniciando Bot de Análisis de Flashscore...")

    data_logger = ImprovedDataLogger()
    scoring_engine = ScoringEngine()

    print("✅ Sistema de scoring inicializado")
    print("✅ Logger de datos históricos inicializado")

    token = os.getenv('TELEGRAM_BOT_TOKEN', idBot)
    chat = os.getenv('TELEGRAM_CHAT_ID', idGrupo)
    if not token or 'TU_TOKEN_DE_BOT' in token or not chat or 'TU_ID_DE_GRUPO_O_USUARIO' in str(chat):
        print("\n=======================================================")
        print("CONFIGURA: 'TELEGRAM_BOT_TOKEN' y 'TELEGRAM_CHAT_ID' (o ajusta idBot/idGrupo) con tus credenciales de Telegram.")
        print("El bot no enviará alertas hasta configurarlo.")
        print("=======================================================\n")

    while True:
        try:
            main_mejorado(data_logger, scoring_engine)
            time.sleep(INTERVALO_ACTUALIZACION)
        except KeyboardInterrupt:
            print("\nBot detenido por el usuario.")
            break
        except Exception as e:
            print(f"Error crítico en loop principal: {e}")
            print("Reintentando en 30s...")
            time.sleep(30)


if __name__ == "__main__":
    run_bot_mejorado()