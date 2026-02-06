import json
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import math

class ExpectedGoalsCalculator:
    """Calcula Expected Goals (xG) basado en estadísticas del partido"""
    
    def __init__(self):
        # Pesos ajustados para valores más realistas
        self.weights = {
            'shots_on_target': 0.40,
            'shots': 0.20,
            'dangerous_attacks': 0.20,
            'possession': 0.10,
            'corners': 0.10
        }
    
    def calculate_xg(self, stats: Dict) -> float:
        """
        Calcula el xG para un equipo basado en sus estadísticas
        
        Args:
            stats: Diccionario con estadísticas del equipo
                   {shots, shots_on_target, dangerous_attacks, possession, corners, big_chances, xgot}
        
        Returns:
            float: Expected Goals (xG) - Valor típico entre 0.3 y 3.0
        """
        try:
            # Normalizar estadísticas
            shots = stats.get('shots', 0)
            shots_on_target = stats.get('shots_on_target', 0)
            dangerous_attacks = stats.get('dangerous_attacks', 0)
            possession = stats.get('possession', 0)
            corners = stats.get('corners', 0)
            big_chances = stats.get('big_chances', 0)
            xgot = stats.get('xgot', None)  # xG On Target (calidad real)
            
            # Calcular componentes del xG con valores más realistas
            # Cada tiro a puerta vale ~0.15 xG, cada tiro fuera ~0.05 xG
            shots_off_target = max(0, shots - shots_on_target)
            xg_shots = (shots_on_target * 0.15 + shots_off_target * 0.05)
            
            # Ataques peligrosos: cada 10 ataques ≈ 0.3 xG
            xg_attacks = (dangerous_attacks / 10) * 0.3
            
            # Posesión: 60% posesión ≈ 0.2 xG adicional
            xg_possession = ((possession - 50) / 50) * 0.2 if possession > 50 else 0
            
            # Corners: cada corner ≈ 0.03 xG
            xg_corners = corners * 0.03
            
            # xG total base
            total_xg = xg_shots + xg_attacks + xg_possession + xg_corners
            
            # ===== AJUSTE DE CALIDAD =====
            quality_factor = 1.0
            
            # Opción 1: Si tenemos xGOT (calidad real de los remates)
            if xgot is not None and shots_on_target > 0:
                # Comparar xGOT real vs xG esperado de los tiros a puerta
                expected_xg_from_shots = shots_on_target * 0.15
                if expected_xg_from_shots > 0:
                    quality_ratio = xgot / expected_xg_from_shots
                    # Si quality_ratio < 1 = remates de mala calidad
                    # Si quality_ratio > 1 = remates de buena calidad
                    quality_factor = max(0.5, min(1.8, quality_ratio))
            
            # Opción 2: Si tenemos grandes ocasiones
            elif big_chances > 0:
                # Cada gran ocasión aumenta el xG significativamente
                quality_factor = 1.0 + (big_chances * 0.35)
            
            # Opción 3: Penalizar volumen sin calidad
            elif shots_on_target >= 3 and big_chances == 0:
                # Muchos tiros a puerta pero ninguna gran ocasión = mala calidad
                quality_factor = 0.65
            
            # Aplicar factor de calidad
            total_xg = total_xg * quality_factor
            
            # Mínimo realista: siempre hay alguna probabilidad
            total_xg = max(0.15, total_xg)
            
            return round(total_xg, 2)
            
        except Exception as e:
            print(f"Error calculando xG: {e}")
            return 0.3  # Valor por defecto más realista


class MomentumAnalyzer:
    """Analiza el momentum del partido en ventanas de tiempo"""
    
    def __init__(self, window_size: int = 5):
        self.window_size = window_size
        self.history = []
    
    def add_snapshot(self, minute: int, home_stats: Dict, away_stats: Dict):
        """Agrega un snapshot de estadísticas"""
        self.history.append({
            'minute': minute,
            'home': home_stats.copy(),
            'away': away_stats.copy()
        })
        
        # Mantener solo los últimos N snapshots
        if len(self.history) > self.window_size:
            self.history.pop(0)
    
    def calculate_momentum(self, team: str = 'home') -> float:
        """
        Calcula el momentum de un equipo en la ventana de tiempo
        
        Returns:
            float: Valor de momentum (-1 a 1, donde 1 es momentum muy positivo)
        """
        if len(self.history) < 2:
            return 0.0
        
        try:
            # Comparar primer y último snapshot
            first = self.history[0][team]
            last = self.history[-1][team]
            
            # Calcular diferencias
            shots_diff = last.get('shots', 0) - first.get('shots', 0)
            attacks_diff = last.get('dangerous_attacks', 0) - first.get('dangerous_attacks', 0)
            possession_diff = last.get('possession', 0) - first.get('possession', 0)
            
            # Normalizar y ponderar
            momentum = (
                shots_diff * 0.4 +
                attacks_diff * 0.4 +
                possession_diff * 100 * 0.2
            ) / self.window_size
            
            # Limitar entre -1 y 1
            return max(-1, min(1, momentum / 5))
            
        except Exception as e:
            print(f"Error calculando momentum: {e}")
            return 0.0


class ScoringEngine:
    """Motor principal de scoring mejorado"""
    
    def __init__(self):
        self.xg_calculator = ExpectedGoalsCalculator()
        self.momentum_analyzer = MomentumAnalyzer()
        
        # Pesos de ligas (puedes ajustar según tus preferencias)
        self.league_weights = {
            'Premier League': 1.0,
            'La Liga': 1.0,
            'Bundesliga': 0.95,
            'Serie A': 0.95,
            'Ligue 1': 0.90,
            'Championship': 0.85,
            'Eredivisie': 0.80,
            'Liga MX': 0.75,
            'MLS': 0.70,
            'default': 0.65
        }
    
    def get_league_weight(self, league: str) -> float:
        """Obtiene el peso de una liga"""
        return self.league_weights.get(league, self.league_weights['default'])
    
    def calculate_pressure_score(self, stats: Dict, opponent_stats: Dict) -> float:
        """
        Calcula un score de presión ofensiva
        
        Args:
            stats: Estadísticas del equipo atacante
            opponent_stats: Estadísticas del equipo defensor
        
        Returns:
            float: Score de presión (0-100)
        """
        try:
            # Componentes de presión
            shot_pressure = stats.get('shots', 0) * 3
            attack_pressure = stats.get('dangerous_attacks', 0) * 2
            possession_pressure = stats.get('possession', 0) * 50
            corner_pressure = stats.get('corners', 0) * 4
            
            # Penalización por defensa sólida del oponente
            opponent_defense = opponent_stats.get('shots_on_target', 0) * 2
            
            total_pressure = (
                shot_pressure + 
                attack_pressure + 
                possession_pressure + 
                corner_pressure - 
                opponent_defense
            )
            
            # Normalizar a 0-100
            return max(0, min(100, total_pressure))
            
        except Exception as e:
            print(f"Error calculando presión: {e}")
            return 0.0
    
    def analyze_match(self, match_data: Dict) -> Dict:
        """
        Análisis completo del partido
        
        Args:
            match_data: Diccionario con toda la información del partido
        
        Returns:
            Dict con el análisis completo y score
        """
        # Estructura por defecto con todas las claves necesarias
        default_result = {
            'score': 0.0,
            'confidence': 'LOW',
            'home_xg': 0.0,
            'away_xg': 0.0,
            'home_momentum': 0.0,
            'away_momentum': 0.0,
            'home_pressure': 0.0,
            'away_pressure': 0.0,
            'league_weight': 0.65,
            'predicted_outcome': 'UNKNOWN',
            'quality_warning': None
        }
        
        try:
            home_stats = match_data.get('home', {})
            away_stats = match_data.get('away', {})
            minute = match_data.get('minute', 0)
            league = match_data.get('league', 'Unknown')
            
            # Calcular xG
            home_xg = self.xg_calculator.calculate_xg(home_stats)
            away_xg = self.xg_calculator.calculate_xg(away_stats)
            
            # Detectar advertencias de calidad
            quality_warning = None
            home_big_chances = home_stats.get('big_chances', 0)
            away_big_chances = away_stats.get('big_chances', 0)
            home_shots_on_target = home_stats.get('shots_on_target', 0)
            away_shots_on_target = away_stats.get('shots_on_target', 0)
            
            if home_shots_on_target >= 3 and home_big_chances == 0:
                quality_warning = 'home_low_quality'
            if away_shots_on_target >= 3 and away_big_chances == 0:
                if quality_warning:
                    quality_warning = 'both_low_quality'
                else:
                    quality_warning = 'away_low_quality'
            
            # Actualizar momentum
            self.momentum_analyzer.add_snapshot(minute, home_stats, away_stats)
            home_momentum = self.momentum_analyzer.calculate_momentum('home')
            away_momentum = self.momentum_analyzer.calculate_momentum('away')
            
            # Calcular presión
            home_pressure = self.calculate_pressure_score(home_stats, away_stats)
            away_pressure = self.calculate_pressure_score(away_stats, home_stats)
            
            # Score final ponderado por liga
            league_weight = self.get_league_weight(league)
            
            final_score = (
                (home_xg + away_xg) * 20 +
                abs(home_momentum - away_momentum) * 30 +
                (home_pressure + away_pressure) / 2
            ) * league_weight
            
            # Determinar confianza
            confidence = 'LOW'
            if final_score > 70:
                confidence = 'HIGH'
            elif final_score > 50:
                confidence = 'MEDIUM'
            
            return {
                'score': round(final_score, 2),
                'confidence': confidence,
                'home_xg': home_xg,
                'away_xg': away_xg,
                'home_momentum': round(home_momentum, 2),
                'away_momentum': round(away_momentum, 2),
                'home_pressure': round(home_pressure, 2),
                'away_pressure': round(away_pressure, 2),
                'league_weight': league_weight,
                'predicted_outcome': self._predict_outcome(home_xg, away_xg, home_momentum, away_momentum),
                'quality_warning': quality_warning
            }
            
        except Exception as e:
            print(f"Error en análisis de partido: {e}")
            import traceback
            traceback.print_exc()
            default_result['error'] = str(e)
            return default_result
    
    def _predict_outcome(self, home_xg: float, away_xg: float, 
                        home_momentum: float, away_momentum: float) -> str:
        """Predice el resultado más probable"""
        
        home_advantage = home_xg + home_momentum
        away_advantage = away_xg + away_momentum
        
        diff = home_advantage - away_advantage
        
        if diff > 0.5:
            return "HOME_WIN"
        elif diff < -0.5:
            return "AWAY_WIN"
        else:
            return "DRAW_OR_CLOSE"


# ============================================================
# FUNCIÓN DE INTEGRACIÓN
# ============================================================

def integrar_scoring_en_partido(scoring_engine: ScoringEngine, partido_data: Dict) -> Dict:
    """
    Función para integrar el scoring en el análisis de partidos
    
    Args:
        scoring_engine: Instancia de ScoringEngine
        partido_data: Datos del partido desde el main
    
    Returns:
        Dict con partido_data actualizado con el análisis
    """
    try:
        # Preparar datos en el formato que espera analyze_match
        match_data_formatted = {
            'home': partido_data.get('home_stats', {}),
            'away': partido_data.get('away_stats', {}),
            'minute': partido_data.get('minute', 0),
            'league': partido_data.get('league', 'Unknown')
        }
        
        # Realizar el análisis
        analysis = scoring_engine.analyze_match(match_data_formatted)
        
        # Agregar el análisis al partido_data de forma segura
        partido_data['analysis'] = analysis
        partido_data['xg_home'] = analysis.get('home_xg', 0.0)
        partido_data['xg_away'] = analysis.get('away_xg', 0.0)
        partido_data['momentum_home'] = analysis.get('home_momentum', 0.0)
        partido_data['momentum_away'] = analysis.get('away_momentum', 0.0)
        partido_data['opportunity_score'] = analysis.get('score', 0.0)
        partido_data['quality_warning'] = analysis.get('quality_warning', None)
        
        # Agregar probabilidades de gol (valores por defecto si no existen)
        partido_data['p_home_goal_10'] = partido_data.get('p_home_goal_10', 0.0)
        partido_data['p_away_goal_10'] = partido_data.get('p_away_goal_10', 0.0)
        partido_data['p_home_goal_15'] = partido_data.get('p_home_goal_15', 0.0)
        partido_data['p_away_goal_15'] = partido_data.get('p_away_goal_15', 0.0)
        partido_data['p_home_goal_30'] = partido_data.get('p_home_goal_30', 0.0)
        partido_data['p_away_goal_30'] = partido_data.get('p_away_goal_30', 0.0)
        
        return partido_data
        
    except Exception as e:
        print(f"Error en integrar_scoring_en_partido: {e}")
        import traceback
        traceback.print_exc()
        
        # Retornar partido_data con valores por defecto en caso de error
        partido_data['analysis'] = {
            'score': 0.0,
            'confidence': 'LOW',
            'home_xg': 0.0,
            'away_xg': 0.0,
            'home_momentum': 0.0,
            'away_momentum': 0.0,
            'home_pressure': 0.0,
            'away_pressure': 0.0,
            'league_weight': 0.65,
            'predicted_outcome': 'UNKNOWN',
            'quality_warning': None,
            'error': str(e)
        }
        partido_data['xg_home'] = 0.0
        partido_data['xg_away'] = 0.0
        partido_data['momentum_home'] = 0.0
        partido_data['momentum_away'] = 0.0
        partido_data['opportunity_score'] = 0.0
        partido_data['quality_warning'] = None
        partido_data['p_home_goal_10'] = 0.0
        partido_data['p_away_goal_10'] = 0.0
        partido_data['p_home_goal_15'] = 0.0
        partido_data['p_away_goal_15'] = 0.0
        partido_data['p_home_goal_30'] = 0.0
        partido_data['p_away_goal_30'] = 0.0
        return partido_data