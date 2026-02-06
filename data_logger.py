import sqlite3
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
from pathlib import Path

class ImprovedDataLogger:
    """Sistema mejorado de logging con análisis histórico y machine learning básico"""
    
    def __init__(self, db_path: str = "football_analysis.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Inicializa la base de datos con todas las tablas necesarias"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Tabla principal de partidos con estadísticas detalladas
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id TEXT NOT NULL,
                league TEXT,
                home_team TEXT,
                away_team TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                minute INTEGER,
                home_score INTEGER,
                away_score INTEGER,
                home_shots INTEGER,
                away_shots INTEGER,
                home_shots_on_target INTEGER,
                away_shots_on_target INTEGER,
                home_dangerous_attacks INTEGER,
                away_dangerous_attacks INTEGER,
                home_possession REAL,
                away_possession REAL,
                home_corners INTEGER,
                away_corners INTEGER,
                home_yellow_cards INTEGER,
                away_yellow_cards INTEGER,
                home_red_cards INTEGER,
                away_red_cards INTEGER,
                home_xg REAL,
                away_xg REAL,
                home_momentum REAL,
                away_momentum REAL,
                home_pressure REAL,
                away_pressure REAL
            )
        ''')
        
        # Tabla de alertas generadas
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                minute INTEGER,
                alert_type TEXT,
                score REAL,
                confidence TEXT,
                predicted_outcome TEXT,
                actual_outcome TEXT,
                was_correct BOOLEAN,
                FOREIGN KEY (match_id) REFERENCES matches(match_id)
            )
        ''')
        
        # Tabla de rendimiento del modelo
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS model_performance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date DATE DEFAULT CURRENT_DATE,
                total_predictions INTEGER,
                correct_predictions INTEGER,
                accuracy REAL,
                avg_confidence REAL,
                high_conf_accuracy REAL,
                medium_conf_accuracy REAL,
                low_conf_accuracy REAL
            )
        ''')
        
        # Índices para mejorar el rendimiento
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_match_id ON matches(match_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON matches(timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_league ON matches(league)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_alert_match ON alerts(match_id)')
        
        conn.commit()
        conn.close()
        print("✅ Base de datos inicializada correctamente")
    
    def log_match_analysis(self, match_data: Dict, analysis_result: Dict):
        """Registra un análisis de partido en la base de datos"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Insertar datos del partido
            cursor.execute('''
                INSERT INTO matches (
                    match_id, league, home_team, away_team, minute,
                    home_score, away_score, home_shots, away_shots,
                    home_shots_on_target, away_shots_on_target,
                    home_dangerous_attacks, away_dangerous_attacks,
                    home_possession, away_possession,
                    home_corners, away_corners,
                    home_yellow_cards, away_yellow_cards,
                    home_red_cards, away_red_cards,
                    home_xg, away_xg, home_momentum, away_momentum,
                    home_pressure, away_pressure
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                match_data['match_id'], match_data['league'],
                match_data['home_team'], match_data['away_team'],
                match_data['minute'], match_data['home_score'], match_data['away_score'],
                match_data['home_shots'], match_data['away_shots'],
                match_data['home_shots_on_target'], match_data['away_shots_on_target'],
                match_data['home_dangerous_attacks'], match_data['away_dangerous_attacks'],
                match_data['home_possession'], match_data['away_possession'],
                match_data['home_corners'], match_data['away_corners'],
                match_data['home_yellow_cards'], match_data['away_yellow_cards'],
                match_data['home_red_cards'], match_data['away_red_cards'],
                analysis_result['home_xg'], analysis_result['away_xg'],
                analysis_result['home_momentum'], analysis_result['away_momentum'],
                analysis_result['home_pressure'], analysis_result['away_pressure']
            ))
            
            conn.commit()
        except Exception as e:
            print(f"Error registrando partido: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def log_alert(self, match_id: str, minute: int, alert_type: str, 
                  score: float, confidence: str, predicted_outcome: str):
        """Registra una alerta generada"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO alerts (match_id, minute, alert_type, score, confidence, predicted_outcome)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (match_id, minute, alert_type, score, confidence, predicted_outcome))
            
            conn.commit()
        except Exception as e:
            print(f"Error registrando alerta: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def get_match_history(self, team: str, limit: int = 10) -> List[Dict]:
        """Obtiene el historial de partidos de un equipo"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM matches 
            WHERE home_team = ? OR away_team = ?
            ORDER BY timestamp DESC
            LIMIT ?
        ''', (team, team, limit))
        
        columns = [description[0] for description in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        conn.close()
        return results
    
    def get_league_stats(self, league: str) -> Dict:
        """Obtiene estadísticas agregadas de una liga"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT 
                AVG(home_score + away_score) as avg_goals,
                AVG(home_shots + away_shots) as avg_shots,
                AVG(home_corners + away_corners) as avg_corners,
                COUNT(DISTINCT match_id) as total_matches
            FROM matches
            WHERE league = ?
        ''', (league,))
        
        result = cursor.fetchone()
        conn.close()
        
        return {
            'avg_goals': result[0] or 0,
            'avg_shots': result[1] or 0,
            'avg_corners': result[2] or 0,
            'total_matches': result[3] or 0
        }
    
    def cleanup_old_data(self, days: int = 90):
        """Limpia datos antiguos para mantener la base de datos optimizada"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cutoff_date = datetime.now() - timedelta(days=days)
        
        try:
            cursor.execute('DELETE FROM matches WHERE timestamp < ?', (cutoff_date,))
            cursor.execute('DELETE FROM alerts WHERE timestamp < ?', (cutoff_date,))
            
            deleted = cursor.rowcount
            conn.commit()
            print(f"Eliminados {deleted} registros antiguos")
        except Exception as e:
            print(f"Error limpiando datos: {e}")
            conn.rollback()
        finally:
            conn.close()


# Clase auxiliar para análisis y reportes
class AnalyticsReporter:
    """Genera reportes y análisis de rendimiento"""
    
    def __init__(self, logger: ImprovedDataLogger):
        self.logger = logger
    
    def generate_daily_report(self) -> str:
        """Genera un reporte diario del rendimiento"""
        conn = sqlite3.connect(self.logger.db_path)
        cursor = conn.cursor()
        
        # Obtener estadísticas del día
        cursor.execute('''
            SELECT COUNT(*) as total_matches,
                   AVG(home_score + away_score) as avg_goals
            FROM matches
            WHERE DATE(timestamp) = DATE('now')
        ''')
        
        result = cursor.fetchone()
        conn.close()
        
        return f"📊 Reporte Diario\n" \
               f"Partidos analizados: {result[0]}\n" \
               f"Promedio de goles: {result[1]:.2f if result[1] else 0}"


# ============================================================
# FUNCIÓN DE INTEGRACIÓN (FUERA DE LA CLASE)
# ============================================================

def integrar_logger_en_main(logger: ImprovedDataLogger, match_id: str, minute: int, snapshot: Dict):
    """
    Función de integración para registrar datos desde el main
    
    Args:
        logger: Instancia de ImprovedDataLogger
        match_id: ID del partido
        minute: Minuto actual
        snapshot: Diccionario con datos del partido
    """
    try:
        # Construir datos del partido
        match_data = {
            'match_id': match_id,
            'league': snapshot.get('league', 'Unknown'),
            'home_team': snapshot.get('home_team', 'Home'),
            'away_team': snapshot.get('away_team', 'Away'),
            'minute': minute,
            'home_score': snapshot.get('score_home', 0),
            'away_score': snapshot.get('score_away', 0),
            'home_shots': snapshot.get('home', {}).get('shots', 0),
            'away_shots': snapshot.get('away', {}).get('shots', 0),
            'home_shots_on_target': snapshot.get('home', {}).get('shots_on_target', 0),
            'away_shots_on_target': snapshot.get('away', {}).get('shots_on_target', 0),
            'home_dangerous_attacks': snapshot.get('home', {}).get('dangerous_attacks', 0),
            'away_dangerous_attacks': snapshot.get('away', {}).get('dangerous_attacks', 0),
            'home_possession': snapshot.get('home', {}).get('possession', 0) * 100,
            'away_possession': snapshot.get('away', {}).get('possession', 0) * 100,
            'home_corners': snapshot.get('home', {}).get('corners', 0),
            'away_corners': snapshot.get('away', {}).get('corners', 0),
            'home_yellow_cards': snapshot.get('home', {}).get('yellow_cards', 0),
            'away_yellow_cards': snapshot.get('away', {}).get('yellow_cards', 0),
            'home_red_cards': snapshot.get('home', {}).get('red_cards', 0),
            'away_red_cards': snapshot.get('away', {}).get('red_cards', 0),
        }
        
        # Construir resultado del análisis
        analysis_result = {
            'home_xg': snapshot.get('home', {}).get('xg', 0),
            'away_xg': snapshot.get('away', {}).get('xg', 0),
            'home_momentum': snapshot.get('home', {}).get('last5_momentum', 0),
            'away_momentum': snapshot.get('away', {}).get('last5_momentum', 0),
            'home_pressure': 0,
            'away_pressure': 0,
            'score': 0,
            'confidence': 'LOW',
            'predicted_outcome': ''
        }
        
        # Registrar en la base de datos
        logger.log_match_analysis(match_data, analysis_result)
        
    except Exception as e:
        print(f"Error en integrar_logger_en_main: {e}")


if __name__ == "__main__":
    # Ejemplo de uso
    logger = ImprovedDataLogger()
    reporter = AnalyticsReporter(logger)
    
    print("✅ Sistema de logging mejorado inicializado")
    print("\nGenerando reporte diario...")
    print(reporter.generate_daily_report())