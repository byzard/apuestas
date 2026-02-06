from typing import Dict, Optional, Tuple

class EdgeCalculator:
    """Calcula edge (ventaja) comparando probabilidades estimadas vs cuotas del book"""
    
    # Cuotas típicas por mercado (ajusta según tu bookmaker)
    CUOTAS_REFERENCIA = {
        'over_2.5': 1.85,
        'over_1.5_2h': 2.10,
        'btts': 2.00,
        'next_goal_home': 2.20,
        'next_goal_away': 2.50,
        'over_corners_9.5': 1.95,
        'over_amarillas_4.5': 2.00,
    }
    
    # Umbral mínimo de edge para alertar (%)
    EDGE_MINIMO = 8.0  # Solo alerta si hay +8% de ventaja
    
    @staticmethod
    def calcular_cuota_justa(probabilidad: float) -> float:
        """Convierte probabilidad (0-1) a cuota justa"""
        if probabilidad <= 0 or probabilidad >= 1:
            return 1.01
        return 1.0 / probabilidad
    
    @staticmethod
    def calcular_edge(prob_estimada: float, cuota_book: float) -> float:
        """
        Calcula edge (ventaja) en %
        Positivo = hay valor, negativo = no hay valor
        """
        cuota_justa = EdgeCalculator.calcular_cuota_justa(prob_estimada)
        edge = (cuota_book / cuota_justa - 1) * 100
        return edge
    
    @staticmethod
    def tiene_valor(prob_estimada: float, mercado: str, cuota_custom: Optional[float] = None) -> Tuple[bool, float, str]:
        """
        Determina si una apuesta tiene valor
        
        Returns:
            (tiene_valor, edge_porcentaje, explicacion)
        """
        cuota_book = cuota_custom if cuota_custom else EdgeCalculator.CUOTAS_REFERENCIA.get(mercado, 2.0)
        edge = EdgeCalculator.calcular_edge(prob_estimada, cuota_book)
        cuota_justa = EdgeCalculator.calcular_cuota_justa(prob_estimada)
        
        tiene_valor = edge >= EdgeCalculator.EDGE_MINIMO
        
        explicacion = (
            f"📊 Análisis de valor:\n"
            f"P estimada: {prob_estimada:.1%}\n"
            f"Cuota justa: {cuota_justa:.2f}\n"
            f"Cuota book: {cuota_book:.2f}\n"
            f"Edge: {edge:+.1f}%"
        )
        
        return tiene_valor, edge, explicacion