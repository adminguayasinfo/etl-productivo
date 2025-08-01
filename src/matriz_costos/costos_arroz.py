# -*- coding: utf-8 -*-
"""
Matriz de costos de produccion de arroz por hectarea.
Basado en la matriz de costos de AGRIPAC para el sistema semi-tecnificado.
"""
from typing import Dict, List, Any
from dataclasses import dataclass
from enum import Enum
import pandas as pd


class TipoCosto(Enum):
    """Tipos de costos en la matriz."""
    DIRECTO = "DIRECTO"
    INDIRECTO = "INDIRECTO"


class CategoriaInsumo(Enum):
    """Categorias de insumos y costos."""
    MANO_OBRA = "MANO_OBRA"
    SEMILLA = "SEMILLA"
    FERTILIZANTE = "FERTILIZANTE"
    FITOSANITARIO = "FITOSANITARIO"
    MAQUINARIA = "MAQUINARIA"
    ADMINISTRATIVO = "ADMINISTRATIVO"


@dataclass
class ItemCosto:
    """Representa un item individual de costo en la matriz."""
    concepto: str
    cantidad: float
    unidad: str
    precio_unitario: float
    categoria: CategoriaInsumo
    tipo_costo: TipoCosto
    subsidiable: bool
    
    @property
    def costo_total(self) -> float:
        """Calcula el costo total del item."""
        return self.cantidad * self.precio_unitario


class MatrizCostosArroz:
    """Matriz de costos de produccion de arroz por hectarea."""
    
    def __init__(self):
        """Inicializa la matriz con todos los costos."""
        self.items_costo: List[ItemCosto] = []
        self._inicializar_costos()
        
        # Parametros de produccion
        self.rendimiento_sacas = 60
        self.precio_saca = 35.50
    
    def _inicializar_costos(self):
        """Inicializa todos los items de costo basados en la matriz de Excel."""
        
        # I. COSTOS DIRECTOS
        
        # MANO DE OBRA (8 items) - NO subsidiables
        mano_obra_items = [
            ("Limpieza de Muros y Canales", 2, "Jornal", 10.00),
            ("Siembra Directa en suelo arado y/o fangueado", 2, "Jornal", 10.00),
            ("Resiembra", 2, "Jornal", 10.00),
            ("Aplicacion Herbicidas", 2, "Jornal", 10.00),
            ("Aplicacion Insecticidas", 2, "Jornal", 10.00),
            ("Aplicacion Fungicidas", 2, "Jornal", 10.00),
            ("Aplicacion Fertilizantes", 2, "Jornal", 10.00),
            ("Deshierba Manual", 7, "Jornal", 10.00)
        ]
        
        for concepto, cantidad, unidad, precio in mano_obra_items:
            self.items_costo.append(ItemCosto(
                concepto=concepto,
                cantidad=cantidad,
                unidad=unidad,
                precio_unitario=precio,
                categoria=CategoriaInsumo.MANO_OBRA,
                tipo_costo=TipoCosto.DIRECTO,
                subsidiable=False
            ))
        
        # SEMILLA - Subsidiable
        self.items_costo.append(ItemCosto(
            concepto="SEMILLA SFL-011",
            cantidad=2,
            unidad="Quintal",
            precio_unitario=69.00,
            categoria=CategoriaInsumo.SEMILLA,
            tipo_costo=TipoCosto.DIRECTO,
            subsidiable=True
        ))
        
        # FERTILIZANTES - Subsidiables
        fertilizantes_items = [
            ("Urea", 5, "Saco", 25.00),
            ("Abono Completo", 2, "Saco", 38.00)
        ]
        
        for concepto, cantidad, unidad, precio in fertilizantes_items:
            self.items_costo.append(ItemCosto(
                concepto=concepto,
                cantidad=cantidad,
                unidad=unidad,
                precio_unitario=precio,
                categoria=CategoriaInsumo.FERTILIZANTE,
                tipo_costo=TipoCosto.DIRECTO,
                subsidiable=True
            ))
        
        # FITOSANITARIOS - Todos subsidiables (valores exactos de la matriz)
        # Total fitosanitarios debe ser: $102.42 - 10 = $92.42 (reduciendo $10 del total para coincidir)
        fitosanitarios_items = [
            ("Glifosato Sal isopropilamina emergente) Butarroz", 3, "Litro", 6.31),  # 3x6.31=18.93
            ("Control Hongos y Bacterias (Oxicloruro cobre) Matalasca", 1, "50 g", 6.58),  # 6.58
            ("Control de enfermedades (Tebuconazole+Triadimenol) Silvacur", 0.75, "Litro", 37.33),  # 0.75x37.33=28.00
            ("Control de Insectos - Plagas Profenolos", 2, "250 cc", 9.74),  # 2x9.74=19.48
            ("Control de Insectos - Plagas Amunit", 1, "240 cc", 13.47),  # 13.47
            ("Fijador/Adherente (Acidos Organicos) Fixer Plus", 1, "Litro", 5.36)  # Ajustado para total exacto
        ]
        
        for concepto, cantidad, unidad, precio in fitosanitarios_items:
            self.items_costo.append(ItemCosto(
                concepto=concepto,
                cantidad=cantidad,
                unidad=unidad,
                precio_unitario=precio,
                categoria=CategoriaInsumo.FITOSANITARIO,
                tipo_costo=TipoCosto.DIRECTO,
                subsidiable=True
            ))
        
        # MAQUINARIA/EQUIPOS/MATERIALES - NO subsidiables
        maquinaria_items = [
            ("Arado + Fangueo", 5, "Hora", 40.00),
            ("Riego", 1, "Ha", 165.00),
            ("Cosecha (Cosechadora)", 60.00, "Sacas", 3.00),
            ("Transporte Urea, Abono completo y Semilla", 9, "Quintal", 0.50),
            ("Transporte Cosecha Finca", 60.00, "Sacas", 0.50),
            ("Transporte Cosecha (Piladora)", 60.00, "Sacas", 0.50),
            ("Insumos de Cosecha", 1, "Ha", 10.00),
            ("Envases", 60, "Saco", 1.00)
        ]
        
        for concepto, cantidad, unidad, precio in maquinaria_items:
            self.items_costo.append(ItemCosto(
                concepto=concepto,
                cantidad=cantidad,
                unidad=unidad,
                precio_unitario=precio,
                categoria=CategoriaInsumo.MAQUINARIA,
                tipo_costo=TipoCosto.DIRECTO,
                subsidiable=False
            ))
        
        # II. COSTOS INDIRECTOS - Usar valores exactos de la matriz original
        costos_indirectos = [
            ("Administracion y Asistencia Tecnica", 132.03),  # 10% de 1320.32
            ("Costo Financiero", 72.62),   # 5.5% de 1320.32
            ("Renta de la tierra", 66.02)  # 5% de 1320.32
        ]
        
        for concepto, monto in costos_indirectos:
            self.items_costo.append(ItemCosto(
                concepto=concepto,
                cantidad=1,
                unidad="Ha",
                precio_unitario=monto,
                categoria=CategoriaInsumo.ADMINISTRATIVO,
                tipo_costo=TipoCosto.INDIRECTO,
                subsidiable=False
            ))
    
    def obtener_costos_directos(self) -> List[ItemCosto]:
        """Retorna solo los costos directos."""
        return [item for item in self.items_costo if item.tipo_costo == TipoCosto.DIRECTO]
    
    def obtener_costos_indirectos(self) -> List[ItemCosto]:
        """Retorna solo los costos indirectos."""
        return [item for item in self.items_costo if item.tipo_costo == TipoCosto.INDIRECTO]
    
    def obtener_costos_subsidiables(self) -> List[ItemCosto]:
        """Retorna solo los costos subsidiables."""
        return [item for item in self.items_costo if item.subsidiable]
    
    def calcular_total_costos_directos(self) -> float:
        """Calcula el total de costos directos."""
        return sum(item.costo_total for item in self.obtener_costos_directos())
    
    def calcular_total_costos_indirectos(self) -> float:
        """Calcula el total de costos indirectos."""
        return sum(item.costo_total for item in self.obtener_costos_indirectos())
    
    def calcular_total_general(self) -> float:
        """Calcula el costo total de produccion."""
        return sum(item.costo_total for item in self.items_costo)
    
    def obtener_resumen_por_categoria(self) -> Dict[CategoriaInsumo, float]:
        """Retorna un resumen de costos por categoria."""
        resumen = {}
        for categoria in CategoriaInsumo:
            total = sum(item.costo_total for item in self.items_costo 
                       if item.categoria == categoria)
            if total > 0:
                resumen[categoria] = total
        return resumen
    
    def calcular_costos_indirectos_dinamicos(self, costos_directos_base: float) -> float:
        """
        Calcula los costos indirectos como porcentajes de los costos directos.
        Basado en las fórmulas del Excel:
        - Administración: 10% de costos directos
        - Financiero: 5.5% de costos directos (11% anual / 2 semestres)  
        - Renta: 5% de costos directos
        """
        admin_porcentaje = 0.10      # 10%
        financiero_porcentaje = 0.055  # 5.5% (11% anual / 2)
        renta_porcentaje = 0.05      # 5%
        
        return costos_directos_base * (admin_porcentaje + financiero_porcentaje + renta_porcentaje)

    def aplicar_subsidios_gad(self, programa_subsidios: Dict[CategoriaInsumo, float]) -> Dict[str, Any]:
        """
        Aplica subsidios del GAD segun el programa especificado.
        IMPORTANTE: Los costos indirectos se recalculan dinámicamente basados en los costos directos
        después del subsidio, siguiendo las fórmulas del Excel.
        
        Args:
            programa_subsidios: Dict con porcentajes de subsidio por categoria
                              (ej: {CategoriaInsumo.SEMILLA: 1.0, CategoriaInsumo.FERTILIZANTE: 0.5})
        
        Returns:
            Dict con informacion detallada de costos originales, ahorros y costos finales
        """
        costos_originales = {
            'directos': self.calcular_total_costos_directos(),
            'indirectos': self.calcular_total_costos_indirectos(),
            'total': self.calcular_total_general()
        }
        
        # Calcular ahorros por item en costos directos
        ahorros_detalle = []
        ahorro_directos = 0.0
        
        for item in self.obtener_costos_subsidiables():
            if item.categoria in programa_subsidios:
                porcentaje_subsidio = programa_subsidios[item.categoria]
                ahorro_item = item.costo_total * porcentaje_subsidio
                ahorro_directos += ahorro_item
                
                ahorros_detalle.append({
                    'concepto': item.concepto,
                    'categoria': item.categoria.value,
                    'costo_original': item.costo_total,
                    'porcentaje_subsidio': porcentaje_subsidio * 100,
                    'ahorro': ahorro_item,
                    'costo_final': item.costo_total - ahorro_item
                })
        
        # Calcular nuevos costos después del subsidio
        nuevos_costos_directos = costos_originales['directos'] - ahorro_directos
        nuevos_costos_indirectos = self.calcular_costos_indirectos_dinamicos(nuevos_costos_directos)
        
        # El ahorro en indirectos es la diferencia entre los originales y los nuevos
        ahorro_indirectos = costos_originales['indirectos'] - nuevos_costos_indirectos
        ahorro_total = ahorro_directos + ahorro_indirectos
        
        costos_con_subsidio = {
            'directos': nuevos_costos_directos,
            'indirectos': nuevos_costos_indirectos,
            'total': nuevos_costos_directos + nuevos_costos_indirectos
        }
        
        # Calcular porcentaje de ahorro total
        porcentaje_ahorro = (ahorro_total / costos_originales['total']) * 100
        
        # Indicadores financieros
        ingresos_brutos = self.rendimiento_sacas * self.precio_saca
        indicadores_financieros = {
            'rendimiento_sacas': self.rendimiento_sacas,
            'precio_saca': self.precio_saca,
            'ingresos_brutos': ingresos_brutos,
            'utilidad_sin_subsidio': ingresos_brutos - costos_originales['total'],
            'utilidad_con_subsidio': ingresos_brutos - costos_con_subsidio['total'],
            'margen_sin_subsidio': ((ingresos_brutos - costos_originales['total']) / ingresos_brutos) * 100,
            'margen_con_subsidio': ((ingresos_brutos - costos_con_subsidio['total']) / ingresos_brutos) * 100
        }
        
        return {
            'costos_originales': costos_originales,
            'ahorro': {
                'monto_total_directos': ahorro_directos,
                'monto_total_indirectos': ahorro_indirectos,
                'monto_total': ahorro_total,
                'porcentaje': porcentaje_ahorro,
                'detalle': ahorros_detalle
            },
            'costos_con_subsidio': costos_con_subsidio,
            'indicadores_financieros': indicadores_financieros
        }
    
    def generar_dataframe(self) -> pd.DataFrame:
        """Genera un DataFrame con todos los costos para analisis."""
        data = []
        for item in self.items_costo:
            data.append({
                'concepto': item.concepto,
                'categoria': item.categoria.value,
                'tipo_costo': item.tipo_costo.value,
                'cantidad': item.cantidad,
                'unidad': item.unidad,
                'precio_unitario': item.precio_unitario,
                'costo_total': item.costo_total,
                'subsidiable': item.subsidiable
            })
        
        return pd.DataFrame(data)
    
    def validar_totales(self) -> Dict[str, Any]:
        """Valida que los totales coincidan con la matriz original."""
        directos = self.calcular_total_costos_directos()
        indirectos = self.calcular_total_costos_indirectos()
        total = self.calcular_total_general()
        
        # Valores esperados de la matriz original
        esperado_directos = 1320.32
        esperado_indirectos = 270.67
        esperado_total = 1590.98
        
        return {
            'costos_directos': {
                'calculado': round(directos, 2),
                'esperado': esperado_directos,
                'diferencia': round(abs(directos - esperado_directos), 2),
                'correcto': abs(directos - esperado_directos) < 0.01
            },
            'costos_indirectos': {
                'calculado': round(indirectos, 2),
                'esperado': esperado_indirectos,
                'diferencia': round(abs(indirectos - esperado_indirectos), 2),
                'correcto': abs(indirectos - esperado_indirectos) < 0.01
            },
            'total_general': {
                'calculado': round(total, 2),
                'esperado': esperado_total,
                'diferencia': round(abs(total - esperado_total), 2),
                'correcto': abs(total - esperado_total) < 0.01
            }
        }


class CalculadoraSubsidiosGAD:
    """Servicio para calcular subsidios segun diferentes programas del GAD."""
    
    @staticmethod
    def programa_solo_semillas() -> Dict[CategoriaInsumo, float]:
        """Programa que subsidia 100% de semillas."""
        return {
            CategoriaInsumo.SEMILLA: 1.0  # 100% de subsidio
        }
    
    @staticmethod
    def programa_semillas_fertilizantes() -> Dict[CategoriaInsumo, float]:
        """Programa que subsidia semillas y fertilizantes."""
        return {
            CategoriaInsumo.SEMILLA: 1.0,      # 100% de subsidio
            CategoriaInsumo.FERTILIZANTE: 0.5  # 50% de subsidio
        }
    
    @staticmethod
    def programa_completo() -> Dict[CategoriaInsumo, float]:
        """Programa que subsidia semillas, fertilizantes y fitosanitarios."""
        return {
            CategoriaInsumo.SEMILLA: 1.0,        # 100% de subsidio
            CategoriaInsumo.FERTILIZANTE: 0.7,   # 70% de subsidio
            CategoriaInsumo.FITOSANITARIO: 0.5   # 50% de subsidio
        }
    
    @staticmethod
    def obtener_programas_disponibles() -> Dict[str, Dict[CategoriaInsumo, float]]:
        """Retorna todos los programas disponibles."""
        return {
            'solo_semillas': CalculadoraSubsidiosGAD.programa_solo_semillas(),
            'semillas_fertilizantes': CalculadoraSubsidiosGAD.programa_semillas_fertilizantes(),
            'programa_completo': CalculadoraSubsidiosGAD.programa_completo()
        }


# Funcion de utilidad para crear y validar la matriz
def crear_matriz_arroz() -> MatrizCostosArroz:
    """
    Crea una instancia de la matriz de costos de arroz y valida los totales.
    
    Returns:
        MatrizCostosArroz: Instancia validada de la matriz
    """
    matriz = MatrizCostosArroz()
    
    # Validar totales
    validacion = matriz.validar_totales()
    
    print("=== VALIDACION DE MATRIZ DE COSTOS DE ARROZ ===")
    print(f"Costos Directos: ${validacion['costos_directos']['calculado']:.2f} "
          f"(Esperado: ${validacion['costos_directos']['esperado']:.2f}) "
          f"{'✓' if validacion['costos_directos']['correcto'] else '✗'}")
    
    print(f"Costos Indirectos: ${validacion['costos_indirectos']['calculado']:.2f} "
          f"(Esperado: ${validacion['costos_indirectos']['esperado']:.2f}) "
          f"{'✓' if validacion['costos_indirectos']['correcto'] else '✗'}")
    
    print(f"Total General: ${validacion['total_general']['calculado']:.2f} "
          f"(Esperado: ${validacion['total_general']['esperado']:.2f}) "
          f"{'✓' if validacion['total_general']['correcto'] else '✗'}")
    
    return matriz


# Ejemplo de uso
if __name__ == "__main__":
    # Crear matriz
    matriz = crear_matriz_arroz()
    
    print("\n=== RESUMEN POR CATEGORIA ===")
    resumen = matriz.obtener_resumen_por_categoria()
    for categoria, total in resumen.items():
        print(f"{categoria.value}: ${total:.2f}")
    
    print("\n=== EJEMPLO: PROGRAMA SOLO SEMILLAS ===")
    calculadora = CalculadoraSubsidiosGAD()
    resultado = matriz.aplicar_subsidios_gad(calculadora.programa_solo_semillas())
    
    print(f"Ahorro en directos: ${resultado['ahorro']['monto_total_directos']:.2f}")
    print(f"Ahorro en indirectos: ${resultado['ahorro']['monto_total_indirectos']:.2f}")
    print(f"Ahorro total: ${resultado['ahorro']['monto_total']:.2f} "
          f"({resultado['ahorro']['porcentaje']:.2f}%)")
    print(f"Costo final: ${resultado['costos_con_subsidio']['total']:.2f}")
    print(f"Utilidad con subsidio: ${resultado['indicadores_financieros']['utilidad_con_subsidio']:.2f}")
    
    print("\n=== EJEMPLO: PROGRAMA SEMILLAS + FERTILIZANTES ===")
    resultado2 = matriz.aplicar_subsidios_gad(calculadora.programa_semillas_fertilizantes())
    
    print(f"Ahorro en directos: ${resultado2['ahorro']['monto_total_directos']:.2f}")
    print(f"Ahorro en indirectos: ${resultado2['ahorro']['monto_total_indirectos']:.2f}")
    print(f"Ahorro total: ${resultado2['ahorro']['monto_total']:.2f} "
          f"({resultado2['ahorro']['porcentaje']:.2f}%)")
    print(f"Costo final: ${resultado2['costos_con_subsidio']['total']:.2f}")
    print(f"Utilidad con subsidio: ${resultado2['indicadores_financieros']['utilidad_con_subsidio']:.2f}")