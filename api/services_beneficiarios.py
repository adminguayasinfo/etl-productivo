# -*- coding: utf-8 -*-
"""
Servicios para el API de beneficiarios.
Contiene toda la lógica de negocio para análisis de beneficiarios y reducción de costos.
"""
import os
from typing import List, Dict, Any, Tuple
from datetime import datetime
from sqlalchemy import text
import logging

from config.connections.database import DatabaseConnection
from src.matriz_costos.costos_arroz import MatrizCostosArroz
from src.matriz_costos.costos_maiz import MatrizCostosMaiz
from api.models_beneficiarios import (
    BeneficiariosResponse, BeneficiariosPorSubvencion, DistribucionSubvenciones,
    TopBeneficiario, BeneficioCultivo, DetalleSubsidio, ResumenBeneficiarios,
    TipoBeneficio, TipoCultivo
)

logger = logging.getLogger(__name__)


class BeneficiariosService:
    """Servicio principal para análisis de beneficiarios."""
    
    def __init__(self):
        """Inicializa el servicio con conexión a BD y matrices de costos."""
        # Usar DatabaseConnection que ya carga las variables del .env
        self.db = DatabaseConnection()
        self.db.init_engine()
        
        # Inicializar matrices de costos
        self.matriz_arroz = MatrizCostosArroz()
        self.matriz_maiz = MatrizCostosMaiz()
        
        # Costos específicos por cultivo y beneficio
        self.costos_mecanizacion = {
            'ARROZ': 200.00,  # "Arado + Fangueo", 5, "Hora", 40.00 = 200.00
            'MAIZ': 70.00     # "Arado + Rastra", 2, "Hora", 35.00 = 70.00
        }
        
        self.costos_produccion_por_hectarea = {
            'ARROZ': self.matriz_arroz.calcular_total_general(),
            'MAIZ': self.matriz_maiz.calcular_total_general()
        }
    
    def obtener_beneficiarios_por_subvencion(self) -> Dict[str, BeneficiariosPorSubvencion]:
        """Obtiene beneficiarios únicos por tipo de subvención y montos totales."""
        result = {}
        
        for beneficio in ['SEMILLAS', 'FERTILIZANTES', 'MECANIZACION']:
            if beneficio == 'MECANIZACION':
                # Para mecanización, calcular monto usando costos de matrices
                query = """
                SELECT 
                    COUNT(DISTINCT b.beneficiario_id) as beneficiarios_unicos,
                    SUM(
                        CASE 
                            WHEN tc.nombre = 'ARROZ' THEN 200.00  -- Arado + Fangueo
                            WHEN tc.nombre = 'MAIZ' THEN 70.00    -- Arado + Rastra  
                            ELSE 0
                        END
                    ) as monto_total
                FROM "etl-productivo".beneficio b
                INNER JOIN "etl-productivo".tipo_cultivo tc ON b.tipo_cultivo_id = tc.id
                WHERE b.tipo_beneficio = :beneficio
                AND tc.nombre IN ('ARROZ', 'MAIZ')
                """
                
                with self.db.engine.connect() as conn:
                    result_data = conn.execute(text(query), {'beneficio': beneficio}).fetchone()
                    
                result[beneficio.lower()] = BeneficiariosPorSubvencion(
                    count=int(result_data[0]) if result_data[0] else 0,
                    monto_total=float(result_data[1]) if result_data[1] else 0.0
                )
            else:
                # Para semillas y fertilizantes, usar montos reales de BD
                query = """
                SELECT 
                    COUNT(DISTINCT b.beneficiario_id) as beneficiarios_unicos,
                    COALESCE(SUM(b.monto), 0) as monto_total
                FROM "etl-productivo".beneficio b
                INNER JOIN "etl-productivo".tipo_cultivo tc ON b.tipo_cultivo_id = tc.id
                WHERE b.tipo_beneficio = :beneficio
                AND tc.nombre IN ('ARROZ', 'MAIZ')
                """
                
                with self.db.engine.connect() as conn:
                    result_data = conn.execute(text(query), {'beneficio': beneficio}).fetchone()
                    
                result[beneficio.lower()] = BeneficiariosPorSubvencion(
                    count=int(result_data[0]) if result_data[0] else 0,
                    monto_total=float(result_data[1]) if result_data[1] else 0.0
                )
        
        return result
    
    def obtener_distribucion_subvenciones(self) -> DistribucionSubvenciones:
        """Obtiene la distribución de beneficiarios por número de subvenciones."""
        query = """
        WITH beneficiario_conteos AS (
            SELECT 
                b.beneficiario_id,
                COUNT(DISTINCT b.tipo_beneficio) as num_subvenciones
            FROM "etl-productivo".beneficio b
            INNER JOIN "etl-productivo".tipo_cultivo tc ON b.tipo_cultivo_id = tc.id
            WHERE tc.nombre IN ('ARROZ', 'MAIZ')
            GROUP BY b.beneficiario_id
        )
        SELECT 
            num_subvenciones,
            COUNT(*) as cantidad_beneficiarios
        FROM beneficiario_conteos
        WHERE num_subvenciones IN (1, 2, 3)
        GROUP BY num_subvenciones
        ORDER BY num_subvenciones;
        """
        
        distribucion = {1: 0, 2: 0, 3: 0}
        
        with self.db.engine.connect() as conn:
            rows = conn.execute(text(query)).fetchall()
            for row in rows:
                num_subv = int(row[0])
                cantidad = int(row[1])
                if num_subv in distribucion:
                    distribucion[num_subv] = cantidad
        
        return DistribucionSubvenciones(
            beneficiarios_1_subvencion=distribucion[1],
            beneficiarios_2_subvenciones=distribucion[2],
            beneficiarios_3_subvenciones=distribucion[3]
        )
    
    def obtener_top_beneficiarios(self, limite: int = 5) -> List[TopBeneficiario]:
        """Obtiene los top N beneficiarios con mayor porcentaje de ahorro."""
        # Query para obtener beneficiarios con mayor porcentaje de ahorro
        query = """
        WITH beneficiario_stats AS (
            SELECT 
                b.beneficiario_id,
                COUNT(*) as total_subvenciones,
                SUM(
                    CASE 
                        WHEN b.tipo_beneficio = 'MECANIZACION' THEN
                            CASE 
                                WHEN tc.nombre = 'ARROZ' THEN 200.00
                                WHEN tc.nombre = 'MAIZ' THEN 70.00
                                ELSE 0
                            END
                        ELSE COALESCE(b.monto, 0)
                    END
                ) as monto_total_recibido,
                SUM(b.hectareas_beneficiadas * 
                    CASE 
                        WHEN tc.nombre = 'ARROZ' THEN 1590.99
                        WHEN tc.nombre = 'MAIZ' THEN 1835.30
                        ELSE 0
                    END
                ) as costo_total_sin_subsidio
            FROM "etl-productivo".beneficio b
            INNER JOIN "etl-productivo".tipo_cultivo tc ON b.tipo_cultivo_id = tc.id
            WHERE tc.nombre IN ('ARROZ', 'MAIZ')
            GROUP BY b.beneficiario_id
        ),
        beneficiario_ahorros AS (
            SELECT 
                beneficiario_id,
                total_subvenciones,
                monto_total_recibido,
                costo_total_sin_subsidio,
                CASE 
                    WHEN costo_total_sin_subsidio > 0 THEN 
                        (monto_total_recibido / costo_total_sin_subsidio * 100)
                    ELSE 0 
                END as porcentaje_ahorro
            FROM beneficiario_stats
            WHERE costo_total_sin_subsidio > 0
        )
        SELECT 
            beneficiario_id,
            total_subvenciones,
            monto_total_recibido,
            porcentaje_ahorro
        FROM beneficiario_ahorros
        ORDER BY porcentaje_ahorro DESC, monto_total_recibido DESC
        LIMIT :limite;
        """
        
        top_beneficiarios = []
        
        with self.db.engine.connect() as conn:
            rows = conn.execute(text(query), {'limite': limite}).fetchall()
            
            for row in rows:
                beneficiario_id = int(row[0])
                total_subvenciones = int(row[1])
                monto_total = float(row[2])
                
                # Obtener detalles completos del beneficiario
                beneficiario_completo = self._obtener_detalle_beneficiario(beneficiario_id)
                top_beneficiarios.append(beneficiario_completo)
        
        return top_beneficiarios
    
    def obtener_top_beneficiarios_por_subvenciones(self, limite: int = 5) -> List[TopBeneficiario]:
        """Obtiene los top N beneficiarios ordenados por número de subvenciones, luego por porcentaje de ahorro."""
        # Query para obtener beneficiarios ordenados por número de subvenciones y luego por porcentaje de ahorro
        query = """
        WITH beneficiario_stats AS (
            SELECT 
                b.beneficiario_id,
                COUNT(*) as total_subvenciones,
                SUM(
                    CASE 
                        WHEN b.tipo_beneficio = 'MECANIZACION' THEN
                            CASE 
                                WHEN tc.nombre = 'ARROZ' THEN 200.00
                                WHEN tc.nombre = 'MAIZ' THEN 70.00
                                ELSE 0
                            END
                        ELSE COALESCE(b.monto, 0)
                    END
                ) as monto_total_recibido,
                SUM(b.hectareas_beneficiadas * 
                    CASE 
                        WHEN tc.nombre = 'ARROZ' THEN 1590.99
                        WHEN tc.nombre = 'MAIZ' THEN 1835.30
                        ELSE 0
                    END
                ) as costo_total_sin_subsidio
            FROM "etl-productivo".beneficio b
            INNER JOIN "etl-productivo".tipo_cultivo tc ON b.tipo_cultivo_id = tc.id
            WHERE tc.nombre IN ('ARROZ', 'MAIZ')
            GROUP BY b.beneficiario_id
        ),
        beneficiario_ahorros AS (
            SELECT 
                beneficiario_id,
                total_subvenciones,
                monto_total_recibido,
                costo_total_sin_subsidio,
                CASE 
                    WHEN costo_total_sin_subsidio > 0 THEN 
                        (monto_total_recibido / costo_total_sin_subsidio * 100)
                    ELSE 0 
                END as porcentaje_ahorro
            FROM beneficiario_stats
            WHERE costo_total_sin_subsidio > 0
        )
        SELECT 
            beneficiario_id,
            total_subvenciones,
            monto_total_recibido,
            porcentaje_ahorro
        FROM beneficiario_ahorros
        ORDER BY total_subvenciones DESC, porcentaje_ahorro DESC, monto_total_recibido DESC
        LIMIT :limite;
        """
        
        top_beneficiarios = []
        
        with self.db.engine.connect() as conn:
            rows = conn.execute(text(query), {'limite': limite}).fetchall()
            
            for row in rows:
                beneficiario_id = int(row[0])
                total_subvenciones = int(row[1])
                monto_total = float(row[2])
                
                # Obtener detalles completos del beneficiario
                beneficiario_completo = self._obtener_detalle_beneficiario(beneficiario_id)
                top_beneficiarios.append(beneficiario_completo)
        
        return top_beneficiarios
    
    def _obtener_detalle_beneficiario(self, beneficiario_id: int) -> TopBeneficiario:
        """Obtiene el análisis completo de un beneficiario específico."""
        # Query para obtener todos los beneficios del beneficiario incluyendo datos personales
        query = """
        SELECT 
            b.tipo_beneficio,
            tc.nombre as cultivo,
            b.monto,
            b.hectareas_beneficiadas,
            b.tipo_cultivo_id,
            ben.cedula,
            ben.nombres_completos
        FROM "etl-productivo".beneficio b
        INNER JOIN "etl-productivo".tipo_cultivo tc ON b.tipo_cultivo_id = tc.id
        INNER JOIN "etl-productivo".beneficiario ben ON b.beneficiario_id = ben.id
        WHERE b.beneficiario_id = :beneficiario_id
        AND tc.nombre IN ('ARROZ', 'MAIZ')
        ORDER BY tc.nombre, b.tipo_beneficio;
        """
        
        subsidios_recibidos = []
        cultivos_data = {}
        monto_total_recibido = 0.0
        cedula = None
        nombres_apellidos = None
        
        with self.db.engine.connect() as conn:
            rows = conn.execute(text(query), {'beneficiario_id': beneficiario_id}).fetchall()
            
            for row in rows:
                tipo_beneficio = row[0]
                cultivo = row[1]
                monto_bd = float(row[2]) if row[2] else 0.0
                hectareas = float(row[3]) if row[3] else 0.0
                # Capturar datos personales (serán los mismos para todos los registros)
                if cedula is None:
                    cedula = row[5]
                    nombres_apellidos = row[6]
                
                # Calcular monto real según tipo de beneficio
                if tipo_beneficio == 'MECANIZACION':
                    monto_real = float(self.costos_mecanizacion[cultivo])
                else:
                    monto_real = float(monto_bd)
                
                monto_total_recibido += monto_real
                
                # Agregar a subsidios recibidos
                subsidios_recibidos.append(DetalleSubsidio(
                    tipo_beneficio=TipoBeneficio(tipo_beneficio),
                    cultivo=TipoCultivo(cultivo),
                    monto=monto_real,
                    hectareas=hectareas
                ))
                
                # Agregar a data de cultivos
                if cultivo not in cultivos_data:
                    cultivos_data[cultivo] = {
                        'hectareas': 0.0,
                        'costo_por_hectarea': self.costos_produccion_por_hectarea[cultivo]
                    }
                cultivos_data[cultivo]['hectareas'] += hectareas
        
        # Calcular análisis por cultivo
        cultivos_beneficiados = []
        costo_total_sin_subsidio = 0.0
        
        for cultivo, data in cultivos_data.items():
            hectareas = data['hectareas']
            costo_por_ha = data['costo_por_hectarea']
            costo_total_cultivo = hectareas * costo_por_ha
            costo_total_sin_subsidio += costo_total_cultivo
            
            cultivos_beneficiados.append(BeneficioCultivo(
                cultivo=TipoCultivo(cultivo),
                hectareas=hectareas,
                costo_produccion_sin_subsidio=costo_por_ha,
                costo_total_sin_subsidio=costo_total_cultivo
            ))
        
        # Calcular reducción de costos
        ahorro_total = costo_total_sin_subsidio - monto_total_recibido
        porcentaje_reduccion = (monto_total_recibido / costo_total_sin_subsidio * 100) if costo_total_sin_subsidio > 0 else 0
        
        return TopBeneficiario(
            beneficiario_id=beneficiario_id,
            cedula=cedula or "N/A",
            nombres_apellidos=nombres_apellidos or "N/A",
            total_subvenciones=len(subsidios_recibidos),
            monto_total_recibido=monto_total_recibido,
            cultivos_beneficiados=cultivos_beneficiados,
            subsidios_recibidos=subsidios_recibidos,
            costo_total_sin_subsidio=costo_total_sin_subsidio,
            ahorro_total=ahorro_total,
            porcentaje_reduccion=porcentaje_reduccion
        )
    
    def generar_resumen_beneficiarios(self, beneficiarios_por_subv: Dict, top_beneficiarios: List[TopBeneficiario]) -> ResumenBeneficiarios:
        """Genera el resumen ejecutivo de beneficiarios."""
        # Calcular totales
        total_beneficiarios_query = """
        SELECT COUNT(DISTINCT b.beneficiario_id) as total_unicos
        FROM "etl-productivo".beneficio b
        INNER JOIN "etl-productivo".tipo_cultivo tc ON b.tipo_cultivo_id = tc.id
        WHERE tc.nombre IN ('ARROZ', 'MAIZ')
        """
        
        total_subvenciones_query = """
        SELECT COUNT(*) as total_subvenciones
        FROM "etl-productivo".beneficio b
        INNER JOIN "etl-productivo".tipo_cultivo tc ON b.tipo_cultivo_id = tc.id
        WHERE tc.nombre IN ('ARROZ', 'MAIZ')
        """
        
        with self.db.engine.connect() as conn:
            total_beneficiarios = conn.execute(text(total_beneficiarios_query)).fetchone()[0]
            total_subvenciones = conn.execute(text(total_subvenciones_query)).fetchone()[0]
        
        # Monto total distribuido
        monto_total_distribuido = sum(subv.monto_total for subv in beneficiarios_por_subv.values())
        
        # Beneficiario con mayor ahorro
        beneficiario_mayor_ahorro = None
        if top_beneficiarios:
            mejor = max(top_beneficiarios, key=lambda x: x.ahorro_total)
            beneficiario_mayor_ahorro = {
                "beneficiario_id": mejor.beneficiario_id,
                "ahorro_total": mejor.ahorro_total,
                "porcentaje_reduccion": mejor.porcentaje_reduccion
            }
        
        # Promedios
        promedio_subv_por_beneficiario = total_subvenciones / total_beneficiarios if total_beneficiarios > 0 else 0
        promedio_ahorro = sum(b.ahorro_total for b in top_beneficiarios) / len(top_beneficiarios) if top_beneficiarios else 0
        
        return ResumenBeneficiarios(
            total_beneficiarios_unicos=total_beneficiarios,
            total_subvenciones_otorgadas=total_subvenciones,
            monto_total_distribuido=monto_total_distribuido,
            beneficiario_con_mayor_ahorro=beneficiario_mayor_ahorro or {},
            promedio_subvenciones_por_beneficiario=promedio_subv_por_beneficiario,
            promedio_ahorro_por_beneficiario=promedio_ahorro
        )
    
    def obtener_analisis_completo(self) -> BeneficiariosResponse:
        """Obtiene el análisis completo de beneficiarios."""
        logger.info("Iniciando análisis completo de beneficiarios...")
        
        try:
            # 1. Beneficiarios por subvención
            beneficiarios_por_subv = self.obtener_beneficiarios_por_subvencion()
            
            # 2. Distribución por número de subvenciones
            distribucion = self.obtener_distribucion_subvenciones()
            
            # 3. Top 5 beneficiarios por porcentaje de ahorro
            top_beneficiarios_ahorro = self.obtener_top_beneficiarios(5)
            
            # 4. Top 5 beneficiarios por número de subvenciones + porcentaje de ahorro
            top_beneficiarios_subvenciones = self.obtener_top_beneficiarios_por_subvenciones(5)
            
            # 5. Resumen ejecutivo (usando el top de ahorro para mantener compatibilidad)
            resumen = self.generar_resumen_beneficiarios(beneficiarios_por_subv, top_beneficiarios_ahorro)
            
            return BeneficiariosResponse(
                beneficiarios_por_subvencion=beneficiarios_por_subv,
                distribucion_subvenciones=distribucion,
                top_beneficiarios_por_ahorro=top_beneficiarios_ahorro,
                top_beneficiarios_por_subvenciones=top_beneficiarios_subvenciones,
                resumen=resumen
            )
            
        except Exception as e:
            logger.error(f"Error en análisis de beneficiarios: {str(e)}")
            raise e