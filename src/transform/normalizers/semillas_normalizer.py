"""Normalización de datos - Separación en entidades."""
import pandas as pd
import hashlib
from typing import Dict, List
from loguru import logger
from datetime import datetime


class SemillasNormalizer:
    """Normaliza datos separ�ndolos en entidades relacionadas."""
    
    def __init__(self):
        self.entities = {
            'personas': [],
            'ubicaciones': [],
            'organizaciones': [],
            'beneficios': [],
            'beneficiarios_semillas': [],
            'cultivos': []  # Nueva entidad para datos enriquecidos de cultivos
        }
        
    def normalize(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Normaliza datos en entidades separadas."""
        logger.info(f"Iniciando normalizaci�n de {len(df)} registros")
        
        # Procesar cada entidad
        self._extract_organizaciones(df)
        self._extract_ubicaciones(df)
        self._extract_personas(df)
        self._extract_cultivos(df)
        self._extract_beneficios(df)
        
        # Convertir listas a DataFrames
        entities_df = {}
        for entity, records in self.entities.items():
            if records:
                entities_df[entity] = pd.DataFrame(records)
                logger.info(f"Entidad {entity}: {len(entities_df[entity])} registros �nicos")
            else:
                entities_df[entity] = pd.DataFrame()
        
        # Limpiar campos temporales de los DataFrames después de la conversión
        if 'personas' in entities_df and not entities_df['personas'].empty:
            entities_df['personas'] = entities_df['personas'].drop(
                columns=['_temp_key', '_temp_org', '_temp_hectarias'], 
                errors='ignore'
            )
                
        return entities_df
    
    def _extract_organizaciones(self, df: pd.DataFrame):
        """Extrae organizaciones �nicas."""
        organizaciones_seen = set()
        
        for _, row in df.iterrows():
            if pd.notna(row.get('organizacion')) and row['organizacion'] != 'None':
                org_nombre = str(row['organizacion']).strip()
                
                if org_nombre and org_nombre not in organizaciones_seen:
                    organizaciones_seen.add(org_nombre)
                    
                    self.entities['organizaciones'].append({
                        'nombre': org_nombre,
                        'tipo_organizacion': self._inferir_tipo_organizacion(org_nombre),
                        'estado': 'ACTIVO'
                    })
                    
        logger.debug(f"Organizaciones extra�das: {len(self.entities['organizaciones'])}")
    
    def _extract_ubicaciones(self, df: pd.DataFrame):
        """Extrae ubicaciones �nicas."""
        ubicaciones_seen = set()
        
        for _, row in df.iterrows():
            # Crear clave �nica para ubicaci�n
            canton = row.get('canton') if pd.notna(row.get('canton')) else None
            parroquia = row.get('parroquia') if pd.notna(row.get('parroquia')) else None
            localidad = row.get('localidad') if pd.notna(row.get('localidad')) else None
            
            if canton and canton != 'None':
                # Clave �nica basada en canton-parroquia-localidad
                ubi_key = f"{canton}|{parroquia or ''}|{localidad or ''}"
                
                if ubi_key not in ubicaciones_seen:
                    ubicaciones_seen.add(ubi_key)
                    
                    self.entities['ubicaciones'].append({
                        'canton': canton,
                        'parroquia': parroquia,
                        'localidad': localidad,
                        'coordenada_x': row.get('coordenada_x'),
                        'coordenada_y': row.get('coordenada_y'),
                        'tipo_ubicacion': 'RURAL'  # Inferir seg�n datos
                    })
                    
        logger.debug(f"Ubicaciones extra�das: {len(self.entities['ubicaciones'])}")
    
    def _extract_personas(self, df: pd.DataFrame):
        """Extrae personas �nicas y beneficiarios semillas."""
        personas_seen = set()
        
        for _, row in df.iterrows():
            nombres = row.get('nombres_apellidos')
            # Verificación más estricta para nombres válidos
            if nombres is not None and str(nombres).strip() != '' and str(nombres).strip().lower() not in ['none', 'nan', 'null']:
                # Usar c�dula como clave �nica si existe, sino usar nombres
                cedula = row.get('cedula') if pd.notna(row.get('cedula')) else None
                persona_key = cedula if cedula and cedula != 'None' else nombres
                
                if persona_key not in personas_seen:
                    personas_seen.add(persona_key)
                    
                    # Datos base de persona
                    persona_data = {
                        'cedula': cedula,
                        'nombres_apellidos': nombres,
                        'telefono': row.get('telefono'),
                        'genero': row.get('genero'),
                        'edad': row.get('edad'),
                        'is_active': True,
                        '_temp_key': persona_key,  # Para relacionar despu�s
                        '_temp_org': row.get('organizacion'),
                        '_temp_hectarias': row.get('hectarias_totales')
                    }
                    
                    self.entities['personas'].append(persona_data)
                    
        logger.debug(f"Personas extra�das: {len(self.entities['personas'])}")
    
    def _extract_beneficios(self, df: pd.DataFrame):
        """Extrae beneficios con sus relaciones."""
        # Primero necesitamos los IDs de las entidades relacionadas
        # En un caso real, esto vendr�a de la BD despu�s de insertar
        
        # Crear mapeos temporales (simulando IDs)
        org_map = {org['nombre']: idx + 1 for idx, org in enumerate(self.entities['organizaciones'])}
        
        ubi_map = {}
        for idx, ubi in enumerate(self.entities['ubicaciones']):
            key = f"{ubi['canton']}|{ubi['parroquia'] or ''}|{ubi['localidad'] or ''}"
            ubi_map[key] = idx + 1
            
        # Crear mapeo de personas - usar la lista original antes de convertir a DataFrame
        persona_map = {}
        for idx, persona in enumerate(self.entities['personas']):
            if '_temp_key' in persona:
                persona_map[persona['_temp_key']] = idx + 1
        
        # Extraer beneficios
        for _, row in df.iterrows():
            # Solo procesar si hay persona v�lida
            nombres = row.get('nombres_apellidos')
            # Usar la misma verificación estricta que en _extract_personas
            if nombres is not None and str(nombres).strip() != '' and str(nombres).strip().lower() not in ['none', 'nan', 'null']:
                
                # Obtener IDs relacionados
                cedula = row.get('cedula') if pd.notna(row.get('cedula')) else None
                persona_key = cedula if cedula and cedula != 'None' else nombres
                persona_id = persona_map.get(persona_key)
                
                canton = row.get('canton') if pd.notna(row.get('canton')) else None
                parroquia = row.get('parroquia') if pd.notna(row.get('parroquia')) else None
                localidad = row.get('localidad') if pd.notna(row.get('localidad')) else None
                ubi_key = f"{canton}|{parroquia or ''}|{localidad or ''}"
                ubicacion_id = ubi_map.get(ubi_key) if canton else None
                
                if persona_id:
                    # Buscar organizacion si existe
                    org_nombre = row.get('organizacion') if pd.notna(row.get('organizacion')) else None
                    org_id = org_map.get(org_nombre) if org_nombre else None
                    
                    beneficio = {
                        # Relaciones
                        'persona_id': persona_id,
                        'persona_nombres': nombres,  # Para mapeo en SQL
                        'persona_cedula': cedula,  # Para mapeo en SQL
                        'ubicacion_id': ubicacion_id,
                        'canton': canton,  # Para mapeo en SQL
                        'parroquia': parroquia,  # Para mapeo en SQL
                        'localidad': localidad,  # Para mapeo en SQL
                        'organizacion_id': org_id,
                        'organizacion_nombre': org_nombre,  # Para mapeo en SQL
                        
                        # Datos base del beneficio
                        'fecha': row.get('fecha_retiro'),  # Usar fecha_retiro como fecha principal
                        'tipo_beneficio': 'SEMILLAS',  # Siempre es SEMILLAS para este ETL
                        'hectarias_beneficiadas': row.get('hectarias_beneficiadas'),
                        'valor_monetario': row.get('inversion'),  # Este es el valor en dólares del beneficio
                        
                        # Datos espec�ficos de semillas
                        'tipo_cultivo': row.get('tipo_cultivo'),
                        'hectarias_beneficiadas': row.get('hectarias_beneficiadas'),
                        'precio_unitario': row.get('precio_unitario'),
                        'inversion': row.get('inversion'),
                        'quintil': row.get('quintil'),
                        'score_quintil': row.get('score_quintil'),
                        
                        # Datos administrativos
                        'numero_acta': row.get('numero_acta'),
                        'documento': row.get('documento'),
                        'proceso': row.get('proceso'),
                        'fecha_retiro': row.get('fecha_retiro'),
                        'anio': row.get('anio'),
                        
                        # Responsables
                        'responsable_agencia': row.get('responsable_agencia'),
                        'cedula_jefe_sucursal': row.get('cedula_jefe_sucursal'),
                        'sucursal': row.get('sucursal'),
                        
                        # Otros
                        'observacion': row.get('observacion'),
                        'estado': 'ACTIVO',
                        
                        # Metadatos de validaci�n
                        'es_valido': row.get('es_valido', True),
                        'tiene_campos_requeridos': row.get('tiene_campos_requeridos', True)
                    }
                    
                    self.entities['beneficios'].append(beneficio)
                    
        # Procesar beneficiarios semillas
        for persona in self.entities['personas']:
            if persona.get('_temp_hectarias') and persona['_temp_hectarias'] > 0:
                org_nombre = persona.get('_temp_org')
                org_id = org_map.get(org_nombre) if org_nombre else None
                
                self.entities['beneficiarios_semillas'].append({
                    'persona_id': persona_map.get(persona['_temp_key']),
                    'persona_nombres': persona.get('nombres_apellidos'),  # Para mapeo en SQL
                    'persona_cedula': persona.get('cedula'),  # Para mapeo en SQL
                    'tipo_productor': 'BENEFICIARIO_SEMILLAS',  # Inferir del rubro
                    'hectarias_totales': persona['_temp_hectarias'],
                    'organizacion_id': org_id,
                    'organizacion_nombre': org_nombre  # Para mapeo en SQL
                })
                
        # Los campos temporales se limpian después de crear los DataFrames
            
        logger.debug(f"Beneficios extra�dos: {len(self.entities['beneficios'])}")
        
    def _inferir_tipo_organizacion(self, nombre: str) -> str:
        """Infiere el tipo de organizaci�n del nombre."""
        nombre_upper = nombre.upper()
        
        if 'ASOCIACION' in nombre_upper or 'ASOC' in nombre_upper:
            return 'ASOCIACION'
        elif 'COOPERATIVA' in nombre_upper or 'COOP' in nombre_upper:
            return 'COOPERATIVA'
        elif 'JUNTA' in nombre_upper:
            return 'JUNTA'
        elif 'CENTRO' in nombre_upper:
            return 'CENTRO'
        elif 'GRUPO' in nombre_upper:
            return 'GRUPO'
        else:
            return 'OTRO'
    
    def _extract_cultivos(self, df: pd.DataFrame):
        """Extrae información única de cultivos enriquecidos."""
        if 'tipo_cultivo' not in df.columns:
            logger.warning("No se encontró columna tipo_cultivo")
            return
            
        # Obtener cultivos únicos con sus datos enriquecidos
        cultivos_unicos = set()
        
        for _, row in df.iterrows():
            tipo_cultivo = row.get('tipo_cultivo')
            if pd.notna(tipo_cultivo) and tipo_cultivo not in cultivos_unicos:
                cultivos_unicos.add(tipo_cultivo)
                
                cultivo_data = {
                    'codigo_cultivo': tipo_cultivo.upper().strip(),
                    'nombre_cultivo': row.get('cultivo_nombre_cultivo', tipo_cultivo),
                    'nombre_cientifico': row.get('cultivo_nombre_cientifico'),
                    'familia_botanica': row.get('cultivo_familia_botanica'),
                    'tipo_ciclo': row.get('cultivo_tipo_ciclo'),
                    'clasificacion_economica': row.get('cultivo_clasificacion_economica'),
                    'uso_principal': row.get('cultivo_uso_principal'),
                    'es_vigente': True
                }
                
                self.entities['cultivos'].append(cultivo_data)
        
        logger.debug(f"Cultivos únicos extraídos: {len(self.entities['cultivos'])}")
            
    def get_summary(self) -> Dict:
        """Retorna resumen de normalizaci�n."""
        return {
            'personas_unicas': len(self.entities['personas']),
            'ubicaciones_unicas': len(self.entities['ubicaciones']),
            'organizaciones_unicas': len(self.entities['organizaciones']),
            'cultivos_unicos': len(self.entities['cultivos']),
            'beneficios_totales': len(self.entities['beneficios']),
            'beneficiarios_semillas_identificados': len(self.entities['beneficiarios_semillas'])
        }