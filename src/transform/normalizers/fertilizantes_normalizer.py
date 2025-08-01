"""Normalizador específico para datos de fertilizantes."""
import pandas as pd
from typing import Dict
from loguru import logger


class FertilizantesNormalizer:
    """Normaliza datos de fertilizantes en entidades separadas."""
    
    def __init__(self):
        self.stats = {
            'total_registros': 0,
            'personas_creadas': 0,
            'organizaciones_unicas': 0,
            'ubicaciones_unicas': 0,
            'beneficiarios_creados': 0,
            'beneficios_creados': 0
        }
        
    def normalize(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Normaliza datos en entidades relacionales."""
        logger.info(f"Normalizando {len(df)} registros de fertilizantes")
        self.stats['total_registros'] = len(df)
        
        # Crear entidades
        entities = {
            'personas': self._extract_personas(df),
            'organizaciones': self._extract_organizaciones(df),
            'ubicaciones': self._extract_ubicaciones(df),
            'cultivos': self._extract_cultivos(df),
            'beneficiarios_fertilizantes': self._extract_beneficiarios(df),
            'beneficios': self._extract_beneficios(df)
        }
        
        logger.info(f"Normalización completada. Entidades creadas: {self._get_entity_counts(entities)}")
        return entities
        
    def _extract_personas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extrae datos únicos de personas."""
        personas_cols = ['nombres_apellidos', 'cedula', 'telefono', 'genero', 'edad']
        
        # Filtrar registros con nombre válido
        df_valid = df[df['nombres_apellidos'].notna()].copy()
        
        # Crear DataFrame de personas
        personas_df = df_valid[personas_cols].copy()
        
        # Eliminar duplicados por cédula (si existe) o por nombre
        if 'cedula' in personas_df.columns:
            # Primero deduplicar por cédula no nula
            personas_con_cedula = personas_df[personas_df['cedula'].notna()].drop_duplicates(subset=['cedula'])
            # Luego por nombre para los que no tienen cédula
            personas_sin_cedula = personas_df[personas_df['cedula'].isna()].drop_duplicates(subset=['nombres_apellidos'])
            # Combinar
            personas_df = pd.concat([personas_con_cedula, personas_sin_cedula], ignore_index=True)
        else:
            personas_df = personas_df.drop_duplicates(subset=['nombres_apellidos'])
            
        self.stats['personas_creadas'] = len(personas_df)
        logger.info(f"Personas únicas extraídas: {len(personas_df)}")
        
        return personas_df
        
    def _extract_organizaciones(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extrae organizaciones únicas."""
        # Filtrar registros con organización válida
        org_valid = df[df['organizacion'].notna()]['organizacion'].unique()
        
        organizaciones_df = pd.DataFrame({
            'nombre': org_valid
        })
        
        self.stats['organizaciones_unicas'] = len(organizaciones_df)
        logger.info(f"Organizaciones únicas extraídas: {len(organizaciones_df)}")
        
        return organizaciones_df
        
    def _extract_ubicaciones(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extrae ubicaciones únicas."""
        ubicacion_cols = ['canton', 'parroquia', 'localidad', 'coordenada_x', 'coordenada_y']
        
        # Filtrar registros con al menos canton
        df_valid = df[df['canton'].notna()].copy()
        
        # Crear DataFrame de ubicaciones
        ubicaciones_df = df_valid[ubicacion_cols].copy()
        
        # Función para limpiar coordenadas concatenadas
        def clean_coordinate(coord):
            if pd.isna(coord):
                return None
            coord_str = str(coord)
            # Si la coordenada tiene más de 10 dígitos antes del punto decimal,
            # probablemente esté concatenada
            if '.' in coord_str:
                int_part = coord_str.split('.')[0]
                if len(int_part) > 10:
                    # Tomar solo la primera mitad como la coordenada real
                    half_len = len(int_part) // 2
                    cleaned = int_part[:half_len] + '.' + coord_str.split('.')[1]
                    try:
                        return float(cleaned)
                    except:
                        return None
            try:
                coord_float = float(coord)
                # Si es mayor a 999999999, está mal
                if coord_float > 999999999:
                    return None
                return coord_float
            except:
                return None
        
        # Limpiar coordenadas antes de agrupar
        ubicaciones_df['coordenada_x'] = ubicaciones_df['coordenada_x'].apply(clean_coordinate)
        ubicaciones_df['coordenada_y'] = ubicaciones_df['coordenada_y'].apply(clean_coordinate)
        
        # Agrupar por canton, parroquia, localidad y tomar las primeras coordenadas no nulas
        ubicaciones_df = ubicaciones_df.groupby(['canton', 'parroquia', 'localidad'], dropna=False).agg({
            'coordenada_x': lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else None,
            'coordenada_y': lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else None
        }).reset_index()
        
        self.stats['ubicaciones_unicas'] = len(ubicaciones_df)
        logger.info(f"Ubicaciones únicas extraídas: {len(ubicaciones_df)}")
        
        return ubicaciones_df
    
    def _extract_cultivos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extrae información única de cultivos enriquecidos."""
        if 'tipo_cultivo' not in df.columns:
            logger.warning("No se encontró columna tipo_cultivo")
            return pd.DataFrame()
            
        # Obtener cultivos únicos con sus datos enriquecidos
        cultivos_unicos = []
        tipos_procesados = set()
        
        for _, row in df.iterrows():
            tipo_cultivo = row.get('tipo_cultivo')
            if pd.notna(tipo_cultivo) and tipo_cultivo not in tipos_procesados:
                tipos_procesados.add(tipo_cultivo)
                
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
                
                cultivos_unicos.append(cultivo_data)
        
        cultivos_df = pd.DataFrame(cultivos_unicos)
        self.stats['cultivos_unicos'] = len(cultivos_df)
        logger.info(f"Cultivos únicos extraídos: {len(cultivos_df)}")
        
        return cultivos_df
        
    def _extract_beneficiarios(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extrae beneficiarios de fertilizantes."""
        # Todos los registros válidos son beneficiarios
        df_valid = df[df['nombres_apellidos'].notna()].copy()
        
        # Crear identificador único para cada persona con su organización
        beneficiarios_df = df_valid[['nombres_apellidos', 'cedula', 'organizacion', 'hectarias_totales']].copy()
        beneficiarios_df['tipo_productor'] = 'BENEFICIARIO_FERTILIZANTES'
        
        # Eliminar duplicados
        if 'cedula' in beneficiarios_df.columns:
            beneficiarios_df = beneficiarios_df.drop_duplicates(subset=['cedula', 'nombres_apellidos'])
        else:
            beneficiarios_df = beneficiarios_df.drop_duplicates(subset=['nombres_apellidos'])
            
        self.stats['beneficiarios_creados'] = len(beneficiarios_df)
        logger.info(f"Beneficiarios únicos extraídos: {len(beneficiarios_df)}")
        
        return beneficiarios_df
        
    def _extract_beneficios(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extrae beneficios de fertilizantes."""
        # Todos los registros son beneficios
        beneficio_cols = [
            'nombres_apellidos', 'cedula', 'organizacion',
            'canton', 'parroquia', 'localidad',
            'numero_acta', 'fecha_entrega', 'anio',
            'hectarias_totales', 'hectarias_beneficiadas',
            'tipo_fertilizante', 'marca_fertilizante', 'tipo_cultivo',  # Agregado tipo_cultivo
            'cantidad_sacos', 'peso_por_saco',
            'precio_unitario', 'costo_total',
            'quintil', 'score_quintil',
            'responsable_agencia', 'cedula_jefe_sucursal',
            'sucursal', 'observacion'
        ]
        
        # Filtrar columnas que existen
        existing_cols = [col for col in beneficio_cols if col in df.columns]
        beneficios_df = df[existing_cols].copy()
        
        # Agregar tipo de beneficio
        beneficios_df['tipo_beneficio'] = 'fertilizantes'
        
        self.stats['beneficios_creados'] = len(beneficios_df)
        logger.info(f"Beneficios extraídos: {len(beneficios_df)}")
        
        return beneficios_df
        
    def _get_entity_counts(self, entities: Dict[str, pd.DataFrame]) -> str:
        """Genera resumen de conteos de entidades."""
        counts = []
        for entity_name, df in entities.items():
            counts.append(f"{entity_name}: {len(df)}")
        return ", ".join(counts)
        
    def get_stats(self) -> Dict[str, int]:
        """Retorna estadísticas de normalización."""
        return self.stats.copy()