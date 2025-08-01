"""Estandarización de datos para semillas."""
import pandas as pd
from typing import Optional
from unidecode import unidecode
from loguru import logger


class DataStandardizer:
    """Estandariza formatos de datos seg�n reglas de negocio."""
    
    def __init__(self):
        self.standardized_count = 0
        
    def standardize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica todas las estandarizaciones."""
        logger.info("Iniciando estandarizaci�n de datos")
        
        df_std = df.copy()
        
        # Aplicar estandarizaciones
        df_std = self._standardize_genero(df_std)
        df_std = self._standardize_telefonos(df_std)
        df_std = self._standardize_cultivos(df_std)
        df_std = self._standardize_ubicaciones(df_std)
        df_std = self._standardize_estados(df_std)
        df_std = self._standardize_coordenadas(df_std)
        
        self.standardized_count = len(df_std)
        logger.info(f"Estandarizaci�n completada: {self.standardized_count} registros")
        
        return df_std
    
    def _standardize_genero(self, df: pd.DataFrame) -> pd.DataFrame:
        """Estandariza g�nero a M/F/O."""
        if 'genero' not in df.columns:
            return df
            
        genero_map = {
            'MASCULINO': 'M',
            'FEMENINO': 'F',
            'HOMBRE': 'M',
            'MUJER': 'F',
            'M': 'M',
            'F': 'F'
        }
        
        df['genero'] = df['genero'].map(lambda x: genero_map.get(str(x).upper(), 'O') if pd.notna(x) else None)
        logger.debug("G�nero estandarizado")
        return df
    
    def _standardize_telefonos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Estandariza tel�fonos al formato internacional."""
        for col in ['telefono', 'cedula_jefe_sucursal']:
            if col not in df.columns:
                continue
                
            def format_phone(phone):
                if pd.isna(phone) or phone == 'None':
                    return None
                    
                # Limpiar caracteres no num�ricos
                phone = str(phone).strip()
                phone = ''.join(filter(str.isdigit, phone))
                
                # Aplicar formato Ecuador
                if len(phone) == 10 and phone.startswith('0'):
                    return f"+593{phone[1:]}"
                elif len(phone) == 9:
                    return f"+593{phone}"
                    
                return phone if phone else None
            
            df[col] = df[col].apply(format_phone)
            
        logger.debug("Tel�fonos estandarizados")
        return df
    
    def _standardize_cultivos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Estandariza nombres de cultivos."""
        if 'cultivo' not in df.columns:
            return df
            
        cultivo_map = {
            'ARROZ': 'ARROZ',
            'MAIZ': 'MAIZ',
            'MA�Z': 'MAIZ',
            'SOYA': 'SOYA',
            'SOJA': 'SOYA',
            'CACAO': 'CACAO',
            'BANANO': 'BANANO',
            'PLATANO': 'PLATANO',
            'PL�TANO': 'PLATANO'
        }
        
        def map_cultivo(cultivo):
            if pd.isna(cultivo) or cultivo == 'None':
                return None
            cultivo_upper = str(cultivo).upper().strip()
            return cultivo_map.get(cultivo_upper, 'OTRO')
        
        # Manejar ambos nombres de columna por compatibilidad
        if 'cultivo' in df.columns:
            df['cultivo'] = df['cultivo'].apply(map_cultivo)
        elif 'tipo_cultivo' in df.columns:
            df['tipo_cultivo'] = df['tipo_cultivo'].apply(map_cultivo)
        logger.debug("Cultivos estandarizados")
        return df
    
    def _standardize_ubicaciones(self, df: pd.DataFrame) -> pd.DataFrame:
        """Estandariza cantones y parroquias."""
        ubicacion_cols = ['canton', 'parroquia', 'localidad']
        
        for col in ubicacion_cols:
            if col not in df.columns:
                continue
                
            def clean_ubicacion(ubi):
                if pd.isna(ubi) or ubi == 'None':
                    return None
                    
                # Remover acentos y caracteres especiales
                ubi = unidecode(str(ubi).upper().strip())
                # Reemplazar m�ltiples espacios por uno
                ubi = ' '.join(ubi.split())
                return ubi
            
            df[col] = df[col].apply(clean_ubicacion)
            
        logger.debug("Ubicaciones estandarizadas")
        return df
    
    def _standardize_estados(self, df: pd.DataFrame) -> pd.DataFrame:
        """Estandariza estados/observaciones."""
        if 'observacion' not in df.columns:
            return df
            
        estado_map = {
            'RECIBIDO': 'RECIBIDO',
            'ENTREGADO': 'RECIBIDO',
            'PENDIENTE': 'PENDIENTE',
            'EN PROCESO': 'PENDIENTE',
            'CANCELADO': 'CANCELADO',
            'ANULADO': 'CANCELADO'
        }
        
        def map_estado(obs):
            if pd.isna(obs) or obs == 'None':
                return 'SIN ESTADO'
            obs_upper = str(obs).upper().strip()
            return estado_map.get(obs_upper, obs_upper)
        
        df['observacion'] = df['observacion'].apply(map_estado)
        logger.debug("Estados estandarizados")
        return df
    
    def _standardize_coordenadas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Intenta corregir coordenadas problem�ticas."""
        coord_cols = ['coordenada_x', 'coordenada_y']
        
        for col in coord_cols:
            if col not in df.columns:
                continue
                
            def fix_coordinate(coord):
                if pd.isna(coord) or coord == 'None':
                    return None
                    
                try:
                    # Si es string, convertir a float
                    if isinstance(coord, str):
                        coord = float(coord)
                    
                    # Detectar coordenadas concatenadas/err�neas
                    if abs(coord) > 10000000:  # Valor muy grande para Ecuador
                        # Intentar dividir si parece concatenaci�n
                        coord_str = str(int(coord))
                        if len(coord_str) > 10:
                            # Tomar primeros 6-7 d�gitos
                            coord = float(coord_str[:6] + '.' + coord_str[6:8])
                    
                    # Validar rango para Ecuador
                    if col == 'coordenada_x':  # Longitud
                        if not (-82 < coord < -75) and not (500000 < coord < 800000):
                            return None
                    else:  # Latitud
                        if not (-5 < coord < 2) and not (9700000 < coord < 10100000):
                            return None
                            
                    return coord
                except:
                    return None
            
            df[col] = df[col].apply(fix_coordinate)
            
        logger.debug("Coordenadas estandarizadas")
        return df
    
    def get_summary(self) -> dict:
        """Retorna resumen de estandarizaci�n."""
        return {
            'registros_estandarizados': self.standardized_count
        }