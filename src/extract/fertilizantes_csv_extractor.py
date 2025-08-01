"""Extractor para leer datos del CSV de fertilizantes."""
import pandas as pd
from typing import Iterator, Dict, Any
from datetime import datetime
from loguru import logger


class FertilizantesCSVExtractor:
    """Extrae datos del archivo CSV de fertilizantes."""
    
    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size
        self.total_rows = 0
        
    def extract(self, csv_path: str) -> pd.DataFrame:
        """Lee el CSV completo y retorna un DataFrame."""
        logger.info(f"Extrayendo datos de fertilizantes: {csv_path}")
        
        try:
            df = pd.read_csv(csv_path)
            self.total_rows = len(df)
            logger.info(f"Total de registros de fertilizantes extraidos: {self.total_rows}")
            return df
            
        except Exception as e:
            logger.error(f"Error extrayendo CSV de fertilizantes: {str(e)}")
            raise
            
    def extract_batches(self, csv_path: str) -> Iterator[pd.DataFrame]:
        """Lee el CSV en lotes para procesamiento eficiente."""
        logger.info(f"Extrayendo datos de fertilizantes en lotes de {self.batch_size} registros")
        
        try:
            # Primero obtener el total de filas
            df_sample = pd.read_csv(csv_path, nrows=0)
            total_rows = sum(1 for line in open(csv_path)) - 1  # -1 por el header
            self.total_rows = total_rows
            
            # Leer en chunks
            for chunk in pd.read_csv(csv_path, chunksize=self.batch_size):
                yield chunk
                
        except Exception as e:
            logger.error(f"Error extrayendo CSV de fertilizantes en lotes: {str(e)}")
            raise
            
    def prepare_row(self, row: pd.Series) -> Dict[str, Any]:
        """Prepara una fila para ser cargada a staging."""
        # Convertir NaN a None
        row = row.where(pd.notna(row), None)
        
        # Preparar datos con conversiones basicas según estructura real del CSV
        data = {
            'numero_acta': row.get('acta'),  # Mapear 'acta' a 'numero_acta'
            'documento': None,  # No existe en el CSV
            'proceso': None,    # No existe en el CSV
            'organizacion': row.get('organizacion'),
            'nombres_apellidos': row.get('nombres_apellidos'),
            'cedula': row.get('cedula'),
            'telefono': row.get('telefono'),
            'genero': row.get('genero'),
            'edad': int(float(row['edad'])) if row.get('edad') is not None else None,
            'coordenada_x': str(row['coordenada_x']) if row.get('coordenada_x') is not None else None,
            'coordenada_y': str(row['coordenada_y']) if row.get('coordenada_y') is not None else None,
            'canton': row.get('canton'),
            'parroquia': row.get('parroquia'),
            'localidad': row.get('localidad'),
            'hectarias_totales': float(row['hactarias']) if row.get('hactarias') is not None else None,  # Mapear 'hactarias'
            'hectarias_beneficiadas': float(row['hactarias']) if row.get('hactarias') is not None else None,  # Mismo valor
            
            # Campos específicos de fertilizantes (mapear desde estructura real)
            'tipo_fertilizante': None,  # No existe en el CSV (sería el tipo químico del fertilizante)
            'marca_fertilizante': None,  # No existe en el CSV
            'tipo_cultivo': row.get('tipo_cultivo'),  # Cultivo al que se aplica el fertilizante
            'cantidad_sacos': int(float(row['numero_kits_entregados'])) if row.get('numero_kits_entregados') is not None else None,
            'peso_por_saco': None,  # No existe en el CSV  
            'precio_unitario': float(row['precio_kit']) if row.get('precio_kit') is not None else None,
            'costo_total': float(row['precio_kit']) if row.get('precio_kit') is not None else None,  # Mismo valor
            
            'responsable_agencia': row.get('responsable'),
            'cedula_jefe_sucursal': row.get('cedula_responsable'),
            'sucursal': row.get('lugar_entrega'),
            'fecha_entrega': pd.to_datetime(row['fecha_entrega']).date() if row.get('fecha_entrega') is not None else None,
            'anio': pd.to_datetime(row['fecha_entrega']).year if row.get('fecha_entrega') is not None else None,
            'observacion': row.get('observacion'),
            'actualizacion': None,  # No existe en el CSV
            'rubro': row.get('rubro'),
            'quintil': None,  # No existe en el CSV
            'score_quintil': None,  # No existe en el CSV
            'processed': False
        }
        
        return data
        
    def get_total_rows(self) -> int:
        """Retorna el total de filas extraidas."""
        return self.total_rows