"""Extractor para leer datos del CSV de semillas."""
import pandas as pd
from typing import Iterator, Dict, Any
from datetime import datetime
from loguru import logger


class SemillasCSVExtractor:
    """Extrae datos del archivo CSV de semillas."""
    
    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size
        self.total_rows = 0
        
    def extract(self, csv_path: str) -> pd.DataFrame:
        """Lee el CSV completo y retorna un DataFrame."""
        logger.info(f"Extrayendo datos de: {csv_path}")
        
        try:
            df = pd.read_csv(csv_path)
            self.total_rows = len(df)
            logger.info(f"Total de registros extraidos: {self.total_rows}")
            return df
            
        except Exception as e:
            logger.error(f"Error extrayendo CSV: {str(e)}")
            raise
            
    def extract_batches(self, csv_path: str) -> Iterator[pd.DataFrame]:
        """Lee el CSV en lotes para procesamiento eficiente."""
        logger.info(f"Extrayendo datos en lotes de {self.batch_size} registros")
        
        try:
            # Primero obtener el total de filas
            df_sample = pd.read_csv(csv_path, nrows=0)
            total_rows = sum(1 for line in open(csv_path)) - 1  # -1 por el header
            self.total_rows = total_rows
            
            # Leer en chunks
            for chunk in pd.read_csv(csv_path, chunksize=self.batch_size):
                yield chunk
                
        except Exception as e:
            logger.error(f"Error extrayendo CSV en lotes: {str(e)}")
            raise
            
    def prepare_row(self, row: pd.Series) -> Dict[str, Any]:
        """Prepara una fila para ser cargada a staging."""
        # Convertir NaN a None
        row = row.where(pd.notna(row), None)
        
        # Preparar datos con conversiones basicas
        data = {
            'numero_acta': row.get('numero_acta'),
            'documento': row.get('documento'),
            'proceso': row.get('proceso'),
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
            'hectarias_totales': float(row['hectarias_totales']) if row.get('hectarias_totales') is not None else None,
            'hectarias_beneficiadas': float(row['hectarias_beneficiadas']) if row.get('hectarias_beneficiadas') is not None else None,
            'cultivo': row.get('cultivo'),
            'precio_unitario': float(row['precio_unitario']) if row.get('precio_unitario') is not None else None,
            'inversion': float(row['inversion']) if row.get('inversion') is not None else None,
            'responsable_agencia': row.get('responsable_agencia'),
            'cedula_jefe_sucursal': row.get('cedula_jefe_sucursal'),
            'sucursal': row.get('sucursal'),
            'fecha_retiro': pd.to_datetime(row['fecha_retiro']).date() if row.get('fecha_retiro') is not None else None,
            'anio': int(float(row['anio'])) if row.get('anio') is not None else None,
            'observacion': row.get('observacion'),
            'actualizacion': row.get('actualizacion'),
            'rubro': row.get('rubro'),
            'quintil': int(float(row['quintil'])) if row.get('quintil') is not None else None,
            'score_quintil': float(row['score_quintil']) if row.get('score_quintil') is not None else None,
            'processed': False
        }
        
        return data
        
    def get_total_rows(self) -> int:
        """Retorna el total de filas extraidas."""
        return self.total_rows