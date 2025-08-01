"""
Extractor para datos de plantas de cacao desde archivo Excel.
"""
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional
from loguru import logger

from src.models.operational.staging.plantas_stg_model import StgPlantas


class PlantasExcelExtractor:
    """Extrae datos de plantas de cacao desde archivo Excel."""
    
    def __init__(self, file_path: str, sheet_name: str = "PLANTAS DE CACAO"):
        """
        Inicializa el extractor.
        
        Args:
            file_path: Ruta al archivo Excel
            sheet_name: Nombre de la pestaña (por defecto "PLANTAS DE CACAO")
        """
        self.file_path = file_path
        self.sheet_name = sheet_name
        
    def extract(self) -> List[StgPlantas]:
        """
        Extrae datos del Excel y los convierte a objetos StgPlantas.
        
        Returns:
            Lista de objetos StgPlantas
        """
        logger.info(f"Extrayendo datos de {self.file_path}, pestaña: {self.sheet_name}")
        
        try:
            # Leer Excel
            df = pd.read_excel(self.file_path, sheet_name=self.sheet_name)
            logger.info(f"Excel leído: {len(df)} filas, {len(df.columns)} columnas")
            
            # Limpiar datos vacíos
            df = df.dropna(how='all')
            logger.info(f"Después de limpiar filas vacías: {len(df)} filas")
            
            # Convertir a objetos StgPlantas
            plantas_records = []
            
            for index, row in df.iterrows():
                try:
                    plantas_record = self._row_to_staging_record(row)
                    plantas_records.append(plantas_record)
                except Exception as e:
                    logger.error(f"Error procesando fila {index}: {str(e)}")
                    continue
            
            logger.info(f"Extracción completada: {len(plantas_records)} registros válidos")
            return plantas_records
            
        except Exception as e:
            logger.error(f"Error durante la extracción: {str(e)}")
            raise
    
    def _row_to_staging_record(self, row) -> StgPlantas:
        """
        Convierte una fila del DataFrame a un objeto StgPlantas.
        
        Args:
            row: Fila del DataFrame
            
        Returns:
            Objeto StgPlantas
        """
        return StgPlantas(
            # Campos identificadores
            actas=self.safe_string(row.get('ACTAS')),
            
            # Información del beneficiario
            asociaciones=self.safe_string(row.get('ASOCIACIONES')),
            apellidos=self.safe_string(row.get('APELLIDOS')),
            nombres=self.safe_string(row.get('NOMBRES')),
            nombres_completos=self.safe_string(row.get('NOMBRES COMPLETOS')),
            cedula=self.safe_string(row.get('CEDULA')),
            telefono=self.safe_string(row.get('TELEFONO')),
            genero=self.safe_string(row.get('GENERO')),
            edad=self.safe_int(row.get('EDAD')),
            
            # Información geográfica
            canton=self.safe_string(row.get('CANTON')),
            parroquia=self.safe_string(row.get('PARROQUIA')),
            recinto_comuna_sector=self.safe_string(row.get('RECINTO, COMUNA O SECTOR')),
            coord_x=self.safe_decimal(row.get('X')),
            coord_y=self.safe_decimal(row.get('Y')),
            
            # Información del beneficio
            hectareas=self.safe_decimal(row.get('HECTAREAS')),
            entrega=self.safe_int(row.get('ENTREGA')),
            cultivo_1=self.safe_string(row.get('CULTIVO 1')),
            fecha_entrega=self.safe_datetime(row.get('FECHA DE ENTREGA')),
            lugar_entrega=self.safe_string(row.get('LUGAR DE ENTREGA')),
            
            # Información administrativa
            contratista=self.safe_string(row.get('CONTRATISTA')),
            cedula_contratista=self.safe_string(row.get('CEDULA2')),
            observacion=self.safe_string(row.get('OBSERVACION')),
            precio_unitario=self.safe_decimal_from_string(row.get('PRECIO UNITARIO')),
            anio=self.safe_int(row.get('AÑO')),
            rubro=self.safe_string(row.get('RUBRO')),
            
            # Campos de control
            processed=False
        )
    
    def safe_string(self, value) -> Optional[str]:
        """Convierte valor a string de forma segura."""
        if pd.isna(value) or value is None:
            return None
        
        str_val = str(value).strip()
        
        # Manejar fórmulas de Excel
        if str_val.startswith('='):
            return None
            
        return str_val if str_val else None
    
    def safe_int(self, value) -> Optional[int]:
        """Convierte valor a entero de forma segura."""
        if pd.isna(value) or value is None:
            return None
        
        try:
            str_val = str(value).strip()
            
            # Manejar fórmulas de Excel
            if str_val.startswith('='):
                return None
            
            # Intentar convertir a float primero (por si hay decimales) y luego a int
            return int(float(str_val))
        except (ValueError, TypeError):
            return None
    
    def safe_decimal(self, value) -> Optional[float]:
        """Convierte valor a decimal de forma segura."""
        if pd.isna(value) or value is None:
            return None
        
        try:
            str_val = str(value).strip()
            
            # Manejar fórmulas de Excel
            if str_val.startswith('='):
                return None
            
            return float(str_val)
        except (ValueError, TypeError):
            return None
    
    def safe_decimal_from_string(self, value) -> Optional[float]:
        """Convierte string con comas decimales a decimal de forma segura."""
        if pd.isna(value) or value is None:
            return None
        
        try:
            str_val = str(value).strip()
            
            # Manejar fórmulas de Excel
            if str_val.startswith('='):
                return None
            
            # Reemplazar coma por punto para decimal
            str_val = str_val.replace(',', '.')
            return float(str_val)
        except (ValueError, TypeError):
            return None
    
    def safe_datetime(self, value) -> Optional[datetime]:
        """Convierte valor a datetime de forma segura."""
        if pd.isna(value) or value is None:
            return None
        
        try:
            # Si ya es datetime
            if isinstance(value, datetime):
                return value
            
            # Si es pd.Timestamp
            if hasattr(value, 'to_pydatetime'):
                return value.to_pydatetime()
            
            # Intentar parsear string
            str_val = str(value).strip()
            
            # Manejar fórmulas de Excel
            if str_val.startswith('='):
                return None
            
            return pd.to_datetime(str_val).to_pydatetime()
        except (ValueError, TypeError):
            return None