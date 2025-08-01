"""Loader para cargar datos transformados a operational."""
import pandas as pd
from typing import Dict, List, Tuple
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from loguru import logger

from config.connections.database import db_connection
from src.models.operational.operational.persona_base_ops import PersonaBase
from src.models.operational.operational.persona_agricultor_ops import PersonaAgricultor
from src.models.operational.operational.ubicaciones_ops import Ubicacion
from src.models.operational.operational.organizaciones_ops import Organizacion
from src.models.operational.operational.beneficio_base_ops import BeneficioBase
from src.models.operational.operational.beneficio_semillas_ops import BeneficioSemillas


class SemillasOperationalLoader:
    """Carga datos normalizados a las tablas operacionales."""
    
    def __init__(self):
        self.stats = {
            'personas_insertadas': 0,
            'personas_actualizadas': 0,
            'ubicaciones_insertadas': 0,
            'organizaciones_insertadas': 0,
            'beneficios_insertados': 0,
            'errores': 0
        }
        # Mapeos para relacionar entidades
        self.persona_id_map = {}  # cedula/nombre -> id
        self.ubicacion_id_map = {}  # key -> id
        self.organizacion_id_map = {}  # nombre -> id
        
    def load_batch(self, entities: Dict[str, pd.DataFrame], session: Session) -> Dict:
        """Carga un batch de entidades a operational."""
        try:
            # Orden importante: primero las entidades independientes
            if 'organizaciones' in entities and len(entities['organizaciones']) > 0:
                self._load_organizaciones(entities['organizaciones'], session)
                
            if 'ubicaciones' in entities and len(entities['ubicaciones']) > 0:
                self._load_ubicaciones(entities['ubicaciones'], session)
                
            if 'personas' in entities and len(entities['personas']) > 0:
                self._load_personas(entities['personas'], session)
                
            if 'personas_agricultores' in entities and len(entities['personas_agricultores']) > 0:
                self._load_personas_agricultores(entities['personas_agricultores'], session)
                
            if 'beneficios' in entities and len(entities['beneficios']) > 0:
                self._load_beneficios(entities['beneficios'], session)
                
            return self.stats
            
        except Exception as e:
            logger.error(f"Error cargando batch: {str(e)}")
            session.rollback()
            raise
            
    def _load_organizaciones(self, df: pd.DataFrame, session: Session):
        """Carga organizaciones únicas."""
        for _, row in df.iterrows():
            try:
                # Verificar si existe
                org = session.query(Organizacion).filter_by(
                    nombre=row['nombre']
                ).first()
                
                if not org:
                    org = Organizacion(
                        nombre=row['nombre'],
                        tipo_organizacion=row.get('tipo_organizacion'),
                        estado=row.get('estado', 'ACTIVO')
                    )
                    session.add(org)
                    session.flush()  # Para obtener el ID
                    self.stats['organizaciones_insertadas'] += 1
                    
                # Guardar mapeo
                self.organizacion_id_map[row['nombre']] = org.id
                
            except IntegrityError:
                session.rollback()
                # Ya existe, obtener el existente
                org = session.query(Organizacion).filter_by(
                    nombre=row['nombre']
                ).first()
                if org:
                    self.organizacion_id_map[row['nombre']] = org.id
                    
    def _load_ubicaciones(self, df: pd.DataFrame, session: Session):
        """Carga ubicaciones únicas."""
        for _, row in df.iterrows():
            try:
                # Crear clave única
                key = f"{row['canton']}|{row.get('parroquia', '')}|{row.get('localidad', '')}"
                
                # Verificar si existe
                ubi = session.query(Ubicacion).filter_by(
                    canton=row['canton'],
                    parroquia=row.get('parroquia'),
                    localidad=row.get('localidad')
                ).first()
                
                if not ubi:
                    ubi = Ubicacion(
                        canton=row['canton'],
                        parroquia=row.get('parroquia'),
                        localidad=row.get('localidad'),
                        coordenada_x=row.get('coordenada_x'),
                        coordenada_y=row.get('coordenada_y'),
                        tipo_ubicacion=row.get('tipo_ubicacion', 'RURAL')
                    )
                    session.add(ubi)
                    session.flush()
                    self.stats['ubicaciones_insertadas'] += 1
                    
                # Guardar mapeo
                self.ubicacion_id_map[key] = ubi.id
                
            except IntegrityError:
                session.rollback()
                # Obtener existente
                ubi = session.query(Ubicacion).filter_by(
                    canton=row['canton'],
                    parroquia=row.get('parroquia'),
                    localidad=row.get('localidad')
                ).first()
                if ubi:
                    self.ubicacion_id_map[key] = ubi.id
                    
    def _load_personas(self, df: pd.DataFrame, session: Session):
        """Carga personas con merge (insert o update)."""
        logger.info(f"Cargando {len(df)} personas")
        
        # Filtrar completamente registros con nombres nulos ANTES de procesar
        valid_df = df[df['nombres_apellidos'].notna() & (df['nombres_apellidos'].str.strip() != '')]
        logger.info(f"Personas válidas tras filtro: {len(valid_df)}/{len(df)}")
        
        if len(valid_df) == 0:
            logger.warning("No hay personas válidas para cargar")
            return
        
        for idx, row in valid_df.iterrows():
            try:
                # Ya sabemos que el nombre es válido por el filtro anterior
                nombres = row['nombres_apellidos']
                    
                # Crear savepoint para poder hacer rollback parcial
                savepoint = session.begin_nested()
                # Buscar por cédula si existe
                persona = None
                if pd.notna(row.get('cedula')):
                    persona = session.query(PersonaBase).filter_by(
                        cedula=row['cedula']
                    ).first()
                
                if persona:
                    # Actualizar datos
                    persona.nombres_apellidos = nombres
                    persona.telefono = row.get('telefono')
                    persona.genero = row.get('genero')
                    # Manejar edad NaN
                    edad = row.get('edad')
                    persona.edad = int(edad) if pd.notna(edad) else None
                    self.stats['personas_actualizadas'] += 1
                    # Guardar mapeo
                    key = row.get('cedula') if pd.notna(row.get('cedula')) else nombres
                    self.persona_id_map[key] = persona.id
                else:
                    # Insertar nueva
                    edad = row.get('edad')
                    edad_val = int(edad) if pd.notna(edad) else None
                    
                    # Verificación final antes de crear el objeto
                    nombres_final = row['nombres_apellidos']
                    if nombres_final is None or str(nombres_final).strip() == '':
                        logger.error(f"Intento de insertar persona sin nombre en índice {idx}")
                        continue
                    
                    persona = PersonaBase(
                        cedula=row.get('cedula'),
                        nombres_apellidos=nombres_final,
                        telefono=row.get('telefono'),
                        genero=row.get('genero'),
                        edad=edad_val,
                        is_active=True
                    )
                    session.add(persona)
                    # Hacer commit individual para evitar bulk insert problemático
                    try:
                        session.flush()
                        self.stats['personas_insertadas'] += 1
                        # Guardar mapeo ahora que tenemos el ID
                        key = row.get('cedula') if pd.notna(row.get('cedula')) else nombres_final
                        self.persona_id_map[key] = persona.id
                    except Exception as e:
                        logger.error(f"Error insertando persona {nombres_final}: {str(e)}")
                        session.rollback()
                        raise
                
                # Commit del savepoint
                savepoint.commit()
                
            except Exception as e:
                logger.error(f"Error insertando persona: {str(e)}")
                self.stats['errores'] += 1
                # Rollback solo de este registro
                savepoint.rollback()
                
    def _load_personas_agricultores(self, df: pd.DataFrame, session: Session):
        """Carga información de agricultores."""
        for _, row in df.iterrows():
            try:
                persona_id = row.get('persona_id')
                
                # Verificar si ya existe
                agricultor = session.query(PersonaAgricultor).filter_by(
                    persona_id=persona_id
                ).first()
                
                if not agricultor:
                    agricultor = PersonaAgricultor(
                        persona_id=persona_id,
                        tipo_productor=row.get('tipo_productor', 'AGRICULTOR'),
                        hectarias_totales=row.get('hectarias_totales'),
                        organizacion_id=row.get('organizacion_id')
                    )
                    session.add(agricultor)
                else:
                    # Actualizar si hay cambios
                    if row.get('hectarias_totales'):
                        agricultor.hectarias_totales = row['hectarias_totales']
                    if row.get('organizacion_id'):
                        agricultor.organizacion_id = row['organizacion_id']
                        
            except Exception as e:
                logger.error(f"Error con agricultor: {str(e)}")
                self.stats['errores'] += 1
                
    def _load_beneficios(self, df: pd.DataFrame, session: Session):
        """Carga beneficios de semillas."""
        for _, row in df.iterrows():
            try:
                # Crear beneficio base
                beneficio_base = BeneficioBase(
                    persona_id=row['persona_id'],
                    ubicacion_id=row.get('ubicacion_id')
                )
                session.add(beneficio_base)
                session.flush()
                
                # Crear beneficio semillas
                beneficio_semillas = BeneficioSemillas(
                    beneficio_id=beneficio_base.id,
                    cultivo=row.get('cultivo'),
                    hectarias_beneficiadas=row.get('hectarias_beneficiadas'),
                    precio_unitario=row.get('precio_unitario'),
                    inversion=row.get('inversion'),
                    quintil=row.get('quintil'),
                    score_quintil=row.get('score_quintil'),
                    numero_acta=row.get('numero_acta'),
                    documento=row.get('documento'),
                    proceso=row.get('proceso'),
                    fecha_retiro=row.get('fecha_retiro'),
                    anio=row.get('anio'),
                    responsable_agencia=row.get('responsable_agencia'),
                    cedula_jefe_sucursal=row.get('cedula_jefe_sucursal'),
                    sucursal=row.get('sucursal'),
                    observacion=row.get('observacion'),
                    estado='ACTIVO'
                )
                session.add(beneficio_semillas)
                self.stats['beneficios_insertados'] += 1
                
            except Exception as e:
                logger.error(f"Error insertando beneficio: {str(e)}")
                self.stats['errores'] += 1
                
    def get_summary(self) -> Dict:
        """Retorna resumen de la carga."""
        return self.stats.copy()
        
    def reset_stats(self):
        """Reinicia las estadísticas."""
        self.stats = {
            'personas_insertadas': 0,
            'personas_actualizadas': 0,
            'ubicaciones_insertadas': 0,
            'organizaciones_insertadas': 0,
            'beneficios_insertados': 0,
            'errores': 0
        }