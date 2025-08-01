"""
Loader para operational de fertilizantes usando SQLAlchemy Core.
Combina la seguridad de ORM con el rendimiento de SQL directo.
"""
import pandas as pd
from typing import Dict, Optional
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import insert, update, select, and_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from loguru import logger

# Importar modelos para acceder a las tablas
from src.models.operational.operational.persona_base_ops import PersonaBase
from src.models.operational.operational.ubicaciones_ops import Ubicacion
from src.models.operational.operational.organizaciones_ops import Organizacion
from src.models.operational.operational.beneficio_base_ops import BeneficioBase
from src.models.operational.operational.beneficio_fertilizantes_ops import BeneficioFertilizantes
from src.models.operational.operational.beneficiario_fertilizantes_ops import BeneficiarioFertilizantes


class FertilizantesOperationalLoader:
    """Carga datos usando SQLAlchemy Core para mejor rendimiento y seguridad."""
    
    def __init__(self):
        self.stats = {
            'personas_insertadas': 0,
            'personas_actualizadas': 0,
            'ubicaciones_insertadas': 0,
            'organizaciones_insertadas': 0,
            'beneficiarios_fertilizantes_insertados': 0,
            'beneficios_insertados': 0,
            'errores': 0
        }
        # Mapeos para mantener relaciones
        self.persona_id_map = {}
        self.ubicacion_id_map = {}
        self.organizacion_id_map = {}
        
    def reset_stats(self):
        """Resetea las estadísticas para un nuevo batch."""
        self.stats = {k: 0 for k in self.stats.keys()}
        
    def get_summary(self) -> Dict:
        """Retorna resumen de estadísticas."""
        return self.stats.copy()
        
    def load_batch(self, entities: Dict[str, pd.DataFrame], session: Session) -> Dict:
        """Carga un batch de entidades usando SQLAlchemy Core."""
        try:
            # Orden importante: primero las entidades independientes
            if 'organizaciones' in entities and len(entities['organizaciones']) > 0:
                self._load_organizaciones(entities['organizaciones'], session)
                
            if 'ubicaciones' in entities and len(entities['ubicaciones']) > 0:
                self._load_ubicaciones(entities['ubicaciones'], session)
                
            if 'personas' in entities and len(entities['personas']) > 0:
                self._load_personas(entities['personas'], session)
                
            # Luego las entidades dependientes
            if 'beneficiarios_fertilizantes' in entities and len(entities['beneficiarios_fertilizantes']) > 0:
                self._load_beneficiarios_fertilizantes(entities['beneficiarios_fertilizantes'], session)
                
            if 'beneficios' in entities and len(entities['beneficios']) > 0:
                self._load_beneficios(entities['beneficios'], session)
                
            return self.stats
            
        except Exception as e:
            self.stats['errores'] += 1
            logger.error(f"Error cargando batch: {str(e)}")
            raise
            
    def _load_personas(self, df: pd.DataFrame, session: Session):
        """Carga personas usando SQLAlchemy Core con UPSERT."""
        logger.info(f"Cargando {len(df)} personas con SQLAlchemy Core")
        
        for idx, row in df.iterrows():
            try:
                # Validar datos requeridos
                nombres = row.get('nombres_apellidos')
                if not nombres or str(nombres).strip() == '':
                    logger.warning(f"Saltando persona sin nombre en índice {idx}")
                    continue
                
                # Preparar datos
                persona_data = {
                    'nombres_apellidos': str(nombres).strip().upper(),
                    'cedula': str(row.get('cedula', '')).strip() if pd.notna(row.get('cedula')) else None,
                    'telefono': str(row.get('telefono', '')).strip() if pd.notna(row.get('telefono')) else None,
                    'genero': str(row.get('genero', '')).strip().upper() if pd.notna(row.get('genero')) else None,
                    'edad': int(row.get('edad')) if pd.notna(row.get('edad')) else None,
                    'is_active': True
                }
                
                # Buscar existente por cédula o nombre
                existing = None
                if persona_data['cedula']:
                    stmt = select(PersonaBase).where(PersonaBase.cedula == persona_data['cedula'])
                    existing = session.execute(stmt).scalars().first()
                
                if not existing:
                    stmt = select(PersonaBase).where(PersonaBase.nombres_apellidos == persona_data['nombres_apellidos'])
                    existing = session.execute(stmt).scalars().first()
                
                if existing:
                    # Actualizar existente
                    stmt = update(PersonaBase).where(
                        PersonaBase.id == existing.id
                    ).values(**{k: v for k, v in persona_data.items() if v is not None})
                    
                    session.execute(stmt)
                    persona_id = existing.id
                    self.stats['personas_actualizadas'] += 1
                    logger.debug(f"Persona actualizada: {nombres}")
                else:
                    # Insertar nueva
                    stmt = insert(PersonaBase).values(**persona_data).returning(PersonaBase.id)
                    result = session.execute(stmt)
                    persona_id = result.scalar()
                    self.stats['personas_insertadas'] += 1
                    logger.debug(f"Persona insertada: {nombres}")
                
                # Guardar mapeo
                key = persona_data['cedula'] if persona_data['cedula'] else persona_data['nombres_apellidos']
                self.persona_id_map[key] = persona_id
                
            except Exception as e:
                logger.error(f"Error procesando persona {idx}: {str(e)}")
                self.stats['errores'] += 1
                
        logger.info(f"Personas insertadas: {self.stats['personas_insertadas']}")
        logger.info(f"Personas actualizadas: {self.stats['personas_actualizadas']}")
        
    def _load_organizaciones(self, df: pd.DataFrame, session: Session):
        """Carga organizaciones usando UPSERT con PostgreSQL."""
        logger.info(f"Cargando {len(df)} organizaciones")
        
        # Preparar datos únicos
        org_data = []
        seen = set()
        
        for _, row in df.iterrows():
            nombre = str(row.get('nombre', '')).strip().upper()
            if nombre and nombre not in seen:
                seen.add(nombre)
                org_data.append({
                    'nombre': nombre,
                    'tipo_organizacion': 'ASOCIACION',
                    'estado': 'ACTIVO'
                })
        
        if org_data:
            # Usar ON CONFLICT DO UPDATE para UPSERT
            stmt = pg_insert(Organizacion).values(org_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['nombre'],
                set_={'estado': 'ACTIVO'}
            ).returning(Organizacion.id, Organizacion.nombre)
            
            result = session.execute(stmt)
            
            # Actualizar mapeo
            for id_val, nombre in result:
                self.organizacion_id_map[nombre] = id_val
                self.stats['organizaciones_insertadas'] += 1
                
        logger.info(f"Organizaciones procesadas: {self.stats['organizaciones_insertadas']}")
        
    def _load_ubicaciones(self, df: pd.DataFrame, session: Session):
        """Carga ubicaciones únicas usando UPSERT."""
        logger.info(f"Cargando {len(df)} ubicaciones")
        
        # Preparar datos únicos
        ubicaciones_data = []
        seen = set()
        
        for _, row in df.iterrows():
            canton = str(row.get('canton', '')).strip().upper()
            parroquia = str(row.get('parroquia', '')).strip().upper()
            localidad = str(row.get('localidad', '')).strip().upper()
            
            key = (canton, parroquia, localidad)
            if key not in seen and canton:
                seen.add(key)
                ubicaciones_data.append({
                    'canton': canton,
                    'parroquia': parroquia if parroquia else None,
                    'localidad': localidad if localidad else None,
                    'coordenada_x': str(row.get('coordenada_x', '')).strip() if pd.notna(row.get('coordenada_x')) else None,
                    'coordenada_y': str(row.get('coordenada_y', '')).strip() if pd.notna(row.get('coordenada_y')) else None
                })
        
        if ubicaciones_data:
            for ubi_data in ubicaciones_data:
                # Buscar existente
                conditions = [Ubicacion.canton == ubi_data['canton']]
                if ubi_data['parroquia']:
                    conditions.append(Ubicacion.parroquia == ubi_data['parroquia'])
                if ubi_data['localidad']:
                    conditions.append(Ubicacion.localidad == ubi_data['localidad'])
                    
                stmt = select(Ubicacion).where(and_(*conditions))
                existing = session.execute(stmt).scalars().first()
                
                if existing:
                    # Actualizar coordenadas si las nuevas no son nulas
                    updates = {}
                    if ubi_data['coordenada_x'] and not existing.coordenada_x:
                        updates['coordenada_x'] = ubi_data['coordenada_x']
                    if ubi_data['coordenada_y'] and not existing.coordenada_y:
                        updates['coordenada_y'] = ubi_data['coordenada_y']
                        
                    if updates:
                        stmt = update(Ubicacion).where(Ubicacion.id == existing.id).values(**updates)
                        session.execute(stmt)
                        
                    ubicacion_id = existing.id
                else:
                    # Insertar nueva
                    stmt = insert(Ubicacion).values(**ubi_data).returning(Ubicacion.id)
                    result = session.execute(stmt)
                    ubicacion_id = result.scalar()
                    self.stats['ubicaciones_insertadas'] += 1
                
                # Guardar mapeo
                key = (ubi_data['canton'], ubi_data['parroquia'], ubi_data['localidad'])
                self.ubicacion_id_map[key] = ubicacion_id
                
        logger.info(f"Ubicaciones insertadas: {self.stats['ubicaciones_insertadas']}")
        
    def _load_beneficiarios_fertilizantes(self, df: pd.DataFrame, session: Session):
        """Carga beneficiarios de fertilizantes."""
        logger.info(f"Cargando {len(df)} beneficiarios fertilizantes")
        
        for _, row in df.iterrows():
            try:
                # Obtener IDs de relaciones
                persona_key = row.get('cedula') if pd.notna(row.get('cedula')) else row.get('nombres_apellidos')
                persona_id = self.persona_id_map.get(persona_key)
                
                if not persona_id:
                    logger.warning(f"No se encontró persona para beneficiario: {persona_key}")
                    continue
                
                # Obtener organización
                org_nombre = str(row.get('organizacion', '')).strip().upper() if pd.notna(row.get('organizacion')) else None
                organizacion_id = self.organizacion_id_map.get(org_nombre) if org_nombre else None
                
                # Datos del beneficiario
                beneficiario_data = {
                    'persona_id': persona_id,
                    'tipo_productor': 'BENEFICIARIO_FERTILIZANTES',
                    'organizacion_id': organizacion_id,
                    'organizacion_nombre': org_nombre,  # Para referencia
                    'hectarias_totales': row.get('hectarias_totales')
                }
                
                # Verificar si ya existe
                stmt = select(BeneficiarioFertilizantes).where(
                    BeneficiarioFertilizantes.persona_id == persona_id
                )
                existing = session.execute(stmt).scalars().first()
                
                if not existing:
                    stmt = insert(BeneficiarioFertilizantes).values(**beneficiario_data)
                    session.execute(stmt)
                    self.stats['beneficiarios_fertilizantes_insertados'] += 1
                    
            except Exception as e:
                logger.error(f"Error procesando beneficiario fertilizantes: {str(e)}")
                self.stats['errores'] += 1
                
        logger.info(f"Beneficiarios fertilizantes insertados: {self.stats['beneficiarios_fertilizantes_insertados']}")
        
    def _load_beneficios(self, df: pd.DataFrame, session: Session):
        """Carga beneficios de fertilizantes."""
        logger.info(f"Cargando {len(df)} beneficios")
        
        for _, row in df.iterrows():
            try:
                # Obtener IDs necesarios
                persona_key = row.get('cedula') if pd.notna(row.get('cedula')) else row.get('nombres_apellidos')
                persona_id = self.persona_id_map.get(persona_key)
                
                ubicacion_key = (
                    str(row.get('canton', '')).strip().upper(),
                    str(row.get('parroquia', '')).strip().upper() if pd.notna(row.get('parroquia')) else None,
                    str(row.get('localidad', '')).strip().upper() if pd.notna(row.get('localidad')) else None
                )
                ubicacion_id = self.ubicacion_id_map.get(ubicacion_key)
                
                org_nombre = str(row.get('organizacion', '')).strip().upper() if pd.notna(row.get('organizacion')) else None
                organizacion_id = self.organizacion_id_map.get(org_nombre) if org_nombre else None
                
                if not persona_id:
                    logger.warning(f"No se encontró persona para beneficio: {persona_key}")
                    continue
                    
                # Preparar todos los datos del beneficio (base + específicos)
                fertilizante_data = {
                    # Campos de BeneficioBase
                    'persona_id': persona_id,
                    'ubicacion_id': ubicacion_id,
                    'fecha': row.get('fecha_entrega'),
                    'tipo_beneficio': 'fertilizantes',
                    'hectarias_beneficiadas': float(row.get('hectarias_beneficiadas')) if pd.notna(row.get('hectarias_beneficiadas')) else None,
                    'valor_monetario': float(row.get('costo_total')) if pd.notna(row.get('costo_total')) else None,
                    # Campos específicos de BeneficioFertilizantes
                    'tipo_cultivo': str(row.get('tipo_cultivo', '')).strip() if pd.notna(row.get('tipo_cultivo')) else 'CULTIVO_GENERAL',
                    'marca_fertilizante': str(row.get('marca_fertilizante', '')).strip() if pd.notna(row.get('marca_fertilizante')) else None,
                    'cantidad_sacos': int(row.get('cantidad_sacos')) if pd.notna(row.get('cantidad_sacos')) else None,
                    'peso_por_saco': float(row.get('peso_por_saco')) if pd.notna(row.get('peso_por_saco')) else None,
                    'precio_unitario': float(row.get('precio_unitario')) if pd.notna(row.get('precio_unitario')) else None,
                    'costo_total': float(row.get('costo_total')) if pd.notna(row.get('costo_total')) else None,
                    'quintil': int(row.get('quintil')) if pd.notna(row.get('quintil')) else None,
                    'score_quintil': float(row.get('score_quintil')) if pd.notna(row.get('score_quintil')) else None,
                    'numero_acta': str(row.get('numero_acta', '')).strip() if pd.notna(row.get('numero_acta')) else None,
                    'documento': str(row.get('documento', '')).strip() if pd.notna(row.get('documento')) else None,
                    'proceso': str(row.get('proceso', '')).strip() if pd.notna(row.get('proceso')) else None,
                    'fecha_entrega': row.get('fecha_entrega'),
                    'anio': int(row.get('anio')) if pd.notna(row.get('anio')) else datetime.now().year,
                    'responsable_agencia': str(row.get('responsable_agencia', '')).strip() if pd.notna(row.get('responsable_agencia')) else None,
                    'cedula_jefe_sucursal': str(row.get('cedula_jefe_sucursal', '')).strip() if pd.notna(row.get('cedula_jefe_sucursal')) else None,
                    'sucursal': str(row.get('sucursal', '')).strip() if pd.notna(row.get('sucursal')) else None,
                    'observacion': str(row.get('observacion', '')).strip() if pd.notna(row.get('observacion')) else None,
                    'estado': 'ACTIVO'
                }
                
                # Insertar en beneficio_fertilizantes
                # Para joined table inheritance, necesitamos usar el ORM
                beneficio_fertilizante = BeneficioFertilizantes(**fertilizante_data)
                session.add(beneficio_fertilizante)
                session.flush()  # Para asegurar que se inserte
                
                self.stats['beneficios_insertados'] += 1
                
            except Exception as e:
                logger.error(f"Error procesando beneficio: {str(e)}")
                self.stats['errores'] += 1
                
        logger.info(f"Beneficios insertados: {self.stats['beneficios_insertados']}")