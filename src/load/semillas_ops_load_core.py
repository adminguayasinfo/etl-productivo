"""
Loader para operational usando SQLAlchemy Core.
Combina la seguridad de ORM con el rendimiento de SQL directo.
"""
import pandas as pd
from typing import Dict, Optional
from sqlalchemy.orm import Session
from sqlalchemy import insert, update, select, and_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from loguru import logger

# Importar modelos para acceder a las tablas
from src.models.operational.operational.persona_base_ops import PersonaBase
from src.models.operational.operational.ubicaciones_ops import Ubicacion
from src.models.operational.operational.organizaciones_ops import Organizacion
from src.models.operational.operational.beneficio_base_ops import BeneficioBase
from src.models.operational.operational.beneficio_semillas_ops import BeneficioSemillas
# from src.models.operational.operational.beneficiario_semillas_ops import BeneficiarioSemillas  # No existe en BD actual


class SemillasOperationalLoaderCore:
    """Carga datos usando SQLAlchemy Core para mejor rendimiento y seguridad."""
    
    def __init__(self):
        self.stats = {
            'personas_insertadas': 0,
            'personas_actualizadas': 0,
            'ubicaciones_insertadas': 0,
            'organizaciones_insertadas': 0,
            'beneficiarios_semillas_insertados': 0,
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
            if 'beneficiarios_semillas' in entities and len(entities['beneficiarios_semillas']) > 0:
                self._load_beneficiarios_semillas(entities['beneficiarios_semillas'], session)
                
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
            # UPSERT usando PostgreSQL ON CONFLICT
            stmt = pg_insert(Organizacion).values(org_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['nombre'],
                set_=dict(tipo_organizacion=stmt.excluded.tipo_organizacion, updated_at='NOW()')
            ).returning(Organizacion.id, Organizacion.nombre)
            
            result = session.execute(stmt)
            
            # Actualizar mapeo y estadísticas
            for org_id, nombre in result:
                self.organizacion_id_map[nombre] = org_id
                self.stats['organizaciones_insertadas'] += 1
                
    def _load_ubicaciones(self, df: pd.DataFrame, session: Session):
        """Carga ubicaciones usando UPSERT."""
        logger.info(f"Cargando {len(df)} ubicaciones")
        
        # Preparar datos únicos
        ubi_data = []
        seen = set()
        
        for _, row in df.iterrows():
            # Crear clave única - canton es requerido
            canton = str(row.get('canton', '')).strip().upper() if pd.notna(row.get('canton')) else 'NO ESPECIFICADO'
            parroquia = str(row.get('parroquia', '')).strip().upper() if pd.notna(row.get('parroquia')) else None
            localidad = str(row.get('localidad', '')).strip() if pd.notna(row.get('localidad')) else None
            
            key = (canton, parroquia, localidad)
            if key not in seen:
                seen.add(key)
                ubi_data.append({
                    'canton': canton,
                    'parroquia': parroquia,
                    'localidad': localidad,
                    'coordenada_x': float(row.get('coordenada_x')) if pd.notna(row.get('coordenada_x')) else None,
                    'coordenada_y': float(row.get('coordenada_y')) if pd.notna(row.get('coordenada_y')) else None,
                    'tipo_ubicacion': 'RURAL'  # Default
                })
        
        if ubi_data:
            # Insertar en batch con ON CONFLICT
            stmt = pg_insert(Ubicacion).values(ubi_data)
            stmt = stmt.on_conflict_do_update(
                constraint='unique_ubicacion',  # Usa el constraint definido en el modelo
                set_=dict(
                    coordenada_x=stmt.excluded.coordenada_x,
                    coordenada_y=stmt.excluded.coordenada_y,
                    updated_at='NOW()'
                )
            ).returning(Ubicacion.id, Ubicacion.canton, Ubicacion.parroquia, Ubicacion.localidad)
            
            result = session.execute(stmt)
            
            # Actualizar mapeo
            for ubi_id, cant, parr, loc in result:
                key = f"{cant}|{parr or ''}|{loc or ''}"
                self.ubicacion_id_map[key] = ubi_id
                self.stats['ubicaciones_insertadas'] += 1
                
    def _load_beneficiarios_semillas(self, df: pd.DataFrame, session: Session):
        """Carga beneficiarios semillas."""
        logger.info(f"Cargando {len(df)} beneficiarios semillas")
        
        beneficiarios_data = []
        
        for _, row in df.iterrows():
            # Obtener el ID real de la persona
            persona_id = self._get_real_persona_id(row, session)
            if not persona_id:
                logger.warning(f"No se encontró persona para beneficiario")
                continue
                
            # NOTA: BeneficiarioSemillas no existe en BD actual - código legacy comentado
            # beneficiarios_data.append({
            #     'persona_id': int(persona_id),
            #     'tipo_productor': 'AGRICULTOR'  # Default
            # })
        
        # NOTA: BeneficiarioSemillas no existe en BD actual - sección legacy comentada
        # if beneficiarios_data:
        #     # UPSERT en batch
        #     stmt = pg_insert(BeneficiarioSemillas).values(beneficiarios_data)
        #     stmt = stmt.on_conflict_do_update(
        #         index_elements=['persona_id'],
        #         set_=dict(
        #             tipo_productor=stmt.excluded.tipo_productor
        #         )
        #     )
        #     
        #     session.execute(stmt)
        #     self.stats['beneficiarios_semillas_insertados'] += len(beneficiarios_data)
        
        # Para compatibilidad, mantener estadística en 0
        self.stats['beneficiarios_semillas_insertados'] = 0
            
    def _load_beneficios(self, df: pd.DataFrame, session: Session):
        """Carga beneficios usando SQLAlchemy Core."""
        logger.info(f"Cargando {len(df)} beneficios")
        
        for _, row in df.iterrows():
            try:
                # Obtener IDs reales
                persona_id = self._get_real_persona_id(row, session)
                ubicacion_id = self._get_real_ubicacion_id(row, session)
                organizacion_id = self._get_real_organizacion_id(row, session)
                
                if not persona_id:
                    logger.warning(f"No se encontró persona para beneficio")
                    continue
                
                # Datos del beneficio base
                beneficio_data = {
                    'persona_id': persona_id,
                    'ubicacion_id': ubicacion_id,
                    'tipo_beneficio': str(row.get('tipo_beneficio', 'SEMILLAS')).upper(),
                    'fecha': pd.to_datetime(row.get('fecha')),
                    'hectarias_beneficiadas': float(row.get('hectarias_beneficiadas')) if pd.notna(row.get('hectarias_beneficiadas')) else None,
                    'valor_monetario': float(row.get('valor_monetario')) if pd.notna(row.get('valor_monetario')) else None
                }
                
                # Verificar si ya existe
                stmt = select(BeneficioBase).where(
                    and_(
                        BeneficioBase.persona_id == persona_id,
                        BeneficioBase.fecha == beneficio_data['fecha'],
                        BeneficioBase.tipo_beneficio == beneficio_data['tipo_beneficio']
                    )
                )
                existing = session.execute(stmt).scalars().first()
                
                if not existing:
                    # Con joined table inheritance, insertamos directamente en BeneficioSemillas
                    # SQLAlchemy automáticamente inserta en BeneficioBase
                    semillas_data = {
                        # Campos de BeneficioBase
                        'persona_id': persona_id,
                        'ubicacion_id': ubicacion_id,
                        'tipo_beneficio': str(row.get('tipo_beneficio', 'SEMILLAS')).upper(),
                        'fecha': pd.to_datetime(row.get('fecha')),
                        'hectarias_beneficiadas': float(row.get('hectarias_beneficiadas')) if pd.notna(row.get('hectarias_beneficiadas')) else None,
                        'valor_monetario': float(row.get('valor_monetario')) if pd.notna(row.get('valor_monetario')) else None,
                        
                        # Campos específicos de BeneficioSemillas
                        'tipo_cultivo': str(row.get('tipo_cultivo', '')).upper() if pd.notna(row.get('tipo_cultivo')) else None,
                        'precio_unitario': float(row.get('precio_unitario')) if pd.notna(row.get('precio_unitario')) else None,
                        'inversion': float(row.get('inversion')) if pd.notna(row.get('inversion')) else None,
                        'quintil': int(row.get('quintil')) if pd.notna(row.get('quintil')) else None,
                        'score_quintil': float(row.get('score_quintil')) if pd.notna(row.get('score_quintil')) else None,
                        'numero_acta': str(row.get('numero_acta', '')) if pd.notna(row.get('numero_acta')) else None,
                        'documento': str(row.get('documento', '')) if pd.notna(row.get('documento')) else None,
                        'proceso': str(row.get('proceso', '')) if pd.notna(row.get('proceso')) else None,
                        'fecha_retiro': pd.to_datetime(row.get('fecha_retiro')) if pd.notna(row.get('fecha_retiro')) else None,
                        'anio': int(row.get('anio')) if pd.notna(row.get('anio')) else pd.to_datetime(row.get('fecha')).year,
                        'responsable_agencia': str(row.get('responsable_agencia', '')) if pd.notna(row.get('responsable_agencia')) else None,
                        'cedula_jefe_sucursal': str(row.get('cedula_jefe_sucursal', '')) if pd.notna(row.get('cedula_jefe_sucursal')) else None,
                        'sucursal': str(row.get('sucursal', '')) if pd.notna(row.get('sucursal')) else None,
                        'observacion': str(row.get('observacion', '')) if pd.notna(row.get('observacion')) else None,
                        'estado': str(row.get('estado', 'ACTIVO')).upper() if pd.notna(row.get('estado')) else 'ACTIVO'
                    }
                    
                    # Usar el ORM para aprovechar la herencia
                    beneficio = BeneficioSemillas(**semillas_data)
                    session.add(beneficio)
                    session.flush()  # Para obtener el ID si es necesario
                    
                    self.stats['beneficios_insertados'] += 1
                    
            except Exception as e:
                logger.error(f"Error procesando beneficio: {str(e)}")
                self.stats['errores'] += 1
                
    def _get_real_persona_id(self, row: pd.Series, session: Session) -> Optional[int]:
        """Obtiene el ID real de persona desde la BD."""
        # Primero intentar del mapeo temporal
        # Usar los campos de lookup que agregamos en el normalizer
        cedula = str(row.get('persona_cedula', '')).strip() if pd.notna(row.get('persona_cedula')) else None
        nombres = str(row.get('persona_nombres', '')).strip().upper() if pd.notna(row.get('persona_nombres')) else None
        
        key = cedula if cedula else nombres
        if key in self.persona_id_map:
            return self.persona_id_map[key]
        
        # Si no está en el mapeo, buscar en BD
        if cedula:
            stmt = select(PersonaBase.id).where(PersonaBase.cedula == cedula)
            result = session.execute(stmt).scalar()
            if result:
                self.persona_id_map[key] = result
                return result
                
        if nombres:
            stmt = select(PersonaBase.id).where(PersonaBase.nombres_apellidos == nombres)
            result = session.execute(stmt).scalar()
            if result:
                self.persona_id_map[key] = result
                return result
                
        return None
        
    def _get_real_ubicacion_id(self, row: pd.Series, session: Session) -> Optional[int]:
        """Obtiene el ID real de ubicación."""
        canton = str(row.get('canton', '')).strip().upper() if pd.notna(row.get('canton')) else 'NO ESPECIFICADO'
        parroquia = str(row.get('parroquia', '')).strip().upper() if pd.notna(row.get('parroquia')) else None
        localidad = str(row.get('localidad', '')).strip() if pd.notna(row.get('localidad')) else None
        
        key = f"{canton}|{parroquia or ''}|{localidad or ''}"
        
        if key in self.ubicacion_id_map:
            return self.ubicacion_id_map[key]
            
        # Buscar en BD
        conditions = [Ubicacion.canton == canton]
        if parroquia:
            conditions.append(Ubicacion.parroquia == parroquia)
        if localidad:
            conditions.append(Ubicacion.localidad == localidad)
            
        stmt = select(Ubicacion.id).where(and_(*conditions))
        result = session.execute(stmt).scalar()
        
        if result:
            self.ubicacion_id_map[key] = result
            return result
            
        return None
        
    def _get_real_organizacion_id(self, row: pd.Series, session: Session) -> Optional[int]:
        """Obtiene el ID real de organización."""
        # Usar el campo de lookup que agregamos en el normalizer
        nombre = str(row.get('organizacion_nombre', '')).strip().upper() if pd.notna(row.get('organizacion_nombre')) else None
        
        if not nombre:
            return None
            
        if nombre in self.organizacion_id_map:
            return self.organizacion_id_map[nombre]
            
        # Buscar en BD
        stmt = select(Organizacion.id).where(Organizacion.nombre == nombre)
        result = session.execute(stmt).scalar()
        
        if result:
            self.organizacion_id_map[nombre] = result
            return result
            
        return None
        
