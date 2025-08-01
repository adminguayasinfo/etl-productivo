"""Cargador dimensional para datos de fertilizantes."""
from datetime import datetime
from typing import Optional, List, Dict
from sqlalchemy import text, select, and_, func, insert
from sqlalchemy.orm import Session
from loguru import logger

from src.models.analytical.dimensions.dim_persona import DimPersona
from src.models.analytical.dimensions.dim_organizacion import DimOrganizacion
from src.models.analytical.dimensions.dim_ubicacion import DimUbicacion
from src.models.analytical.dimensions.dim_tiempo import DimTiempo
from src.models.analytical.dimensions.dim_cultivo import DimCultivo
from src.models.analytical.facts.fact_beneficio import FactBeneficio
from src.models.operational.operational.persona_base_ops import PersonaBase
from src.models.operational.operational.organizaciones_ops import Organizacion
from src.models.operational.operational.ubicaciones_ops import Ubicacion
from src.models.operational.operational.beneficio_fertilizantes_ops import BeneficioFertilizantes
from src.models.operational.operational.beneficiario_fertilizantes_ops import BeneficiarioFertilizantes


class FertilizantesDimensionalLoader:
    """Carga datos de fertilizantes al modelo dimensional."""
    
    def __init__(self):
        self.stats = {
            'personas_sync': 0,
            'organizaciones_sync': 0,
            'ubicaciones_sync': 0,
            'tiempos_sync': 0,
            'beneficios_loaded': 0
        }
    
    def sync_dim_persona(self, session: Session) -> int:
        """Sincroniza la dimensión persona desde operational."""
        try:
            # Obtener personas que no están en la dimensión
            subquery = session.query(DimPersona.persona_id)
            
            new_personas = session.query(PersonaBase).filter(
                ~PersonaBase.id.in_(subquery)
            ).all()
            
            count = 0
            for persona in new_personas:
                dim_persona = DimPersona(
                    persona_id=persona.id,
                    nombres_apellidos=persona.nombres_apellidos,
                    cedula=persona.cedula[:20] if persona.cedula else None,
                    telefono=persona.telefono[:20] if persona.telefono else None,
                    genero=persona.genero[:20] if persona.genero else None,
                    edad_actual=persona.edad,
                    tipo_productor='BENEFICIARIO',
                    fecha_inicio=persona.created_at.date() if persona.created_at else datetime.now().date(),
                    es_vigente=True
                )
                session.add(dim_persona)
                count += 1
            
            if count > 0:
                session.flush()
                logger.info(f"Sincronizadas {count} nuevas personas")
            
            # Retornar total en dimensión
            total = session.query(func.count(DimPersona.persona_key)).scalar()
            self.stats['personas_sync'] = total
            return total
            
        except Exception as e:
            logger.error(f"Error sincronizando dim_persona: {str(e)}")
            raise
    
    def sync_dim_organizacion(self, session: Session) -> int:
        """Sincroniza la dimensión organización desde operational."""
        try:
            # Obtener organizaciones que no están en la dimensión
            subquery = session.query(DimOrganizacion.organizacion_id)
            
            new_orgs = session.query(Organizacion).filter(
                ~Organizacion.id.in_(subquery)
            ).all()
            
            count = 0
            for org in new_orgs:
                dim_org = DimOrganizacion(
                    organizacion_id=org.id,
                    nombre=org.nombre,
                    tipo=org.tipo_organizacion or 'ASOCIACION',
                    estado=org.estado,
                    fecha_inicio=org.created_at.date() if org.created_at else datetime.now().date(),
                    es_vigente=True
                )
                session.add(dim_org)
                count += 1
            
            if count > 0:
                session.flush()
                logger.info(f"Sincronizadas {count} nuevas organizaciones")
            
            # Retornar total en dimensión
            total = session.query(func.count(DimOrganizacion.organizacion_key)).scalar()
            self.stats['organizaciones_sync'] = total
            return total
            
        except Exception as e:
            logger.error(f"Error sincronizando dim_organizacion: {str(e)}")
            raise
    
    def sync_dim_ubicacion(self, session: Session) -> int:
        """Sincroniza la dimensión ubicación desde operational."""
        try:
            # Obtener ubicaciones que no están en la dimensión
            subquery = session.query(DimUbicacion.ubicacion_id)
            
            new_ubics = session.query(Ubicacion).filter(
                ~Ubicacion.id.in_(subquery)
            ).all()
            
            count = 0
            for ubic in new_ubics:
                # Determinar provincia basada en cantón (simplificado)
                provincia = self._get_provincia_from_canton(ubic.canton)
                
                dim_ubic = DimUbicacion(
                    ubicacion_id=ubic.id,
                    provincia=provincia,
                    canton=ubic.canton,
                    parroquia=ubic.parroquia or 'NO ESPECIFICADO',
                    comunidad=ubic.localidad,
                    zona=self._get_zona_geografica(provincia),
                    region=self._get_zona_geografica(provincia),
                    latitud=float(ubic.coordenada_y) if ubic.coordenada_y else None,
                    longitud=float(ubic.coordenada_x) if ubic.coordenada_x else None,
                    fecha_inicio=ubic.created_at.date() if ubic.created_at else datetime.now().date(),
                    es_vigente=True
                )
                session.add(dim_ubic)
                count += 1
            
            if count > 0:
                session.flush()
                logger.info(f"Sincronizadas {count} nuevas ubicaciones")
            
            # Retornar total en dimensión
            total = session.query(func.count(DimUbicacion.ubicacion_key)).scalar()
            self.stats['ubicaciones_sync'] = total
            return total
            
        except Exception as e:
            logger.error(f"Error sincronizando dim_ubicacion: {str(e)}")
            raise
    
    def sync_dim_tiempo(self, session: Session) -> int:
        """Sincroniza la dimensión tiempo basada en fechas de beneficios."""
        try:
            # Obtener fechas únicas de beneficios que no están en dim_tiempo
            subquery = session.query(DimTiempo.fecha)
            
            unique_dates = session.query(
                func.distinct(func.date(BeneficioFertilizantes.fecha_entrega))
            ).filter(
                BeneficioFertilizantes.fecha_entrega.isnot(None),
                ~func.date(BeneficioFertilizantes.fecha_entrega).in_(subquery)
            ).all()
            
            count = 0
            for (fecha,) in unique_dates:
                if fecha:
                    dim_tiempo = DimTiempo(
                        fecha=fecha,
                        anio=fecha.year,
                        mes=fecha.month,
                        dia=fecha.day,
                        dia_semana=fecha.weekday() + 1,  # 1-7 en lugar de 0-6
                        nombre_dia=self._get_nombre_dia(fecha.weekday()),
                        semana=fecha.isocalendar()[1],
                        semana_anio=fecha.isocalendar()[1],
                        nombre_mes=self._get_nombre_mes(fecha.month),
                        mes_abrev=self._get_nombre_mes(fecha.month)[:3],
                        trimestre=(fecha.month - 1) // 3 + 1,
                        nombre_trimestre=f'Q{(fecha.month - 1) // 3 + 1}',
                        anio_mes=f'{fecha.year}-{fecha.month:02d}',
                        es_fin_semana=fecha.weekday() >= 5
                    )
                    session.add(dim_tiempo)
                    count += 1
            
            if count > 0:
                session.flush()
                logger.info(f"Sincronizadas {count} nuevas fechas")
            
            # Retornar total en dimensión
            total = session.query(func.count(DimTiempo.tiempo_key)).scalar()
            self.stats['tiempos_sync'] = total
            return total
            
        except Exception as e:
            logger.error(f"Error sincronizando dim_tiempo: {str(e)}")
            raise
    
    def get_pending_beneficios_count(self, session: Session) -> int:
        """Obtiene cantidad de beneficios pendientes de cargar a fact."""
        try:
            # Beneficios que no están en fact_beneficio
            subquery = session.query(FactBeneficio.beneficio_id).filter(
                FactBeneficio.tipo_beneficio == 'FERTILIZANTES'
            )
            
            count = session.query(func.count(BeneficioFertilizantes.id)).filter(
                ~BeneficioFertilizantes.id.in_(subquery)
            ).scalar()
            
            return count or 0
            
        except Exception as e:
            logger.error(f"Error obteniendo beneficios pendientes: {str(e)}")
            raise
    
    def load_fact_beneficios_batch(self, session: Session, batch_size: int, offset: int) -> int:
        """Carga un lote de beneficios a la tabla de hechos."""
        try:
            # Obtener beneficios no cargados
            subquery = session.query(FactBeneficio.beneficio_id).filter(
                FactBeneficio.tipo_beneficio == 'FERTILIZANTES'
            )
            
            beneficios = session.query(BeneficioFertilizantes).filter(
                ~BeneficioFertilizantes.id.in_(subquery)
            ).limit(batch_size).all()
            
            if not beneficios:
                return 0
            
            count = 0
            for beneficio in beneficios:
                # Obtener persona_id directamente del beneficio (herencia joined table)
                persona_id = beneficio.persona_id
                
                # Obtener beneficiario para obtener la organización
                beneficiario = session.query(BeneficiarioFertilizantes).filter_by(
                    persona_id=persona_id
                ).first()
                
                # Obtener IDs de dimensiones
                dim_persona_id = self._get_dim_persona_id(session, persona_id)
                dim_org_id = self._get_dim_organizacion_id(session, beneficiario.organizacion_id if beneficiario else None)
                dim_ubic_id = self._get_dim_ubicacion_id(session, beneficio.ubicacion_id)
                dim_tiempo_id = self._get_dim_tiempo_id(session, beneficio.fecha_entrega)
                dim_cultivo_id = self._get_dim_cultivo_id(session, beneficio.tipo_cultivo)
                
                # Solo crear el hecho si tenemos cultivo válido
                if not dim_cultivo_id:
                    logger.warning(f"Tipo cultivo '{beneficio.tipo_cultivo}' no encontrado en dim_cultivo, saltando beneficio {beneficio.id}")
                    continue
                
                # Crear hecho
                fact = FactBeneficio(
                    beneficio_id=beneficio.id,
                    persona_key=dim_persona_id,
                    organizacion_key=dim_org_id or -1,  # -1 para sin organización
                    ubicacion_key=dim_ubic_id,
                    tiempo_key=dim_tiempo_id,
                    cultivo_key=dim_cultivo_id,
                    tipo_beneficio='FERTILIZANTES',
                    categoria_beneficio=beneficio.tipo_cultivo,
                    hectarias_sembradas=float(beneficio.hectarias_beneficiadas or 0),
                    valor_monetario=float(beneficio.costo_total or 0),
                    fecha_beneficio=beneficio.fecha_entrega if beneficio.fecha_entrega else datetime.now().date(),
                    anio=beneficio.fecha_entrega.year if beneficio.fecha_entrega else datetime.now().year,
                    mes=beneficio.fecha_entrega.month if beneficio.fecha_entrega else datetime.now().month,
                    trimestre=(beneficio.fecha_entrega.month - 1) // 3 + 1 if beneficio.fecha_entrega else (datetime.now().month - 1) // 3 + 1
                )
                session.add(fact)
                count += 1
            
            session.flush()
            self.stats['beneficios_loaded'] += count
            logger.debug(f"Cargados {count} beneficios al fact")
            return count
            
        except Exception as e:
            logger.error(f"Error cargando beneficios: {str(e)}")
            raise
    
    # Métodos auxiliares
    def _get_dim_persona_id(self, session: Session, persona_ops_id: int) -> Optional[int]:
        """Obtiene el ID de dimensión persona."""
        if not persona_ops_id:
            return None
        dim = session.query(DimPersona.persona_key).filter_by(persona_id=persona_ops_id).first()
        return dim[0] if dim else None
    
    def _get_dim_organizacion_id(self, session: Session, org_ops_id: int) -> Optional[int]:
        """Obtiene el ID de dimensión organización."""
        if not org_ops_id:
            return None
        dim = session.query(DimOrganizacion.organizacion_key).filter_by(organizacion_id=org_ops_id).first()
        return dim[0] if dim else None
    
    def _get_dim_ubicacion_id(self, session: Session, ubic_ops_id: int) -> Optional[int]:
        """Obtiene el ID de dimensión ubicación."""
        if not ubic_ops_id:
            return None
        dim = session.query(DimUbicacion.ubicacion_key).filter_by(ubicacion_id=ubic_ops_id).first()
        return dim[0] if dim else None
    
    def _get_dim_tiempo_id(self, session: Session, fecha: datetime) -> Optional[int]:
        """Obtiene el ID de dimensión tiempo."""
        if not fecha:
            return None
        # Si fecha ya es date, usarla directamente, si es datetime convertir a date
        fecha_date = fecha if hasattr(fecha, 'year') and not hasattr(fecha, 'hour') else fecha.date()
        dim = session.query(DimTiempo.tiempo_key).filter_by(fecha=fecha_date).first()
        return dim[0] if dim else None
    
    def _get_dim_cultivo_id(self, session: Session, tipo_cultivo: str) -> Optional[int]:
        """Obtiene el ID de dimensión cultivo."""
        if not tipo_cultivo:
            return None
        # Normalizar el tipo de cultivo (mayúsculas)
        tipo_cultivo_upper = tipo_cultivo.upper().strip()
        dim = session.query(DimCultivo.cultivo_key).filter_by(codigo_cultivo=tipo_cultivo_upper).first()
        return dim[0] if dim else None
    
    def _get_provincia_from_canton(self, canton: str) -> str:
        """Determina la provincia basada en el cantón."""
        # Mapeo simplificado de cantones a provincias
        canton_upper = (canton or '').upper()
        
        if canton_upper in ['GUAYAQUIL', 'DURAN', 'SAMBORONDON', 'DAULE', 'MILAGRO', 'NARANJAL', 'BALAO', 'SALITRE']:
            return 'GUAYAS'
        elif canton_upper in ['QUITO', 'CAYAMBE', 'MEJIA', 'RUMIÑAHUI']:
            return 'PICHINCHA'
        elif canton_upper in ['CUENCA', 'GUALACEO', 'PAUTE', 'SIGSIG', 'CHORDELEG', 'GIRON']:
            return 'AZUAY'
        elif canton_upper in ['LOJA', 'CATAMAYO', 'SARAGURO', 'PALTAS']:
            return 'LOJA'
        elif canton_upper in ['IBARRA', 'OTAVALO', 'COTACACHI', 'ANTONIO ANTE']:
            return 'IMBABURA'
        elif canton_upper in ['AMBATO', 'BAÑOS', 'PELILEO', 'PILLARO']:
            return 'TUNGURAHUA'
        elif canton_upper in ['RIOBAMBA', 'GUANO', 'CHAMBO', 'COLTA']:
            return 'CHIMBORAZO'
        elif canton_upper in ['MACHALA', 'PASAJE', 'SANTA ROSA', 'EL GUABO', 'HUAQUILLAS']:
            return 'EL ORO'
        elif canton_upper in ['PORTOVIEJO', 'MANTA', 'MONTECRISTI', 'CHONE']:
            return 'MANABI'
        elif canton_upper in ['ESMERALDAS', 'QUININDE', 'SAN LORENZO']:
            return 'ESMERALDAS'
        elif canton_upper in ['SANTO DOMINGO']:
            return 'SANTO DOMINGO'
        elif canton_upper in ['LATACUNGA', 'SALCEDO', 'PUJILI', 'SAQUISILI']:
            return 'COTOPAXI'
        elif canton_upper in ['BABAHOYO', 'QUEVEDO', 'VENTANAS', 'VINCES']:
            return 'LOS RIOS'
        elif canton_upper in ['GUARANDA', 'CHILLANES', 'SAN MIGUEL']:
            return 'BOLIVAR'
        elif canton_upper in ['TULCAN', 'MONTUFAR', 'ESPEJO']:
            return 'CARCHI'
        elif canton_upper in ['TENA', 'ARCHIDONA']:
            return 'NAPO'
        elif canton_upper in ['PUYO', 'PASTAZA']:
            return 'PASTAZA'
        elif canton_upper in ['MACAS', 'SUCUA', 'GUALAQUIZA']:
            return 'MORONA SANTIAGO'
        elif canton_upper in ['ZAMORA', 'YANTZAZA']:
            return 'ZAMORA CHINCHIPE'
        elif canton_upper in ['PUERTO FRANCISCO DE ORELLANA', 'JOYA DE LOS SACHAS']:
            return 'ORELLANA'
        elif canton_upper in ['NUEVA LOJA', 'SHUSHUFINDI']:
            return 'SUCUMBIOS'
        elif canton_upper in ['SANTA ELENA', 'LA LIBERTAD', 'SALINAS']:
            return 'SANTA ELENA'
        elif canton_upper in ['GALAPAGOS', 'SAN CRISTOBAL', 'ISABELA']:
            return 'GALAPAGOS'
        else:
            return 'NO ESPECIFICADO'
    
    def _get_zona_geografica(self, provincia: str) -> str:
        """Determina la zona geográfica basada en la provincia."""
        costa = ['GUAYAS', 'MANABI', 'ESMERALDAS', 'EL ORO', 'LOS RIOS', 'SANTA ELENA']
        sierra = ['PICHINCHA', 'AZUAY', 'LOJA', 'IMBABURA', 'TUNGURAHUA', 'CHIMBORAZO', 
                  'COTOPAXI', 'BOLIVAR', 'CARCHI', 'CAÑAR']
        oriente = ['NAPO', 'PASTAZA', 'MORONA SANTIAGO', 'ZAMORA CHINCHIPE', 'ORELLANA', 'SUCUMBIOS']
        
        if provincia in costa:
            return 'COSTA'
        elif provincia in sierra:
            return 'SIERRA'
        elif provincia in oriente:
            return 'ORIENTE'
        elif provincia == 'GALAPAGOS':
            return 'INSULAR'
        else:
            return 'NO ESPECIFICADO'
    
    def _get_nombre_mes(self, mes: int) -> str:
        """Retorna el nombre del mes."""
        meses = {
            1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril',
            5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto',
            9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
        }
        return meses.get(mes, 'Unknown')
    
    def _get_nombre_dia(self, dia: int) -> str:
        """Retorna el nombre del día de la semana."""
        dias = {
            0: 'Lunes', 1: 'Martes', 2: 'Miércoles', 3: 'Jueves',
            4: 'Viernes', 5: 'Sábado', 6: 'Domingo'
        }
        return dias.get(dia, 'Unknown')