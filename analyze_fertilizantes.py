"""
Análisis de datos de staging de fertilizantes.
"""
from config.connections.database import db_connection
from sqlalchemy import text

def main():
    with db_connection.get_session() as session:
        print('=== ANÁLISIS DETALLADO DE STAGING FERTILIZANTES ===')
        
        # Análisis de beneficiarios únicos
        benef_query = text("""
            SELECT COUNT(DISTINCT cedula) as cedulas_unicas,
                   COUNT(DISTINCT nombres_apellidos) as nombres_unicos,
                   COUNT(*) as total_registros
            FROM "etl-productivo".stg_fertilizante 
            WHERE cedula IS NOT NULL AND cedula <> 'None'
        """)
        result = session.execute(benef_query)
        benef_data = result.fetchone()
        print(f'Cédulas únicas: {benef_data[0]}')
        print(f'Nombres únicos: {benef_data[1]}')
        print(f'Total registros con cédula: {benef_data[2]}')
        
        # Análisis de ubicaciones únicas
        ubic_query = text("""
            SELECT COUNT(DISTINCT CONCAT(COALESCE(canton, ''), '-', COALESCE(parroquia, ''), '-', COALESCE(recinto, ''))) as ubicaciones_unicas,
                   COUNT(DISTINCT canton) as cantones,
                   COUNT(DISTINCT parroquia) as parroquias,
                   COUNT(DISTINCT recinto) as recintos
            FROM "etl-productivo".stg_fertilizante 
        """)
        result = session.execute(ubic_query)
        ubic_data = result.fetchone()
        print(f'Ubicaciones únicas: {ubic_data[0]}')
        print(f'Cantones únicos: {ubic_data[1]}')
        print(f'Parroquias únicas: {ubic_data[2]}')
        print(f'Recintos únicos: {ubic_data[3]}')
        
        # Análisis de asociaciones
        asoc_query = text("""
            SELECT COUNT(DISTINCT asociaciones) as asociaciones_unicas,
                   COUNT(*) as total_registros
            FROM "etl-productivo".stg_fertilizante 
            WHERE asociaciones IS NOT NULL
        """)
        result = session.execute(asoc_query)
        asoc_data = result.fetchone()
        print(f'Asociaciones únicas: {asoc_data[0]} de {asoc_data[1]} registros')
        
        # Análisis de tipos de cultivo
        cultivo_query = text("""
            SELECT cultivo, COUNT(*) as cantidad
            FROM "etl-productivo".stg_fertilizante 
            WHERE cultivo IS NOT NULL
            GROUP BY cultivo
            ORDER BY cantidad DESC
        """)
        result = session.execute(cultivo_query)
        cultivos = result.fetchall()
        print(f'\nTipos de cultivo únicos: {len(cultivos)}')
        for cultivo, cantidad in cultivos:
            print(f'  {cultivo}: {cantidad} registros')
        
        # Análisis de fertilizantes
        print('\n=== ANÁLISIS DE FERTILIZANTES ===')
        fert_query = text("""
            SELECT 
                AVG(fertilizante_nitrogenado) as avg_nitrogenado,
                AVG(npk_elementos_menores) as avg_npk,
                AVG(organico_foliar) as avg_organico,
                MAX(fertilizante_nitrogenado) as max_nitrogenado,
                MAX(npk_elementos_menores) as max_npk,
                MAX(organico_foliar) as max_organico
            FROM "etl-productivo".stg_fertilizante 
        """)
        result = session.execute(fert_query)
        fert_data = result.fetchone()
        print(f'Fertilizante Nitrogenado - Promedio: {fert_data[0]:.2f}, Máximo: {fert_data[3]}')
        print(f'NPK + Elementos Menores - Promedio: {fert_data[1]:.2f}, Máximo: {fert_data[4]}')
        print(f'Orgánico Foliar - Promedio: {fert_data[2]:.2f}, Máximo: {fert_data[5]}')
        
        # Análisis de precios
        precio_query = text("""
            SELECT 
                AVG(precio_kit) as precio_promedio,
                MIN(precio_kit) as precio_min,
                MAX(precio_kit) as precio_max,
                COUNT(DISTINCT precio_kit) as precios_unicos
            FROM "etl-productivo".stg_fertilizante 
            WHERE precio_kit IS NOT NULL
        """)
        result = session.execute(precio_query)
        precio_data = result.fetchone()
        print(f'\nPrecios - Promedio: ${precio_data[0]:.2f}, Min: ${precio_data[1]:.2f}, Max: ${precio_data[2]:.2f}')
        print(f'Precios únicos: {precio_data[3]}')
        
        print('\n=== COMPARACIÓN CON MODELO OPERATIONAL ===')
        print('El modelo operational actual tiene las siguientes entidades:')
        print('- Direccion (provincia, canton, parroquia, recinto, coord_x, coord_y)')
        print('- Asociacion (nombre)')
        print('- TipoCultivo (nombre)')
        print('- Beneficiario (cedula, nombres_completos, telefono, genero, fecha_nacimiento, direccion)')
        print('- Beneficio (fecha_entrega, tipo_beneficio, monto, beneficiario)')
        print('- BeneficioSemillas (responsable_agripac, cedula_responsable, lugar_entrega, variedad, etc.)')
        
        print('\n=== NECESIDADES PARA FERTILIZANTES ===')
        print('Los datos de fertilizantes requieren:')
        print('✓ Direccion - Compatible (canton, parroquia, recinto, coord_x, coord_y)')
        print('✓ Asociacion - Compatible (asociaciones)')
        print('✓ TipoCultivo - Compatible (cultivo)')
        print('✓ Beneficiario - Compatible (cedula, nombres_apellidos, telefono, genero, edad)')
        print('✓ Beneficio - Compatible (fecha_entrega, precio_kit como monto)')
        print('? BeneficioFertilizantes - NUEVO SUBTIPO NECESARIO')
        
        print('\nCampos específicos de fertilizantes que requieren nuevo subtipo:')
        print('- fertilizante_nitrogenado (Integer)')
        print('- npk_elementos_menores (Integer)')
        print('- organico_foliar (Integer)')
        print('- hectareas (Decimal)')
        print('- lugar_entrega (String)')
        print('- observacion (Text)')

if __name__ == "__main__":
    main()