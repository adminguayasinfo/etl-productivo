"""Test simple insert without batch complexity."""
import pandas as pd
from src.transform.semillas_transformer_batch import SemillasTransformerBatch
from config.connections.database import DatabaseConnection
from src.models.operational.staging.semillas_stg_model import StgSemilla

# Get first 5 records and transform them
db = DatabaseConnection()
with db.get_session() as session:
    records = session.query(StgSemilla).filter(
        StgSemilla.processed == False
    ).order_by(StgSemilla.id).limit(5).all()
    
    # Convert to DataFrame
    data = []
    for record in records:
        row_dict = {col.name: getattr(record, col.name) 
                   for col in record.__table__.columns}
        data.append(row_dict)
        
    df = pd.DataFrame(data)
    
    # Transform
    transformer = SemillasTransformerBatch(batch_size=5)
    entities = transformer._transform_batch(df)
    
    print(f"Persons to insert: {len(entities['personas'])}")
    print(f"Names: {entities['personas']['nombres_apellidos'].tolist()}")
    
    # Try simple load
    try:
        # Clear mappings first
        transformer.operational_loader.persona_id_map.clear()
        transformer.operational_loader.ubicacion_id_map.clear()
        transformer.operational_loader.organizacion_id_map.clear()
        
        # Load
        stats = transformer.operational_loader.load_batch(entities, session)
        session.commit()
        print("Success!")
        print(f"Stats: {stats}")
    except Exception as e:
        print(f"Error: {e}")
        session.rollback()