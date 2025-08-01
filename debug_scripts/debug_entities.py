"""Debug entities from normalizer."""
import pandas as pd
from src.transform.semillas_transformer_batch import SemillasTransformerBatch
from config.connections.database import DatabaseConnection
from src.models.operational.staging.semillas_stg_model import StgSemilla

# Get first 5 records from staging
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
    print(f"Input batch: {len(df)} records")
    print(f"nombres_apellidos values: {df['nombres_apellidos'].tolist()}")
    
    # Transform
    transformer = SemillasTransformerBatch(batch_size=5)
    entities = transformer._transform_batch(df)
    
    print(f"\n=== ENTITIES OUTPUT ===")
    for name, entity_df in entities.items():
        if isinstance(entity_df, pd.DataFrame) and not entity_df.empty:
            print(f"\n{name}: {len(entity_df)} records")
            if name == 'personas':
                print(f"Column names: {entity_df.columns.tolist()}")
                print(f"nombres_apellidos values: {entity_df['nombres_apellidos'].tolist()}")
                print(f"NULL count: {entity_df['nombres_apellidos'].isna().sum()}")
                if entity_df['nombres_apellidos'].isna().any():
                    print("NULL ENTRIES FOUND!")
                    print(entity_df[entity_df['nombres_apellidos'].isna()])