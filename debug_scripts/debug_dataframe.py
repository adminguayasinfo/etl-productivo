"""Debug DataFrame issue in personas loader."""
import pandas as pd
from config.connections.database import DatabaseConnection
from src.models.operational.staging.semillas_stg_model import StgSemilla
from src.transform.semillas_transformer_batch import SemillasTransformerBatch

# Get first batch
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
    
    personas_df = entities['personas']
    print(f"Personas DataFrame shape: {personas_df.shape}")
    print(f"Columns: {personas_df.columns.tolist()}")
    print(f"\nDataFrame info:")
    print(personas_df.info())
    
    print(f"\nFirst 3 rows:")
    print(personas_df.head(3))
    
    print(f"\nIterating over rows:")
    for idx, row in personas_df.iterrows():
        print(f"\nRow {idx}:")
        print(f"  Type of row: {type(row)}")
        print(f"  nombres_apellidos value: '{row['nombres_apellidos']}'")
        print(f"  nombres_apellidos type: {type(row['nombres_apellidos'])}")
        print(f"  row.get('nombres_apellidos'): '{row.get('nombres_apellidos')}'")
        
        # Test different access methods
        nombres1 = row['nombres_apellidos']
        nombres2 = row.get('nombres_apellidos')
        nombres3 = getattr(row, 'nombres_apellidos', None)
        
        print(f"  Access methods: []: '{nombres1}', get(): '{nombres2}', getattr: '{nombres3}'")