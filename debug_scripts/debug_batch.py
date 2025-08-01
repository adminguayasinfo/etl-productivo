"""Debug batch processing issue."""
import pandas as pd
from config.connections.database import DatabaseConnection
from src.models.operational.staging.semillas_stg_model import StgSemilla
from src.transform.semillas_transformer_batch import SemillasTransformerBatch

# Initialize database
db = DatabaseConnection()
with db.get_session() as session:
    # Get a small batch from staging
    query = session.query(StgSemilla).filter(
        StgSemilla.processed == False
    ).order_by(StgSemilla.id).limit(10)

    records = query.all()
    print(f"Records from staging: {len(records)}")

    # Convert to DataFrame
    data = []
    for record in records:
        row_dict = {col.name: getattr(record, col.name) 
                   for col in record.__table__.columns}
        data.append(row_dict)
        
    df = pd.DataFrame(data)
    print(f"\nDataFrame shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")

    # Check nombres_apellidos
    print(f"\nNombres_apellidos null count: {df['nombres_apellidos'].isna().sum()}")
    print(f"Nombres_apellidos empty count: {(df['nombres_apellidos'] == '').sum()}")

    # Show first few records
    print("\nFirst 5 records nombres_apellidos:")
    print(df[['id', 'nombres_apellidos', 'cedula']].head())

    # Now let's transform
    transformer = SemillasTransformerBatch(batch_size=10)
    entities = transformer._transform_batch(df)

    print("\n=== After transformation ===")
    for entity_name, entity_df in entities.items():
        if isinstance(entity_df, pd.DataFrame) and not entity_df.empty:
            print(f"\n{entity_name}: {len(entity_df)} records")
            if entity_name == 'personas':
                print(f"Personas con nombres nulos: {entity_df['nombres_apellidos'].isna().sum()}")
                if entity_df['nombres_apellidos'].isna().sum() > 0:
                    print("Personas con nombres nulos:")
                    print(entity_df[entity_df['nombres_apellidos'].isna()])