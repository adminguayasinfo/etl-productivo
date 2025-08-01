"""Debug normalizer output."""
import pandas as pd
from src.transform.normalizers.semillas_normalizer import SemillasNormalizer
from src.transform.cleaners.semillas_cleaner import SemillasCleaner
from src.transform.cleaners.data_standardizer import DataStandardizer
from src.transform.validators.semillas_validator import SemillasValidator

# Load sample data
df = pd.read_csv('data/raw/productivo/agricola/semillas.csv', nrows=100)

# Apply transformation chain
cleaner = SemillasCleaner()
df_clean = cleaner.clean(df)

standardizer = DataStandardizer()
df_std = standardizer.standardize(df_clean)

validator = SemillasValidator()
df_valid = validator.validate(df_std)

normalizer = SemillasNormalizer()
entities = normalizer.normalize(df_valid)

# Check personas
print("\n=== PERSONAS ===")
if 'personas' in entities and not entities['personas'].empty:
    print(f"Total personas: {len(entities['personas'])}")
    print("\nPrimeras 5 personas:")
    print(entities['personas'][['nombres_apellidos', 'cedula']].head())
    
    # Check for null names
    null_names = entities['personas']['nombres_apellidos'].isna()
    print(f"\nPersonas con nombres nulos: {null_names.sum()}")
    
    if null_names.sum() > 0:
        print("\nPersonas con nombres nulos:")
        print(entities['personas'][null_names])
else:
    print("No hay personas en el resultado")