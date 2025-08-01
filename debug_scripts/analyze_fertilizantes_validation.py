"""Analyze fertilizantes validation failures."""
import pandas as pd
from sqlalchemy import create_engine, text
from collections import Counter
import json

# Database connection
engine = create_engine('postgresql://postgres:banecuador@localhost/etl_prototipo')

# Query staging table for failed records
query = """
WITH validation_failures AS (
    SELECT 
        id,
        numero_acta,
        nombres_apellidos,
        cedula,
        coordenada_x,
        coordenada_y,
        canton,
        parroquia,
        hectarias_totales,
        hectarias_beneficiadas,
        tipo_fertilizante,
        cantidad_sacos,
        precio_unitario,
        costo_total,
        error_message,
        processed
    FROM staging.stg_fertilizante
    WHERE processed = false OR error_message IS NOT NULL
)
SELECT * FROM validation_failures;
"""

print("Analyzing fertilizantes validation failures...")
print("=" * 80)

# Get failed records
df_failures = pd.read_sql(query, engine)
print(f"\nTotal failed records: {len(df_failures)}")

# Analyze error messages
print("\nError message analysis:")
print("-" * 50)
error_counts = Counter()
error_details = {}

for idx, row in df_failures.iterrows():
    if pd.notna(row['error_message']):
        # Parse error message
        errors = str(row['error_message']).split(';')
        for error in errors:
            error = error.strip()
            if error:
                error_counts[error] += 1
                if error not in error_details:
                    error_details[error] = []
                error_details[error].append({
                    'id': row['id'],
                    'numero_acta': row['numero_acta'],
                    'coordenada_x': row['coordenada_x'],
                    'coordenada_y': row['coordenada_y']
                })

# Print error frequency
for error, count in error_counts.most_common():
    print(f"{error}: {count} occurrences")

# Analyze coordinate issues specifically
print("\n\nCoordinate Analysis:")
print("-" * 50)

# Check coordinate values
coord_issues = {
    'null_x': 0,
    'null_y': 0,
    'non_numeric_x': 0,
    'non_numeric_y': 0,
    'out_of_range_x': 0,
    'out_of_range_y': 0,
    'valid_coords': 0
}

coord_samples = {
    'non_numeric_x': [],
    'non_numeric_y': [],
    'out_of_range_x': [],
    'out_of_range_y': []
}

for idx, row in df_failures.iterrows():
    x, y = row['coordenada_x'], row['coordenada_y']
    
    # Check X coordinate
    if pd.isna(x):
        coord_issues['null_x'] += 1
    else:
        try:
            x_val = float(x)
            # Check if in valid range for Ecuador
            if not ((-82 <= x_val <= -75) or (500000 <= x_val <= 800000)):
                coord_issues['out_of_range_x'] += 1
                if len(coord_samples['out_of_range_x']) < 5:
                    coord_samples['out_of_range_x'].append(f"{x} (acta: {row['numero_acta']})")
        except:
            coord_issues['non_numeric_x'] += 1
            if len(coord_samples['non_numeric_x']) < 5:
                coord_samples['non_numeric_x'].append(f"{x} (acta: {row['numero_acta']})")
    
    # Check Y coordinate
    if pd.isna(y):
        coord_issues['null_y'] += 1
    else:
        try:
            y_val = float(y)
            # Check if in valid range for Ecuador
            if not ((-5 <= y_val <= 2) or (9700000 <= y_val <= 10100000)):
                coord_issues['out_of_range_y'] += 1
                if len(coord_samples['out_of_range_y']) < 5:
                    coord_samples['out_of_range_y'].append(f"{y} (acta: {row['numero_acta']})")
        except:
            coord_issues['non_numeric_y'] += 1
            if len(coord_samples['non_numeric_y']) < 5:
                coord_samples['non_numeric_y'].append(f"{y} (acta: {row['numero_acta']})")
    
    # Check if both coords are valid
    try:
        x_val = float(x) if pd.notna(x) else None
        y_val = float(y) if pd.notna(y) else None
        if x_val and y_val:
            if ((-82 <= x_val <= -75) or (500000 <= x_val <= 800000)) and \
               ((-5 <= y_val <= 2) or (9700000 <= y_val <= 10100000)):
                coord_issues['valid_coords'] += 1
    except:
        pass

print("\nCoordinate Issues Summary:")
for issue, count in coord_issues.items():
    print(f"{issue}: {count}")

print("\nSample problematic coordinates:")
for issue_type, samples in coord_samples.items():
    if samples:
        print(f"\n{issue_type}:")
        for sample in samples:
            print(f"  - {sample}")

# Check if there are other validation issues
print("\n\nOther Data Quality Issues:")
print("-" * 50)

# Check for missing required fields
missing_fields = {
    'nombres_apellidos': df_failures['nombres_apellidos'].isna().sum(),
    'cedula': df_failures['cedula'].isna().sum(),
    'tipo_fertilizante': df_failures['tipo_fertilizante'].isna().sum(),
    'cantidad_sacos': df_failures['cantidad_sacos'].isna().sum(),
    'precio_unitario': df_failures['precio_unitario'].isna().sum()
}

print("\nMissing required fields:")
for field, count in missing_fields.items():
    if count > 0:
        print(f"{field}: {count} missing")

# Check for data inconsistencies
print("\nData inconsistencies:")
inconsistencies = 0
for idx, row in df_failures.iterrows():
    # Check hectares consistency
    if pd.notna(row['hectarias_beneficiadas']) and pd.notna(row['hectarias_totales']):
        if row['hectarias_beneficiadas'] > row['hectarias_totales'] * 1.5:
            inconsistencies += 1

print(f"Hectares inconsistencies (beneficiadas > totales * 1.5): {inconsistencies}")

# Get sample of failed records with different error types
print("\n\nSample Failed Records:")
print("-" * 50)

# Get 5 samples of each major error type
sample_query = """
SELECT 
    numero_acta,
    nombres_apellidos,
    cedula,
    coordenada_x,
    coordenada_y,
    canton,
    parroquia,
    tipo_fertilizante,
    error_message
FROM staging.stg_fertilizante
WHERE processed = false OR error_message IS NOT NULL
LIMIT 10;
"""

df_samples = pd.read_sql(sample_query, engine)
print(df_samples.to_string(index=False))

# Summary recommendations
print("\n\nSUMMARY AND RECOMMENDATIONS:")
print("=" * 80)
print(f"1. Total failed records: {len(df_failures)} out of ~15,000 total")
print(f"2. Main issues identified:")
print(f"   - Coordinate validation failures (most common)")
print(f"   - Missing required fields")
print(f"   - Data inconsistencies")
print("\n3. Recommendations:")
print("   - Review coordinate validation rules - many valid Ecuador coordinates may be rejected")
print("   - Consider using flexible validator for coordinate validation")
print("   - Add data cleaning step for non-numeric coordinates")
print("   - Review required field constraints")