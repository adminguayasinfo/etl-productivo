import pandas as pd

# Leer CSV directamente
df = pd.read_csv('data/raw/productivo/agricola/semillas.csv')
print(f'Total registros: {len(df)}')

# Ver registros con nombres vacíos
mask = df['nombres_apellidos'].isna() | (df['nombres_apellidos'].str.strip() == '')
print(f'Registros sin nombres válidos: {mask.sum()}')

# Ver algunos ejemplos
if mask.sum() > 0:
    print('\nEjemplos:')
    ejemplos = df[mask][['numero_acta', 'nombres_apellidos', 'organizacion', 'cedula']].head(10)
    print(ejemplos)