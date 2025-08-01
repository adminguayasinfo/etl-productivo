# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an ETL (Extract, Transform, Load) system for processing agricultural benefit data in Ecuador. The system handles four main benefit programs:
- **Semillas** (Seeds) - Agricultural seed distribution
- **Fertilizantes** (Fertilizers) - Fertilizer distribution
- **Plantas** (Plants) - Plant material distribution
- **Mecanización** (Mechanization) - Agricultural machinery and equipment support

The system also includes a FastAPI REST API for cost analysis and a comprehensive cost matrix analysis for different crops (rice, cacao, corn, musaceae).

## Architecture

The system follows a 3-layer data warehouse architecture within a single schema:

```
CSV Files → Staging Layer → Operational Layer → Analytical Layer
```

All tables are now located in the **`etl-productivo`** schema:

1. **Staging Layer** (`stg_*` tables) - Raw data from CSV files
2. **Operational Layer** (`*_base`, `beneficio_*`, `beneficiario_*` tables) - Normalized, clean operational data  
3. **Analytical Layer** (`dim_*`, `fact_*` tables) - Dimensional model for reporting

## Common Commands

### Environment Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables (required for database connection)
export OLTP_DB_HOST=localhost
export OLTP_DB_PORT=5432
export OLTP_DB_NAME=your_db_name
export OLTP_DB_USER=your_user
export OLTP_DB_PASSWORD=your_password
```

### Database Setup
```bash
# Initialize database (creates all schemas and tables)
python init_database.py

# Clean all data (keeps structure)
python scripts/clean_database.py

# Recreate schema (drops and recreates etl-productivo schema)
python scripts/recreate_schemas.py

# Create logs directory (required for ETL execution)
mkdir -p logs
```

### Running ETL Pipelines

#### Semillas Pipeline
```bash
# Run all stages
python scripts/run_semillas.py --stage all

# Run specific stage
python scripts/run_semillas.py --stage staging
python scripts/run_semillas.py --stage operational
python scripts/run_semillas.py --stage analytical

# With custom parameters
python scripts/run_semillas.py --stage all --batch-size 5000 --csv-path data/raw/custom.csv
```

#### Fertilizantes Pipeline
```bash
# Run staging only (default)
python scripts/run_fertilizantes.py

# Run all stages
python scripts/run_fertilizantes.py --stage all

# Initialize DB and run
python scripts/run_fertilizantes.py --stage all --init-db
```

#### Plantas Pipeline
```bash
# Run operational pipeline
python scripts/run_plantas.py --batch-size 100

# Run in dry-run mode (no data changes)
python scripts/run_plantas.py --batch-size 100 --dry-run
```

#### Mecanización Pipeline
```bash
# Run operational pipeline
python scripts/run_mecanizacion.py --batch-size 100

# Run in dry-run mode
python scripts/run_mecanizacion.py --batch-size 100 --dry-run
```

#### Complete ETL Process
```bash
# Run complete ETL for all benefit types (15-20 minutes)
bash run_full_etl.sh

# Individual staging loads
python scripts/load_all_staging.py
```

### API Development
```bash
# Start the FastAPI server (loads .env automatically)
python start_api.py

# Alternative: Use shell script to manually load environment
bash start_api_with_env.sh

# Server will be available at http://localhost:8000
# API documentation at http://localhost:8000/docs
# Cost analysis endpoint: http://localhost:8000/prod/analisis-costos-arroz
```

### Testing and Debugging
```bash
# Test database connection
python config/connections/test_connection.py

# Run tests (if available)
pytest

# Verify system integrity
python scripts/verify_system.py

# Debug specific issues
python debug_scripts/analyze_validation_failures.py
python debug_scripts/check_data.py
python debug_scripts/final_etl_summary.py
python debug_scripts/financial_statistics.py

# Check analytical layer metrics
python debug_scripts/check_analytical_metrics.py
```

## Key Implementation Details

### Database Models
- Uses SQLAlchemy 2.0 with PostgreSQL
- All tables are in the single `etl-productivo` schema
- Implements joined table inheritance for benefit types in operational layer
- Two operational model architectures:
  - `operational/` - Legacy models for staging tables
  - `operational_refactored/` - New normalized structure with proper relationships
- Models organized by layer: `staging/`, `operational_refactored/`, `analytical/`
- Table naming conventions: `stg_*` (staging), `dim_*`/`fact_*` (analytical), others (operational)
- Core entities: Beneficiario, Direccion, Asociacion, TipoCultivo, Beneficio (with subtypes)

### ETL Components
- **Extractors** (`src/extract/`) - Read CSV files and staging data
- **Transformers** (`src/transform/`) - Clean, validate, normalize, and enrich data
- **Loaders** (`src/load/`) - Load data into appropriate database layer
- **Pipelines** (`src/pipelines/`) - Orchestrate the ETL process

### Data Processing Features
- Batch processing with configurable batch sizes (default: 10,000)
- Error tracking and failed record management
- Data validation with detailed error reporting
- Automatic enrichment of crop data with botanical information
- Progress tracking via `procesado` flags in staging tables

### Important Patterns
1. **Idempotent Operations** - Pipelines can be safely re-run
2. **Entity Normalization** - Data is split into personas, ubicaciones, organizaciones
3. **Dimensional Modeling** - Star schema with fact_beneficio at center
4. **Error Handling** - Failed records saved to `data/failed/` for analysis

### Configuration
- Database connection via environment variables (OLTP_DB_HOST, OLTP_DB_PORT, OLTP_DB_NAME, OLTP_DB_USER, OLTP_DB_PASSWORD)
- Crop metadata in `config/catalogs/cultivos.json`
- Logging configured with loguru to write to `logs/` directory
- Default batch sizes: 10,000 for semillas, 1,000 for fertilizantes, 100 for plantas/mecanización
- Excel data source: `data/raw/BASE PROYECTOS DESARROLLO PRODUCTIVO.xlsx`

### Additional Components
- **FastAPI REST API** (`api/`) - Cost analysis endpoints for agricultural production
- **Cost Analysis** (`src/matriz_costos/`) - Crop-specific cost calculations with visual charts
- **Orchestration** (`run_full_etl.sh`) - Complete ETL automation script with parallel processing
- **Analysis Tools** - Multiple scripts for data exploration and financial analysis

## Development Notes

When modifying the ETL pipeline:
1. Always maintain the 3-layer architecture
2. Ensure batch processing is used for large datasets
3. Add appropriate indexes for performance
4. Use the existing validation framework
5. Follow the established naming conventions (stg_, dim_, fact_)
6. Test with small batches first using --batch-size parameter
7. For new benefit types, extend the operational_refactored models using joined table inheritance
8. Use `run_full_etl.sh` for complete testing of changes

When debugging:
- Check `debug_scripts/` for extensive analysis tools (>20 debugging scripts available)
- Use `analytics_queries.sql` for data verification
- Review logs in `logs/` directory with timestamped files
- Failed records are in `data/failed/`
- Use `scripts/verify_system.py` for comprehensive system checks
- Financial analysis tools available in `debug_scripts/financial_statistics*.py`

When working with the API:
- API follows RESTful conventions with FastAPI
- Cost analysis focuses on rice production but extensible to other crops
- API requires all database environment variables to be set
- Use `start_api.py` for proper server initialization with checks
- If environment variables aren't loading, use `bash start_api_with_env.sh`
- Make sure the virtual environment is activated before running API commands
- Common dependencies needed: fastapi==0.104.1, uvicorn==0.24.0, httptools, h11<0.15

## Project Structure

```
etl-productivo/
├── src/                           # Core ETL components
│   ├── extract/                  # CSV/Excel extractors and staging readers
│   │   ├── semillas_*.py        # Semillas extraction modules
│   │   ├── fertilizantes_*.py   # Fertilizantes extraction modules
│   │   ├── plantas_*.py         # Plantas extraction modules
│   │   └── mecanizacion_*.py    # Mecanización extraction modules
│   ├── transform/                # Data transformation layers
│   │   ├── cleaners/            # Data cleaning and standardization
│   │   ├── validators/          # Data validation rules
│   │   ├── normalizers/         # Entity normalization
│   │   └── enrichers/           # Data enrichment (crop metadata)
│   ├── load/                    # Layer-specific data loaders
│   ├── pipelines/               # ETL orchestration by layer
│   │   ├── staging/            # Staging pipeline orchestrators
│   │   ├── operational_refactored/ # Operational pipeline orchestrators
│   │   └── analytical/         # Analytical pipeline orchestrators
│   ├── models/                  # SQLAlchemy models organized by layer
│   │   ├── operational/staging/ # Staging table models
│   │   ├── operational_refactored/ # Normalized operational models
│   │   └── analytical/         # Dimensional models (dims, facts)
│   ├── matriz_costos/          # Crop cost analysis modules
│   └── utils/                  # Shared utilities
├── api/                        # FastAPI REST API
├── scripts/                    # Main execution scripts (run_*.py)
├── config/                     # Database connections and catalogs
├── data/                       # Input Excel files and failed records
├── logs/                       # ETL execution logs (timestamped)
├── debug_scripts/              # Extensive debugging and analysis tools
├── run_full_etl.sh            # Complete ETL automation script
└── start_api.py               # API server startup script
```

## Data Volume and Performance
- **Expected Data Volume**: ~34,000 staging records → ~27,000 operational benefits
- **Processing Time**: Complete ETL takes 15-20 minutes
- **Success Rate**: Target >80% record processing success
- **Parallel Processing**: Plants and Mechanization pipelines run concurrently
- **Monitoring**: Comprehensive progress tracking and error reporting