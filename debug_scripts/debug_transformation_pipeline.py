#!/usr/bin/env python3
"""
Debug script to trace transformation pipeline failures.
Runs a small batch of pending records through each transformation step.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
import pandas as pd
from datetime import datetime
import traceback

from config.connections.database import get_db_connection
from src.models.operational.staging.semillas_stg_model import SemillasSTG
from src.transform.semillas_transformer import SemillasTransformer
from src.transform.validators.data_validator import DataValidator
from src.transform.normalizers.semillas_normalizer import SemillasNormalizer
from src.transform.cleaners.semillas_cleaner import SemillasCleaner
from src.load.semillas_ops_load_core import SemillasOperationalLoader

def get_pending_records(session, limit=100):
    """Get a sample of pending records from staging"""
    query = text("""
        SELECT * FROM public.semillas_stg 
        WHERE processed = false 
        LIMIT :limit
    """)
    
    result = session.execute(query, {'limit': limit})
    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    print(f"\nFound {len(df)} pending records in staging")
    return df

def debug_transformation_step(step_name, func, data, *args, **kwargs):
    """Run a transformation step and capture results/errors"""
    print(f"\n{'='*60}")
    print(f"STEP: {step_name}")
    print(f"{'='*60}")
    
    try:
        result = func(data, *args, **kwargs)
        
        if isinstance(result, tuple) and len(result) == 2:
            processed, errors = result
            print(f"✓ Success: {len(processed)} records processed")
            if errors:
                print(f"✗ Errors: {len(errors)} records failed")
                for i, error in enumerate(errors[:5]):  # Show first 5 errors
                    print(f"\n  Error {i+1}:")
                    print(f"    Record ID: {error.get('id', 'N/A')}")
                    print(f"    Error: {error.get('error', 'N/A')}")
                    if 'record' in error:
                        print(f"    Record data: {error['record']}")
            return processed, errors
        else:
            print(f"✓ Success: Returned {type(result).__name__}")
            return result, []
            
    except Exception as e:
        print(f"✗ EXCEPTION in {step_name}:")
        print(f"  Type: {type(e).__name__}")
        print(f"  Message: {str(e)}")
        print(f"  Traceback:\n{traceback.format_exc()}")
        return None, [{'error': str(e), 'traceback': traceback.format_exc()}]

def analyze_validation_errors(errors):
    """Analyze and categorize validation errors"""
    error_categories = {}
    
    for error in errors:
        error_msg = str(error.get('error', '')).lower()
        
        # Categorize errors
        if 'coordinate' in error_msg or 'latitud' in error_msg or 'longitud' in error_msg:
            category = 'Coordinate Validation'
        elif 'cedula' in error_msg or 'cédula' in error_msg:
            category = 'Cédula Validation'
        elif 'fecha' in error_msg or 'date' in error_msg:
            category = 'Date Validation'
        elif 'missing' in error_msg or 'required' in error_msg:
            category = 'Missing Required Fields'
        elif 'format' in error_msg or 'tipo' in error_msg:
            category = 'Format/Type Validation'
        else:
            category = 'Other Validation'
        
        if category not in error_categories:
            error_categories[category] = []
        error_categories[category].append(error)
    
    return error_categories

def main():
    print("="*80)
    print("TRANSFORMATION PIPELINE DEBUG SCRIPT")
    print("="*80)
    print(f"Started at: {datetime.now()}")
    
    # Initialize components
    engine = create_engine(get_db_connection())
    session = Session(engine)
    
    try:
        # Get pending records
        df = get_pending_records(session, limit=100)
        
        if df.empty:
            print("\nNo pending records found in staging!")
            return
        
        # Show sample of data
        print("\nSample of pending records:")
        print(df[['id', 'cedula', 'nombres', 'apellidos', 'organizacion_sigla']].head())
        
        # Initialize transformation components
        transformer = SemillasTransformer()
        validator = DataValidator()
        normalizer = SemillasNormalizer()
        cleaner = SemillasCleaner()
        
        # Track results through pipeline
        all_errors = []
        
        # Step 1: Clean data
        print("\n" + "="*80)
        print("RUNNING TRANSFORMATION PIPELINE")
        print("="*80)
        
        cleaned_df, clean_errors = debug_transformation_step(
            "Data Cleaning",
            cleaner.clean,
            df
        )
        all_errors.extend(clean_errors)
        
        if cleaned_df is None or cleaned_df.empty:
            print("\n❌ Pipeline stopped: No records survived cleaning")
            return
        
        # Step 2: Validate data
        validated_df, validation_errors = debug_transformation_step(
            "Data Validation",
            validator.validate,
            cleaned_df
        )
        all_errors.extend(validation_errors)
        
        if validated_df is None or validated_df.empty:
            print("\n❌ Pipeline stopped: No records passed validation")
        
        # Step 3: Normalize data
        if validated_df is not None and not validated_df.empty:
            normalized_data, norm_errors = debug_transformation_step(
                "Data Normalization",
                normalizer.normalize,
                validated_df
            )
            all_errors.extend(norm_errors)
        
        # Analyze all errors
        print("\n" + "="*80)
        print("ERROR ANALYSIS")
        print("="*80)
        
        if all_errors:
            error_categories = analyze_validation_errors(all_errors)
            
            print(f"\nTotal errors: {len(all_errors)}")
            print("\nError breakdown by category:")
            for category, errors in error_categories.items():
                print(f"\n{category}: {len(errors)} errors")
                # Show first 3 examples
                for i, error in enumerate(errors[:3]):
                    print(f"  Example {i+1}: {error.get('error', 'N/A')}")
                    if 'record' in error and isinstance(error['record'], dict):
                        print(f"    Record ID: {error['record'].get('id', 'N/A')}")
                        print(f"    Cédula: {error['record'].get('cedula', 'N/A')}")
        
        # Test with a specific problematic record if we found validation errors
        if validation_errors:
            print("\n" + "="*80)
            print("DETAILED ANALYSIS OF FIRST FAILED RECORD")
            print("="*80)
            
            first_error = validation_errors[0]
            if 'record' in first_error:
                problem_record = pd.DataFrame([first_error['record']])
                print("\nProblem record:")
                print(problem_record.T)
                
                # Run through validator with verbose mode
                print("\nRunning detailed validation...")
                try:
                    # Check each validation rule
                    record = problem_record.iloc[0].to_dict()
                    
                    # Check coordinates
                    if 'latitud' in record and 'longitud' in record:
                        lat = record.get('latitud')
                        lon = record.get('longitud')
                        print(f"\nCoordinates: lat={lat}, lon={lon}")
                        print(f"  Latitude type: {type(lat)}, value: {lat}")
                        print(f"  Longitude type: {type(lon)}, value: {lon}")
                        
                        # Costa Rica bounds
                        if lat is not None and lon is not None:
                            try:
                                lat_float = float(lat)
                                lon_float = float(lon)
                                print(f"  Converted: lat={lat_float}, lon={lon_float}")
                                print(f"  Valid range: lat=[8.0, 11.5], lon=[-86.0, -82.5]")
                                print(f"  In range: lat={8.0 <= lat_float <= 11.5}, lon={-86.0 <= lon_float <= -82.5}")
                            except:
                                print("  Failed to convert to float")
                    
                    # Check cédula
                    if 'cedula' in record:
                        cedula = record.get('cedula')
                        print(f"\nCédula: {cedula}")
                        print(f"  Type: {type(cedula)}")
                        print(f"  Length: {len(str(cedula)) if cedula else 0}")
                        print(f"  Is digit: {str(cedula).isdigit() if cedula else False}")
                    
                except Exception as e:
                    print(f"\nError in detailed analysis: {e}")
        
        # Summary
        print("\n" + "="*80)
        print("SUMMARY")
        print("="*80)
        print(f"Total records tested: {len(df)}")
        print(f"Total errors encountered: {len(all_errors)}")
        
        if validated_df is not None:
            print(f"Records that passed validation: {len(validated_df)}")
            success_rate = (len(validated_df) / len(df)) * 100
            print(f"Success rate: {success_rate:.1f}%")
            
            # Extrapolate to full dataset
            print(f"\nExtrapolation to 7,000 records:")
            print(f"Expected to pass: ~{int(7000 * success_rate / 100)}")
            print(f"Expected to fail: ~{int(7000 * (100 - success_rate) / 100)}")
        
    except Exception as e:
        print(f"\n❌ Script error: {e}")
        traceback.print_exc()
    
    finally:
        session.close()
        print(f"\nCompleted at: {datetime.now()}")

if __name__ == "__main__":
    main()