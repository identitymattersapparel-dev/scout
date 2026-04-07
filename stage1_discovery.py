import os
import csv
import re
import json
from datetime import datetime
import supabase

# Load Supabase credentials from environment
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

# Initialize Supabase client
supabase_client = supabase.create_client(SUPABASE_URL, SUPABASE_KEY)

def load_client_config(client_id):
    """Load client configuration from Supabase"""
    try:
        response = supabase_client.table('client_configs').select('field_mapping').eq('client_id', client_id).single().execute()
        return response.data['field_mapping']
    except Exception as e:
        print(f"❌ Error loading config: {e}")
        return None

def download_csv_from_storage(bucket, file_path):
    """Download CSV from Supabase Storage"""
    try:
        response = supabase_client.storage.from_(bucket).download(file_path)
        return response.decode('utf-8')
    except Exception as e:
        print(f"❌ Error downloading file: {e}")
        return None

def validate_config(config, csv_headers):
    """Validate that all mapped fields exist in CSV"""
    errors = []
    
    # Flatten config to get all field references
    def extract_field_names(obj, path=""):
        fields = []
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key == "source" or key == "regex":
                    continue
                if isinstance(value, str):
                    fields.append(value)
                elif isinstance(value, dict):
                    fields.extend(extract_field_names(value, f"{path}.{key}"))
        return fields
    
    mapped_fields = extract_field_names(config)
    
    for field in mapped_fields:
        if field not in csv_headers:
            errors.append(f"Field '{field}' in config not found in CSV headers")
    
    return errors

def test_regex_extraction(csv_content, config, sample_size=5):
    """Test regex extraction on sample rows"""
    results = []
    
    csv_reader = csv.DictReader(csv_content.splitlines())
    headers = csv_reader.fieldnames
    
    # Validate config
    config_errors = validate_config(config, headers)
    if config_errors:
        return {
            'status': 'ERROR',
            'validation_errors': config_errors,
            'samples': []
        }
    
    # Test extraction on sample rows
    for idx, row in enumerate(csv_reader):
        if idx >= sample_size:
            break
        
        sample = {
            'row': idx + 2,  # row number (accounting for header)
            'name': row.get(config['identity']['name'], 'N/A'),
            'email': row.get(config['identity']['email'], 'N/A'),
            'extracted_data': {}
        }
        
        # Test purchase_date extraction
        if 'property' in config and 'purchase_date' in config['property']:
            purchase_config = config['property']['purchase_date']
            if purchase_config.get('source') == 'notes_html':
                notes_field = config.get('notes')
                notes_content = row.get(notes_field, '')
                regex_pattern = purchase_config.get('regex')
                
                if notes_content and regex_pattern:
                    try:
                        match = re.search(regex_pattern, notes_content)
                        if match:
                            sample['extracted_data']['purchase_date'] = match.group(1)
                        else:
                            sample['extracted_data']['purchase_date'] = 'No match'
                    except Exception as e:
                        sample['extracted_data']['purchase_date'] = f'Regex error: {e}'
        
        results.append(sample)
    
    return {
        'status': 'SUCCESS',
        'validation_errors': [],
        'samples': results
    }

def stage1_discovery(client_id, storage_bucket, file_path):
    """Main Stage 1 Discovery function"""
    print(f"\n{'='*80}")
    print(f"Stage 1: Discovery (Configuration Validation)")
    print(f"{'='*80}")
    print(f"Client ID: {client_id}")
    print(f"File: {file_path}\n")
    
    # Load config
    print("📋 Loading client configuration...")
    config = load_client_config(client_id)
    if not config:
        print("❌ Failed to load client configuration")
        return False
    print("✅ Configuration loaded")
    
    # Download CSV
    print(f"\n📥 Downloading CSV from Storage...")
    csv_content = download_csv_from_storage(storage_bucket, file_path)
    if not csv_content:
        print("❌ Failed to download CSV")
        return False
    print("✅ CSV downloaded")
    
    # Parse and count rows
    csv_lines = csv_content.splitlines()
    row_count = len(csv_lines) - 1  # Exclude header
    print(f"   Total rows: {row_count}")
    
    # Test extraction
    print(f"\n🔍 Testing field extraction on first 5 rows...")
    extraction_results = test_regex_extraction(csv_content, config, sample_size=5)
    
    if extraction_results['status'] == 'ERROR':
        print("❌ Validation failed:")
        for error in extraction_results['validation_errors']:
            print(f"   - {error}")
        return False
    
    print("✅ Validation passed")
    print(f"\n📊 Sample extractions:")
    for sample in extraction_results['samples']:
        print(f"\n   Row {sample['row']}: {sample['name']} ({sample['email']})")
        if sample['extracted_data']:
            for key, value in sample['extracted_data'].items():
                print(f"      {key}: {value}")
        else:
            print(f"      (no special extractions)")
    
    print(f"\n{'='*80}")
    print("✅ Stage 1 Discovery Complete - Configuration is valid")
    print(f"{'='*80}\n")
    
    return True

# Main execution
if __name__ == "__main__":
    # Brian's client ID
    BRIAN_CLIENT_ID = "62960ae5-4e6f-4b03-82b0-1c3396271268"
    STORAGE_BUCKET = "bluefuse-brian-white"
    FILE_PATH = "raw_exports/bluefuse_brian_white_raw_20260407.csv"
    
    success = stage1_discovery(BRIAN_CLIENT_ID, STORAGE_BUCKET, FILE_PATH)
    exit(0 if success else 1)
