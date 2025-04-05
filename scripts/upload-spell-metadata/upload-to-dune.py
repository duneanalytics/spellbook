import json
import csv
import os
import requests
import dotenv
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up paths
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(script_dir))

# Get API key from environment variable
api_key = os.getenv("DUNE_API_KEY")

# Directory containing the manifest files
manifests_dir = os.path.join(project_root, 'dbt_subprojects', 'manifests')

# CSV file to store combined data
csv_file_path = os.path.join(project_root, 'dbt_subprojects', 'manifests' , 'combined_dbt_models_sources.csv')

def parse_manifest(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)

    info = {}

    # Parse sources
    for unique_id, source_details in data['sources'].items():
        schema = source_details.get('schema')
        name = source_details.get('name')
        full_name = f"{schema}.{name}"
        source_columns = source_details.get('columns', {})
        info[full_name] = {
            'schema': schema,
            'name': name,
            'columns': source_columns,
            'partition_by': '',
            'meta_origin': 'source'
        }

    # Parse models (merging with source info if exists)
    for node_id, node_details in data['nodes'].items():
        if node_details.get('resource_type') == 'model':
            schema = node_details.get('schema')
            alias = node_details.get('alias')
            full_name = f"{schema}.{alias}"
            partition_by = node_details.get('config', {}).get('partition_by')
            model_columns = node_details.get('columns', {})
            
            if full_name in info:
                # Merge columns, prioritizing model descriptions
                merged_columns = {**info[full_name]['columns'], **model_columns}
            else:
                merged_columns = model_columns
            
            info[full_name] = {
                'schema': schema,
                'name': alias,
                'columns': merged_columns,
                'partition_by': partition_by,
                'meta_origin': 'model'
            }

    return info

# Combine all manifests into a single CSV
with open(csv_file_path, 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['model_name', 'schema', 'alias', 'partition_by', 'column', 'column_description', 'meta_origin'])

    combined_info = {}

    for filename in os.listdir(manifests_dir):
        if filename.endswith('_manifest.json'):
            file_path = os.path.join(manifests_dir, filename)
            manifest_info = parse_manifest(file_path)
            
            # Update combined_info, merging column information
            for full_name, details in manifest_info.items():
                if full_name not in combined_info:
                    combined_info[full_name] = details
                else:
                    # Merge columns, keeping all descriptions
                    combined_info[full_name]['columns'] = {
                        **combined_info[full_name]['columns'],
                        **details['columns']
                    }
                    # Update meta_origin to 'model' if it's a model
                    if details['meta_origin'] == 'model':
                        combined_info[full_name]['meta_origin'] = 'model'
                        combined_info[full_name]['partition_by'] = details['partition_by']

    # Write combined information to CSV
    for full_name, details in combined_info.items():
        schema = details['schema']
        name = details['name']
        partition_by = details['partition_by']
        meta_origin = details['meta_origin']
        
        if details['columns']:
            for column_name, column_details in details['columns'].items():
                column_description = column_details.get('description', 'No description available')
                csvwriter.writerow([full_name, schema, name, partition_by, column_name, column_description, meta_origin])
        else:
            csvwriter.writerow([full_name, schema, name, partition_by, '', '', meta_origin])

# Upload the CSV to Dune
url = 'https://api.dune.com/api/v1/table/upload/csv'

with open(csv_file_path, 'r') as file:
    data = file.read()
    # Print the size of the CSV file
    file_size = os.path.getsize(csv_file_path)
    print('CSV file size:', file_size/1000, 'kilobytes')

    # Set the headers and metadata for the CSV data
    headers = {'X-Dune-Api-Key': api_key}

    # Construct the payload for the API
    payload = {
        "table_name": "spellbook_table_infos",
        "description": "Combined Spellbook models and sources metadata",
        "data": str(data),
        "is_private": False
    }
 
    # Send the POST request to the HTTP endpoint
    response = requests.post(url, data=json.dumps(payload), headers=headers)

    # Print the response status code and content
    print('Response status code:', response.status_code)
    print('Response content:', response.content)