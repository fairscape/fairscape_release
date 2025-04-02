import argparse
import requests
import sys
import os
import json
from pathlib import Path

def upload_file_to_dataverse(token, dataverse_url, dataset_id, file_path, description=None):
    # Check if file exists
    file_path = Path(file_path)
    if not file_path.exists():
        return None
    
    # Prepare the API endpoint
    dataverse_url = dataverse_url.rstrip('/')
    # Handle both PID and DOI formats
    if dataset_id.startswith('doi:'):
        url = f"{dataverse_url}/api/datasets/:persistentId/add?persistentId={dataset_id}"
    else:
        url = f"{dataverse_url}/api/datasets/{dataset_id}/add"
    
    # Set up headers
    headers = {
        "X-Dataverse-key": token
    }
    
    # Get directory path for directoryLabel
    directory_label = os.path.dirname(str(file_path))
    
    # Prepare file for upload
    with open(file_path, 'rb') as f:
        file_content = f.read()
    
    # Create JSON data with only description and directoryLabel
    json_data = {
        'directoryLabel': directory_label
    }
    
    if description:
        json_data['description'] = description
    
    # Set up multipart form data
    files = {
        'file': (file_path.name, file_content, 'application/octet-stream'),
        'jsonData': (None, json.dumps(json_data))
    }
    
    try:
        # Upload the file
        response = requests.post(url, headers=headers, files=files)
        
        if response.status_code == 200:
            result = response.json()
            file_id = result['data']['files'][0]['dataFile']['id']
            
            # Construct the download URL
            if dataset_id.startswith('doi:'):
                # For persistent IDs
                download_url = f"{dataverse_url}/api/access/datafile/{file_id}"
            else:
                # Alternative format if needed
                download_url = f"{dataverse_url}/api/access/datafile/{file_id}"
            
            # Only print the download URL
            print(download_url)
            
            return download_url
        else:
            return None
    except Exception:
        return None

def upload_multiple_files(token, dataverse_url, dataset_id, file_paths, descriptions=None):
    download_urls = []
    
    # Upload each file
    for i, file_path in enumerate(file_paths):
        # Get description for this file if available
        description = None
        if descriptions and i < len(descriptions):
            description = descriptions[i]
        
        # Upload the file
        download_url = upload_file_to_dataverse(token, dataverse_url, dataset_id, file_path, description)
        
        if download_url:
            download_urls.append(download_url)
    
    return download_urls

def main():
    parser = argparse.ArgumentParser(description='Upload files to an existing Dataverse dataset')
    parser.add_argument('--token', required=True, help='Dataverse API token')
    parser.add_argument('--url', required=True, help='Dataverse base URL')
    parser.add_argument('--dataset', required=True,
                      help='Dataset ID (can be numeric ID or persistent ID like doi:10.1234/ABC)')
    parser.add_argument('--file', action='append', required=True, 
                      help='Path to file for upload (can be specified multiple times)')
    parser.add_argument('--description', action='append',
                      help='Optional description for the file (can be specified multiple times)')
    
    args = parser.parse_args()
    
    upload_multiple_files(args.token, args.url, args.dataset, args.file, args.description)

if __name__ == "__main__":
    main()