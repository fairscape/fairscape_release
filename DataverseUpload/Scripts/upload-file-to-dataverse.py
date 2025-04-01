import argparse
import requests
import sys
from pathlib import Path

def upload_file_to_dataverse(token, dataverse_url, dataset_id, file_path, description=None):
    # Check if file exists
    file_path = Path(file_path)
    if not file_path.exists():
        print(f"Error: File {file_path} does not exist")
        sys.exit(1)
    
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
    
    # Prepare file for upload
    with open(file_path, 'rb') as f:
        file_content = f.read()
    
    # Set up file parameters
    files = {
        'file': (file_path.name, file_content, 'application/octet-stream')
    }
    
    # Add description if provided
    params = {}
    if description:
        params['description'] = description
    
    try:
        # Upload the file
        print(f"Uploading {file_path.name} to dataset {dataset_id}...")
        response = requests.post(url, headers=headers, files=files, params=params)
        
        if response.status_code == 200:
            result = response.json()
            file_id = result['data']['files'][0]['dataFile']['id']
            print(f"File uploaded successfully!")
            print(f"File ID: {file_id}")
            return file_id
        else:
            print(f"Error uploading file: {response.status_code}")
            print(response.text)
            sys.exit(1)
    except Exception as e:
        print(f"Error during upload: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description='Upload a file to an existing Dataverse dataset')
    parser.add_argument('--token', required=True, help='Dataverse API token')
    parser.add_argument('--url', required=True, help='Dataverse base URL')
    parser.add_argument('--dataset', required=True, 
                       help='Dataset ID (can be numeric ID or persistent ID like doi:10.1234/ABC)')
    parser.add_argument('--file', required=True, help='Path to file for upload')
    parser.add_argument('--description', help='Optional description for the file')
    
    args = parser.parse_args()
    
    upload_file_to_dataverse(args.token, args.url, args.dataset, args.file, args.description)

if __name__ == "__main__":
    main()