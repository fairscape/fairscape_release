#!/usr/bin/env python
import json
import sys
import requests
import argparse
from datetime import datetime

def create_datacite_doi(rocrate_path, prefix, username, password, repository_id, api_url='https://api.test.datacite.org/dois'):
    # Load the RO-Crate metadata
    with open(rocrate_path, 'r') as f:
        rocrate = json.load(f)
    
    # Extract dataset from the graph
    dataset = None
    for item in rocrate.get('@graph', []):
        if '@type' in item and ('Dataset' in item['@type'] or 'https://w3id.org/EVI#ROCrate' in item['@type']):
            dataset = item
            break
    
    if not dataset:
        print("Error: No dataset found in RO-Crate metadata")
        sys.exit(1)

    # Extract the relevant metadata for DataCite
    dataset_id = dataset.get('@id', '')
    title = dataset.get('name', 'Untitled Dataset')
    description = dataset.get('description', '')
    keywords = dataset.get('keywords', [])
    version = dataset.get('version', '1.0')
    url = dataset.get('url', '')
    content_url = dataset.get('contentUrl', '')
    authors = []
    
    # Handle authors
    if 'author' in dataset:
        if isinstance(dataset['author'], list):
            for author in dataset['author']:
                if isinstance(author, str):
                    authors.append({"name": author})
                else:
                    name = author.get('name', 'Unknown')
                    authors.append({"name": name})
        else:
            authors.append({"name": dataset['author']})
    
    if not authors:
        authors = [{"name": "Unknown"}]

    # Format DataCite metadata according to their schema
    datacite_metadata = {
        "data": {
            "type": "dois",
            "attributes": {
                "event": "publish",
                "prefix": prefix,
                "creators": authors,
                "titles": [{"title": title}],
                "publisher": "Your Institution Name",  # Replace with your institution
                "publicationYear": datetime.now().year,
                "types": {
                    "resourceTypeGeneral": "Dataset"
                },
                "descriptions": [
                    {
                        "description": description,
                        "descriptionType": "Abstract"
                    }
                ],
                "schemaVersion": "http://datacite.org/schema/kernel-4",
                "subjects": [{"subject": kw} for kw in keywords],
                "version": version,
                "distributions": []
            }
        }
    }
    
    if url:
        datacite_metadata["data"]["attributes"]["url"] = url
    
    if content_url:
        content_type = "application/octet-stream"  
        distribution = {
            "file": {
                "mediaType": content_type,
                "contentURL": content_url,
                "accessLevel": {
                    "@value": "open access",
                    "@language": "en",
                    "accessLevelUri": "http://purl.org/coar/access_right/c_abf2"
                }
            }
        }
        datacite_metadata["data"]["attributes"]["distributions"].append(distribution)
    
    # Remove distributions if empty
    if not datacite_metadata["data"]["attributes"]["distributions"]:
        del datacite_metadata["data"]["attributes"]["distributions"]
    
    # Determine if we have a license
    if 'license' in dataset:
        datacite_metadata['data']['attributes']['rightsList'] = [
            {"rights": dataset['license']}
        ]

    # Add publication date if available
    if 'datePublished' in dataset:
        datacite_metadata['data']['attributes']['dates'] = [
            {
                "date": dataset['datePublished'].split('T')[0],
                "dateType": "Issued"
            }
        ]
    
    # Submit to DataCite API
    datacite_url = api_url
    
    response = requests.post(
        datacite_url,
        json=datacite_metadata,
        auth=requests.auth.HTTPBasicAuth(username, password),
        headers={
            "Content-Type": "application/vnd.api+json",
            "Accept": "application/vnd.api+json"
        }
    )
    
    if response.status_code in (200, 201):
        doi_response = response.json()
        new_doi = doi_response['data']['id']
        print(f"https://doi.org/{new_doi}")
        return new_doi
    else:
        print(f"Error creating DOI. Status code: {response.status_code}")
        print(f"Response: {response.text}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create a DOI on DataCite from RO-Crate metadata')
    parser.add_argument('rocrate_path', help='Path to ro-crate-metadata.json file')
    parser.add_argument('--prefix', required=True, help='Your DataCite prefix (e.g., 10.XXXX)')
    parser.add_argument('--username', required=True, help='DataCite API username')
    parser.add_argument('--password', required=True, help='DataCite API password')
    parser.add_argument('--repository', required=True, help='DataCite repository ID (format: MEMBER.REPOSITORY, e.g., uva.clarklab)')
    parser.add_argument('--api-url', default='https://api.test.datacite.org/dois', help='DataCite API URL')
    
    args = parser.parse_args()
    
    create_datacite_doi(
        args.rocrate_path, 
        args.prefix, 
        args.username, 
        args.password, 
        args.repository,
        args.api_url
    )