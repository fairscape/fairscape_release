import argparse
import json
import requests
import csv
from pathlib import Path
import sys
from datetime import datetime

def load_authors_info(authors_csv_path):
    authors_info = {}
    if authors_csv_path:
        try:
            with open(authors_csv_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    name = row.get('name', '').strip()
                    if name:
                        authors_info[name] = {
                            'affiliation': row.get('affiliation', ''),
                            'orcid': row.get('orcid', '')
                        }
        except Exception as e:
            print(f"Warning: Error loading authors CSV: {e}")
    return authors_info

def create_dataverse_dataset(token, dataverse_url, dataverse_collection, rocrate_path, authors_csv=None):
    # Load authors information from CSV if provided
    authors_info = load_authors_info(authors_csv)
    
    # Read RO-Crate metadata
    try:
        with open(rocrate_path, 'r') as f:
            rocrate_data = json.load(f)
    except Exception as e:
        print(f"Error reading RO-Crate metadata: {e}")
        sys.exit(1)
    
    # Find the root node (dataset)
    root_node = None
    for item in rocrate_data.get('@graph', []):
        # Skip the metadata descriptor node
        if item.get('@id') == 'ro-crate-metadata.json':
            continue
        
        # The root node is typically the main dataset node
        if item.get('@type') and ('Dataset' in item.get('@type') or 
                                 (isinstance(item.get('@type'), list) and 'Dataset' in item.get('@type'))):
            root_node = item
            break
    
    if not root_node:
        print("Error: Could not find root dataset node in RO-Crate")
        sys.exit(1)
    
    # Transform metadata to Dataverse format
    license_map = {
        "https://creativecommons.org/licenses/by/4.0": {
            "name": "CC BY 4.0",
            "uri": "https://creativecommons.org/licenses/by/4.0"
        },
        "https://creativecommons.org/licenses/by-nc-sa/4.0": {
            "name": "CC BY-NC-SA 4.0",
            "uri": "https://creativecommons.org/licenses/by-nc-sa/4.0"
        },
        "https://creativecommons.org/publicdomain/zero/1.0": {
            "name": "CC0 1.0",
            "uri": "http://creativecommons.org/publicdomain/zero/1.0"
        }
    }
    
    # Get license info or default to CC BY 4.0
    license_url = root_node.get('license', "https://creativecommons.org/licenses/by/4.0")
    license_info = license_map.get(license_url, license_map["https://creativecommons.org/licenses/by/4.0"])
    
    # Extract authors
    authors = root_node.get('author', '')
    author_entries = []
    
    if isinstance(authors, list):
        author_list = authors
    elif isinstance(authors, str):
        author_list = [author.strip() for author in authors.split(';') if author.strip()]
        if not author_list:
            author_list = [authors]
    else:
        author_list = []
    
    for author in author_list:
        author_name = author.strip()
        author_info = authors_info.get(author_name, {})
        
        author_entry = {
            "authorName": {
                "value": author_name,
                "typeClass": "primitive",
                "multiple": False,
                "typeName": "authorName"
            },
            "authorAffiliation": {
                "value": author_info.get('affiliation', ''),
                "typeClass": "primitive",
                "multiple": False,
                "typeName": "authorAffiliation"
            }
        }
        
        if author_info.get('orcid'):
            author_entry["authorIdentifierScheme"] = {
                "value": "ORCID",
                "typeClass": "controlledVocabulary",
                "multiple": False,
                "typeName": "authorIdentifierScheme"
            }
            author_entry["authorIdentifier"] = {
                "value": author_info.get('orcid'),
                "typeClass": "primitive",
                "multiple": False,
                "typeName": "authorIdentifier"
            }
        
        author_entries.append(author_entry)
    
    # Extract keywords
    keywords = root_node.get('keywords', [])
    if isinstance(keywords, str):
        keywords = [keywords]
    
    # Add required dataset contact info
    contact_name = root_node.get("principalInvestigator", "")
    contact_email = root_node.get("contactEmail", "")
    
    # Get publication date or use today's date
    publish_date = root_node.get("datePublished", "")
    if not publish_date:
        publish_date = datetime.today().strftime("%Y-%m-%d")
    
    # Format date to ensure YYYY-MM-DD
    try:
        if '/' in publish_date:
            # Handle MM/DD/YYYY format
            date_parts = publish_date.split('/')
            if len(date_parts) == 3:
                publish_date = f"{date_parts[2]}-{date_parts[0].zfill(2)}-{date_parts[1].zfill(2)}"
        elif len(publish_date) == 10 and publish_date[4] == '-' and publish_date[7] == '-':
            # Already in YYYY-MM-DD format
            pass
        else:
            # Try to parse the date
            date_obj = datetime.strptime(publish_date, "%m/%d/%Y")
            publish_date = date_obj.strftime("%Y-%m-%d")
    except (ValueError, IndexError):
        # Fall back to today's date if parsing fails
        publish_date = datetime.today().strftime("%Y-%m-%d")
    
    print(f"Using publication date: {publish_date}")
    
    dataverse_metadata = {
        "datasetVersion": {
            "license": license_info,
            "metadataBlocks": {
                "citation": {
                    "fields": [
                        {
                            "value": root_node.get("name", "Untitled Dataset"),
                            "typeClass": "primitive",
                            "multiple": False,
                            "typeName": "title"
                        },
                        {
                            "value": author_entries,
                            "typeClass": "compound",
                            "multiple": True,
                            "typeName": "author"
                        },
                        {
                            "value": [
                                {
                                    "datasetContactName": {"value": contact_name, "typeClass": "primitive", 
                                                         "multiple": False, "typeName": "datasetContactName"},
                                    "datasetContactEmail": {"value": contact_email, "typeClass": "primitive", 
                                                          "multiple": False, "typeName": "datasetContactEmail"}
                                }
                            ],
                            "typeClass": "compound",
                            "multiple": True,
                            "typeName": "datasetContact"
                        },
                        {
                            "value": [
                                {
                                    "dsDescriptionValue": {"value": root_node.get("description", ""), 
                                                        "typeClass": "primitive", "multiple": False, 
                                                        "typeName": "dsDescriptionValue"}
                                }
                            ],
                            "typeClass": "compound",
                            "multiple": True,
                            "typeName": "dsDescription"
                        },
                        {
                            "value": ["Computer and Information Science"],
                            "typeClass": "controlledVocabulary",
                            "multiple": True,
                            "typeName": "subject"
                        },
                        {
                            "value": [
                                {
                                    "keywordValue": {"value": keyword, "typeClass": "primitive", 
                                                   "multiple": False, "typeName": "keywordValue"}
                                } for keyword in keywords
                            ],
                            "typeClass": "compound",
                            "multiple": True,
                            "typeName": "keyword"
                        },
                        {
                            "value": publish_date,
                            "typeClass": "primitive",
                            "multiple": False,
                            "typeName": "datasetPublicationDate"
                        },
                        {
                            "value": publish_date,
                            "typeClass": "primitive",
                            "multiple": False,
                            "typeName": "distributionDate"
                        },
                        {
                            "value": publish_date,
                            "typeClass": "primitive",
                            "multiple": False,
                            "typeName": "productionDate"
                        }
                    ]
                }
            }
        }
    }
    
    # Create dataset on Dataverse
    dataverse_url = dataverse_url.rstrip('/')
    url = f"{dataverse_url}/api/dataverses/{dataverse_collection}/datasets"
    
    headers = {
        "X-Dataverse-key": token,
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(url, headers=headers, json=dataverse_metadata)
        
        if response.status_code == 201:
            result = response.json()
            print(f"Dataset created successfully!")
            print(f"Persistent ID: {result['data']['persistentId']}")
            return result['data']['persistentId']
        else:
            print(f"Error creating dataset: {response.status_code}")
            print(response.text)
            sys.exit(1)
    except Exception as e:
        print(f"Error connecting to Dataverse: {e}")
        sys.exit(1)
        
def main():
    parser = argparse.ArgumentParser(description='Create a Dataverse dataset from RO-Crate metadata')
    parser.add_argument('--token', required=True, help='Dataverse API token')
    parser.add_argument('--url', required=True, help='Dataverse base URL')
    parser.add_argument('--collection', required=True, help='Dataverse collection/alias')
    parser.add_argument('--rocrate', required=True, help='Path to ro-crate-metadata.json file')
    parser.add_argument('--authors-csv', help='Path to CSV file with author affiliations and ORCIDs')
    
    args = parser.parse_args()
    
    create_dataverse_dataset(args.token, args.url, args.collection, args.rocrate, args.authors_csv)

if __name__ == "__main__":
    main()