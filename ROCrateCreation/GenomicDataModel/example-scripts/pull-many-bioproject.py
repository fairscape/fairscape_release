import json
import os
import random
import requests
from GenomicData import GenomicData
import time


# Configuration
API_KEY = "b5842d8d17966b13241247e793b879532d07"
OUTPUT_DIR = "./bioprojects"
NUM_PROJECTS = 100

# Create output directory if it doesn't exist
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# Fetch a list of BioProject accessions
response = requests.get(
    "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
    params={
        "db": "bioproject",
        "term": "all[filter]",
        "retmax": 1000,
        "retmode": "json",
        "api_key": API_KEY
    }
)

# Get project IDs and convert to accessions
all_ids = response.json()["esearchresult"]["idlist"]
random_ids = random.sample(all_ids, min(NUM_PROJECTS, len(all_ids)))

# Get accessions for these IDs
response = requests.get(
    "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi",
    params={
        "db": "bioproject",
        "id": ",".join(random_ids),
        "retmode": "json",
        "api_key": API_KEY
    }
)

# Extract accessions from response
accessions = ["PRJDB2884","PRJNA1235416"]
data = response.json()["result"]
for project_id in random_ids:
    if project_id in data and "project_acc" in data[project_id]:
        accessions.append(data[project_id]["project_acc"])

print(f"Processing {len(accessions)} BioProjects: {accessions}")

# Process each BioProject
for accession in accessions:
    time.sleep(1)
    # Create directory for this project
    project_dir = os.path.join(OUTPUT_DIR, accession)
    if not os.path.exists(project_dir):
        os.makedirs(project_dir)
    
    try:
        # Create the GenomicData model
        genomic_data = GenomicData.from_api(accession, API_KEY)
        
        # Generate RO-Crate and PEP
        genomic_data.to_rocrate(project_dir)
        
        print(f"Completed {accession}")
    except Exception as e:
        print(f"Error processing {accession}: {e}")