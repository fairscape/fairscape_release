from pydantic import BaseModel
from typing import List, Dict

class Project(BaseModel):
    id: str
    accession: str
    archive: str = "Unknown"
    organism_name: str = ""
    title: str = ""
    description: str = ""
    release_date: str = ""
    
    # Flattened project type data
    target_capture: str = ""
    target_material: str = ""
    target_sample_scope: str = ""
    organism_species: str = ""
    organism_taxID: str = ""
    organism_supergroup: str = ""
    method: str = ""
    data_types: List[str] = []
    project_data_type: str = ""
    
    # Flattened submission data
    submitted_date: str = ""
    organization_role: str = "owner"
    organization_type: str = "center"
    organization_name: str = ""
    access: str = "public"