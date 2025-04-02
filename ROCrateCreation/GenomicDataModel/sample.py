from pydantic import BaseModel
from typing import List, Dict, Optional

class Sample(BaseModel):
    accession: str
    title: Optional[str] = ""
    scientific_name: str
    taxon_id: str
    attributes: Dict[str, str]
    
    study_accession: Optional[str] = None
    study_center_name: Optional[str] = None
    study_title: Optional[str] = None
    study_abstract: Optional[str] = None
    study_description: Optional[str] = None


class Samples(BaseModel):
    items: List[Sample]