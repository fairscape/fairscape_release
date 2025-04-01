from pydantic import BaseModel
from typing import List

class Experiment(BaseModel):
    accession: str
    title: str
    study_ref: str
    sample_ref: str
    
    library_name: str
    library_strategy: str
    library_source: str
    library_selection: str
    library_layout: str
    nominal_length: str
    
    platform_type: str
    instrument_model: str


class Experiments(BaseModel):
    items: List[Experiment]
