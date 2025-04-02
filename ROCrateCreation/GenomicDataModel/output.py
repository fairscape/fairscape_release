from pydantic import BaseModel
from typing import List, Optional, Union

class OutputFile(BaseModel):
    filename: str
    size: str
    date: str
    url: str
    md5: str


class Output(BaseModel):
    accession: str
    title: str
    experiment_ref: str
    total_spots: Optional[Union[str,int]]
    total_bases: Optional[Union[str,int]]
    size: str
    published: str
    files: List[OutputFile]
    nreads: Optional[Union[str,int]]
    nspots: Optional[Union[str,int]]
    
    a_count: Optional[Union[str,int]]
    c_count: Optional[Union[str,int]]
    g_count: Optional[Union[str,int]]
    t_count: Optional[Union[str,int]]
    n_count: Optional[Union[str,int]]


class Outputs(BaseModel):
    items: List[Output]