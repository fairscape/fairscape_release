from pydantic import BaseModel
from typing import Optional, List, Dict, Union, Any
import json
import pathlib
from datetime import datetime
import os

from fairscape_cli.models.rocrate import GenerateROCrate, AppendCrate
from fairscape_cli.models.dataset import GenerateDataset
from fairscape_cli.models.software import GenerateSoftware

class ResearchData(BaseModel):
    """
    Base model for handling scientific research data from various repositories.
    """
    repository_name: str
    project_id: str
    title: str
    description: str
    authors: List[str]
    license: Optional[str] = None
    keywords: List[str] = []
    publication_date: Optional[str] = None
    doi: Optional[str] = None
    files: List[Dict[str, Any]] = []
    software: List[Dict[str, Any]] = []
    metadata: Dict[str, Any] = {}
    
    def to_rocrate(self, output_dir: str) -> str:
        """
        Convert to RO-Crate format and save to the specified directory.
        
        Args:
            output_dir: Output directory for RO-Crate
            
        Returns:
            GUID of the created RO-Crate
        """
        output_path = pathlib.Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        rocrate_data = GenerateROCrate(
            path=output_path,
            guid="",
            name=self.title,
            description=self.description,
            keywords=self.keywords,
            license=self.license or "https://creativecommons.org/licenses/by/4.0/",
            hasPart=[],
            author=self.authors,
            datePublished=self.publication_date or datetime.now().isoformat(),
            associatedPublication=self.doi or "",
            isPartOf=[],
            version="1.0"
        )
        

        generated_guids = []
        
        for file_info in self.files:
            
            file_format = file_info.get("format", "")
            if not file_format and "name" in file_info:   
                ext = os.path.splitext(file_info["name"])[1].lstrip(".")
                file_format = ext or "unknown"
            if file_info.get("description") == "":
                file_info["description"] = self.description
                
            dataset = GenerateDataset(
                guid=None,
                url=file_info.get("url"),
                author=self.authors,
                name=file_info.get("name", "Unnamed file"),
                description=file_info.get("description", f"File from {self.repository_name}"),
                keywords=self.keywords,
                datePublished=file_info.get("uploaded_date", self.publication_date or datetime.now().isoformat()),
                version="1.0",
                associatedPublication=self.doi or None,
                additionalDocumentation=None,
                format=file_format,
                schema="",
                derivedFrom=[],
                usedBy=[],
                generatedBy=[],
                filepath=None,
                contentUrl=file_info.get("download_url", ""),
                cratePath=output_path
            )
            
            generated_guids.append(dataset.guid)
            AppendCrate(pathlib.Path(output_path), [dataset])
            
        for software_info in self.software:
            software_version = software_info.get("version", "0.1.0")
            
            software = GenerateSoftware(
                guid=None,
                name=software_info.get("name", "Unnamed software"),
                author=self.authors[0] if self.authors else "Unknown",
                dateModified=software_info.get("date_modified", datetime.now().isoformat()),
                version=software_version,
                description=software_info.get("description", f"Software from {self.repository_name}"),
                associatedPublication=self.doi or None,
                additionalDocumentation=software_info.get("documentation_url", None),
                format=software_info.get("format", "application/zip"),
                usedByComputation=[],
                contentUrl=software_info.get("download_url", "")
            )
            
            generated_guids.append(software.guid)
            AppendCrate(pathlib.Path(output_path), [software])
        
        metadata_path = output_path / "ro-crate-metadata.json"
        with open(metadata_path, 'r') as f:
            crate_data = json.load(f)
            root_id = crate_data["@graph"][1]["@id"]
        
        return root_id
    
    @classmethod
    def from_repository(cls, repository_type: str, identifier: str, **kwargs) -> 'ResearchData':
        """
        Factory method to create ResearchData from different repository types.
        
        Args:
            repository_type: Type of repository (e.g., 'figshare', 'dataverse')
            identifier: Identifier for the dataset in the repository
            **kwargs: Additional parameters specific to the repository
            
        Returns:
            Populated ResearchData instance
        """
        if repository_type.lower() == 'figshare':
            from ROCrateCreation.GenericResearchData.connectors.FigShareConnector import FigshareConnector
            connector = FigshareConnector(token=kwargs.get('token'))
            return connector.fetch_data(identifier, include_files=kwargs.get('include_files', True))
        elif repository_type.lower() == 'dataverse':
            from ROCrateCreation.GenericResearchData.connectors.DataverseConnector import DataverseConnector
            connector = DataverseConnector(
                server_url=kwargs.get('server_url', 'https://dataverse.harvard.edu'),
                api_token=kwargs.get('token')
            )
            return connector.fetch_data(identifier, include_files=kwargs.get('include_files', True))
        else:
            raise ValueError(f"Unsupported repository type: {repository_type}")

    def add_to_existing_rocrate(self, rocrate_path: str) -> str:
        """
        Add this research data to an existing RO-Crate.
        
        Args:
            rocrate_path: Path to the existing RO-Crate directory
            
        Returns:
            GUID of the updated RO-Crate
        """
        from fairscape_cli.models.rocrate import AppendCrate
        from fairscape_cli.models.dataset import GenerateDataset
        
        rocrate_path = pathlib.Path(rocrate_path)
        metadata_path = rocrate_path / "ro-crate-metadata.json"
        
        if not metadata_path.exists():
            raise ValueError(f"No RO-Crate metadata found at {rocrate_path}")
        
        # Create datasets for each file
        dataset_objs = []
        
        for file_info in self.files:
            file_format = file_info.get("format", "")
            if not file_format and "name" in file_info:
                ext = os.path.splitext(file_info["name"])[1].lstrip(".")
                file_format = ext or "unknown"
            print("HELLO")
            print(file_info.get("description", f"File from {self.repository_name}"))
            print("HI")
            dataset = GenerateDataset(
                guid=None,
                url=None,
                author=self.authors,
                name=file_info.get("name", "Unnamed file"),
                description=file_info.get("description", f"File from {self.repository_name}"),
                keywords=self.keywords,
                datePublished=file_info.get("uploaded_date", self.publication_date or datetime.now().isoformat()),
                version="1.0",
                associatedPublication=self.doi or None,
                additionalDocumentation=None,
                format=file_format,
                schema="",
                derivedFrom=[],
                usedBy=[],
                generatedBy=[],
                filepath=None,
                contentUrl=file_info.get("download_url", ""),
                cratePath=rocrate_path
            )
            
            dataset_objs.append(dataset)
        
        AppendCrate(rocrate_path, dataset_objs)
        
        # Read the metadata file to get the root id
        with open(metadata_path, 'r') as f:
            crate_data = json.load(f)
            root_id = crate_data["@graph"][1]["@id"]
        
        return root_id