import requests
import os
from typing import Dict, Any, List, Optional
from datetime import datetime

from ROCrateCreation.GenericResearchData.ResearchData import ResearchData

class DataverseConnector:
    """
    Connector for fetching data from Dataverse repositories.
    """
    
    def __init__(self, server_url: str = "https://dataverse.harvard.edu", api_token: Optional[str] = None):
        """
        Initialize the Dataverse connector.
        
        Args:
            server_url: URL of the Dataverse server
            api_token: Optional API token for accessing restricted datasets
        """
        self.server_url = server_url.rstrip("/")
        self.api_token = api_token
        self.headers = {}
        if api_token:
            self.headers["X-Dataverse-key"] = api_token
    
    def fetch_dataset(self, doi: str) -> Dict[str, Any]:
        """
        Fetch dataset metadata from Dataverse.
        
        Args:
            doi: Dataset DOI or persistent ID
            
        Returns:
            Dictionary containing dataset metadata
        """
        # Handle DOI format
        persistent_id = doi
        if doi.startswith("doi:"):
            persistent_id = doi
        elif doi.startswith("10."):
            persistent_id = f"doi:{doi}"
        
        url = f"{self.server_url}/api/datasets/:persistentId/"
        params = {"persistentId": persistent_id}
        
        response = requests.get(url, params=params, headers=self.headers)
        
        if response.status_code != 200:
            raise ValueError(f"Failed to fetch dataset {doi}: {response.text}")
        
        data = response.json()
        if "status" in data and data["status"] == "ERROR":
            raise ValueError(f"API error: {data.get('message', 'Unknown error')}")
        
        return data.get("data", {})
    
    def fetch_files(self, dataset_id: str) -> List[Dict[str, Any]]:
        """
        Fetch files associated with a Dataverse dataset.
        
        Args:
            dataset_id: Dataverse dataset ID
            
        Returns:
            List of dictionaries containing file metadata
        """
        url = f"{self.server_url}/api/datasets/{dataset_id}/versions/:latest/files"
        response = requests.get(url, headers=self.headers)
        
        if response.status_code != 200:
            raise ValueError(f"Failed to fetch files for dataset {dataset_id}: {response.text}")
        
        data = response.json()
        if "status" in data and data["status"] == "ERROR":
            raise ValueError(f"API error: {data.get('message', 'Unknown error')}")
        
        return data.get("data", [])
    
    def fetch_data(self, doi: str, include_files: bool = True) -> ResearchData:
        """
        Fetch dataset and files data from Dataverse and convert to ResearchData.
        
        Args:
            doi: Dataset DOI or persistent ID
            include_files: Whether to include file metadata
            
        Returns:
            ResearchData object populated with Dataverse data
        """
        dataset = self.fetch_dataset(doi)
        
        # Extract metadata
        metadata = dataset.get("latestVersion", {}).get("metadataBlocks", {})
        citation_metadata = metadata.get("citation", {}).get("fields", [])
        
        # Parse citation metadata
        title = ""
        description = ""
        authors = []
        keywords = []
        publication_date = ""
        
        for field in citation_metadata:
            field_name = field.get("typeName", "")
            
            if field_name == "title":
                title = field.get("value", "")
            
            elif field_name == "dsDescription":
                descriptions = field.get("value", [])
                if descriptions:
                    description = descriptions[0].get("dsDescriptionValue", {}).get("value", "")
            
            elif field_name == "author":
                for author in field.get("value", []):
                    author_name = author.get("authorName", {}).get("value", "")
                    if author_name:
                        authors.append(author_name)
            
            elif field_name == "keyword":
                for keyword in field.get("value", []):
                    keyword_value = keyword.get("keywordValue", {}).get("value", "")
                    if keyword_value:
                        keywords.append(keyword_value)
            
            elif field_name == "distributionDate":
                publication_date = field.get("value", "")
        
        # Get files if requested
        files = []
        software = []
        
        if include_files:
            dataset_id = dataset.get("id")
            if dataset_id:
                file_metadata = self.fetch_files(dataset_id)
                dataset_url = f"{self.server_url}/dataset.xhtml?persistentId={doi}"
                
                for file_data in file_metadata:
                    data_file = file_data.get("dataFile", {})
                    file_name = data_file.get("filename", "")
                    file_extension = os.path.splitext(file_name)[1].lower() if file_name else ""
                    

                    software_extensions = ['.py', '.r', '.sh', '.exe', '.java', '.cpp', '.js', '.jsx', '.css']
                    is_software = file_extension.lower() in software_extensions
                                        
                    file_id = data_file.get("id")
                    download_url = f"{self.server_url}/api/access/datafile/{file_id}"
                    
                    if is_software:
                        software.append({
                            "id": file_id,
                            "name": file_name,
                            "version": "1.0.0",  # Default version
                            "description": data_file.get("description", ""),
                            "download_url": download_url,
                            "url": dataset_url,
                            "date_modified": data_file.get("creationDate", ""),
                            "format": file_extension.lstrip(".") or "application/octet-stream",
                            "documentation_url": dataset_url
                        })
                    else:
                        files.append({
                            "id": file_id,
                            "name": file_name,
                            "size": data_file.get("filesize", 0),
                            "format": file_extension.lstrip(".") or "",
                            "description": data_file.get("description", ""),
                            "download_url": download_url,
                            "url": dataset_url,
                            "uploaded_date": data_file.get("creationDate", "")
                        })
        
        # Create and return ResearchData instance
        return ResearchData(
            repository_name="Dataverse",
            project_id=doi,
            title=title,
            description=description,
            authors=authors,
            license=dataset.get("license", ""),
            keywords=keywords,
            publication_date=publication_date,
            doi=doi,
            files=files,
            software=software,
            metadata={
                "dataverse_url": f"{self.server_url}/dataset.xhtml?persistentId={doi}",
                "publisher": dataset.get("publisher", ""),
                "version": dataset.get("versionNumber", "")
            }
        )
    
    def search_datasets(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Search for datasets on Dataverse.
        
        Args:
            query: Search query
            limit: Maximum number of results to return
            
        Returns:
            List of dataset metadata dictionaries
        """
        url = f"{self.server_url}/api/search"
        params = {
            "q": query,
            "type": "dataset",
            "per_page": limit
        }
        
        response = requests.get(url, params=params, headers=self.headers)
        
        if response.status_code != 200:
            raise ValueError(f"Failed to search datasets: {response.text}")
        
        data = response.json()
        if "status" in data and data["status"] == "ERROR":
            raise ValueError(f"API error: {data.get('message', 'Unknown error')}")
        
        return data.get("data", {}).get("items", [])