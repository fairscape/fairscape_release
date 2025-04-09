import requests
import os
from typing import Dict, Any, List, Optional
from datetime import datetime

from ..ResearchData import ResearchData

class FigshareConnector:
    """
    Connector for fetching data from Figshare repositories.
    """
    
    BASE_URL = "https://api.figshare.com/v2"
    
    def __init__(self, token: Optional[str] = None):
        """
        Initialize the Figshare connector.
        
        Args:
            token: Optional API token for accessing private datasets
        """
        self.token = token
        self.headers = {}
        if token:
            self.headers["Authorization"] = f"token {token}"
    
    def fetch_article(self, article_id: str) -> Dict[str, Any]:
        """
        Fetch article metadata from Figshare.
        
        Args:
            article_id: Figshare article ID
            
        Returns:
            Dictionary containing article metadata
        """
        url = f"{self.BASE_URL}/articles/{article_id}"
        response = requests.get(url, headers=self.headers)
        
        if response.status_code != 200:
            raise ValueError(f"Failed to fetch article {article_id}: {response.text}")
        
        return response.json()
    
    def fetch_files(self, article_id: str) -> List[Dict[str, Any]]:
        """
        Fetch files associated with a Figshare article.
        
        Args:
            article_id: Figshare article ID
            
        Returns:
            List of dictionaries containing file metadata
        """
        url = f"{self.BASE_URL}/articles/{article_id}/files"
        response = requests.get(url, headers=self.headers)
        
        if response.status_code != 200:
            raise ValueError(f"Failed to fetch files for article {article_id}: {response.text}")
        
        return response.json()
    
    def fetch_data(self, article_id: str, include_files: bool = True) -> ResearchData:
        """
        Fetch article and files data from Figshare and convert to ResearchData.
        
        Args:
            article_id: Figshare article ID
            include_files: Whether to include file metadata
            
        Returns:
            ResearchData object populated with Figshare data
        """
        article = self.fetch_article(article_id)
        
        # Extract authors
        authors = []
        for author in article.get("authors", []):
            author_name = author.get("full_name", "")
            if author_name:
                authors.append(author_name)
        
        # Extract keywords from tags
        keywords = article.get("tags", [])
        
        # Format publication date
        pub_date = article.get("published_date")
        if pub_date:
            try:
                # Try to convert to ISO format if needed
                dt = datetime.strptime(pub_date, "%Y-%m-%dT%H:%M:%SZ")
                pub_date = dt.isoformat()
            except ValueError:
                # Already in ISO format or unknown format
                pass
        
        # Get the figshare landing page URL
        figshare_url = article.get("url", "")
        
        # Get files if requested
        files = []
        software = []
        if include_files:
            raw_files = self.fetch_files(article_id)
            
            for file_data in raw_files:
                file_name = file_data.get("name", "")
                file_extension = os.path.splitext(file_name)[1].lower() if file_name else ""

                software_extensions = ['.py', '.r', '.sh', '.exe', '.java', '.cpp', '.js', '.jsx', '.css']
                is_software = file_extension.lower() in software_extensions
                            
                if is_software:
                    software.append({
                        "id": file_data.get("id"),
                        "name": file_name,
                        "version": article.get("version", "0.1.0"),
                        "description": file_data.get("description", ""),
                        "download_url": file_data.get("download_url", ""),
                        "url": figshare_url,  # Add landing page URL
                        "date_modified": file_data.get("uploaded_date", pub_date),
                        "format": file_extension.lstrip(".") or "application/octet-stream",
                        "documentation_url": figshare_url
                    })
                else:
                    files.append({
                        "id": file_data.get("id"),
                        "name": file_name,
                        "size": file_data.get("size", 0),
                        "format": file_extension.lstrip(".") or "",
                        "description": file_data.get("description", ""),
                        "download_url": file_data.get("download_url", ""),
                        "url": figshare_url,  # Add landing page URL
                        "uploaded_date": file_data.get("uploaded_date", pub_date)
                    })
        
        # Create and return ResearchData instance
        return ResearchData(
            repository_name="Figshare",
            project_id=article_id,
            title=article.get("title", ""),
            description=article.get("description", ""),
            authors=authors,
            license=article.get("license", {}).get("url", None),
            keywords=keywords,
            publication_date=pub_date,
            doi=article.get("doi", ""),
            files=files,
            software=software,
            metadata={
                "figshare_url": figshare_url,
                "figshare_citation": article.get("citation", ""),
                "figshare_categories": [cat.get("title") for cat in article.get("categories", [])],
                "views": article.get("views", 0),
                "downloads": article.get("downloads", 0),
                "version": article.get("version", "")
            }
        )
    
    def search_articles(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Search for articles on Figshare.
        
        Args:
            query: Search query
            limit: Maximum number of results to return
            
        Returns:
            List of article metadata dictionaries
        """
        url = f"{self.BASE_URL}/articles/search"
        params = {
            "search_for": query,
            "limit": limit
        }
        
        response = requests.post(url, json=params, headers=self.headers)
        
        if response.status_code != 200:
            raise ValueError(f"Failed to search articles: {response.text}")
        
        return response.json()