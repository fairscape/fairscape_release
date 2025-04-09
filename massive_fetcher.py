import requests
import uuid
from typing import Dict, Any, List, Optional
from datetime import datetime
from pathlib import Path

def from_massive(massive_id: str) -> Dict[str, Any]:
    """
    Fetch data from a MassIVE dataset and convert to Fairscape models.
    
    Parameters
    ----------
    massive_id : str
        MassIVE identifier (e.g., MSV000097523)
        
    Returns
    -------
    Dict[str, Any]
        Dictionary containing Sample, Experiment, Instrument, and Dataset objects
    """
    massive_data = fetch_massive_data(massive_id)
    
    samples = create_samples(massive_data, massive_id)
    instruments = create_instruments(massive_data, massive_id)
    experiments = create_experiments(massive_data, samples, instruments, massive_id)
    
    datasets = create_datasets(massive_data, experiments, massive_id)
    
    update_experiments_with_datasets(experiments, datasets)
    
    return {
        "samples": samples,
        "instruments": instruments,
        "experiments": experiments,
        "datasets": datasets,
        "raw_data": massive_data
    }

def fetch_massive_data(massive_id: str, timeout: float = 30.0) -> Dict[str, Any]:
    """
    Fetch data for a MassIVE dataset.
    
    Parameters
    ----------
    massive_id : str
        MassIVE identifier
    timeout : float
        Request timeout in seconds
        
    Returns
    -------
    Dict[str, Any]
        Raw MassIVE data
    """

    massive_id = str(massive_id).upper()
    
    api_url = f"https://massive.ucsd.edu/ProteoSAFe/proxi/v0.1/datasets/{massive_id}"
    
    response = requests.get(api_url, timeout=timeout)
    
    if response.status_code != 200:
        raise ValueError(f"Failed to fetch MassIVE data: HTTP {response.status_code}")
    
    return response.json()

def create_samples(massive_data: Dict[str, Any], massive_id: str) -> List[Dict[str, Any]]:
    """
    Create sample objects from MassIVE data.
    
    Parameters
    ----------
    massive_data : Dict[str, Any]
        Raw MassIVE data
    massive_id : str
        MassIVE identifier
        
    Returns
    -------
    List[Dict[str, Any]]
        List of sample objects compatible with the Sample model
    """
    samples = []
    
    contacts = []
    for contact_group in massive_data.get("contacts", []):
        contact_info = {}
        for item in contact_group:
            if item.get("name") == "contact name" and "value" in item:
                contact_info["name"] = item["value"]
            elif item.get("name") == "contact affiliation" and "value" in item:
                contact_info["affiliation"] = item["value"]
        
        if contact_info.get("name"):
            contacts.append(contact_info.get("name"))
    
    author = contacts[0] if contacts else "Unknown"
    
    keywords = ["proteomics", "mass spectrometry"]
    for keyword_item in massive_data.get("keywords", []):
        if "value" in keyword_item:
            kw_value = keyword_item["value"]
            if kw_value and kw_value not in keywords:
                if " " in kw_value and len(kw_value) > 15:
                    keywords.extend(kw_value.split())
                else:
                    keywords.append(kw_value)
    
    for i, species_group in enumerate(massive_data.get("species", []), 1):
        species_name = "Unknown"
        taxon_id = ""
        
        for species in species_group:
            if species.get("accession") == "MS:1001469" and "value" in species:
                species_name = species["value"]
            if species.get("accession") == "MS:1001467" and "value" in species:
                taxon_id = species["value"]
        
        sample_name = f"Sample from {species_name}"
        if massive_data.get("title"):
            sample_name = f"{species_name} sample from {massive_data.get('title')}"
        
        sample_id = f"ark:59852/sample-{massive_id.lower()}-{i}"
        
        description = f"Sample of {species_name} used in mass spectrometry experiment."
        if massive_data.get("summary"):
            summary = massive_data.get("summary")
            if len(summary) > 100:
                description = f"Sample of {species_name} used in experiments described as: {summary[:150]}..."
            else:
                description = f"Sample of {species_name} used in experiments described as: {summary}"
        
        content_url = None
        for link in massive_data.get("datasetLink", []):
            if link.get("accession") == "MS:1002488" and "value" in link: 
                content_url = link["value"]
                break
        
        sample = {
            "@id": sample_id,
            "name": sample_name,
            "metadataType": "https://w3id.org/EVI#Sample",
            "author": author,
            "description": description,
            "keywords": keywords,
            "contentUrl": content_url,
            "additionalProperty": [
                {
                    "@type": "PropertyValue",
                    "propertyID": "taxon_id",
                    "value": taxon_id
                },
                {
                    "@type": "PropertyValue",
                    "propertyID": "scientific_name",
                    "value": species_name
                },
                {
                    "@type": "PropertyValue",
                    "propertyID": "dataset_id",
                    "value": massive_id
                }
            ]
        }
        
        samples.append(sample)
    
    if not samples:
        sample_id = f"ark:59852/sample-{massive_id.lower()}-1"
        
        description = "Sample used in mass spectrometry experiment."
        if massive_data.get("summary"):
            summary = massive_data.get("summary")
            if len(summary) > 100:
                description = f"Sample used in experiment described as: {summary[:150]}..."
            else:
                description = f"Sample used in experiment described as: {summary}"
        
        content_url = None
        for link in massive_data.get("datasetLink", []):
            if link.get("accession") == "MS:1002488" and "value" in link:
                content_url = link["value"]
                break
        
        sample = {
            "@id": sample_id,
            "name": f"Sample from {massive_data.get('title', 'MassIVE experiment')}",
            "metadataType": "https://w3id.org/EVI#Sample",
            "author": author,
            "description": description,
            "keywords": keywords,
            "contentUrl": content_url,
            "additionalProperty": [
                {
                    "@type": "PropertyValue",
                    "propertyID": "dataset_id",
                    "value": massive_id
                }
            ]
        }
        
        samples.append(sample)
    
    return samples

def create_instruments(massive_data: Dict[str, Any], massive_id: str) -> List[Dict[str, Any]]:
    """
    Create instrument objects from MassIVE data.
    
    Parameters
    ----------
    massive_data : Dict[str, Any]
        Raw MassIVE data
    massive_id : str
        MassIVE identifier
        
    Returns
    -------
    List[Dict[str, Any]]
        List of instrument objects compatible with the Instrument model
    """
    instruments = []
    
    for i, instrument_info in enumerate(massive_data.get("instruments", []), 1):
        instrument_name = instrument_info.get("name", "Unknown instrument")
        
        instrument_id = f"ark:59852/instrument-{massive_id.lower()}-{i}"
        
        instrument = {
            "@id": instrument_id,
            "name": instrument_name,
            "metadataType": "https://w3id.org/EVI#Instrument",
            "manufacturer": "",
            "model": instrument_name,
            "description": f"{instrument_name} mass spectrometer used for proteomics analysis in MassIVE dataset {massive_id}.",
            "usedByExperiment": [],
            "additionalProperty": [
                {
                    "@type": "PropertyValue",
                    "propertyID": "accession",
                    "value": instrument_info.get("accession", "")
                },
                {
                    "@type": "PropertyValue",
                    "propertyID": "cvLabel",
                    "value": instrument_info.get("cvLabel", "")
                }
            ]
        }
        
        instruments.append(instrument)
    
    return instruments

def create_experiments(
    massive_data: Dict[str, Any], 
    samples: List[Dict[str, Any]], 
    instruments: List[Dict[str, Any]],
    massive_id: str
) -> List[Dict[str, Any]]:
    """
    Create experiment objects from MassIVE data.
    
    Parameters
    ----------
    massive_data : Dict[str, Any]
        Raw MassIVE data
    samples : List[Dict[str, Any]]
        List of sample objects
    instruments : List[Dict[str, Any]]
        List of instrument objects
    massive_id : str
        MassIVE identifier
        
    Returns
    -------
    List[Dict[str, Any]]
        List of experiment objects compatible with the Experiment model
    """
    experiments = []
    
    publication = None
    for pub in massive_data.get("publications", []):
        if pub.get("name") != "Dataset with no associated published manuscript":
            publication = pub.get("value", "")
    
    run_by = "Unknown"
    for contact_group in massive_data.get("contacts", []):
        for item in contact_group:
            if item.get("name") == "contact name" and "value" in item:
                run_by = item["value"]
                break
    
    date_performed = datetime.now().isoformat()
    experiment_types = ["Proteomics"]
    
    sample_refs = [{"@id": sample["@id"]} for sample in samples]
    instrument_refs = [{"@id": instrument["@id"]} for instrument in instruments]
    
    for instrument in instruments:
        instrument["usedByExperiment"] = []
    
    for i, exp_type in enumerate(experiment_types, 1):

        experiment_id = f"ark:59852/experiment-{massive_id.lower()}-{i}"
        
        if massive_data.get("title"):
            experiment_name = f"{exp_type} experiment: {massive_data.get('title')}"
        else:
            experiment_name = f"{exp_type} experiment from MassIVE dataset {massive_id}"
        
        description = f"{exp_type} mass spectrometry experiment from MassIVE dataset {massive_id}."
        if massive_data.get("summary"):
            summary = massive_data.get("summary")
            if len(summary) > 100:
                description = f"{exp_type} experiment described as: {summary[:150]}..."
            else:
                description = f"{exp_type} experiment described as: {summary}"
        
        experiment = {
            "@id": experiment_id,
            "name": experiment_name,
            "metadataType": "https://w3id.org/EVI#Experiment",
            "experimentType": exp_type,
            "runBy": run_by,
            "description": description,
            "datePerformed": date_performed,
            "associatedPublication": publication,
            "usedInstrument": instrument_refs,
            "usedSample": sample_refs,
            "generated": [], 
            "additionalProperty": [
                {
                    "@type": "PropertyValue",
                    "propertyID": "dataset_id",
                    "value": massive_id
                },
                {
                    "@type": "PropertyValue",
                    "propertyID": "dataset_url",
                    "value": f"https://massive.ucsd.edu/ProteoSAFe/QueryMSV?id={massive_id}"
                }
            ]
        }
        
        experiments.append(experiment)
        
        for instrument in instruments:
            instrument["usedByExperiment"].append({"@id": experiment_id})
    
    return experiments

def create_datasets(massive_data: Dict[str, Any], experiments: List[Dict[str, Any]], massive_id: str) -> List[Dict[str, Any]]:
    """
    Create dataset objects from MassIVE data.
    
    Parameters
    ----------
    massive_data : Dict[str, Any]
        Raw MassIVE data
    experiments : List[Dict[str, Any]]
        List of experiment objects
    massive_id : str
        MassIVE identifier
        
    Returns
    -------
    List[Dict[str, Any]]
        List of dataset objects compatible with the Dataset model
    """
    datasets = []
    
    author = "Unknown"
    for contact_group in massive_data.get("contacts", []):
        for item in contact_group:
            if item.get("name") == "contact name" and "value" in item:
                author = item["value"]
                break
    
    keywords = ["proteomics", "mass spectrometry"]
    for keyword_item in massive_data.get("keywords", []):
        if "value" in keyword_item:
            kw_value = keyword_item["value"]
            if kw_value and kw_value not in keywords:
                if " " in kw_value and len(kw_value) > 15:
                    keywords.extend(kw_value.split())
                else:
                    keywords.append(kw_value)
    
    publication = None
    for pub in massive_data.get("publications", []):
        if pub.get("name") != "Dataset with no associated published manuscript":
            publication = pub.get("value", "")
    
    date_published = datetime.now().isoformat()
    
    content_url = None
    for link in massive_data.get("datasetLink", []):
        if link.get("accession") == "MS:1002852" and "value" in link:  # Dataset FTP location
            content_url = link["value"]
            break
    
    if not content_url:
        for link in massive_data.get("datasetLink", []):
            if link.get("accession") == "MS:1002488" and "value" in link:  # MassIVE dataset URI
                content_url = link["value"]
                break
    
    dataset_types = []
    
    for mod in massive_data.get("modifications", []):
        mod_name = mod.get("name", "")
        if mod_name and mod_name not in dataset_types:
            dataset_types.append(f"{mod_name} Data")
    
    default_types = ["Raw Data", "Processed Results", "Protein Identifications", "Peptide Spectra"]
    for default_type in default_types:
        if default_type not in dataset_types:
            dataset_types.append(default_type)
    
    for experiment in experiments:
        experiment_id = experiment["@id"]
        experiment_name = experiment["name"]
        experiment_type = experiment["experimentType"]

        
        for i, dataset_type in enumerate(dataset_types, 1):

            dataset_id = f"ark:59852/dataset-{massive_id.lower()}-{experiment_type.lower().replace(' ', '-')}-{i}"
            
            dataset_name = f"{dataset_type} from {experiment_name}"
            
            description = f"{dataset_type} generated from {experiment_name} in MassIVE dataset {massive_id}."
            if massive_data.get("summary"):
                summary = massive_data.get("summary")
                if len(summary) > 100:
                    description = f"{dataset_type} from experiment described as: {summary[:150]}..."
                else:
                    description = f"{dataset_type} from experiment described as: {summary}"
            
            file_format = "raw"
            if "Raw" in dataset_type:
                file_format = "raw"
            elif "Processed" in dataset_type:
                file_format = "mzIdentML"
            elif "Protein" in dataset_type:
                file_format = "fasta"
            elif "Peptide" in dataset_type:
                file_format = "mgf"
            elif "Spectra" in dataset_type:
                file_format = "mzML"
            
            dataset = {
                "@id": dataset_id,
                "name": dataset_name,
                "@type": "https://w3id.org/EVI#Dataset",
                "author": author,
                "datePublished": date_published,
                "version": "1.0",
                "description": description,
                "keywords": keywords,
                "associatedPublication": publication,
                "format": file_format,
                "contentUrl": content_url,
                "generatedBy": [{"@id": experiment_id}],
                "additionalProperty": [
                    {
                        "@type": "PropertyValue",
                        "propertyID": "dataset_id",
                        "value": massive_id
                    },
                    {
                        "@type": "PropertyValue",
                        "propertyID": "dataset_type",
                        "value": dataset_type
                    }
                ]
            }
            
            datasets.append(dataset)
    
    return datasets

def update_experiments_with_datasets(experiments: List[Dict[str, Any]], datasets: List[Dict[str, Any]]) -> None:
    """
    Update experiments with generated datasets.
    
    Parameters
    ----------
    experiments : List[Dict[str, Any]]
        List of experiment objects
    datasets : List[Dict[str, Any]]
        List of dataset objects
    """
    experiment_to_datasets = {}
    for dataset in datasets:
        generated_by = dataset.get("generatedBy", [])
        for generator in generated_by:
            experiment_id = generator.get("@id")
            if experiment_id:
                if experiment_id not in experiment_to_datasets:
                    experiment_to_datasets[experiment_id] = []
                experiment_to_datasets[experiment_id].append(dataset)
    
    for experiment in experiments:
        experiment_id = experiment["@id"]
        if experiment_id in experiment_to_datasets:
            experiment["generated"] = [{"@id": dataset["@id"]} for dataset in experiment_to_datasets[experiment_id]]

def create_rocrate_from_massive(massive_id, output_path, author="Unknown"):
    """
    Create an RO-Crate from a MassIVE dataset
    
    Parameters
    ----------
    massive_id : str
        MassIVE identifier (e.g., MSV000097523)
    output_path : str
        Output path for the RO-Crate
    author : str, optional
        Author name for the RO-Crate
        
    Returns
    -------
    str
        Path to the created RO-Crate
    """

    results = from_massive(massive_id)
    
    raw_data = results["raw_data"]
    
    output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)
    
    crate_name = raw_data.get("title", f"MassIVE Dataset {massive_id}")
    crate_description = raw_data.get("summary", f"Proteomics data from MassIVE dataset {massive_id}")
    keywords = ["proteomics", "mass spectrometry", "MassIVE"]
    for keyword_item in raw_data.get("keywords", []):
        if "value" in keyword_item:
            kw_value = keyword_item["value"]
            if kw_value and kw_value not in keywords:
                keywords.append(kw_value)
    
    publication = ""
    for pub in raw_data.get("publications", []):
        if pub.get("name") != "Dataset with no associated published manuscript":
            publication = pub.get("value", "")
    
    dataset_url = ""
    for link in raw_data.get("datasetLink", []):
        if link.get("accession") == "MS:1002488":  # MassIVE dataset URI
            dataset_url = link.get("value", "")
    
    try:
        from fairscape_cli.models.rocrate import GenerateROCrate, AppendCrate
        from fairscape_models.experiment import Experiment
        from fairscape_models.sample import Sample
        from fairscape_models.instrument import Instrument
        from pathlib import Path
        
        rocrate_data = GenerateROCrate(
            path=output_path,
            guid="",  # Will be generated
            name=crate_name,
            description=crate_description,
            keywords=keywords,
            license="https://creativecommons.org/publicdomain/zero/1.0/",
            hasPart=[],
            author=author,
            datePublished=datetime.now().isoformat(),
            associatedPublication=publication,
            isPartOf=[],
            version="1.0",
            sameAs=dataset_url
        )
        
        samples = [Sample.model_validate(s) for s in results["samples"]]
        instruments = [Instrument.model_validate(i) for i in results["instruments"]]
        experiments = [Experiment.model_validate(e) for e in results["experiments"]]
        
        AppendCrate(output_path, samples)
        
        AppendCrate(output_path, instruments)
        
        AppendCrate(output_path, experiments)
        
        return str(output_path / "ro-crate-metadata.json")
    
    except ImportError as e:
        print(f"Error: {e}")
        print("Make sure fairscape_cli and fairscape_models are installed")
        return None