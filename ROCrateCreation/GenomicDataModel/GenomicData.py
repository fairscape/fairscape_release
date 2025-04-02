from pydantic import BaseModel
import json
import pathlib
from datetime import datetime
import os
import csv
import yaml
import pathlib

from ROCrateCreation.GenomicDataModel.project import Project
from ROCrateCreation.GenomicDataModel.sample import Samples, Sample
from ROCrateCreation.GenomicDataModel.experiment import Experiments, Experiment
from ROCrateCreation.GenomicDataModel.output import Outputs, Output, OutputFile
from ROCrateCreation.GenomicDataModel.cell_line_api import get_cell_line_entity
from ROCrateCreation.GenomicDataModel.bioproject_fetcher import fetch_bioproject_data

from fairscape_cli.models.rocrate import GenerateROCrate, AppendCrate
from fairscape_cli.models.dataset import GenerateDataset
from fairscape_cli.models.experiment import GenerateExperiment
from fairscape_cli.models.instrument import GenerateInstrument
from fairscape_cli.models.sample import GenerateSample

class GenomicData(BaseModel):
    project: Project
    samples: Samples
    experiments: Experiments
    outputs: Outputs
    
    def to_pep(self, output_dir: str) -> str:
        """
        Convert GenomicData to a Portable Encapsulated Project (PEP).
        
        Creates a project_config.yaml, samples.csv, and subsamples.csv in the specified directory.
        
        Args:
            output_dir: Output directory for PEP files
            
        Returns:
            Path to the created project_config.yaml file
        """
        
        output_path = pathlib.Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        project = self.project
        
        project_config = {
            "name": project.accession.lower(),
            "pep_version": "2.1.0",
            "sample_table": f"{project.accession}_samples.csv",
            "subsample_table": f"{project.accession}_subsamples.csv",
            "experiment_metadata": {
                "series_contact_institute": project.organization_name,
                "series_bioproject_accession": project.accession,
                "series_last_update_date": project.release_date.split("T")[0] if "T" in project.release_date else project.release_date,
                "series_overall_design": project.description,
                "series_platform_organism": project.organism_name,
                "series_platform_taxid": project.organism_taxID,
                "series_relation": f"BioProject: https://www.ncbi.nlm.nih.gov/bioproject/{project.accession}",
                "series_sample_id": " + ".join([sample.accession for sample in self.samples.items]),
                "series_sample_organism": project.organism_name,
                "series_sample_taxid": project.organism_taxID,
                "series_submission_date": project.submitted_date,
                "series_summary": project.description,
                "series_title": project.title,
                "series_type": f"Expression profiling by {project.method}"
            },
            "description": f"Data from {project.archive} {project.accession} - {project.title}"
        }
        
        config_path = output_path / "project_config.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(project_config, f, default_flow_style=False, sort_keys=False)
        
        # Map experiments to samples and outputs
        sample_to_experiment_map = {}
        for experiment in self.experiments.items:
            if experiment.sample_ref not in sample_to_experiment_map:
                sample_to_experiment_map[experiment.sample_ref] = []
            sample_to_experiment_map[experiment.sample_ref].append(experiment)
        
        experiment_to_output_map = {}
        for output in self.outputs.items:
            if output.experiment_ref not in experiment_to_output_map:
                experiment_to_output_map[output.experiment_ref] = []
            experiment_to_output_map[output.experiment_ref].append(output)
        

        samples_data = []        
        for sample in self.samples.items:
            sex = sample.attributes.get("sex", "")
            cell_type = sample.attributes.get("cell_type", "")
            tissue_type = sample.attributes.get("tissue_type", "")
            
            row = {
                "sample_name": sample.accession.lower().replace("-", "_"),
                "protocol": "", 
                "organism": sample.scientific_name,
                "sample_title": sample.title or f"{sample.scientific_name} {sample.accession}",
                "sample_accession": sample.accession,
                "sample_status": f"Public on {project.release_date.split('T')[0] if 'T' in project.release_date else project.release_date}",
                "sample_submission_date": project.submitted_date,
                "sample_last_update_date": project.release_date.split("T")[0] if "T" in project.release_date else project.release_date,
                "sample_type": "SRA",
                "sample_channel_count": "1",
                "sample_source_name_ch1": tissue_type or cell_type or sample.scientific_name,
                "sample_organism_ch1": sample.scientific_name,
                "sample_taxid_ch1": sample.taxon_id,
                "sample_contact_institute": project.organization_name,
                "sample_series_id": project.accession,
                "gsm_id": sample.accession,
                "sex": sex,
                "tissue": tissue_type,
                "celltype": cell_type,
                "treatment": "",  
                "biosample": f"https://www.ncbi.nlm.nih.gov/biosample/{sample.accession}"
            }
            
            if sample.study_accession:
                row["study_accession"] = sample.study_accession
                row["study_title"] = sample.study_title or ""
                row["study_center_name"] = sample.study_center_name or ""
                row["study_abstract"] = sample.study_abstract or ""
                row["study_description"] = sample.study_description or ""
            
            experiments = sample_to_experiment_map.get(sample.accession, [])
            if experiments:
                first_exp = experiments[0]
                row["protocol"] = first_exp.library_strategy
            
            for key, value in sample.attributes.items():
                if key not in row:
                    row[key] = value
            
            samples_data.append(row)
        
        subsamples_data = []
        
        for sample in self.samples.items:
            sample_name = sample.accession.lower().replace("-", "_")
            
            experiments = sample_to_experiment_map.get(sample.accession, [])
            
            for experiment in experiments:
                outputs = experiment_to_output_map.get(experiment.accession, [])
                
                for output in outputs:
                    subsample_name = output.accession.lower().replace("-", "_")
                    
                    row = {
                        "sample_name": sample_name,  
                        "subsample_name": subsample_name,
                        "srr": output.accession,  
                        "srx": experiment.accession,
                        "total_spots": output.total_spots,
                        "total_bases": output.total_bases,
                        "size": output.size,
                        "published": output.published,
                        "nreads": output.nreads,
                        "nspots": output.nspots,
                        "a_count": output.a_count,
                        "c_count": output.c_count,
                        "g_count": output.g_count,
                        "t_count": output.t_count,
                        "n_count": output.n_count,
                        
                        # Experiment metadata
                        "read_type": experiment.library_layout,
                        "data_source": experiment.platform_type,
                        "library_name": experiment.library_name,
                        "library_strategy": experiment.library_strategy,
                        "library_source": experiment.library_source,
                        "library_selection": experiment.library_selection,
                        "library_layout": experiment.library_layout,
                        "nominal_length": experiment.nominal_length,
                        "instrument_model": experiment.instrument_model,
                        "platform_type": experiment.platform_type,
                        "experiment_title": experiment.title
                    }
                    
                    for i, file in enumerate(output.files, 1):
                        row[f"file_{i}"] = file.url
                        row[f"file_{i}_md5"] = file.md5
                        row[f"file_{i}_size"] = file.size
                        row[f"file_{i}_date"] = file.date
                    
                    subsamples_data.append(row)
        
        samples_csv_path = output_path / f"{project.accession}_samples.csv"
        
        if samples_data:
            fieldnames = set()
            for row in samples_data:
                fieldnames.update(row.keys())
            
            important_fields = [
                "sample_name", "protocol", "organism", "sample_title", 
                "sample_accession", "gsm_id"
            ]
            
            # Create the ordered fieldnames list
            ordered_fieldnames = [f for f in important_fields if f in fieldnames]
            ordered_fieldnames.extend([f for f in sorted(fieldnames) if f not in ordered_fieldnames])
            
            with open(samples_csv_path, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=ordered_fieldnames)
                writer.writeheader()
                writer.writerows(samples_data)
        
        subsamples_csv_path = output_path / f"{project.accession}_subsamples.csv"
        
        if subsamples_data:
            fieldnames = set()
            for row in subsamples_data:
                fieldnames.update(row.keys())
            
            important_fields = [
                "sample_name", "subsample_name", "srr", "srx", "library_strategy", 
                "library_source", "library_selection", "instrument_model"
            ]
            
            ordered_fieldnames = [f for f in important_fields if f in fieldnames]
            ordered_fieldnames.extend([f for f in sorted(fieldnames) if f not in ordered_fieldnames])
            
            with open(subsamples_csv_path, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=ordered_fieldnames)
                writer.writeheader()
                writer.writerows(subsamples_data)
        
        return str(config_path)
        
    def to_rocrate(self, output_dir: str) -> str:
        """
        Convert GenomicData to an RO-Crate.
        
        Args:
            output_dir: Output directory for RO-Crate
            
        Returns:
            GUID of the created RO-Crate
        """
        
        output_path = pathlib.Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        bioproject = self.project
        
        crate_name = bioproject.title
        crate_description = bioproject.description
        
        crate_keywords = ["bioproject", "bioinformatics"]
        if bioproject.organism_name:
            crate_keywords.append(bioproject.organism_name)
        
        crate_keywords.extend([dt.replace('e', '') for dt in bioproject.data_types])
        if bioproject.project_data_type:
            crate_keywords.append(bioproject.project_data_type)
        
        rocrate_data = GenerateROCrate(
            path=output_path,
            guid="",
            name=crate_name,
            description=crate_description,
            keywords=crate_keywords,
            license="https://creativecommons.org/publicdomain/zero/1.0/",
            hasPart=[],
            author="Bioproject Contributors",
            datePublished=datetime.now().isoformat(),
            associatedPublication="",
            isPartOf=[],
            version="1.0",
            url=f"https://www.ncbi.nlm.nih.gov/bioproject/{bioproject.accession}"
        )
        
        metadata_path = output_path / "ro-crate-metadata.json"
        with open(metadata_path, 'r') as f:
            crate_data = json.load(f)
        
        id_mapping = {}
        cell_line_entities = {}
        
        sample_guids = []
        for sample in self.samples.items:
            accession = sample.accession
            
            cell_line = None
            for attr_name in ['cell_line', 'cell line', 'cell_line_name']:
                if attr_name in sample.attributes:
                    cell_line = sample.attributes[attr_name]
                    break
            
            cell_line_entity = None
            keywords = ["biosample", sample.scientific_name]
            #If cell line given look it up on cellasasrous
            if cell_line:
                keywords.append(cell_line)
                try:
                    cell_line_entity = get_cell_line_entity(cell_line)
                    if cell_line_entity:
                        cell_line_entities[cell_line] = cell_line_entity
                        cell_line_file = output_path / f"cell_line_{cell_line.replace(' ', '_')}.json"
                        with open(cell_line_file, 'w') as f:
                            json.dump(cell_line_entity, f, indent=2)
                except Exception:
                    pass
            
            sample_data = {
                "guid": None,
                "accession": accession,
                "name": sample.title or "",
                "taxon_id": sample.taxon_id,
                "author": "Bioproject Contributors",
                "description": sample.title or f"Sampple {accession}",
                "keywords": keywords,
                "version": "1.0",
                "contentUrl":f"https://www.ncbi.nlm.nih.gov/biosample/{accession}",
            }
            additional_properties = []

            additional_properties.append({
                "@type": "PropertyValue",
                "propertyID": "accession",
                "value": accession
            })
            additional_properties.append({
                "@type": "PropertyValue",
                "propertyID": "scientific_name",
                "value": sample.scientific_name
            })
            additional_properties.append({
                "@type": "PropertyValue",
                "propertyID": "taxon_id",
                "value": sample.taxon_id
            })

            if cell_line_entity:
                sample_data["cellLineReference"] = {"@id": cell_line_entity["@id"]}
            else:
                for attr_name, attr_value in sample.attributes.items():
                    additional_properties.append({
                        "@type": "PropertyValue",
                        "propertyID": attr_name,
                        "value": attr_value
                    })

            sample_data["additionalProperty"] = additional_properties

            if cell_line_entity:
                sample_data["cell_line_reference"] = {"@id": cell_line_entity["@id"]}
                sample_data["attributes"] = {}
            else:
                sample_data["attributes"] = sample.attributes
                
            sample = GenerateSample(**sample_data)
            
            id_mapping[accession] = sample.guid
            sample_guids.append(sample.guid)
            
            AppendCrate(pathlib.Path(output_path), [sample])
        
        instrument_software_guids = {}
        experiment_objects = {}
        experiment_guids = {}
        
        for experiment in self.experiments.items:
            accession = experiment.accession
            title = experiment.title
            

            instrument_model = experiment.instrument_model
            platform_type = experiment.platform_type
            
            instrument_key = f"{platform_type}_{instrument_model}"
            if instrument_key not in instrument_software_guids:
                instrument_software = GenerateInstrument(
                    guid=None,
                    name=instrument_model,
                    manufacturer=f"{platform_type}", 
                    model=instrument_model,
                    description=f"{instrument_model} used for sequencing",
                    serialNumber=None,
                    associatedPublication=None,
                    additionalDocumentation=None,
                    usedByExperiment=[],  
                    contentUrl=None, 
                    cratePath=output_path,
                )
                AppendCrate(pathlib.Path(output_path), [instrument_software])
                instrument_software_guids[instrument_key] = instrument_software.guid
            
            instrument_software_guid = instrument_software_guids[instrument_key]
            
            sample_ref = experiment.sample_ref
            sample_guid = id_mapping.get(sample_ref, None)
            used_samples = [sample_guid] if sample_guid else []
            

            experiment_properties = []

            experiment_properties.append({
                "@type": "PropertyValue",
                "propertyID": "accession",
                "value": accession
            })
            experiment_properties.append({
                "@type": "PropertyValue",
                "propertyID": "library_name",
                "value": experiment.library_name
            })
            experiment_properties.append({
                "@type": "PropertyValue",
                "propertyID": "library_strategy",
                "value": experiment.library_strategy
            })
            experiment_properties.append({
                "@type": "PropertyValue",
                "propertyID": "library_source",
                "value": experiment.library_source
            })
            experiment_properties.append({
                "@type": "PropertyValue",
                "propertyID": "library_selection",
                "value": experiment.library_selection
            })
            experiment_properties.append({
                "@type": "PropertyValue",
                "propertyID": "library_layout",
                "value": experiment.library_layout
            })
            experiment_properties.append({
                "@type": "PropertyValue",
                "propertyID": "nominal_length",
                "value": experiment.nominal_length
            })

            experiment_instance = GenerateExperiment(
                guid=None,
                name=title,
                experimentType=experiment.library_strategy,
                runBy="Bioproject Contributors", 
                description=f"{title} using {instrument_model}",
                datePerformed=datetime.now().isoformat(),
                protocol=f"{experiment.library_strategy} protocol",
                usedInstrument=[instrument_software_guid] if instrument_software_guid else [],
                usedSample=used_samples,
                generated=[],
                associatedPublication=None,
                additionalDocumentation=None,
                additionalProperty=experiment_properties 
            )
            
            id_mapping[accession] = experiment_instance.guid
            experiment_guids[accession] = experiment_instance.guid
            experiment_objects[accession] = experiment_instance
        
        for output in self.outputs.items:
            accession = output.accession
            title = output.title
            experiment_ref = output.experiment_ref
            
            file_urls = [file.url for file in output.files if file.url]
            
            run_dataset = GenerateDataset(
                guid=None,
                url=None,
                author="Bioproject Contributors",
                name=title,
                description=f"Sequencing run {accession} from experiment {experiment_ref}",
                keywords=["sequencing", "run", output.total_spots],
                datePublished=output.published,
                version="1.0",
                associatedPublication=None,
                additionalDocumentation=None,
                format="sra",
                schema="",
                derivedFrom=[],
                usedBy=[],
                generatedBy=[experiment_guids.get(experiment_ref, "")],
                filepath=None,
                contentUrl=file_urls if file_urls else None,
                cratePath=output_path
            )
            
            id_mapping[accession] = run_dataset.guid
            AppendCrate(pathlib.Path(output_path), [run_dataset])
            
            if experiment_ref in experiment_guids:
                experiment_computation = experiment_objects.get(experiment_ref)
                if experiment_computation:
                    if not experiment_computation.generated:
                        experiment_computation.generated = []
                    experiment_computation.generated.append({"@id": run_dataset.guid})
        
        AppendCrate(pathlib.Path(output_path), list(experiment_objects.values()))
        
        if cell_line_entities:
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
            
            for entity in cell_line_entities.values():
                entity_id = entity["@id"]
                if not any(item.get('@id') == entity_id for item in metadata.get('@graph', [])):
                    metadata['@graph'].append(entity)
            
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)
        
        with open(metadata_path, 'r') as f:
            crate_data = json.load(f)
            root_id = crate_data["@graph"][1]["@id"]
        
        return root_id
    
    @classmethod
    def from_api(cls, accession: str, api_key: str = "", details_dir: str = "details") -> 'GenomicData':
        """
        Create a GenomicData model by fetching data directly from NCBI API.
        
        Args:
            accession: BioProject accession number (e.g., PRJDB2884)
            api_key: NCBI API key (optional, default provided)
            
        Returns:
            Populated GenomicData instance
        """
        
        data = fetch_bioproject_data(accession, api_key, details_dir)
        
        if not data:
            raise ValueError(f"Failed to fetch data for BioProject: {accession}")
            
        # Use the from_json method to convert to GenomicData
        return cls.from_json(data)

    @classmethod
    def from_json(cls, data: dict) -> 'GenomicData':
        """
        Create a GenomicData model from the JSON data structure.
        
        Args:
            data: Dictionary containing project, samples, studies, experiments, and outputs
            
        Returns:
            Populated GenomicData instance
        """
        sample_to_study_map = {}
        for experiment in data.get("experiments", []):
            sample_ref = experiment.get("sample_ref") or experiment.get("title", "")
            study_ref = experiment.get("study_ref") or experiment.get("title", "")
            sample_to_study_map[sample_ref] = study_ref
        
        studies_map = {}
        if data.get("studies"):
            studies_map = {study.get("accession") or study.get("title", ""): study for study in data.get("studies", [])}
        
        project_data = data.get("bioproject", {})
        project_type = project_data.get("project_type", {})
        target = project_type.get("target", {})
        organism = target.get("organism", {})
        submission = project_data.get("submission", {})
        organization = submission.get("organization", {})
        
        project = Project(
            id=project_data.get("id", ""),
            accession=project_data.get("accession") or project_data.get("title", ""),
            archive=project_data.get("archive", ""),
            organism_name=project_data.get("organism_name", ""),
            title=project_data.get("title", ""),
            description=project_data.get("description", ""),
            release_date=project_data.get("release_date", ""),
            
            target_capture=target.get("capture", ""),
            target_material=target.get("material", ""),
            target_sample_scope=target.get("sample_scope", ""),
            organism_species=organism.get("species", ""),
            organism_taxID=organism.get("taxID", ""),
            organism_supergroup=organism.get("supergroup", ""),
            method=project_type.get("method", ""),
            data_types=project_type.get("data_types", []),
            project_data_type=project_type.get("project_data_type", ""),
            
            submitted_date=submission.get("submitted", ""),
            organization_role=organization.get("role", ""),
            organization_type=organization.get("type", ""),
            organization_name=organization.get("name", ""),
            access=submission.get("access", "")
        )
        
        samples = []
        for biosample in data.get("biosamples", []):
            sample_ref = biosample.get("accession") or biosample.get("title") or biosample.get("scientific_name", "")
            study_ref = sample_to_study_map.get(sample_ref)
            study_data = studies_map.get(study_ref, {})
            
            sample = Sample(
                accession=biosample.get("accession") or biosample.get("title") or biosample.get("scientific_name", ""),
                title=biosample.get("title", ""),
                scientific_name=biosample.get("scientific_name", ""),
                taxon_id=biosample.get("taxon_id", ""),
                attributes=biosample.get("attributes", {}),
                
                study_accession=study_ref,
                study_center_name=study_data.get("center_name"),
                study_title=study_data.get("title"),
                study_abstract=study_data.get("abstract"),
                study_description=study_data.get("description")
            )
            samples.append(sample)
        
        experiments = []
        for exp in data.get("experiments", []):
            design = exp.get("design", {})
            platform = exp.get("platform", {})
            
            experiment = Experiment(
                accession=exp.get("accession") or exp.get("title", ""),
                title=exp.get("title", ""),
                study_ref=exp.get("study_ref") or exp.get("title", ""),
                sample_ref=exp.get("sample_ref") or exp.get("title", ""),
                
                library_name=design.get("library_name", ""),
                library_strategy=design.get("library_strategy", ""),
                library_source=design.get("library_source", ""),
                library_selection=design.get("library_selection", ""),
                library_layout=design.get("library_layout", ""),
                nominal_length=design.get("nominal_length", ""),
                
                platform_type=platform.get("type", ""),
                instrument_model=platform.get("instrument_model", "")
            )
            experiments.append(experiment)
        
        outputs = []
        for run in data.get("runs", []):
            base_composition = run.get("base_composition", {})
            
            output_files = [
                OutputFile(
                    filename=file.get("filename") or file.get("url", "").split("/")[-1] or "",
                    size=file.get("size", 0),
                    date=file.get("date", ""),
                    url=file.get("url", ""),
                    md5=file.get("md5", "")
                )
                for file in run.get("files", [])
            ]
            
            output = Output(
                accession=run.get("accession") or run.get("title", ""),
                title=run.get("title", ""),
                experiment_ref=run.get("experiment_ref") or run.get("title", ""),
                total_spots=run.get("total_spots", 0),
                total_bases=run.get("total_bases", 0),
                size=run.get("size", 0),
                published=run.get("published", ""),
                files=output_files,
                nreads=run.get("nreads", 0),
                nspots=run.get("nspots", 0),
                
                a_count=base_composition.get("A", 0),
                c_count=base_composition.get("C", 0),
                g_count=base_composition.get("G", 0),
                t_count=base_composition.get("T", 0),
                n_count=base_composition.get("N", 0)
            )
            outputs.append(output)
        
        return cls(
            project=project,
            samples=Samples(items=samples),
            experiments=Experiments(items=experiments),
            outputs=Outputs(items=outputs)
        )
    
    @classmethod
    def from_pep(cls, config_path: str) -> 'GenomicData':
        """
        Create a GenomicData model from a PEP (Portable Encapsulated Project).
        
        Args:
            config_path: Path to the project_config.yaml file
            
        Returns:
            Populated GenomicData instance
        """
        
        config_path = pathlib.Path(config_path)
        base_dir = config_path.parent
        
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        sample_file = base_dir / config.get('sample_table', '')
        subsample_file = base_dir / config.get('subsample_table', '')
        
        samples_data = []
        sample_map = {}
        
        if sample_file.exists():
            with open(sample_file, 'r', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    samples_data.append(row)
                    sample_map[row['sample_name']] = row
        
        subsamples_data = []
        
        if subsample_file.exists():
            with open(subsample_file, 'r', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    subsamples_data.append(row)
        
        experiment_metadata = config.get('experiment_metadata', {})
        
        project = Project(
            id=config.get('name', '').upper(),
            accession=experiment_metadata.get('series_bioproject_accession', config.get('name', '').upper()),
            archive=experiment_metadata.get('series_relation', '').split(':')[0] if 'series_relation' in experiment_metadata else 'Unknown',
            organism_name=experiment_metadata.get('series_sample_organism', ''),
            title=experiment_metadata.get('series_title', ''),
            description=experiment_metadata.get('series_summary', experiment_metadata.get('series_overall_design', '')),
            release_date=experiment_metadata.get('series_last_update_date', datetime.now().strftime('%Y-%m-%d')),
            
            organism_species=experiment_metadata.get('series_platform_taxid', ''),
            organism_taxID=experiment_metadata.get('series_platform_taxid', ''),
            method=experiment_metadata.get('series_type', '').replace('Expression profiling by ', ''),
            
            submitted_date=experiment_metadata.get('series_submission_date', ''),
            organization_role="owner",
            organization_type="center",
            organization_name=experiment_metadata.get('series_contact_institute', ''),
            access="public"
        )
        
        samples = []
        
        for sample_row in samples_data:
            sample_name = sample_row.get('sample_name', '')
            accession = sample_row.get('sample_accession', sample_row.get('gsm_id', sample_name))
            
            attributes = {}
            for key, value in sample_row.items():
                #Skip keys that are stored outside of attributes
                if key in ['sample_name', 'sample_accession', 'scientific_name', 'taxon_id', 
                          'title', 'accession', 'study_accession', 'study_title', 
                          'study_center_name', 'study_abstract', 'study_description']:
                    continue
                    
                if value and value != "":
                    attributes[key] = value
            
            sample = Sample(
                accession=accession,
                title=sample_row.get('title', sample_row.get('sample_title', '')),
                scientific_name=sample_row.get('scientific_name', sample_row.get('organism', sample_row.get('sample_organism_ch1', ''))),
                taxon_id=sample_row.get('taxon_id', sample_row.get('sample_taxid_ch1', '')),
                attributes=attributes,
                
                study_accession=sample_row.get('study_accession', ''),
                study_center_name=sample_row.get('study_center_name', ''),
                study_title=sample_row.get('study_title', ''),
                study_abstract=sample_row.get('study_abstract', ''),
                study_description=sample_row.get('study_description', '')
            )
            
            samples.append(sample)
        
        experiments = []
        experiment_map = {}
        
        for subsample_row in subsamples_data:
            sample_name = subsample_row.get('sample_name', '')
            experiment_accession = subsample_row.get('srx', '')
            
            if experiment_accession in experiment_map:
                continue
                
            sample_accession = ""
            for sample in samples:
                if sample.accession.lower().replace("-", "_") == sample_name:
                    sample_accession = sample.accession
                    break
            
            experiment = Experiment(
                accession=experiment_accession,
                title=subsample_row.get('experiment_title', ''),
                study_ref=subsample_row.get('study_accession', ''),
                sample_ref=sample_accession,
                
                library_name=subsample_row.get('library_name', ''),
                library_strategy=subsample_row.get('library_strategy', ''),
                library_source=subsample_row.get('library_source', ''),
                library_selection=subsample_row.get('library_selection', ''),
                library_layout=subsample_row.get('library_layout', subsample_row.get('read_type', '')),
                nominal_length=subsample_row.get('nominal_length', ''),
                
                platform_type=subsample_row.get('platform_type', subsample_row.get('data_source', '')),
                instrument_model=subsample_row.get('instrument_model', '')
            )
            
            experiments.append(experiment)
            experiment_map[experiment_accession] = experiment
        
        outputs = []
        
        for subsample_row in subsamples_data:
            output_accession = subsample_row.get('srr', '')
            experiment_accession = subsample_row.get('srx', '')
            
            output_files = []
            i = 1
            while f'file_{i}' in subsample_row:
                file_url = subsample_row.get(f'file_{i}', '')
                if file_url:
                    output_file = OutputFile(
                        filename=os.path.basename(file_url),
                        size=subsample_row.get(f'file_{i}_size', ''),
                        date=subsample_row.get(f'file_{i}_date', ''),
                        url=file_url,
                        md5=subsample_row.get(f'file_{i}_md5', '')
                    )
                    output_files.append(output_file)
                i += 1
            
            output = Output(
                accession=output_accession,
                title=subsample_row.get('subsample_name', ''),
                experiment_ref=experiment_accession,
                total_spots=subsample_row.get('total_spots', ''),
                total_bases=subsample_row.get('total_bases', ''),
                size=subsample_row.get('size', ''),
                published=subsample_row.get('published', ''),
                files=output_files,
                nreads=subsample_row.get('nreads', ''),
                nspots=subsample_row.get('nspots', ''),
                
                a_count=subsample_row.get('a_count', ''),
                c_count=subsample_row.get('c_count', ''),
                g_count=subsample_row.get('g_count', ''),
                t_count=subsample_row.get('t_count', ''),
                n_count=subsample_row.get('n_count', '')
            )
            
            outputs.append(output)
        
        genomic_data = cls(
            project=project,
            samples=Samples(items=samples),
            experiments=Experiments(items=experiments),
            outputs=Outputs(items=outputs)
        )
        
        return genomic_data

    @classmethod
    def from_rocrate(cls, metadata_path: str) -> 'GenomicData':
        """
        Create GenomicData from RO-Crate metadata.
        
        Args:
            metadata_path: Path to the ro-crate-metadata.json file
                
        Returns:
            Populated GenomicData instance
        """
        
        metadata_path = pathlib.Path(metadata_path)
        
        with open(metadata_path, 'r') as f:
            crate_data = json.load(f)
                
        graph = crate_data.get('@graph', [])
        
        entities_by_id = {}
        samples_list = []
        computations_list = []
        software_list = []
        datasets_list = []
        
        root_dataset = None
        
        for entity in graph:
            entity_id = entity.get('@id', '')
            entity_type = entity.get('@type', '')
            
            entities_by_id[entity_id] = entity
            
            if isinstance(entity_type, list):
                root_dataset = entity
            elif 'Sample' in entity_type:
                samples_list.append(entity)
            elif 'Computation' in entity_type:
                computations_list.append(entity)
            elif 'Software' in entity_type:
                software_list.append(entity)
            elif 'Dataset' in entity_type and entity_id != './':
                datasets_list.append(entity)
        
        project = Project(
            id=root_dataset.get('identifier', 'unknown'),
            accession=root_dataset.get('identifier', root_dataset.get('name', 'unknown')),
            archive=root_dataset.get('sdPublisher', {}).get('name', 'Unknown') if isinstance(root_dataset.get('sdPublisher'), dict) else 'Unknown',
            organism_name=next((k for k in root_dataset.get('keywords', []) if 'sapiens' in k or 'elegans' in k), ''),
            title=root_dataset.get('name', ''),
            description=root_dataset.get('description', ''),
            release_date=root_dataset.get('datePublished', datetime.now().strftime('%Y-%m-%d')),
            
            submitted_date=root_dataset.get('dateCreated', datetime.now().strftime('%Y-%m-%d')),
            organization_role="owner",
            organization_type="center",
            organization_name=root_dataset.get('creator', {}).get('name', '') if isinstance(root_dataset.get('creator'), dict) else '',
            access="public"
        )
        
        samples = []
        sample_id_to_accession = {}
        
        for sample_entity in samples_list:
            entity_id = sample_entity.get('@id', '')
            
            accession = sample_entity.get('accession', entity_id)
            
            attributes = {}
            for key, value in sample_entity.items():
                if key not in ['@id', '@type', 'accession', 'title', 'scientific_name', 'taxon_id', 'name', 'description', 'keywords', 'author', 'format', 'version']:
                    attributes[key] = str(value)
            
            sample = Sample(
                accession=accession,
                title=sample_entity.get('name', sample_entity.get('title', '')),
                scientific_name=sample_entity.get('scientific_name', ''),
                taxon_id=sample_entity.get('taxon_id', ''),
                attributes=attributes,
            )
            
            samples.append(sample)
            sample_id_to_accession[entity_id] = accession
        
        experiments = []
        computation_id_to_experiment = {}
        
        for computation in computations_list:
            entity_id = computation.get('@id', '')
            
            accession = computation.get('accession', f"exp-{entity_id.split('-')[-1]}")
            
            library_name = computation.get('library_name', '')
            library_strategy = computation.get('library_strategy', '')
            library_source = computation.get('library_source', '')
            library_selection = computation.get('library_selection', '')
            library_layout = computation.get('library_layout', '')
            nominal_length = computation.get('nominal_length', '')
            
            if not library_strategy:
                command = computation.get('command', '')
                if 'sequencing' in command.lower():
                    parts = command.split()
                    if len(parts) > 0:
                        library_strategy = parts[0]
                
                keywords = computation.get('keywords', [])
                if isinstance(keywords, list):
                    for keyword in keywords:
                        if not library_strategy and keyword in ['RNA-Seq', 'WGS', 'ChIP-Seq', 'Bisulfite-Seq']:
                            library_strategy = keyword
                        if not library_source and keyword in ['GENOMIC', 'TRANSCRIPTOMIC', 'METAGENOMIC']:
                            library_source = keyword
                        if not library_selection and keyword in ['PCR', 'RANDOM', 'Hybrid Selection']:
                            library_selection = keyword
                        if not library_layout and keyword in ['PAIRED', 'SINGLE']:
                            library_layout = keyword
            
            instrument_model = computation.get('instrument_model', '')
            platform_type = computation.get('platform_type', '')
            
            if not instrument_model or not platform_type:
                used_software_ids = []
                if 'usedSoftware' in computation:
                    if isinstance(computation['usedSoftware'], list):
                        used_software_ids = [sw.get('@id') if isinstance(sw, dict) else sw for sw in computation['usedSoftware']]
                    else:
                        used_software_id = computation['usedSoftware'].get('@id') if isinstance(computation['usedSoftware'], dict) else computation['usedSoftware']
                        used_software_ids = [used_software_id]
                
                for software_id in used_software_ids:
                    software = entities_by_id.get(software_id, {})
                    if not instrument_model and 'instrument_model' in software:
                        instrument_model = software.get('instrument_model', '')
                    elif not instrument_model and 'name' in software:
                        instrument_model = software.get('name', '')
                    
                    if not platform_type and 'platform_type' in software:
                        platform_type = software.get('platform_type', '')
                    elif not platform_type and 'keywords' in software:
                        keywords = software.get('keywords', [])
                        if isinstance(keywords, list):
                            platform_candidates = [k for k in keywords if k.upper() in ['ILLUMINA', 'PACBIO', 'NANOPORE', 'OXFORD']]
                            if platform_candidates:
                                platform_type = platform_candidates[0]
            
            sample_refs = []
            if 'usedDataset' in computation:
                if isinstance(computation['usedDataset'], list):
                    sample_refs = [sample_id_to_accession.get(ds.get('@id')) if isinstance(ds, dict) else sample_id_to_accession.get(ds) 
                                for ds in computation['usedDataset']]
                else:
                    sample_id = computation['usedDataset'].get('@id') if isinstance(computation['usedDataset'], dict) else computation['usedDataset']
                    if sample_id in sample_id_to_accession:
                        sample_refs = [sample_id_to_accession[sample_id]]
            
            for sample_ref in sample_refs:
                if not sample_ref:
                    continue
                    
                experiment = Experiment(
                    accession=accession,
                    title=computation.get('name', ''),
                    sample_ref=sample_ref,
                    
                    library_name=library_name,
                    library_strategy=library_strategy,
                    library_source=library_source,
                    library_selection=library_selection,
                    library_layout=library_layout,
                    nominal_length=nominal_length,
                    
                    platform_type=platform_type,
                    instrument_model=instrument_model
                )
                
                experiments.append(experiment)
                computation_id_to_experiment[entity_id] = experiment
        
        outputs = []
        for dataset in datasets_list:
            entity_id = dataset.get('@id', '')
            
            if entity_id == './' or 'Sample' in dataset.get('@type', ''):
                continue
            
            accession = dataset.get('accession', f"run-{entity_id.split('-')[-1]}")
            
            experiment_ref = ""
            if 'generatedBy' in dataset:
                if isinstance(dataset['generatedBy'], list):
                    for comp_ref in dataset['generatedBy']:
                        comp_id = comp_ref.get('@id') if isinstance(comp_ref, dict) else comp_ref
                        if comp_id in computation_id_to_experiment:
                            experiment_ref = computation_id_to_experiment[comp_id].accession
                            break
                else:
                    comp_id = dataset['generatedBy'].get('@id') if isinstance(dataset['generatedBy'], dict) else dataset['generatedBy']
                    if comp_id in computation_id_to_experiment:
                        experiment_ref = computation_id_to_experiment[comp_id].accession
            
            if not experiment_ref:
                continue
            
            output_files = []
            content_urls = dataset.get('contentUrl', [])
            if content_urls:
                if not isinstance(content_urls, list):
                    content_urls = [content_urls]
                    
                for url in content_urls:
                    output_file = OutputFile(
                        filename=os.path.basename(url) if isinstance(url, str) else '',
                        size=dataset.get('size', ""),
                        date=dataset.get('dateCreated', ''),
                        url=url,
                        md5=dataset.get('md5', "")
                    )
                    output_files.append(output_file)
            
            total_spots = dataset.get('total_spots', "")
            total_bases = dataset.get('total_bases', "")
            nreads = dataset.get('nreads', "")
            nspots = dataset.get('nspots', "")
            
            base_comp = dataset.get('base_composition', {})
            a_count = base_comp.get('A', "")
            c_count = base_comp.get('C', "")
            g_count = base_comp.get('G', "")
            t_count = base_comp.get('T', "")
            n_count = base_comp.get('N', "")
            
            output = Output(
                accession=accession,
                title=dataset.get('name', ''),
                experiment_ref=experiment_ref,
                total_spots=total_spots,
                total_bases=total_bases,
                size=dataset.get('size', ""),
                published=dataset.get('datePublished', ''),
                files=output_files,
                nreads=nreads,
                nspots=nspots,
                
                a_count=a_count,
                c_count=c_count,
                g_count=g_count,
                t_count=t_count,
                n_count=n_count
            )
            
            outputs.append(output)
        
        genomic_data = cls(
            project=project,
            samples=Samples(items=samples),
            experiments=Experiments(items=experiments),
            outputs=Outputs(items=outputs)
        )
        
        return genomic_data