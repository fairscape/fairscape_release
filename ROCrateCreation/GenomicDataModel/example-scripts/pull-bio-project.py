import json
from GenomicData import GenomicData

API_KEY = "b5842d8d17966b13241247e793b879532d07"
accession = "PRJDB2884"
acession = "PRJNA34129"

# Create the GenomicData model
genomic_data = GenomicData.from_api(accession, API_KEY)

genomic_data.to_rocrate('./ro-crate-metadata')
genomic_data.to_pep('./pep-metadata')

# genomic_data2 = GenomicData.from_pep('./pep-metadata/project_config.yaml')
# genomic_data2.to_rocrate('./ro-crate-metadata2')

# genomic_data3 = GenomicData.from_rocrate('./ro-crate-metadata/ro-crate-metadata.json')
# genomic_data3.to_pep('./pep-metadata2')
