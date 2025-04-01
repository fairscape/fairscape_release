# FAIRSCAPE Release Workflow

## Steps

### 1. Data Collection & Preparation

- **Input:** Raw data from lab groups, metadata from repositories (NIH BioProject, PRIDE/Massive), or manifest files
- **Tools Available:** 1/2 Baked HTML Forms to generate genomic metadata.
- **Tools Needed:** Forms need review based on perturb-seq need to add details for other modalities.

### 2. Generate Dataset RO-Crates

- **Input:** Prepared data with metadata
- **Tools Available:**
  - Tool for BioProject metadata → RO-Crate
  - General-purpose CLI for building crates
- **Tools Needed:**
  - Tool for PRIDE/Massive → RO-Crate
  - Tool to build RO-Crate from manifest files (with data from form above possibly)
  - Read from Box build best guess crate

### 3. Create Release RO-Crate

- **Input:** Individual dataset RO-Crates
- **Tools Available:** None
- **Tools Needed:**
  - CLI extension to create a "crate of crates"
  - Functionality to include datasheet metadata

### 4. Generate Datasheets

- **Input:** Release RO-Crate and individual dataset RO-Crates
- **Tools Available:** Python code for datasheet generation
- **Tools Needed:**
  - Iterate on datasheet (no new tool required).

### 5. Review Datasheets

- **Input:** Generated datasheets
- **Tools Available:** Email out pdf
- **Tools Needed:**
  - None, but plan for review google drive?

### 6. Mint DOIs

- **Input:** Finalized RO-Crates with approved metadata
- **Tools Available:** Basic DataCite script
- **Tools Needed:**
  - Better DataCite Script (build request from RO-Crate)

### 7. Upload to Dataverse

- **Input:** Complete release RO-Crate with DOIs
- **Tools Available:** Publish tool in FAIRSCAPE server
- **Tools Needed:**
  - Standalone scripts for Dataverse upload
  - One for metadata creation one for data. The server tool will run into problems on really large crates.

## Tool Development Priorities

1. **Release Crate Creator**

   - Add new command to CLI for creating a crate of crates
   - Include datasheet metadata functionality

2. **Release Repo Setup**

   - Centralized repository structure
   - Documentation for reproducible workflow
   - Script organization and integration

3. **DOI Management**

   - DataCite Script that builds request from RO-Crate
   - Add validation before DOI minting

4. **Dataverse Integration**

   - Create standalone script for Dataverse upload
   - Add upload confirmation and verification

5. **Data Collection Improvements** (Future Work)
   - Review and enhance HTML forms for perturb-seq
   - Add support for other modalities in metadata forms
   - Extend CLI for PRIDE/Massive metadata extraction
   - Create manifest-to-RO-Crate converter
