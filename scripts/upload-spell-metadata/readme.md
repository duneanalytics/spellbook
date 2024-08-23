# upload-to-dune.py

## Overview

`upload-to-dune.py` collects metadata from models and sources, combines it into a CSV, and uploads it to Dune Analytics. This maintains an up-to-date record of Spellbook models and sources on Dune.

Before being able to run the script, you need to run the `collect_manifests.py` script first to generate the manifest files.

## Functionality

### Manifest Parsing

Extracts information from multiple JSON manifest files. Sources only get read ones, models all get read. If there is a table that is defined as both source and model, the model's information is used.

### CSV Creation

Creates a CSV file with the combined model and source information.

### Dune Upload

Uploads the CSV to Dune Analytics via API.

## Requirements

- Python 3.x
- Python packages: `json`, `csv`, `os`, `requests`, `dotenv`
- Dune Analytics API key (stored in a `.env` file as `DUNE_API_KEY`)
- Correct project structure with manifest files in `dbt_subprojects/manifests`

## How to Run

1. Ensure all requirements are met.
2. Navigate to the script's directory.
3. Run: `python upload-to-dune.py`
4. Check console output for upload status.

## Output

- CSV file: `combined_dbt_models_sources.csv`
- Console output showing CSV file size and response from Dune API after upload attempt.

| Column Name        | Description                                                                     |
| ------------------ | ------------------------------------------------------------------------------- |
| model_name         | The name of the dbt model e.g. `dex.trades`                                     |
| schema             | The database schema where the model is located e.g. `dex`                       |
| alias              | The alias of the model e.g. `trades`                                            |
| partition_by       | The column(s) used for partitioning the model, if applicable. empty for sources |
| column             | The name of a specific column in the model                                      |
| column_description | A description of the column's content or purpose defined in spellbook yml files |
| meta_origin        | The origin of the metadata for this model. Either `source` or `model`           |

## Troubleshooting

- Verify Python packages are installed.
- Check Dune API key is correct and set properly.
- Ensure manifest files are in the correct location and format.
