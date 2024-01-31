# Models

Models are simply SQL queries that transform raw data into a structured format suitable for analytics downstream on the Dune app. Think of models as the blueprint for how raw data is transformed into meaningful insights in easy-to-use end tables, i.e., spells.

## Naming Standards

### Directories

For larger sector spells:

- `models/_sector/<sector/schema_name>/<table_alias>/<blockchain>/platforms/`
  - **Example**: `models/_sector/dex/trades/arbitrum/platforms/`

  - **Note**: Spellbook uses ‘_’ prefix in the directory path to force it at the top, more of a convenience design for important spells.

### File Names

- `<project_version_blockchain_alias.sql>`
  - **Example**: `uniswap_v3_arbitrum_base_trades.sql`

  - **Note**: Not all files will contain a version, or a specific blockchain, especially when at cross-chain level.

## Source & Schema YML Files

### Source YML Files

**_Note:_** with the release of [sub-projects](https://github.com/duneanalytics/spellbook/discussions/5238), all sources were migrated to a [new directory](https://github.com/duneanalytics/spellbook/tree/main/sources) at the root of Spellbook. The intention is to centralize sources, so they can be reused across sub-projects.

Required for DBT to understand the source schema & table names. While there aren’t strict guidelines to follow on these source files, most include:

- Schema name of source
  - Typically base or decoded tables, but can be anything live on Dune.
  - **Note**: future state Spellbook will contain [sub-projects](https://github.com/duneanalytics/spellbook/discussions/5238), where spells in other sub-projects will be brought in as sources across projects
- Table name of source
- (optional) Column names
- (optional) Tests / descriptions on columns

DBT can generate docs based on source YML files. While not heavily used today, these can be handy for clean documentation.

### Schema YML Files

Required for DBT to understand the models within the project. Key areas within schema files:

- **Model name**
- **Model metadata**
  - Mostly used for DBT docs.
- **Model tags**
  - Helpful for searching the project, ‘dbt ls’ filters, DBT docs, etc.
- **Model description**
- **Tests**
  - Unique key tests to ensure data quality & no duplicate data.
  - Seeds tests to prove out development is as expected, think about test-driven development – seeds are hardcoded to expected result, developed model output data should match the seed.
  - …other test case examples throughout Spellbook.
## **Model materialization**
  - **View**
    - Spellbook default, if not overwritten in model config block. Views will not physically process & store additional data. They will contain the SQL query and run fully on each execution in the Dune app.
  - **Incremental**
    - Best use case for spells which need up-to-date data refreshes most often. Incremental models will refresh every ~1 hour on average.
  - **Table**
    - For spells which don’t need to be updated as frequently, or contains data which isn’t frequently updated at the source, tables are the next option. Tables will perform a full refresh 1x/day.