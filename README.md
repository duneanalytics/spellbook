![spellbook-logo@10x](https://user-images.githubusercontent.com/2520869/200791687-76f1bc4f-05d0-4384-a753-e3b5da0e7a4a.png#gh-light-mode-only)
![spellbook-logo-negative_10x](https://user-images.githubusercontent.com/2520869/200865128-426354af-8059-494d-83f7-46947aae271c.png#gh-dark-mode-only)

Welcome to [Spellbook](https://youtu.be/o7p0BNt7NHs). Cast a magical incantation to tame the blockchain.

## TL;DR

- **Before contributing:** Spellbook accepts selective contributions. To avoid wasted effort, please [raise a GitHub issue](#submitting-a-contribution) with the title prefix `[CONTRIBUTION]` describing your proposed changes before writing code. The Dune team will review and confirm if we'll accept the contribution.
  - **Exception:** Small bug fixes can be submitted directly as PRs without prior approval.
  - **Priority:** PRs from Dune enterprise customers receive priority review. 
  
  PRs from contributors who aren't customers are accepted on a case by case basis, depending on whether datasets benefit the broader community (as an example, contributions to `dex.trades` are welcome)

- Questions about how Spellbook works? Check the [docs](docs/) directory.
- Spellbook uses [sub-projects](#sub-projects) to organize different datasets (dex, nft, solana, etc.).
- Setup: Follow [dev environment setup](#setting-up-your-local-dev-environment) to get started with dbt locally.
- Questions? Join #spellbook on [Discord](https://discord.com/channels/757637422384283659/999683200563564655).
- Your spellbook contributions are your own IP. See [Contributor License Agreement](CLA.md) for details.

## 🚀 Enterprise Customers: Use Your Own dbt Project

**If you're a Dune enterprise customer, you don't need to contribute to Spellbook.** Instead, you can run your own data transformation projects directly on DuneSQL using the **dbt Connector**.

**What it is:** Run dbt projects on top of DuneSQL—read from any Dune dataset and write results into managed tables within your org namespace. All writes are metered and private by default, stored securely by Dune.

**Why it matters:**
- **No PR bottlenecks** – Ship on your own schedule without waiting for Spellbook reviews
- **Keep it private** – Your logic and data stay within your org, giving you full autonomy
- **Full dbt compatibility** – Use your existing dbt workflows and tooling

📖 **[Read the dbt Connector documentation](https://docs.dune.com/api-reference/connectors/dbt/overview#dbt-connector-overview)** to get started.

👉 **[Contact us](https://dune.com/enterprise)** for a demo and to get set up with your own dbt project.

## Table of Contents

- [Enterprise Customers: Use Your Own dbt Project](#-enterprise-customers-use-your-own-dbt-project)
- [Introduction](#introduction)
- [Sub-projects](#sub-projects)
- [How to contribute](#how-to-contribute)
  - [Submitting a contribution](#submitting-a-contribution)
  - [Testing your spell](#testing-your-spell)
- [Setting up your dev environment](#setting-up-your-local-dev-environment)
- [Using dbt to write spells](#how-to-use-dbt-to-create-spells)
        
## Introduction

Spellbook is Dune's interpretation layer, transforming raw blockchain data into clean, usable datasets. It's a [dbt](https://docs.getdbt.com/docs/introduction) project where each model is a SQL query that handles dependencies and builds tables from raw and decoded tables.

While historically community-driven, Spellbook now accepts selective contributions. The [docs](docs/) directory contains design principles and best practices for contributors.

## Sub-projects

Spellbook is organized into multiple dbt sub-projects within `dbt_subprojects/`:

- **`daily_spellbook`** - Default location for new spells, refreshed daily. Project-specific, standalone spells.
- **`hourly_spellbook`** - Promoted spells with more frequent refreshes. Requires Dune team approval.
- **`dex`** - DEX and DEX aggregator spells, including `dex.trades`.
- **`nft`** - NFT-related spells.
- **`solana`** - Solana-specific spells.
- **`tokens`** - Token metadata, transfers, and balances.

See [this discussion](https://github.com/duneanalytics/spellbook/discussions/6037) for more details.

## How to contribute

### Submitting a contribution

**Before writing code**, please create a [GitHub issue](https://github.com/duneanalytics/spellbook/issues) with the title prefix `[CONTRIBUTION]` describing your proposed changes. Include:
- High-level description of what you want to add/change
- Which sub-project(s) it affects (dex, nft, etc.)
- Why it would benefit the community

The Dune team will review and respond with whether we'll accept the contribution. This saves you from investing time in code that may not be merged.

**Exception:** Bug fixes can be submitted directly as PRs without prior approval. When reporting bugs, include:
- Link to block explorer showing expected value
- Dune query showing incorrect value
- Scale of impact (number of rows, affected USD volume)

**Priority:** Dune enterprise customers receive priority review.

### Testing your spell

Once you submit a PR, our CI pipeline tests it against Dune's engine. Query your test data using:

`test_schema.git_dunesql_{{commit_hash}}_{{table_name}}`

Find exact table names in the `dbt slim ci` action logs under `dbt run initial model(s)`. 

Test tables exist for ~24 hours. If your table doesn't exist, trigger the pipeline to run again.

Join #spellbook on [Discord](https://discord.com/invite/ErrzwBz) for help.

## Setting up your Local Dev Environment

**Prerequisites:**
- Fork and clone the repo ([GitHub guide](https://docs.github.com/en/get-started/quickstart/contributing-to-projects))
- Python 3.9+ ([installation guide](https://docs.python-guide.org/starting/installation/))
- [pip](https://pip.pypa.io/en/stable/installation/) and [pipenv](https://pypi.org/project/pipenv/)
- Windows users: Set `git config --global core.autocrlf true` for unix line endings

**Initial Installation:**

Navigate to the spellbook repo in your CLI:

```console
cd user/directory/github/spellbook
# Change this to wherever spellbook is stored locally on your machine.
```

Run the install command to create a pipenv:

```console
pipenv install
```

If the install fails due to Python version mismatch, check your version with `python --version`, then update the Python version in the Pipfile to match (must be at least 3.9). Run `pipenv install` again.

Activate the virtual environment:

```console
pipenv shell
```

Navigate to the appropriate sub-project:

```console
cd dbt_subprojects/<subproject_name>/
```

Each subproject has its own dbt project file with varying configs. Run the following commands:

```console
dbt clean # cleans up the project
dbt deps # pull the dependencies
dbt compile
```

`dbt compile` converts JINJA/SQL templates into plain SQL in the `target/` folder, which you can test on dune.com.

Each subproject includes a `profiles.yml` file that tells dbt how to run commands. You **must** be in the subproject root directory to run `dbt compile` correctly.

## How to use dbt to create spells

**Key concepts:**

- **Refs:** Reference other dbt models using `{{ ref('model_name') }}` (use filename without .sql)
- **Sources:** Reference raw data using `{{ source('schema', 'table') }}`
- **Tests:** Add `unique` and `not_null` tests to primary keys in `schema.yml` files
- **Descriptions:** Document tables and columns in `schema.yml` to help others

**Example schema.yml:**

```yaml
models:
  - name: 1inch_ethereum
    description: "Trades on 1inch, a DEX aggregator"
    columns:
      - name: tx_hash
        description: "Table primary key"
        data_tests:
          - unique
          - not_null

sources:
  - name: ethereum
    freshness:
      warn_after: { count: 12, period: hour }
    tables:
      - name: traces
```
**dbt Resources:**
- [dbt docs](https://docs.getdbt.com/docs/introduction)
- [dbt Discourse](https://discourse.getdbt.com/)
- [dbt Slack](https://getdbt.com/community/join-the-community/)
