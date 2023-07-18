# Spellbook on DuneSQL is here 🪄
It's time. Spellbook is ready to accept PRs to build spells on the DuneSQL engine!  

For any questions or help from Dune team, please use the open GH discussion [here](https://github.com/duneanalytics/spellbook/discussions/3558).

## Initial step(s)
We can continue to safely use the setup instructions below in the new DuneSQL world: [Setting up your dev environment](#setting-up-your-local-dev-environment).  

To be safe, it's recommended in your local setup to:
- exit any existing running python environment(s)
- ensure you are in spellbook root directory
- rerun `pipenv install`
- rerun `pipenv shell` to re-enter the environment

The process to compile code locally, grab from target directory & run on the Dune app remains the same.  

Ensure you are still in Spellbook root directory, then run the following command:

```console
dbt compile
```

A recent change in the new setup was to include a `profiles.yml` file, which helps tell dbt how to run commands. The profile is located in the root directory [here](https://github.com/duneanalytics/spellbook/blob/main/profiles.yml). This should never need modified, unless done intentionally by the Dune team.  
Due to the `profiles.yml` file being stored in the root directory, this is why users **must** be in the root directory on the command line to run `dbt compile`.

## High-level overview of Spellbook current state
As we enter the phase of building on DuneSQL, we will continue to run spells in parallel on both the legacy engine (spark) and the new engine (DuneSQL).  
**Note:** the below processes will simplify post-migration and we will eventually deprecate spark engine and all legacy files.
- the single most important piece of code to help differentiate engines is through usage of dbt tags, specifically a `tag:dunesql` value applied in each spell's config block when ready to run on DuneSQL -- more on this exact setup below.
- the `dbt slim ci` gh action attached to two PR's will show two jobs: one for spark, one for DuneSQL -- you will notice depending on how dbt tags are used in spell config, one of the two engines will skip all steps to ignore running there.
- the process to contribute will differ between net new spells & modifying/migrating existing spells -- details for each process [here](#how-can-i-contribute-spells-on-dunesql).

## How can I contribute spells on DuneSQL?
The process will differ for new spells vs. migrating existing spells.

### How can I tell the status of a particular spell?
Here are a few things to look for, when looking for spells in the repo:
- all spells have two files
  - one with `_legacy.sql` suffix
    - the legacy file refers to **spark** engine, as we are looking to deprecate once fully migrated
    - all legacy files contain a `tags = ['legacy']` to help our orchestration engines determine spark engine
  - one without the legacy suffix
    - these files refer to **dunesql** engine
    - all of these files will either contain a tag for dunesql or not (`tags = ['dunesql']`)
      - if there is a tag present, the spell has been successfully migrated to dunesql syntax and is ready to run on the dunesql engine
      - if there is no tag present, the spell is sitting idle and waiting for migration and will not run on any engine until migrated and tag is added

### New spells
If creating a new spell, follow the below steps:
- Follow the same process from the spark engine setup
  - SQL file for spell, YAML files for source & schema files, seeds & tests as needed
- add tag for DuneSQL in spell config block
  - `tags = ['dunesql'],` -- this is vital for orchestration and testing on the correct engine. If `tags` property already exists in the spell, then simply append new value after a comma: `tags = ['static', 'dunesql'],`
- ensure alias property follows the below format:
  - `alias = alias('blocks'),`
- PR CI tests will check for this `tag:dunesql` applied and run on spark (`tag:legacy`) or dunesql dependent on if tag exists. the opposite engine will run too, but all steps should have no output and succeed.  
  - the logs of the CI test gh action can still be used to grab table names and query on dune app for ~24 hours – be sure to query on the engine you modify!
### Existing spells
If modifying existing spells which haven't been migrated to DuneSQL yet, it is recommended to migrate at the same time.  

Steps to migrate:
- Find the spell SQL file to translate in the repo
  - **note:** you can safely ignore the _legacy file, as they are intended to maintain the spark syntax (unless you need to alter logic in spark spell for bug fixes)
    - previously, it was requested for users to generate the _legacy files in the migration process, but the Dune team has gone ahead and universally built the legacy files for simplicity in the process of migration
  - add tag for DuneSQL in spell config block
    - `tags = ['dunesql'],` -- this is vital for orchestration and testing on the correct engine. If `tags` property already exists in the spell, then simply append new value after a comma: `tags = ['static', 'dunesql'],`
  - update alias property to leverage the new alias macro -- Dune team will maintain this alias macro, it's simply to help differentiate engines & metastores
    - `alias = alias('blocks'),`
  - now it's time to translate the code within to DuneSQL syntax!
- to find downstream spells from modified upstream spells, the following can be run: `dbt ls --resource-type model --output name --select <insert spell name>+`
  - **future note:** when downstream spells are also migrated, we can double check to reference DuneSQL versioned spells. We will be working in a upstream --> downstream lineage path to full migration.
- PR CI tests will check for this `tag:dunesql` applied and run on spark (`tag:legacy`) or dunesql dependent on if tag exists. the opposite engine will run too, but all steps should have no output and succeed.  
  - the logs of the CI test gh action can still be used to grab table names and query on dune app for ~24 hours – be sure to query on the engine you modify!

![spellbook-logo@10x](https://user-images.githubusercontent.com/2520869/200791687-76f1bc4f-05d0-4384-a753-e3b5da0e7a4a.png#gh-light-mode-only)
![spellbook-logo-negative_10x](https://user-images.githubusercontent.com/2520869/200865128-426354af-8059-494d-83f7-46947aae271c.png#gh-dark-mode-only)

Welcome to [Spellbook](https://youtu.be/o7p0BNt7NHs). Cast a magical incantation to tame the blockchain.

## TL;DR
- Are you building something new? **Please make sure to open a Draft PR**, so we minimize duplicated work, and other wizards can help you if you need
- Don't know where to start? The docs below will guide you, but as a summary:
  - Want to make an incremental improvement to one of our spells? (add a new project, fix a bug you found), simply open a PR with your changes. 
    - Follow the guide for [Submitting a PR](), [Setting up your dev environment]() and [Using dbt to write spells]() if you find yourself lost.
    - Not sure how to start? Follow the walkthrough [here](https://dune.com/docs/data-tables/spellbook/contributing/).
    - Make sure to open a draft PR if you will work on your spell for longer than a few days, to avoid duplicated work
  - Do you want to get started building spells and you don't know what to build? Check [Issues]() to see what the community needs.
  - Check the Discussions section to see what problems the community is trying to solve (i.e. non-incremental changes) or propose your own!
- Have questions? Head over to #spellbook on our [discord](https://discord.com/channels/757637422384283659/999683200563564655) and the community will be happy to help out!
- Like with most OSS projects, your contributions to Spellbook are your own IP, you can find more details in the [Contributor License Agreement](https://github.com/duneanalytics/spellbook/edit/main/CLA.md)


## Table of Contents

- [Introduction](#introduction)
- [How to contribute](#ways-to-contribute-to-spellbook)
  - [Submitting a PR](#submitting-a-pr)
  - [Testing your Spell](#testing-your-spell)
  - [Connecting with other wizards](#connecting-with-other-wizards)
- [Setting up your dev environment](#setting-up-your-local-dev-environment)
- [Using dbt to write spells](#how-to-use-dbt-to-create-spells)


## Introduction

Spellbook is Dune's interpretation layer, built for and by the community.

Spellbook is a [dbt](https://docs.getdbt.com/docs/introduction) project. Each model is a simple SQL query with minor syntactic sugar (meant to capture dependencies and help build the resulting tables), and does a small part of the task of turning raw and decoded records into interpretable blockchain data.

Spellbook is built for and by the community, you are welcome to close any gaps that you find by sending a PR, creating issues to propose small changes or track bugs, or participate in discussions to help steer the future of this project.

## Ways to contribute to Spellbook

- **Build** spells - if you want to write code, simply clone the repo, write your code, and open a PR
  - If you already know what to build, there's no red tape to skip around, simply open a PR when you're ready. We advice opening draft PRs early, so we avoid duplication of efforts and you can get help from other wizards
  - If you don't know where to start, check out Issues for ideas. We're always looking for help fixing small bugs or implementing spells for small projects
- **Flag** gaps in spellbook - have you found a bug, or is there a project missing from one of the sectors that you'd like to add? You can create an [issue](https://github.com/duneanalytics/spellbook/issues) and bring other wizards to your aid.
  - **Bugs**: Found a record on a Spell that doesn't reflect chain data correctly? Please make sure you link to a block explorer showing the expected value, and a dune query showing the wrong value. If there's multiple records affected, any sense of scale (how many rows, affected USD volume) will also be very helpful.
- **Propose** changes to spellbook - [Discussions](https://github.com/duneanalytics/spellbook/discussions) are where we bring up, challenge and develop ideas to continue building spellbook. If you want to make a major change to a spell (e.g. major overhaul to a sector, launching a new sector, designing a new organic volume filter, etc.).

### Submitting a PR
Want to get right to work? Follow the guide [here](https://dune.com/docs/data-tables/spellbook/contributing/#7-steps-to-adding-a-spell) to get started.

### Testing your spell
You don't need a complex local setup to test spells against Dune's engine. Once you send a PR, our CI pipeline will run and test it, and, if the job finishes successfully, you'll be able to query the data your PR created directly from dune.com.

Simply write a query like you would for any of our live tables, and use the test schema to fetch the tables your PR created.

`test_schema.git_{{commit_hash}}_{{table_name}}` 

You can find the exact names easily by looking at the logs from the `dbt slim ci` action, under `dbt run initial model(s)`.

Please note: the test tables built in the CI pipeline will exist for ~24 hours. If your table doesn't exist, trigger the pipeline to run again and recreate the test table.

### Connecting with other wizards

We use Discord to connect with our community. Head over to spellbook channel on [Dune's Discord](https://discord.gg/dunecom) for questions or to ask for help with a particular PR. We encourage you to learn by doing, and leverage our vibrant community to help you get going.


## Setting up your Local Dev Environment

### Prerequisites

- Fork this repo and clone your fork locally. See Github's [guide](https://docs.github.com/en/get-started/quickstart/contributing-to-projects) on contributing to projects.
- We default to use unix (LF) line endings, windows users please set: `git config --global core.autocrlf true`. [more info](https://docs.github.com/en/get-started/getting-started-with-git/configuring-git-to-handle-line-endings)
- python 3.9 installed. Our recommendation is to follow the [Hitchhiker's Guide to Python](https://docs.python-guide.org/starting/installation/)
- [pip](https://pip.pypa.io/en/stable/installation/) installed
- [pipenv](https://pypi.org/project/pipenv/) installed
- paths for both pip and pipenv are set (this should happen automatically but sometimes does not). If you run into issues like "pipenv: command not found", try troubleshooting with the pip or pipenv documentation.

### Initial Installation

You can watch the video version of this if you scroll down a bit.

Navigate to the spellbook repo within your CLI (Command line interface).

```console
cd user\directory\github\spellbook
# Change this to wherever spellbook is stored locally on your machine.
```

Using the pipfile located in the spellbook repo, run the below install command to create a pipenv.

```console
pipenv install
```

If the install fails, one likely reason is our script looks for a static python version and the likelihood of an error for a wrong python version is pretty high. If that error occurs, check your python version with:

```console
py --version
```

Now use any text editor program to change the python version in the pipfile within the spellbook directory to your python version. You need to have at least python 3.9.
If you have changed the python version in the pipfile, run `pipenv install` again.

You are now ready to activate this project's virtual environment. Run the following command to enter the environment:

```console
pipenv shell
```

You have now created a virtual environment for this project. You can read more about virtual environments [here](https://realpython.com/pipenv-guide/).

To pull the dbt project dependencies run:

```console
dbt deps
```

Then, run the following command:

```console
dbt compile
```

dbt compile will compile the JINJA and SQL templated SQL into plain SQL which can be executed in the Dune UI. Your spellbook directory now has a folder named `target` containing plain SQL versions of all models in Dune. If you have made changes to the repo before completing all these actions, you can now be certain that at least the compile process works correctly, if there are big errors the compile process will not complete.
If you haven't made changes to the directory beforehand, you can now start adding, editing, or deleting files within the repository.
Afterwards, simply run `dbt compile` again once you are finished with your work in the directory and test the plain language sql queries on dune.com.

### Coming back

If you have done this installation on your machine once, to get back into dbt, simply navigate to the spellbook repo, run `pipenv shell`, and you can run `dbt compile` again.

### What did I just do?

You now have the ability to compile your dbt model statements and test statements into plain SQL. This allows you to test those queries on the usual dune.com environment and should therefore lead to a better experience while developing spells. Running the queries will immediately give you feedback on typos, logical errors, or mismatches.
This in turn will help us deploy these spells faster and avoid any potential mistakes.

## How to use dbt to create spells

There are a couple of new concepts to consider when making spells in dbt. The most common ones wizards will encounter are refs, sources, freshness, and tests.

In the body of each query, tables are referred to either as refs, ex `{{ ref('1inch_ethereum') }}` or sources, ex `{{ source('ethereum', 'traces') }}`. Refs refer to other dbt models and they should refer to the file name like `1inch_ethereum.sql`, even if the model itself is aliased. Sources refer to "raw" data or tables/views not generated by dbt. Using refs and sources allows us to automatically build dependency trees.

Sources and models are defined in schema.yml files where tests and other attributes are defined.

The best practice is to add tests unique and non_null tests to the primary key for every new model. Similarly, a freshness check should be added to every new source (although we will try not to re-test freshness if the source is used elsewhere).

Adding descriptions to tables and columns will help people find and use your tables.

```yaml
models:
  - name: 1inch_ethereum
    description: "Trades on 1inch, a DEX aggregator"
    columns:
      - name: tx_hash
        description: "Table primary key: a transaction hash (tx_hash) is a unique identifier for a transaction."
        tests:
          - unique
          - not_null

  sources:
  - name: ethereum
    freshness:
      warn_after: { count: 12, period: hour }
      error_after: { count: 24, period: hour }
    tables:
      - name: traces
        loaded_at_field: block_time
```

See links to more docs on dbt below.

### Generating and serving documentation:

To generate documentation and view it as a website, run the following commands:

- `dbt docs generate`
- `dbt docs serve`
  You must have set up dbt with `dbt init` but you don't need database credentials to run these commands.

See [dbt docs documentation](https://docs.getdbt.com/docs/building-a-dbt-project/documentation) for more information on
how to contribute to documentation.

As a preview, you can do [things](https://docs.getdbt.com/reference/resource-properties/description) like:

- Write simple one or many line descriptions of models or columns.
- Write longer descriptions as code blocks using markdown.
- Link to other models in your descriptions.
- Add images / project logos from the repo into descriptions.
- Use HTML in your description.

### DBT Resources:

- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://getdbt.com/community/join-the-community/) on Slack for live discussions and support
- Find [dbt events](https://getdbt.com/events/) near you
- Check out [the blog](https://getdbt.com/blog/) for the latest news on dbt's development and best practices
