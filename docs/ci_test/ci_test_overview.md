# CI Tests as GH Workflows in Spellbook

Any time a PR is opened in Spellbook, there are a few GH workflows which automatically run on each commit. The `dbt slim ci` workflow kicks off the continuous integration (CI) tests, which are required to pass in order to prepare for final merge into the main branch. The code which runs the CI workflow can be found [here](/.github/workflows/dbt_slim_ci.yml), but in general, the steps included are:

- Setup environment variables:
  - `GIT_SHA` – unique hash value tied to the commit within a PR, used downstream to name CI output test spells.
  - `PROFILE` – used to tell CI’s local DBT setup which DBT profile to use, `dunesql` in this case.
- Add the `GIT_SHA` variable value to the spell schema name of each spell within a PR, in order to keep CI table names unique.
- Pull the latest DBT manifest file from data lake, which is built based off the current main branch of Spellbook.
- Run `dbt deps` – install all necessary dependencies in the project.
- Check if the DuneSQL cluster for Spellbook CI environment is running – if not, activate and turn on for usage.
- Run `dbt compile` to prepare a new DBT manifest file, used to compare to previous step manifest on main branch.
- Check schemas script to ensure all spells within PR assign a schema value in the model config block, in order to avoid a default schema name.
- Run `dbt seed` – includes a filter for ‘state:modified’ to only run seeds which are present in the PR.
- Run `dbt run` – includes a filter for ‘state:modified’ to only run models which are present in the PR, first run here will be all historical data.
  - **Note**: this step includes a fail-fast flag, in order to stop the workflow from running when any model fails.
- Run `dbt test` – includes a filter for `state:modified` to only run tests associated with models in the PR.
- Set an environment variable for incremental model flag, to determine if the final steps are applicable.
- Run `dbt run` again, for incremental models only, to ensure incremental logic runs as expected.
- Run `dbt test` again, to test the data again post-incremental run.

## CI Tests workflow matrix

To handle the dbt sub-project separation within Spellbook, there are two projects which run separately in CI: `tokens` & `spellbook`. Within the PR, you will likely see two CI test workflows running, one for each project. As sub-projects grow, this matrix could also grow. If sub-projects grow much larger, the matrix will be replaced with a cleaner solution.

- Expect to see two CI workflows, but each workflow will be able to automatically detect which spells to run

## CI Tests Leverage Prod Data

If spells within a PR reference (i.e. dbt ref usage) another spell which isn’t included in PR, the CI environment will default to read from prod environment. This is setup to expedite the process and not rebuild entire lineages for each PR. If there is ever an issue with the CI test due to this prod data, modify the troubled spell within the same PR to force the spell to run in CI and refresh.

## How Can I Leverage CI Tests as a Developer in Spellbook?

The CI tests tied to PRs were created in order to bypass giving each and every user direct access to the databases. In other DBT setups, users can typically set up their local environments to connect directly with a DBT profile, but that proposes too many risks in the open-source Spellbook. The end goal of CI tests attached to PRs is to provide each user with their “local” development environment to test out building new spells.

Within each PR, either in the ‘Checks’ tab at the top or in the final review box at the bottom, users can navigate into the ‘dbt slim ci’ GH workflow associated with current PR. If you click into the details, GH will navigate to the specific workflow run in the ‘Actions’ section. Each step of the CI process is listed, with expandable sections to see the logs associated with all the steps.

### The Key Things to Navigate To and Use:

- Under `dbt seed` – find any seeds which ran and built on Dune, if applicable to given PR.
- Under `dbt run initial model(s)` – find all spells from the PR, with unique CI table names, that are built on Dune.
  - **Note**: these can be queried on Dune & are highly encouraged, in order to ensure highest data quality!
  - Each set of CI tables persist for ~24 hours on Dune, before being automatically cleaned up.
  - If you notice the CI table can’t be queried, simply re-run the CI tests to rebuild.
  - CI test tables will maintain a universal format: `test_schema.git_dunesql_<GIT_HASH>_` – prefix assigned to all table aliases. Example: `test_schema.git_dunesql_bb29a782_cipher_arbitrum_base_trades`.
- Under `dbt test initial model(s)` – find all the tests which ran on the models within PR.

## Advanced CI Tests Usage

Simple rule of thumb – a green check success on CI tests does not guarantee a fully successful PR. While this a great start in the direction towards merge, there are other considerations:

- **Monitor Performance of the CI Runs**
  - Did the historical initial run step take longer than expected, i.e., much longer than the compiled query takes to run on Dune?
  - Did the incremental step take longer to run than the historical step? If so, why?
- **Data Quality**
  - While the jobs can be successful, there can still be bugs in how the data is being written to the output spells.
  - Query the CI tables and monitor data quality!
  - Required tests didn’t run due to not being assigned correctly in PR.

### Other Minor CI Details

- There is a 90-minute timeout window, to cancel any long-running spells.
  - This timeframe has worked well for 95%+ of PRs, but there are instances where the timeout needs increased, which needs handled by the Dune team.
- Wizards are unable to modify objects in the `spellbook/.github/` directory. If modified in a PR, a bot may auto-close the PR. Please request help from the Dune team to modify.
- Concurrency is set to 1, meaning each new commit which triggers a new workflow action will cancel any currently running.

### Common Issues to Look Out For in CI Tests

- **Models which are not in my PR are running in my CI workflow, why is that?**
  - Due to the manifest comparison steps, there are times the manifest files are out of sync, therefore PR manifest vs. main branch manifest pulls more than it should to run.
  - To fix, the `commit manifest` workflow in `Actions` section likely needs rerun to upload a fresh main branch manifest file.
  - When the Dune team merges a batch of PRs, this `commit manifest` workflow automatically kicks off to set up a new manifest file. However, this workflow takes a few minutes, so if a commit is pushed to another PR during this workflow run, it can pull in more models than expected – simply monitor this job and rerun CI tests on PR once complete.
- **The DuneSQL cluster for CI is down.**
  - Dune team will need to fix internally.
- **Error: ‘Metadata is not found for \_\_\_\_’.**
  - This typically occurs when a spell is being modified in a PR & the spell has a seed test associated in the schema yml file.
  - Seeds are not run frequently in production, therefore it’s easier to just force the seed to run in the PR by making a slight modification to the file and start fresh.
