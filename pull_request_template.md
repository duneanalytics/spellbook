# Thank you for contributing to Spellbook!

Thank you for taking the time to submit code in Spellbook. A few things to consider:

- If you are a first-time contributor, please sign the CLA by copy & pasting **_exactly_** what the bot mentions in PR comment
- Refer to [docs](#spellbook-contribution-docs) section below to answer questions
- Dune team will review submitted PRs as soon as possible

## Best practices
### To speed up your development process in PRs, keep these tips in mind:

- Each commit to your feature branch will rerun CI tests ([see example](https://github.com/duneanalytics/spellbook/actions/runs/8202519819/job/22433451880?pr=5519))
    - This includes *all* modified models on your branch
    - This includes *all* history of the data
- Two tips for faster development iteration:
    - Ensure dbt is installed locally (refer to main `readme`) and run `dbt compile`
        - This will output raw SQL in `target/` directory to copy/paste and run on Dune directly for initial query testing
    - Hardcode a `WHERE` filter for only ~7 days of history on large source tables, i.e. `ethereum.transactions`
        - This will speed up the CI tests and output results quicker -- whether that's an error or fully successful run
        - Once comfortable with small timeframe, remove filter and let full history run

### Incremental model setup
- Make sure your unique key columns are *exactly* the same in the model config block, schema yml file, and seed match columns (where applicable)
- There cannot be nulls in the unique key columns
    - Be sure to double check key columns are correct or `COALESCE()` as needed on key column(s), otherwise the tests may fail on duplicates

### 🪄 Use the built CI tables for testing 🪄

Once CI completes, you can query the CI tables and errors in dune when it finishes running.
- For example:
    - In the `run initial models` and `test initial models`, there will be a schema that looks like this: `test_schema.git_dunesql_4da8bae_sudoswap_v2_base_pool_creations`
    - This can be temporarily queried in Dune for ~24 hours

Leverage these tables to perform QA testing on Dune query editor -- or even full test dashboards!

## Spellbook contribution docs

The [docs](docs) directory has been implemented to answer as many questions as possible. Please take the time to reference each `.md` file within this directory to understand how to efficiently contribute & why the repo is designed as it is 🪄

Example questions to be answered:

- What does each property in the [model config block](/docs/models/model_config_block.md) mean?
- What is the [CI test](/docs/ci_test/ci_test_overview.md) attached to PRs and how can I best utilize it?
- What are the Spellbook [best practices](/docs/general/best_practices.md)?

Please navigate through the [docs](/docs) directory to find as much info as you can.

**Note**: happy to take PRs to improve the docs, let us know 🤝
