# Thank you for contributing to Spellbook!

Thank you for taking the time to submit code in Spellbook. A few things to consider:

- If you are a first-time contributor, please sign the CLA by copy & pasting **_exactly_** what the bot mentions in PR comment
- Refer to [docs](#spellbook-contribution-docs) section below to answer questions
- Dune team will review submitted PRs as soon as possible

# IMPORTANT: To speed up your contribution (PR) process, keep these tips in mind

- Each commit will rerun the CI which builds all the models and runs all tests ([see example](https://github.com/duneanalytics/spellbook/actions/runs/8202519819/job/22433451880?pr=5519)
- Hardcode a WHERE filter for only 7 days of history on tables when testing, to make the CI run faster.
- Install dbt locally (pip install dbt), and run `dbt compile` to quickly test for syntax errors on your own machine.
- Make sure your unique key columns are EXACTLY the same in the model config, schema, and seed match.
- You can't have nulls in your unique key columns - be sure to COALESCE() where needed otherwise the tests will say you have duplicates.
- You are able to query the CI tables and errors in dune when it finishes running. For example, in the "run initial models" and "test initial models" there will be a schema that looks like this `test_schema.git_dunesql_4da8bae_sudoswap_v2_base_pool_creations` which can be temporarily queried in Dune for roughly an hour or two. Use this to quickly QA and test for errors.

## Spellbook contribution docs

The [docs](docs) directory has been implemented to answer as many questions as possible. Please take the time to reference each `.md` file within this directory to understand how to efficiently contribute & why the repo is designed as it is 🪄

Example questions to be answered:

- What does each property in the [model config block](/docs/models/model_config_block.md) mean?
- What is the [CI test](/docs/ci_test/ci_test_overview.md) attached to PRs and how can I best utilize it?
- What are the Spellbook [best practices](/docs/general/best_practices.md)?

Please navigate through the [docs](/docs) directory to find as much info as you can.

**Note**: happy to take PRs to improve the docs, let us know 🤝
