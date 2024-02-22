Enjoy working with Dune? We're [hiring developers](https://dune.com/careers) remotely!

# Contributing an abstraction to `spellbook`

`spellbook` is a dbt project for user-created abstractions to the Dune data platform.
Contributions in the form of issues and pull requests are very much welcome here.

## At a glance:

- We are working in SQL + JINJA templating and using dbt-core to compile and build abstractions henceforth models.
- Models live in the model's directory and can be materialized as views, tables, and incremental tables.
- [BETA] We use pre-push hooks (similar to pre-commit hooks) to catch common errors before pushing changes.

## Guidelines and conventions

- Each file should only contain one table, view, incremental table, or macro.
- Each SQL file should be a SELECT statement.
- We default to building a view and consider switching to a table or incremental table if performance becomes an issue.

## [BETA] Pre-push hooks

UPDATE: These pre-push hooks require running `dbt compile` which is a fairly slow step due to the size of our project. We intend to rewrite these hooks to be more efficient but for the time being they remain cumbersome. Feel free to use them if you find them useful but the same checks will run in a Github Action when you commit your code. Feel free to uninstall if they do not bring joy, we'll let wizards know when we think we've improved them enought to warrant making them part of the general development flow.

We are testing out adding pre-push hooks to our workflow. The goal is to catch common errors before code is pushed and
streamline the pull request review process.

If you are a Github Desktop user, this flow might not work for you! If you are a Github CLI user, please give this a shot. We will incorporate a github action for the Desktop users.

You may be familiar with [pre-commit hooks](https://pre-commit.com/) which run checks every time you commit new code.
Because dbt compile is required for the more meaningful checks, we have decided to only apply these tests when
pushing code to minimize the waiting time. If any of these checks fail, the git push will fail.

To install pre-push hooks, follow these steps:

- If your pipenv is activated, exit it with `exit`.
- Reinstall your pipenv with `pipenv install` from the root of spellbook.
- Enter your pipenv with `pipenv shell`.
- If you're paranoid like me, run `pip freeze` and check to see if pre-commit is installed.
- Install the prepush hooks with `pre-commit install --hook-type pre-push`.

To use pre-push hooks:
Manually

- If you want to manually run the checks, stage your changed files on git e.g. `git add {file_name.sql}`.
- Run `pre-commit run --hook-stage manual`.
- Resolve any errors and re-add your files to git.
- Rerun `pre-commit run --hook-stage manual`.

On push

- Add and commit your changes to git, as you would normally.
- Push your code.
- Pre-push hooks (if they are installed correctly) will run and return check results.
- Resolve any errors and re-add your files to git.
- Try pushing again.
- If all the checks pass, your code will be pushed to Github. If any checks fail, the push will fail.
- If you cannot resolve the error, run `git push --no-verify` and paste the output of the failed checks in your PR.

Please reach out to meghan@dune.com if you need help or have feedback on this BETA feature.

## Contributing your first abstraction

We can't grant access to run dbt-core directly to our database. But you can set up dbt-core without a database connection. See the [README](README.md) for instructions.

### Writing your first model

After dbt-core is set up, you'll be able to use commands like `dbt compile` and `dbt parse` from the `spellbook` directory to test your work for syntax issues and generate runnable SQL to test on dune.com.

First, add your desired model (your SQL query!) to the appropriate directory under models. We follow the convention /`metric`/`chain`/`project` e.g. `/balances/ethereum/uniswap`.

Your model should be a `SELECT` statement only. It will fail if you try to use any other SQL statement types.

The dbt_project.yml file defines default materialization and schema settings for each directory. You can override these settings with a config string at the top of your model.
You can also override the model name with an alias in this config. Otherwise, the table name will match the file name `mock_table.sql`.

```sql
{{ config(
        alias='mock_tbl',
        materialized ='table'
        )
}}

select 2 as col1, "moon" as col2, tx_id
from {{ source('mock', 'source_table') }}
```

If your models refers to another model it should use a `ref` with the model's file name. e.g. `{{ ref('transfers_ethereum_erc20_rolling_hour') }}`.

Raw or decoded tables (anything that isn't an abstraction) should be referenced as `sources` e.g. `{{ source('erc20_ethereum', 'evt_transfer') }}`.

Using refs and sources is important because it allows us to build a dependency tree for deploying your model and run tests on its output!

To test your queries on dune, run `dbt compile` and copy the compiled SQL from the target folder which will mirror the directory structure of the rest of the project.
You can then test the query with compiled JINJA directly on `dune.com`.
Important: The target defined when you ran `dbt init` has to be set to `wizard`, otherwise the SQL rendered by dbt compile will have a target attached to it and won't be able to integrate with existing models.

You can add generic tests to your model in the directory's schema.yml file. These tests are compiled as SQL on runtime. If you want, you can test them yourself with a little work. We recommend wrapping your model as a CTE and running what the SQL would compile to if you need to debug.

```sql
models:
  - name: mock_table
    columns:
      - name: tx_id
        tests:
          - unique
          - not_null
```

example manually compiled generic test:

```sql
    with my_model_cte as
        (select 2 as col1, "moon" as col2, tx_id
        from mock.source_table)

    select *
    from my_model_cte
    where tx_id is null

```

Custom tests can also be added. A passing test will return zero rows. A good way to use these tests is to check individual values. Commenting with an etherscan or similar link is helpful!

example custom test:

```sql
with unit_test1 as
(select
    case when col1 == 2 and col2 == 'moon' then True else False end as test
    from {{ ref('mock_table' )}}
    where tx_id = '102'),

unit_test2 as
(select
    case when col1 == 2 and col2 == 'moon' then True else False end as test
    from {{ ref('mock_table' )}}
    where tx_id = '103'),

select * from
(select * from unit_test1
union
select * from unit_test2)
where test = False
```

You should add a description for your model to the schema.yml file in the model's directory. This can be as detailed as you want. You can view the documentation by running `dbt docs generate` followed by `dbt docs serve`.

By default, tables or views are not publicly accessible. In complex cases, you may build multiple models and only expose the final one.

To make your final model publicly accessible on dune.com, you'll need to use the expose_spells macro in your model's config.

Define the following information in expose_spells:

```
expose_spells(\'["*blockchain*"]\',
                  "*sector or project*",
                   "*schema name*",
                   \'[*"contributor Dune username"*]\')
```

Example from dex_trades.sql

```
{{ config(
        alias ='trades',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "dex",
                                \'["jeff-dude", "hosuke", "0xRob"]\') }}'
        )
```
