Enjoy working with Dune? We're [hiring developers](https://dune.com/careers) remotely!

# Contributing an abstraction to `spellbook`

`spellbook` is a dbt project for user-created abstractions to the Dune data platform. 
Contributions in the form of issues and pull requests are very much welcome here.

## At a glance:
- We are working in SQL + JINJA templating and using dbt-core to compile and build abstractions henceforth models.
- Models live in the model's directory and can be materialized as views, tables, and incremental tables.

## Guidelines and conventions
- Each file should only contain one table, view, incremental table, or macro.
- Each SQL file should be a SELECT statement. 
- We default to building a view and consider switching to a table or incremental table if performance becomes an issue.


## Contributing your first abstraction
We can't grant access to run dbt-core directly to our database. But you can set up dbt-core without a database connection. See the [README](https://github.com/duneanalytics/spellbook/blob/main/README.md) for instructions. 

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

To make your final model publicly accessible on dune.com, you'll need tblproperties set to `dune.public = true` and define the `dune.data_explorer` settings. These are defined in macros under alter_tblproperties. (One file per model)
```sql
{% macro alter_tblproperties_mock_tbl() -%}
{%- if target.name == 'prod'-%}
alter view mock.mock_tbl set tblproperties ('dune.public'='true',
                                            'dune.data_explorer.blockchains'='["tutorial"]',
                                            'dune.data_explorer.category'='abstraction',
                                            'dune.data_explorer.abstraction.type'='tutorial',
                                            'dune.data_explorer.abstraction.name'='tutorial');
{%- else -%}
{%- endif -%}
{%- endmacro %}

```
