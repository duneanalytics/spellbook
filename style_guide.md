## SQL Style Guide

This document outlines our SQL style guide, which defines the standards we follow when writing SQL code. These guidelines are enforced by the SQLFluff linter and through code reviews.

### Usage

We expect developers to adhere to the style guide presented in this document during their development process. While the CI pipeline is currently run manually, enforcement will be applied during code reviews in the future. Ultimately, the goal is to make it mandatory for the pipeline to pass with each change.

### SQLFluff

SQLFluff is a SQL linter that integrates with templating tools like dbt. It helps establish the basic structure and style of the SQL code we write and shifts the responsibility of reviewing that structure and style to the authors. SQLFluff is included in the dbt development environment and utilizes the dbt templating engine for the linting process.

SQLFluff provides a fix command that automatically applies fixes to rule violations when possible. However, not all rule violations can be fixed automatically. Therefore, it is recommended to run the lint command after using the fix command to ensure that all rule violations are resolved. The following commands can be used:

```console
pre-commit run sqlfluff-fix --hook-stage=manual --files models/path/to/file/file-to-lint.sql
pre-commit run sqlfluff-lint --hook-stage=manual --files models/path/to/file/file-to-lint.sql
```

Another option is stage the files and run the pre-commit hooks:

```console
pre-commit run sqlfluff-fix --hook-stage=manual
pre-commit run sqlfluff-lint --hook-stage=manual
```

For more information, refer to the following resources:

- [SQLFluff Documentation](https://docs.sqlfluff.com/en/latest/index.html)
- [SQLFluff Default configuration](https://docs.sqlfluff.com/en/latest/configuration.html#default-configuration)

#### Changes from the default configuration

- Dialect selected: databricks
- Templater selected: dbt
- Excluded rules:
    1. RF04: Keywords should not be used as identifiers.
    2. ST06: Select wildcards then simple targets before calculations and aggregates.
- Capitalization for keywords, functions, literals, and types set to uppercase.
- Capitalization for identifiers set to lowercase.

### General Guidance

Avoid optimizing for fewer lines of code. New lines are cheap, but [brain time is expensive](https://blog.getdbt.com/write-better-sql-a-defense-of-group-by-1/). Read more about this here.

Familiarize yourself with the [DRY Principle](https://docs.getdbt.com/docs/design-patterns). Utilize CTEs, jinja, and macros in dbt, and snippets in Sisense. If you find yourself typing the same line twice, it needs to be maintained in two places.

Be consistent. Even if you are unsure of the best way to do something, do it the same way throughout your code. This will make it easier to read and make changes if needed.

Be explicit. Defining something explicitly ensures that it works as expected and makes it easier for the next person (which may be you) to understand the SQL code.

### Best Practices

- Use spaces instead of tabs. Set up your editor to convert tabs to spaces.
- Wrap long lines of code (between 80 and 100 characters) to a new line.
- Use the AS operator when aliasing a column or table.
- Prefer using != over <> for "not equal" comparison.
- Prefer LOWER(column) LIKE '%match%' over column ILIKE '%Match%' to avoid unexpected results caused by stray capital letters.
- Prefer using WHERE instead of HAVING when either would suffice.

### Commenting

- Use the `--` syntax for single-line comments in a model.
- Use the `/* */` syntax for multi-line comments in a model.
- Respect the character line limit when making comments. If the comment is too long, move it to a new line or to the model documentation.
- Utilize the dbt model documentation when available.
- Provide a brief description of calculations made in SQL, including a link to the handbook defining the metric and how it's calculated.
- Instead of leaving `TODO` comments, create new issues for improvement.

### Naming Conventions

- An ambiguous field name such as `id`, `name`, or `type` should always be prefixed by what it is identifying or naming:

    ```sql
    -- Preferred
    SELECT
        id AS account_id,
        name AS account_name,
        type AS account_type,
        ...

    -- vs

    -- Not Preferred
    SELECT
        id,
        name,
        type,
        ...

    ```

- All field names should be [snake-cased](https://en.wikipedia.org/wiki/Snake_case):

    ```sql
    -- Preferred
    SELECT
        dvcecreatedtstamp AS device_created_timestamp
        ...

    -- vs

    -- Not Preferred
    SELECT
        dvcecreatedtstamp AS DeviceCreatedTimestamp
        ...
    ```

- Boolean field names should start with `has_`, `is_`, or `does_`:

    ```sql
    -- Preferred
    SELECT
        deleted AS is_deleted,
        sla AS has_sla
        ...

    -- vs

    -- Not Preferred
    SELECT
        deleted,
        sla,
        ...
    ```

- Timestamps should end with `_at` and should always be in UTC.
- Dates should end with `_date`.
- When truncating dates name the column in accordance with the truncation.

    ```sql
    SELECT
        original_at, -- 2020-01-15 12:15:00.00
        original_date, -- 2020-01-15
        DATE_TRUNC('month',original_date) AS original_month -- 2020-01-01
        ...
    ```

- Avoid key words like `date` or `month` as a column name.

### Reference Conventions

- When joining tables and referencing columns from both tables consider the following:
  - reference the full table name instead of an alias when the table name is shorter, maybe less than 20 characters.  (try to rename the CTE if possible, and lastly consider aliasing to something descriptive)
  - always qualify each column in the SELECT statement with the table name / alias for easy navigation

    ```sql
    -- Preferred
    SELECT
        budget_forecast.account_id,
        date_details.fiscal_year,
        date_details.fiscal_quarter,
        date_details.fiscal_quarter_name,
        cost_category.cost_category_level_1,
        cost_category.cost_category_level_2
    FROM budget_forecast_cogs_opex AS budget_forecast
    LEFT JOIN date_details
        ON date_details.first_day_of_month = budget_forecast.accounting_period
    LEFT JOIN cost_category
        ON budget_forecast.unique_account_name = cost_category.unique_account_name

    -- vs

    -- Not Preferred
    SELECT
        a.account_id,
        b.fiscal_year,
        b.fiscal_quarter,
        b.fiscal_quarter_name,
        c.cost_category_level_1,
        c.cost_category_level_2
    FROM budget_forecast_cogs_opex a
    LEFT JOIN date_details b
        ON b.first_day_of_month = a.accounting_period
    LEFT JOIN cost_category c
        ON b.unique_account_name = c.unique_account_name
    ```

- Only use double quotes when necessary, such as columns that contain special characters or are case sensitive.

    ```sql
        -- Preferred
        SELECT
            "First_Name_&_" AS first_name,
            ...

        -- vs

        -- Not Preferred
        SELECT
            FIRST_NAME AS first_name,
            ...
    ```

- Prefer explicit join statements.

    ```sql
        -- Preferred
        SELECT *
        FROM first_table
        INNER JOIN second_table
        ...

        -- vs

        -- Not Preferred
        SELECT *
        FROM first_table,
            second_table
        ...
    ```

### Common Table Expressions (CTEs)

- Prefer CTEs over sub-queries as [CTEs make SQL more readable and are more performant](https://www.alisa-in.tech/post/2019-10-02-ctes/):

    ```sql
    -- Preferred
    WITH important_list AS (

        SELECT DISTINCT
            specific_column
        FROM other_table
        WHERE specific_column != 'foo'

    )

    SELECT
        primary_table.column_1,
        primary_table.column_2
    FROM primary_table
    INNER JOIN important_list
        ON primary_table.column_3 = important_list.specific_column

    -- vs

    -- Not Preferred
    SELECT
        primary_table.column_1,
        primary_table.column_2
    FROM primary_table
    WHERE primary_table.column_3 IN (
        SELECT DISTINCT specific_column
        FROM other_table
        WHERE specific_column != 'foo')
    ```

- Use CTEs to reference other tables.
- CTEs should be placed at the top of the query.
- Where performance permits, CTEs should perform a single, logical unit of work.
- CTE names should be as concise as possible while still being clear.
- Avoid long names like `replace_sfdc_account_id_with_master_record_id` and prefer a shorter name with a comment in the CTE. This will help avoid table aliasing in joins.
- CTEs with confusing or notable logic should be commented in file and documented in dbt docs.
- CTEs that are duplicated across models should be pulled out into their own models.

### Example Code

This example code has been processed though SQLFluff linter and had the style guide applied.

```sql

WITH my_data AS (

  SELECT my_data.*
  FROM prod.my_data_with_a_long_table_name AS my_data
  INNER JOIN prod.other_thing
  WHERE my_data.filter = 'my_filter'

),

some_cte AS (

  SELECT DISTINCT
    id AS other_id,
    other_field_1,
    other_field_2,
    date_field_at,
    data_by_row,
    field_4,
    field_5,
    LAG(
      other_field_2
    ) OVER (PARTITION BY other_id, other_field_1 ORDER BY 5) AS previous_other_field_2
  FROM prod.my_other_data

),
/*
This is a very long comment: It is good practice to leave comments in code to
explain complex logic in CTEs or business logic which may not be intuitive to
someone who does not have intimate knowledge of the data source. This can help
new users familiarize themselves with the code quickly.
*/

final AS (

  SELECT
    -- This is a singel line comment
    my_data.field_1 AS detailed_field_1,
    my_data.field_2 AS detailed_field_2,
    my_data.detailed_field_3,
    DATE_TRUNC('month', some_cte.date_field_at) AS date_field_month,
    some_cte.data_by_row['id']::NUMBER AS id_field,
    IFF(my_data.detailed_field_3 > my_data.field_2, TRUE, FALSE) AS is_boolean,
    CASE
      WHEN my_data.cancellation_date IS NULL
        AND my_data.expiration_date IS NOT NULL
        THEN my_data.expiration_date
      WHEN my_data.cancellation_date IS NULL
        THEN my_data.start_date + 7 -- There is a reason for this number
      ELSE my_data.cancellation_date
    END AS adjusted_cancellation_date,
    COUNT(*) AS number_of_records,
    SUM(some_cte.field_4) AS field_4_sum,
    MAX(some_cte.field_5) AS field_5_max
  FROM my_data
  LEFT JOIN some_cte
    ON my_data.id = some_cte.id
  WHERE my_data.field_1 = 'abc'
    AND (my_data.field_2 = 'def' OR my_data.field_2 = 'ghi')
  GROUP BY 1, 2, 3, 4, 5, 6
  HAVING COUNT(*) > 1
  ORDER BY 8 DESC
)

SELECT *
FROM final
```

### Other SQL Style Guides

- [Brooklyn Data Co](https://github.com/brooklyn-data/co/blob/master/sql_style_guide.md)
- [dbt Labs](https://github.com/dbt-labs/corp/blob/main/dbt_style_guide.md)
- [Matt Mazur](https://github.com/mattm/sql-style-guide)
- [Kickstarter](https://gist.github.com/fredbenenson/7bb92718e19138c20591)
