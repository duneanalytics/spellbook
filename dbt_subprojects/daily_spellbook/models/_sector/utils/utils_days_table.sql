{{ config(
    schema = 'utils',
    alias = 'days_table',
    materialized = 'incremental',
    file_format = 'delta',
    unique_key = 'timestamp',
    incremental_strategy = 'merge'
    )
}}


SELECT timestamp
FROM unnest(
    sequence(
        {%if is_incremental() %}
        cast(date_trunc('day', now()) as timestamp)- interval '{{var("DBT_ENV_INCREMENTAL_TIME")}}' {{var("DBT_ENV_INCREMENTAL_TIME_UNIT")}}
        {% else %}
        timestamp '2009-01-03'
        {% endif %}
        , cast(date_trunc('day', now()) as timestamp)+ interval '3' day  -- add some padding to account for materialization lag
        , interval '1' day
        )
    ) as foo(timestamp)