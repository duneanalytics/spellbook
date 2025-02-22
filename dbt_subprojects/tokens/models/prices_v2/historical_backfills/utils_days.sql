{{ config(
        schema='utils',
        alias = 'days',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = 'timestamp',
        event_time = 'timestamp'
        )
}}

-- todo: move this model in a better place, put a view on top that always goes to now() and expose to public
-- todo: replicate for hours and minutes
-- todo: Use this instead of sequence stuff in other places


{% set start_date = '2000-01-01' %}


{% if not is_incremental() %}
    select timestamp
    from unnest(
         sequence(timestamp '{{start_date}}'
                , cast(date_trunc('day', now()) as timestamp)
                , interval '1' day
                )
         ) as foo(timestamp)
{% else %}
    select timestamp
    from unnest(
         sequence(cast(date_trunc('day', now() - interval '10' day) as timestamp)
                , cast(date_trunc('day', now()) as timestamp)
                , interval '1' day
                )
         ) as foo(timestamp)
    {% if is_incremental() %}
    where {{ incremental_predicate('timestamp') }}
    {% endif %}
{% endif %}


