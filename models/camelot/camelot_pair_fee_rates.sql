{{ config(
    schema = 'camelot_arbitrum',
    alias = 'pair_fee_rates',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['minute' 'blockchain', 'pair', 'version', 'token0_fee_percentage', 'token1_fee_percentage'])
}}

{% set blockchain = "arbitrum" %}
{% set project_start_date = "2022-06-14" %}
{% set v2_fee_precision = "1e5" %}
{% set v2_default_fee = "300" %} -- 0.3%
{% set v3_fee_precision = "1e6" %}
{% set v3_default_fee = "100" %} -- 0.01%

with
    v2_pairs_with_initial_fee_rates as (
        select
            date_trunc('minute', evt_block_time) as minute,
            pair,
            '2' as version,
            token0,
            {{v2_default_fee}} / {{v2_fee_precision}} as token0_fee_rate,
            token1,
            {{v2_default_fee}} / {{v2_fee_precision}} as token1_fee_rate
        from {{ source("camelot_arbitrum", "CamelotFactory_evt_PairCreated") }}
        {% if not is_incremental() %}
            where evt_block_time >= timestamp '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
            where evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    ),
    time_series as (
        select distinct date_trunc('minute', block_time) as minute
        from dex.trades
        where block_time >= timestamp '{{project_start_date}}'
    ),
    -- Prepare data structure (1 row for every minute since each pair was created)
    time_series_with_pair as (
        select time_series.minute, pair
        from v2_pairs_with_initial_fee_rates as pairs
        cross join time_series
        where time_series.minute >= pairs.minute
    )

select *
from time_series_with_pair
order by minute desc, pair asc
