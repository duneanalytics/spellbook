{{
    config(
        schema="camelot_arbitrum",
        alias="pair_fee_rates",
        partition_by=["blockchain"],
        materialized="incremental",
        file_format="delta",
        incremental_strategy="merge",
        unique_key=[
            "minute",
            "blockchain",
            "pair",
            "version",
            "token0_fee_percentage",
            "token1_fee_percentage",
        ],
    )
}}

{% set blockchain = "arbitrum" %}
{% set project_start_date = "2022-06-14" %}
{% set v2_fee_precision = "1e5" %}
{% set v2_default_fee = "300" %}  -- 0.3%
{% set v3_fee_precision = "1e6" %}
{% set v3_default_fee = "100" %}  -- 0.01%

with
    v2_pairs_with_initial_fee_rates as (
        select
            date_trunc('minute', evt_block_time) as minute,
            pair,
            '2' as version,
            token0,
            {{ v2_default_fee }} / {{ v2_fee_precision }} as token0_fee_rate,
            token1,
            {{ v2_default_fee }} / {{ v2_fee_precision }} as token1_fee_rate
        from {{ source("camelot_arbitrum", "CamelotFactory_evt_PairCreated") }}
        {% if not is_incremental() %}
            where evt_block_time >= timestamp '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
            where evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    ),
    v2_directional_fee_rate_updates as (
        select
            date_trunc('minute', evt_block_time) as minute,
            pair,
            version,
            token0,
            avg(token0feepercent) / {{ v2_fee_precision }} as token0_fee_rate,  -- Handle multiple updates per minute
            token1,
            avg(token1feepercent) / {{ v2_fee_precision }} as token1_fee_rate  -- Handle multiple updates per minute
        from
            {{ source("camelot_arbitrum", "CamelotPair_evt_FeePercentUpdated") }}
            as fee_updates
        join
            v2_pairs_with_initial_fee_rates as pairs
            on fee_updates.contract_address = pairs.pair
        group by date_trunc('minute', evt_block_time), pair, version, token0, token1
    ),
    v3_pairs_with_initial_fee_rates as (
        select
            date_trunc('minute', evt_block_time) as minute,
            pool as pair,
            '3' as version,
            token0,
            {{ v3_default_fee }} / {{ v3_fee_precision }} as token0_fee_rate,
            token1,
            {{ v3_default_fee }} / {{ v3_fee_precision }} as token1_fee_rate
        from {{ source("camelot_v3_arbitrum", "AlgebraFactory_evt_Pool") }}
        {% if not is_incremental() %}
            where evt_block_time >= timestamp '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
            where evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    ),
    v3_directional_fee_rate_updates as (
        select
            date_trunc('minute', evt_block_time) as minute,
            pair,
            version,
            token0,
            avg(feezto) / {{ v3_fee_precision }} as token0_fee_rate,  -- Handle edge case where pair fees gets changed multiple times per minute
            token1,
            avg(feeotz) / {{ v3_fee_precision }} as token1_fee_rate  -- Handle edge case where pair fees gets changed multiple times per minute
        from {{ source("camelot_v3_arbitrum", "AlgebraPool_evt_Fee") }} as fee_updates
        join
            v3_pairs_with_initial_fee_rates as pairs
            on fee_updates.contract_address = pairs.pair
        group by date_trunc('minute', evt_block_time), pair, version, token0, token1
    ),
    pairs as (
        select *
        from v2_pairs_with_initial_fee_rates
        union all
        select *
        from v3_pairs_with_initial_fee_rates
    ),
    fee_rate_updates as (
        select *
        from pairs
        union all
        select *
        from v2_directional_fee_rate_updates
        union all
        select *
        from v3_directional_fee_rate_updates
    ),
    -- This approach does not work: Result of sequence function must not have more
    -- than 10000 entries
    /*time_series as (
        select date_trunc('minute', block_time) as minute
        from
            (
                select
                    sequence(
                        timestamp '{{project_start_date}}'
                        cast(current_timestamp as timestamp),
                        interval '1' minute
                    ) as timestamp_array
            )
        cross join unnest(timestamp_array) as t(block_time)
    ),*/
    time_series as (
        select distinct date_trunc('minute', block_time) as minute
        from dex.trades
        where
            block_time >= timestamp '{{project_start_date}}'
            and blockchain = '{{blockchain}}'
            and project = 'camelot'

    ),
    -- Prepare data structure (1 row for every minute since each pair was created)
    pairs_by_minute as (
        select time_series.minute, pair, version, token0, token1
        from pairs
        cross join time_series
        where time_series.minute >= pairs.minute
    )

select
    '{{blockchain}}' as blockchain,
    pairs.minute,
    pairs.pair,
    pairs.version,
    pairs.token0,
    {# coalesce(
        token0_fee_rate,
        last_value(token0_fee_rate) ignore nulls over (
            partition by pairs.pair
            order by pairs.minute
            rows between unbounded preceding and current row
        )
    ) as token0_fee_rate, #}
    token0_fee_rate
    pairs.token1,
    {# coalesce(
        token1_fee_rate,
        last_value(token1_fee_rate) ignore nulls over (
            partition by pairs.pair
            order by pairs.minute
            rows between unbounded preceding and current row
        )
    ) as token1_fee_rate #}
    token1_fee_rate
from pairs_by_minute as pairs
left join
    fee_rate_updates
    on (pairs.minute = fee_rate_updates.minute and pairs.pair = fee_rate_updates.pair)
order by minute desc, pair asc
