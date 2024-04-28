{{
    config(
        schema="camelot_arbitrum",
        alias="pair_fee_rates",
        partition_by=["minute"],
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
    -- TODO: add v3 fee rate updates
    v2_fee_rates_updates as (
        select *
        from v2_pairs_with_initial_fee_rates
        union all
        select *
        from v2_directional_fee_rate_updates
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
        where block_time >= timestamp '{{project_start_date}}'
    ),
    -- Prepare data structure (1 row for every minute since each pair was created)
    time_series_with_pair as (
        select time_series.minute, pair
        from v2_pairs_with_initial_fee_rates as pairs
        cross join time_series
        where time_series.minute >= pairs.minute
    ),
    pairs_by_minute as (
        select time_series.minute, pair, version, token0, token1
        from v2_pairs_with_initial_fee_rates as pairs
        cross join time_series
        where time_series.minute >= pairs.minute
    ),
    pairs_by_minute_with_fee_rates as (
        select
            pairs.minute,
            pairs.pair,
            pairs.version,
            pairs.token0,
            token0_fee_rate,
            pairs.token1,
            token1_fee_rate
        from pairs_by_minute as pairs
        left join
            v2_fee_rates_updates
            on (
                pairs.minute = v2_fee_rates_updates.minute
                and pairs.pair = v2_fee_rates_updates.pair
            )
    )
select '{{blockchain}}' as blockchain, *
from pairs_by_minute_with_fee_rates
order by minute desc, pair asc
