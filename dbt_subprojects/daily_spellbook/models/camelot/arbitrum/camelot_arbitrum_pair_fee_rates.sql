{{
    config(
        schema="camelot_arbitrum",
        alias="pair_fee_rates",
        materialized="incremental",
        file_format="delta",
        incremental_strategy="merge",
        unique_key=["minute", "blockchain", "pair", "version"],
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
    ),
    v2_directional_fee_rate_updates as (
        select
            date_trunc('minute', evt_block_time) as minute,
            pair,
            version,
            token0,
            token0feepercent / {{ v2_fee_precision }} as token0_fee_rate,
            token1,
            token1feepercent / {{ v2_fee_precision }} as token1_fee_rate
        from
            {{ source("camelot_arbitrum", "CamelotPair_evt_FeePercentUpdated") }}
            as fee_updates
        join
            v2_pairs_with_initial_fee_rates as pairs
            on fee_updates.contract_address = pairs.pair
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
    ),
    v3_directional_fee_rate_updates as (
        select
            date_trunc('minute', evt_block_time) as minute,
            pair,
            version,
            token0,
            feezto / {{ v3_fee_precision }} as token0_fee_rate,
            token1,
            feeotz / {{ v3_fee_precision }} as token1_fee_rate
        from {{ source("camelot_v3_arbitrum", "AlgebraPool_evt_Fee") }} as fee_updates
        join
            v3_pairs_with_initial_fee_rates as pairs
            on fee_updates.contract_address = pairs.pair
    ),
    pairs as (
        select *
        from v2_pairs_with_initial_fee_rates
        union all
        select *
        from v3_pairs_with_initial_fee_rates
    ),
    fee_rate_updates as (
        select
            minute,
            pair,
            version,
            token0,
            avg(token0_fee_rate) as token0_fee_rate,  -- Handle edge case where pair fees gets changed multiple times per minute
            token1,
            avg(token1_fee_rate) as token1_fee_rate  -- Handle edge case where pair fees gets changed multiple times per minute
        from
            (
                select *
                from pairs
                union all
                select *
                from v2_directional_fee_rate_updates
                union all
                select *
                from v3_directional_fee_rate_updates
            )
        group by minute, pair, version, token0, token1
    ),
    camelot_pair_trades_by_minute as (
        select distinct
            date_trunc('minute', block_time) as minute, project_contract_address as pair
        from {{ source('dex', 'trades') }}
        where
            blockchain = '{{blockchain}}' and project = 'camelot'
            {% if not is_incremental() %}
                and block_time >= timestamp '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
                and {{ incremental_predicate("block_time") }}
            {% endif %}
    ),
    -- Prepare data structure (1 row for every minute where pair trades happened
    -- and/or fee rates got updated)
    pairs_by_minute as (
        select minute, pair
        from camelot_pair_trades_by_minute
        union distinct
        select minute, pair
        from fee_rate_updates
    ),
    -- Add version, token0, token1 columns
    pairs_by_minute_with_metadata as (
        select pairs_by_minute.minute, pairs_by_minute.pair, version, token0, token1
        from pairs_by_minute
        left join pairs on pairs_by_minute.pair = pairs.pair
    )

select
    '{{blockchain}}' as blockchain,
    pairs.minute,
    pairs.pair,
    pairs.version,
    pairs.token0,
    coalesce(
        token0_fee_rate,
        last_value(token0_fee_rate) ignore nulls over (
            partition by pairs.pair
            order by pairs.minute
            rows between unbounded preceding and current row
        )
    ) as token0_fee_rate,
    pairs.token1,
    coalesce(
        token1_fee_rate,
        last_value(token1_fee_rate) ignore nulls over (
            partition by pairs.pair
            order by pairs.minute
            rows between unbounded preceding and current row
        )
    ) as token1_fee_rate
from pairs_by_minute_with_metadata as pairs
left join
    fee_rate_updates
    on (pairs.minute = fee_rate_updates.minute and pairs.pair = fee_rate_updates.pair)
order by minute desc, pair asc
