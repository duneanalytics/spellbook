{{ config(
    schema='prices_v2'
    , alias = 'dex_minute_raw'
    , materialized = 'incremental'
    , file_format = 'delta'
    , partition_by = ['date']
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'contract_address', 'timestamp']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.timestamp')]
    )
}}

WITH dex_trades_filter_and_unnest as (
    SELECT
        d.blockchain,
        d.token_bought_address as contract_address,
        d.block_time as timestamp,
        d.amount_usd/d.token_bought_amount as price,
        d.amount_usd as volume -- in USD
    FROM {{ source('dex','trades') }} d
    INNER JOIN {{ref('prices_trusted_tokens')}} t
        on t.blockchain = d.blockchain
        and t.contract_address = d.token_sold_address -- the token traded against is trusted
    LEFT JOIN {{ref('prices_trusted_tokens')}} anti_t
        on anti_t.blockchain = d.blockchain
        and anti_t.contract_address = d.token_bought_address -- the subjected token is already in trusted
    WHERE d.amount_usd > 0 and token_bought_amount > 0 and token_bought_address is not null
    and anti_t.contract_address is null
    {% if is_incremental() %}
    AND {{ incremental_predicate('d.block_time') }}
    {% endif %}

    UNION ALL

    SELECT
        d.blockchain,
        d.token_sold_address as contract_address,
        d.block_time as timestamp,
        d.amount_usd/d.token_sold_amount as price,
        d.amount_usd as volume -- in USD
    FROM {{ source('dex','trades') }} d
    INNER JOIN {{ref('prices_trusted_tokens')}} t
        on t.blockchain = d.blockchain
        and t.contract_address = d.token_bought_address -- the token traded against is trusted
    LEFT JOIN {{ref('prices_trusted_tokens')}} anti_t
        on anti_t.blockchain = d.blockchain
        and anti_t.contract_address = d.token_sold_address -- the subjected token is already in trusted
    WHERE d.amount_usd > 0 and token_sold_amount > 0 and token_sold_address is not null
    and anti_t.contract_address is null
    {% if is_incremental() %}
    AND {{ incremental_predicate('d.block_time') }}
    {% endif %}
)


SELECT
    blockchain,
    contract_address,
    date_trunc('minute',timestamp) as timestamp,
    approx_percentile(price,0.5) as price, -- median
    sum(volume) as volume,
    'dex.trades' as source,
    date_trunc('day',timestamp) as date -- partition
FROM dex_trades_filter_and_unnest
group by 1,2,3,7
