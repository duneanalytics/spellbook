{{ config(
    schema='prices_v2'
    , alias = 'dex_minute'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'contract_address', 'minute']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.timestamp')]
    )
}}

WITH dex_trades_filter_and_unnest as (
    SELECT
        d.blockchain
        d.token_bought_address as contract_address,
        d.block_time as timestamp,
        d.amount_usd/d.token_bought_amount as price,
        d.amount_usd as volume -- in USD
    FROM {{ source('dex','trades') }} d
    INNER JOIN {{ref('prices_trusted_tokens')}} t
        on t.blockchain = d.blockchain
        and t.contract_address = d.token_sold_address -- the token traded against is trusted
    ANTI JOIN {{ref('prices_trusted_tokens')}} t2
        on t.blockchain = d.blockchain
        and t.contract_address = d.token_bought_address -- the subjected token is already in trusted
    WHERE d.amount_usd > 0 and token_bought_amount > 0
    {% if is_incremental() %}
    AND {{ incremental_predicate('d.block_time') }}
    {% endif %}

    UNION ALL

    SELECT
        d.blockchain
        d.token_sold_address as contract_address,
        d.block_time as timestamp,
        d.amount_usd/d.token_sold_amount as price,
        d.amount_usd as volume -- in USD
    FROM {{ source('dex','trades') }} d
    INNER JOIN {{ref('prices_trusted_tokens')}} t
        on t.blockchain = d.blockchain
        and t.contract_address = d.token_bought_address -- the token traded against is trusted
    ANTI JOIN {{ref('prices_trusted_tokens')}} t2
        on t.blockchain = d.blockchain
        and t.contract_address = d.token_sold_address -- the subjected token is already in trusted
    WHERE d.amount_usd > 0 and token_sold_amount > 0
    {% if is_incremental() %}
    AND {{ incremental_predicate('d.block_time') }}
    {% endif %}
)

SELECT
    blockchain
    contract_address,
    date_trunc('minute',timestamp) as timestamp,
    sum(price*volume)/sum(volume) as price, -- vwap
    sum(volume) as volume,
    'dex.trades' as source
FROM dex_trades_filter_and_unnest
group by 1,2,3
