{% macro enrich_dex_liq_with_prices(
      base_liquidity = null
    , tokens_erc20_model = null
    )
%}

WITH base_liquidity as (
    SELECT
        *
    FROM
        {{ base_liquidity }}
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('block_time') }}
    {% endif %}
)
, tokens_metadata as (
    --erc20 tokens
    select
          blockchain
        , contract_address
        , symbol
        , decimals
    from
        {{ tokens_erc20_model }}
)
, prices AS (
    SELECT
          blockchain
        , contract_address
        , minute
        , price
    FROM
        {{ source('prices','usd_with_native') }}
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('minute') }}
    {% endif %}
)      
, enrichments AS (
    SELECT
        base.*
        , date_trunc('minute', base.block_time) as block_minute 
        , tk0.symbol as token0_symbol 
        , tk1.symbol as token1_symbol
        , base.amount0_raw/pow(10,tk0.decimals) as amount0
        , base.amount1_raw/pow(10,tk1.decimals) as amount1
    FROM
        base_liquidity base
    LEFT JOIN
        tokens_metadata as tk0 ON tk0.contract_address = base.token0
                              AND tk0.blockchain = base.blockchain
    LEFT JOIN
        tokens_metadata as tk1 ON tk1.contract_address = base.token1
                              AND tk1.blockchain = base.blockchain

)
, enrichment_with_prices AS (
    SELECT
            en.*
            , en.amount0 * p0.price AS amount0_usd
            , en.amount1 * p1.price AS amount1_usd
    FROM enrichments en    
    LEFT JOIN prices p0
           ON en.token0 = p0.contract_address
          AND en.blockchain = p0.blockchain
          AND p0.minute = en.block_minute
    LEFT JOIN prices p1
           ON en.token1 = p1.contract_address
          AND en.blockchain = p1.blockchain
          AND p1.minute = en.block_minute 
)


SELECT
    *
FROM
    enrichment_with_prices

{% endmacro %}
