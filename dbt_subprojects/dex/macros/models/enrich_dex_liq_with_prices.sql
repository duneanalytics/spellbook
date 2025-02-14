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
    union all
    --native tokens
    select
        blockchain
        , {{var('ETH_ERC20_ADDRESS')}} as contract_address -- 0x00..00
        , native_token_symbol as symbol
        , 18 as decimals
    from {{ source('evms','info') }}
)
, prices AS (
    SELECT
          blockchain
        , contract_address
        , minute
        , price
    FROM
        {{ source('prices','usd') }}
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('minute') }}
    {% endif %}
)      
, enrichments AS (
    SELECT
          base.blockchain
        , base.project
        , base.version
        , base.block_month
        , base.block_date
        , base.block_time
        , base.block_number
        , base.id
        , base.tx_hash
        , base.evt_index
        , base.salt
        , base.token0
        , base.token1
        , base.amount0_raw
        , base.amount1_raw
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
              en.blockchain
            , en.project
            , en.version
            , en.block_month
            , en.block_date
            , en.block_time
            , en.block_number
            , en.id
            , en.tx_hash
            , en.evt_index
            , en.salt
            , en.token0
            , en.token1
            , en.amount0_raw
            , en.amount1_raw
            , en.amount0
            , en.amount1
            , en.amount0 * p0.price AS amount0_usd
            , en.amount1 * p1.price AS amount1_usd
    FROM enrichments en    
    LEFT JOIN prices p0
           ON en.token0 = p0.contract_address
          AND en.blockchain = p0.blockchain
          AND p0.minute = date_trunc('minute', en.block_time)
    LEFT JOIN prices p1
           ON en.token1 = p1.contract_address
          AND en.blockchain = p1.blockchain
          AND p1.minute = date_trunc('minute', en.block_time)
)


SELECT
    blockchain
    , project
    , version
    , block_month
    , block_date
    , block_time
    , block_number
    , id
    , tx_hash
    , evt_index
    , salt
    , token0
    , token1
    , amount0_raw
    , amount1_raw  
    , amount0
    , amount1
    , amount0_usd
    , amount1_usd
FROM
    enrichment_with_prices

{% endmacro %}
