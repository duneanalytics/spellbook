{{ config(
    schema = 'eulerswap'
    , alias = 'trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set base_models = [
    ref('eulerswap_ethereum_raw_trades')
    , ref('eulerswap_bnb_raw_trades')
    , ref('eulerswap_unichain_raw_trades')
] %}

with

base_trades as (
    {% for base_model in base_models %}
    SELECT 
        blockchain
        , project
        , version
        , block_month
        , block_date
        , block_time
        , block_number
        , token_bought_amount_raw
        , token_sold_amount_raw
        , token_bought_address
        , token_sold_address
        , taker
        , maker
        , project_contract_address
        , tx_hash
        , tx_from 
        , tx_to 
        , evt_index
        , fee 
        , protocolFee 
        , instance 
        , eulerAccount 
        , factory_address 
        , sender 
        , source 
    FROM
    {{ base_model }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)

, tokens_metadata as (
    --erc20 tokens
    select
        blockchain
        , contract_address
        , symbol
        , decimals
    from
        {{ source('tokens', 'erc20') }}
)
, enrichments AS (
    SELECT
        base_trades.blockchain
        , base_trades.project
        , base_trades.version
        , base_trades.block_month
        , base_trades.block_date
        , base_trades.block_time
        , base_trades.block_number
        , erc20_bought.symbol AS token_bought_symbol
        , erc20_sold.symbol AS token_sold_symbol
        , case
            when lower(erc20_bought.symbol) > lower(erc20_sold.symbol) then concat(erc20_sold.symbol, '-', erc20_bought.symbol)
            else concat(erc20_bought.symbol, '-', erc20_sold.symbol)
            end AS token_pair
        , base_trades.token_bought_amount_raw / power(10, erc20_bought.decimals) AS token_bought_amount
        , base_trades.token_sold_amount_raw / power(10, erc20_sold.decimals) AS token_sold_amount
        , base_trades.token_bought_amount_raw
        , base_trades.token_sold_amount_raw
        , base_trades.token_bought_address
        , base_trades.token_sold_address
        , coalesce(base_trades.taker, base_trades.tx_from) AS taker
        , base_trades.maker
        , base_trades.project_contract_address
        , base_trades.tx_hash
        , base_trades.tx_from
        , base_trades.tx_to
        , base_trades.evt_index
        , base_trades.fee 
        , base_trades.protocolFee 
        , base_trades.instance 
        , base_trades.eulerAccount 
        , base_trades.factory_address 
        , base_trades.sender 
        , base_trades.source
    FROM
        base_trades
    LEFT JOIN
        tokens_metadata as erc20_bought
        ON erc20_bought.contract_address = base_trades.token_bought_address
        AND erc20_bought.blockchain = base_trades.blockchain
    LEFT JOIN
        tokens_metadata as erc20_sold
        ON erc20_sold.contract_address = base_trades.token_sold_address
        AND erc20_sold.blockchain = base_trades.blockchain
)

, enrichments_with_prices AS (
    {{
        add_amount_usd(
            trades_cte = 'enrichments'
        )
    }}
)

SELECT
    blockchain
    , project
    , version
    , block_month
    , block_date
    , block_time
    , block_number
    , token_bought_symbol
    , token_sold_symbol
    , token_pair
    , token_bought_amount
    , token_sold_amount
    , token_bought_amount_raw
    , token_sold_amount_raw
    , amount_usd
    , token_bought_address
    , token_sold_address
    , taker
    , maker
    , project_contract_address
    , tx_hash
    , tx_from
    , tx_to
    , evt_index
    , fee 
    , protocolFee 
    , instance 
    , eulerAccount 
    , factory_address 
    , sender 
    , source 
FROM
    enrichments_with_prices