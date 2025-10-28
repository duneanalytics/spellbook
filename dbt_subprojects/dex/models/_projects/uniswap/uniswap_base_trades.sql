{{ config(
    schema = 'uniswap'
    , alias = 'base_trades'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set v4_models = [
    ref('uniswap_v4_arbitrum_base_trades')
    , ref('uniswap_v4_ethereum_base_trades')
    , ref('uniswap_v4_unichain_base_trades')
] %}

with 

v4_trades as (
    {% for model in v4_models %}
    select
        blockchain
        , tx_hash 
        , evt_index 
        , fee 
        , block_time 
        , block_date 
        , block_number 
    from
        {{ model }}
    {% if is_incremental() %}
    where
        {{ incremental_predicate('block_time') }}
    {% endif %}
    {% if not loop.last %}
    union all 
    {% endif %}
    {% endfor %}
),

get_trades as (
    select
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
    from
    {{ ref('dex_trades') }}
    where 1 = 1
    {% if is_incremental() %}
    and {{ incremental_predicate('block_time') }}
    {% endif %}
    and project = 'uniswap'
),

add_fees as (
    select 
        gt.*
        , coalesce (
            v4.fee
            , case 
                when gt.version = '2' then unp.fee 
                when gt.version = '3' then unp.fee/1e6 
            end -- v2 fees are set to 0.25 while v3 fees are the raw values
        ) as fee 
    from 
    get_trades gt 
    left join 
    v4_trades v4 
        on gt.blockchain = v4.blockchain 
        and gt.block_date = v4.block_date 
        and gt.tx_hash = v4.tx_hash 
        and gt.evt_index = v4.evt_index
        and gt.version = '4'
    left join 
    {{ ref('uniswap_pools') }} unp 
        on gt.blockchain = unp.blockchain
        and gt.project_contract_address = unp.pool 
        and gt.version = unp.version 
        and gt.version in ('2', '3')
)

    select
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
        -- fee columns 
        , token_sold_amount * fee * 1 as fee_amount_usd
        , token_sold_amount * fee as fee_amount 
        , token_sold_amount_raw * fee as fee_amount_raw
        , fee 
    from 
    add_fees

