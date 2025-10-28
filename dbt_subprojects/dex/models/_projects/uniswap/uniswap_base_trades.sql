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

{% set bunni_models = [
    {"source": source('bunni_v2_base', 'bunnihook_evt_swap'), "chain": "base"}
    , {"source": source('bunni_v2_arbitrum', 'bunnihook_evt_swap'), "chain": "arbitrum"}
    , {"source": source('bunni_v2_ethereum', 'bunnihook_evt_swap'), "chain": "ethereum"}
    , {"source": source('bunni_v2_unichain', 'bunnihook_evt_swap'), "chain": "unichain"}
    , {"source": source('bunni_v2_bnb', 'bunnihook_evt_swap'), "chain": "bnb"}
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

bunni_fees as (
    {% for model in bunni_models %}
    select
        '{{ model.chain }}' as blockchain
        , evt_tx_hash  as tx_hash
        , fee  
        , evt_block_date as block_date
        , id 
        , evt_index + 1 as evt_index
        , 'bunni' as hooks 
    from
        {{ model.source }}
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
            v4.fee/1e6
            , case 
                when gt.version = '2' then unp.fee/1e2 
                when gt.version = '3' then unp.fee/1e6 
            end -- v2 fees are set to 0.3 while v3 fees are the raw values
        ) as uni_fee 
        , coalesce (
            bf.fee/1e6
            , 0
        ) as hooks_fee 
        coalesce (
            bf.hooks 
            , 'unlabelled'
        ) as hooks 
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
    left join 
    bunni_fees bf 
        on gt.blockchain = bf.blockchain 
        and gt.block_date = bf.block_date
        and gt.tx_hash = bf.tx_hash
        and gt.evt_index = bf.evt_index
        and gt.maker = bf.id 
        and gt.version = '4'
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
        -- uni fee columns 
        , token_sold_amount * uni_fee * 1 as uni_fee_amount_usd
        , token_sold_amount * uni_fee as uni_fee_amount 
        , token_sold_amount_raw * uni_fee as uni_fee_amount_raw
        , uni_fee * 1e2 as uni_fee -- convert back to correct value 
        -- hooks fee columns 
        , token_sold_amount * hooks_fee * 1 as hooks_fee_amount_usd
        , token_sold_amount * hooks_fee as hooks_fee_amount 
        , token_sold_amount_raw * hooks_fee as hooks_fee_amount_raw
        , hooks_fee * 1e2 as hooks_fee -- convert back to correct value 
        , hooks 
    from 
    add_fees

