{{ config(
    schema = 'eulerswap_bnb'
    , alias = 'raw_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

with 

base_model as (
{{
    eulerswap_compatible_trades(
        blockchain = 'bnb'
        , project = 'eulerswap'
        , version = '1'
        , eulerswapinstance_evt_swap = source('eulerswap_bnb', 'eulerswapinstance_evt_swap')
        , eulerswap_pools_created = ref('eulerswap_bnb_pool_creations')
        , filter = "(1 = 1)"
        , univ4_PoolManager_evt_Swap = source('uniswap_v4_bnb', 'PoolManager_evt_Swap') 
    )
}}
)

, base_union as (
    select 
        * 
    from (
        select 
            blockchain
            , project
            , version
            , block_month
            , block_date
            , block_time
            , block_number
            , cast(token_bought_amount_raw as uint256) as token_bought_amount_raw
            , cast(token_sold_amount_raw as uint256) as token_sold_amount_raw
            , token_bought_address
            , token_sold_address
            , taker
            , maker
            , project_contract_address
            , tx_hash
            , evt_index
            , fee 
            , protocolFee 
            , instance 
            , eulerAccount 
            , factory_address 
            , sender 
            , source
            , row_number() over (partition by tx_hash, evt_index order by tx_hash) as duplicates_rank
        from 
        base_model 
        where
           token_sold_amount_raw >= 0 and token_bought_amount_raw >= 0
    ) 
    where
        duplicates_rank = 1
) 

{{
    add_tx_columns(
        model_cte = 'base_union'
        , blockchain = 'bnb'
        , columns = ['from', 'to', 'index']
    )
}}