{{ config(
    schema = 'uniswap_v3_lens'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

Select 
            NULL AS blockchain
            , NULL AS project
            , NULL AS version
            , NULL AS block_month
            , NULL AS block_date
            , NULL AS block_time
            , NULL AS block_number
            , NULL AS token_bought_amount_raw
            , NULL AS token_sold_amount_raw
            , NULL AS token_bought_address
            , NULL AS token_sold_address
            , NULL AS taker
            , NULL AS maker
            , NULL AS project_contract_address
            , NULL AS tx_hash
            , NULL AS evt_index

/*
temp disable while uniswap_v3_lens is not properly decoded
{{
    uniswap_compatible_v3_trades(
        blockchain = 'lens'
        , project = 'uniswap'
        , version = '3'
        , Pair_evt_Swap = source('uniswap_v3_lens', 'Pair_evt_Swap')
        , Factory_evt_PoolCreated = source('uniswap_v3_lens', 'Factory_evt_PoolCreated')
    )
}}

*/
