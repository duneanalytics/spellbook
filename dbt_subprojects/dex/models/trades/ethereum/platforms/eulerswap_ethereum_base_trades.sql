{{ config(
    schema = 'eulerswap_ethereum'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}


select * from {{ ref('eulerswap_ethereum_raw_trades') }}
where source != 'uni_v4' -- exclude trades logged in Uniswap V4 
{% if is_incremental() %}
and {{ incremental_predicate('block_time') }}
{% endif %}

