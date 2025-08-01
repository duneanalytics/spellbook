{% set blockchain = 'abstract' %}

{{ config(
    schema = 'reservoir_swap_v3_' + blockchain
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)
}}

with dexs as (
    select
        dexs.*
    from 
        {{ ref('uniswap_v3_forks_' + blockchain + '_base_trades') }} as dexs
    where
        dexs.factory_address = 0xA1160e73B63F322ae88cC2d8E700833e71D0b2a1
        {% if is_incremental() -%}
        and {{ incremental_predicate('dexs.block_time') }}
        {% endif -%}
)
SELECT
    '{{ blockchain }}' AS blockchain
    , 'reservoir_swap' AS project
    , '3' AS version
    , CAST(date_trunc('month', dexs.block_time) AS date) AS block_month
    , CAST(date_trunc('day', dexs.block_time) AS date) AS block_date
    , dexs.block_time
    , dexs.block_number
    , dexs.token_bought_amount_raw
    , dexs.token_sold_amount_raw
    , dexs.token_bought_address
    , dexs.token_sold_address
    , dexs.taker
    , dexs.maker
    , dexs.project_contract_address
    , dexs.tx_hash
    , dexs.evt_index
FROM
    dexs