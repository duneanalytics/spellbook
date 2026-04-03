{{
    config(
        schema = 'oneinch_lop_arbitrum',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

SELECT
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
FROM {{ ref('oneinch_lop_own_trades') }}
WHERE blockchain = 'arbitrum'
{% if var('dev_dates', false) -%}
    AND block_date > current_date - interval '3' day
{%- else -%}
    {% if is_incremental() %}
    AND {{ incremental_predicate('block_time') }}
    {% endif %}
{%- endif %}
