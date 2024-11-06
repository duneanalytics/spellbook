{{  config(
    schema = 'zeroex_ethereum',
    alias = 'v2_rfq_trades',
    materialized='incremental',
    partition_by = ['block_month'],
    unique_key = ['block_month', 'block_date', 'tx_hash', 'evt_index'],
    on_schema_change='sync_all_columns',
    file_format ='delta',
    incremental_strategy='merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)}}

{% set zeroex_settler_start_date = '2024-07-15' %}

WITH zeroex_tx AS (
    {{
        settler_txs_cte(
            blockchain = 'ethereum',
            start_date = zeroex_settler_start_date
        )
    }}
),
all_tx AS (
    {{
        zeroex_rfq_events(
            blockchain = 'ethereum',
            start_date = zeroex_settler_start_date
            
        )
    }}
)



select 
    blockchain,
    project,
    version,
    block_date,
    block_month,
    block_time,
    block_number,
    taker_symbol,
    maker_symbol,
    token_pair,
    taker_token_amount,
    maker_token_amount,
    taker_token_amount_raw as token_sold_amount_raw,
    maker_token_amount_raw as token_bought_amount_raw,
    amount_usd as volume_usd,
    taker_token as token_sold_address,
    maker_token as token_bought_address,
    taker,
    maker,
    tag,
    zid,
    tx_hash,
    tx_from,
    tx_to,
    tx_index AS evt_index,
    (ARRAY[-1]) AS trace_address,
    'settler' AS type,
    TRUE AS swap_flag,
    contract_address as project_contract_address
 from all_tx 
