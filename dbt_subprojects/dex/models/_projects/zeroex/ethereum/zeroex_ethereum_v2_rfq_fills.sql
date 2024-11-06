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
    *
 from all_tx 
