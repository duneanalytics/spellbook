{{  config(
    schema = 'zeroex_v2_optimism',
    alias = 'trades',
    materialized='incremental',
    partition_by = ['block_month'],
    unique_key = ['block_month', 'block_date', 'tx_hash', 'evt_index'],
    on_schema_change='sync_all_columns',
    file_format ='delta',
    incremental_strategy='merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)}}

{% set zeroex_settler_start_date = '2024-07-15' %}
{% set blockchain = 'optimism' %}

WITH zeroex_tx AS (
    {{
        zeroex_settler_txs_cte(
            blockchain = blockchain,
            start_date = zeroex_settler_start_date
        )
    }}
),
zeroex_v2_trades_direct AS (
    {{
        zeroex_v2_trades_direct(
            blockchain = blockchain,
            start_date = zeroex_settler_start_date
            
        )
    }}
),
zeroex_v2_trades_indirect AS (
    {{
        zeroex_v2_trades_indirect(
            blockchain = blockchain,
            start_date = zeroex_settler_start_date
            
        )
    }}
),
tbl_trades AS (
    SELECT *
    FROM zeroex_v2_trades_direct
    UNION ALL
    SELECT *
    FROM zeroex_v2_trades_indirect
),
fills_count as(
    {{
        zeroex_v2_trades_fills_count(
            blockchain = blockchain,
            start_date = zeroex_settler_start_date
        )   
    }}
),
trade_details as (
    {{
        zeroex_v2_trades_detail(
            blockchain = blockchain,
            start_date = zeroex_settler_start_date
            
        )
    }}

)
select 
    t.*, fills_within 
 from trade_details t 
    left join fills_count f on t.tx_hash = f.tx_hash and t.block_time = f.block_time 
    