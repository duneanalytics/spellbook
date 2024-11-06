{{
    config(
        schema = 'zeroex_v2_ethereum',
        alias ='base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

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



select * from all_tx 