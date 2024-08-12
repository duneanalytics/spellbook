{{  config(
        schema = 'zeroex_avalanche_c',
        alias = 'api_fills',
        materialized='incremental',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'tx_hash', 'evt_index'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set zeroex_v3_start_date = '2019-12-01' %}
{% set zeroex_v4_start_date = '2021-01-06' %}

-- Test Query here: https://dune.com/queries/1855493

WITH zeroex_tx AS (
    {{
        zeroex_tx_cte(
            blockchain = 'avalanche_c',
            start_date = zeroex_v3_start_date
        )
    }}
)

, all_tx AS (
    {{
        zeroex_main_events_cte(
            blockchain = 'avalanche_c',
            start_date = zeroex_v4_start_date,
            contract_address = '0xdef1c0ded9bec7f1a1670819833240f027b25eff'
        )
    }}
)

{{
    zeroex_api_fills(
        blockchain = 'avalanche_c',
        native_token_address = '0x0000000000000000000000000000000000000000',
        wrapped_native_token_address = '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7',
        stablecoin_addresses = [
            '0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7',
            '0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e',
            '0xd586e7f844cea2f87f50152665bcbc2c279d8d70'
        ]
    )
}}
