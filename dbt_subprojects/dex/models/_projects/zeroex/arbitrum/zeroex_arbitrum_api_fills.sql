{{  config(
        schema = 'zeroex_arbitrum',
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

-- Original Test Query here: https://dune.com/queries/1855986

WITH zeroex_tx AS (
    {{
        zeroex_tx_cte(
            blockchain = 'arbitrum',
            start_date = zeroex_v3_start_date
        )
    }}
)

, all_tx AS (
    {{
        zeroex_main_events_cte(
            blockchain = 'arbitrum',
            start_date = zeroex_v4_start_date,
            contract_address = '0xdb6f1920a889355780af7570773609bd8cb1f498'
        )
    }}
)

{{
    zeroex_api_fills(
        blockchain = 'arbitrum',
        native_token_address = '0x0000000000000000000000000000000000000000',
        wrapped_native_token_address = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1',
        stablecoin_addresses = [
            '0xff970a61a04b1ca14834a43f5de4533ebddb5cc8',
            '0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9',
            '0xda10009cbd5d07dd0cecc66161fc93d7c9000da1'
        ]
    )
}}
