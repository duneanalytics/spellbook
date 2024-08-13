{{
    config(
        tags = ['prod_exclude'],
        materialized='incremental',
        schema = 'safe_zksync',
        alias = 'eth_transfers',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'tx_hash', 'trace_address', 'amount_raw'],
        on_schema_change='fail',
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(\'["zksync"]\',
                                    "project",
                                    "safe",
                                    \'["danielpartida", "kryptaki"]\') }}'
    )
}}

{% set project_start_date = '2023-09-01' %}

{{ safe_native_transfers(
    blockchain = 'zksync',
    native_token_symbol = 'ETH',
    project_start_date = project_start_date
) }}
