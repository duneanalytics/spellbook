{{ config(
    tags = ['dunesql'],
    alias = alias('send'),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'tx_hash', 'user_address', 'trace_address', 'source_chain_id', 'destination_chain_id']
    )
}}

{{layerzero_send(
  blockchain='polygon',
  transaction_start_date="2022-03-15",
  endpoint_contract="0x66a71dcef29a0ffbdbe3c6a460a3b5bc225cd675",
  native_token_contract="0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270",
  source_chain_id=109,
  endpoint_call_send = source('layerzero_polygon', 'Endpoint_call_send'),
)}}
