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
  blockchain='avalanche_c',
  transaction_start_date="2022-03-15",
  endpoint_contract="0x66a71dcef29a0ffbdbe3c6a460a3b5bc225cd675",
  native_token_contract="0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7",
  source_chain_id=106,
  endpoint_call_send = source ('layerzero_avalanche_c', 'Endpoint_call_send'),
)}}
