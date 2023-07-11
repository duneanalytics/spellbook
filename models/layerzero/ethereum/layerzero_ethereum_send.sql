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
  native_token_contract="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
  source_chain_id=101,
  endpoint_call_send = source ('layerzero_ethereum', 'Endpoint_call_send'),
  wrapped_native_symbol = 'WETH',
  native_symbol = 'ETH',
)}}
