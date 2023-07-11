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
  blockchain='arbitrum',
  transaction_start_date="2022-03-15",
  endpoint_contract="0x66a71dcef29a0ffbdbe3c6a460a3b5bc225cd675",
  native_token_contract="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
  source_chain_id=110,
  endpoint_call_send = source ('layerzero_arbitrum', 'Endpoint_call_send'),
  wrapped_native_symbol = 'WETH',
  native_symbol = 'ETH',
)}}