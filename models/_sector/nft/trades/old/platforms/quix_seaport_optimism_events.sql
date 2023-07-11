{{ config(
    schema = 'quix_seaport_optimism',
    alias = 'seaport_events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'tx_hash', 'evt_index', 'nft_contract_address', 'token_id', 'sub_type', 'sub_idx']
    )
}}

{{ seaport_trades(
     blockchain = 'optimism'
     ,source_transactions = source('optimism','transactions')
     ,ref_base_pairs= ref('quix_seaport_optimism_base_pairs')
     ,native_token_address = '0x0000000000000000000000000000000000000000'
     ,alternative_token_address = '0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000'
     ,native_token_symbol = 'ETH'
     ,start_date = '2022-07-29'
     )
}}
