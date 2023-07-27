{{ config(
    schema = 'oneplanet_polygon',
    alias = alias('events'),
    tags = ['dunesql'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index', 'token_id']
    )
}}

{{ seaport_trades(
     blockchain = 'polygon'
     ,source_transactions = source('polygon','transactions')
     ,ref_base_pairs= ref('oneplanet_polygon_base_pairs')
     ,native_token_address = '0x0000000000000000000000000000000000000000'
     ,alternative_token_address = '0x0000000000000000000000000000000000001010'
     ,native_token_symbol = 'MATIC'
     ,start_date = '2023-09-03'
     )
}}
