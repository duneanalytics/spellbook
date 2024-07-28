{{
  config(
    schema = 'mento_v2_celo',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

{{
    generic_spot_compatible_trades(
        blockchain = 'celo',
        project = 'mento',
        version = '2',
        source_evt_swap = source('mento_celo', 'Broker_evt_Swap'),
        taker = 'trader'
    )
}}
