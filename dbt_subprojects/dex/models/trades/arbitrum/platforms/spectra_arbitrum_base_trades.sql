{{ config(
    schema = 'spectra_arbitrum',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
) }}

{{ spectra_compatible_trades(
    blockchain = 'arbitrum',
    project = 'spectra',
    version = '1',
    TokenExchange_evt = source('spectra_multichain', 'vyper_contract_evt_tokenexchange'),
    Coins_call = source('spectra_multichain', 'vyper_contract_call_coins')
) }}
