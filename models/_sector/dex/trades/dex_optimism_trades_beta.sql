{{ config(
    tags=['dunesql'],
    schema = 'dex_optimism',
    alias ='trades_beta',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'project', 'version', 'tx_hash', 'evt_index']
    )
}}


-- (blockchain, project, project_version, model)
{% set base_models = [
    ('optimism',   'uniswap',    '3',    ref('uniswap_optimism_v3_base_trades'))
] %}


-- macros/models/sector/dex
{{
    dex_enrich_trades(
        blockchain = 'optimism'
        ,models = base_models
        ,transactions_model = source('optimism', 'transactions')
        ,tokens_erc20_model = ref('tokens_erc20')
        ,prices_model = source('prices', 'usd')
    )
}}
