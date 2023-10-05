{{ config(
    tags=['dunesql'],
    schema = 'dex_ethereum',
    alias ='trades_beta',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'project', 'version', 'tx_hash', 'evt_index']
    )
}}
-- (project, project_version, model)
{% set base_models = [
    -- defiswap
     ('defiswap',   '1',    ref('defiswap_ethereum_base_trades'))
    -- uniswap
    ,('uniswap',    '1',    ref('uniswap_ethereum_v1_base_trades'))
    ,('uniswap',    '2',    ref('uniswap_ethereum_v2_base_trades'))
    ,('uniswap',    '3',    ref('uniswap_ethereum_v3_base_trades'))
] %}


-- macros/models/sector/dex
{{
    dex_enrich_trades(
        blockchain = 'ethereum',
        models = base_models,
        transactions_model = source('ethereum','transactions'),
        tokens_erc20_model = ref('tokens_erc20'),
        prices_model = source('prices', 'usd')
    )
}}
