{{ config(
    tags=['dunesql'],
    schema = 'defiswap_ethereum',
    alias ='trades_beta',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'project', 'version', 'tx_hash', 'evt_index']
    )
}}

-- (blockchain, project, project_version, model, project_start_date)
{% set base_model = (
    'ethereum',
    'defiswap',
    '1',
    ref('defiswap_ethereum_base_trades'),
    '2020-09-09'
) %}


-- macros/models/sector/dex
{{
    dex_enrich_trades(
        model = base_model
        ,transactions_model = source(base_model[0], 'transactions')
        ,tokens_erc20_model = ref('tokens_erc20')
        ,prices_model = source('prices', 'usd')
    )
}}
