{{ config(
    schema = 'dex_ethereum',
    alias ='trades_beta',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    )
}}
-- (project, project_version, model)
{% set base_models = [
     ('defiswap',   '1',    ref('defiswap_ethereum_base_trades'))
] %}


-- macros/models/sector/dex
{{
    enrich_trades(
        blockchain = 'ethereum',
        models = base_models,
        transactions_model = source('ethereum','transactions'),
        tokens_erc20_model = ref('tokens_erc20'),
        prices_model = source('prices', 'usd')
    )
}}
