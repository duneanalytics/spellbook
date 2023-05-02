{{ config(
    schema = 'nft_ethereum',
    alias ='trades_beta',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','project_version','tx_hash','sub_tx_trade_id']
    )
}}
-- (project, project_version, model)
{% set base_models = [
     ('archipelago',    'v1',   ref('archipelago_ethereum_base_trades'))
    ,('superrare',    'v1',   ref('superrare_ethereum_base_trades'))
    ,('foundation',    'v1',   ref('foundation_ethereum_base_trades'))
] %}


-- macros/models/sector/nft
{{
    enrich_trades(
        blockchain='ethereum',
        models=base_models,
        transactions_model=source('ethereum','transactions'),
        tokens_nft_model=ref('tokens_ethereum_nft'),
        tokens_erc20_model=ref('tokens_ethereum_erc20'),
        prices_model=ref('prices_usd_forward_fill'),
        aggregators=ref('nft_ethereum_aggregators'),
        aggregator_markers=ref('nft_ethereum_aggregators_markers')
    )
}}
