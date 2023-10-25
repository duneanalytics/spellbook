{{ config(
    tags=['dunesql'],
    schema = 'dex_arbitrum',
    alias = 'trades_beta',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'project', 'version', 'tx_hash', 'evt_index']
    )
}}

-- macros/models/sector/dex
{{
    enrich_dex_trades(
        blockchain = 'arbitrum'
        , base_trades = ref('dex_arbitrum_base_trades')
        , tokens_erc20_model = ref('tokens_erc20')
        , prices_model = source('prices', 'usd')
    )
}}
