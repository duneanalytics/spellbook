{{ config(
    schema = 'dex'
    , alias ='trades_beta'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    )
}}

-- macros/models/sector/dex
{{
    enrich_dex_trades(
        stg_trades = ref('dex_stg_trades')
        , tokens_erc20_model = ref('tokens_erc20')
        , prices_model = source('prices', 'usd')
    )
}}