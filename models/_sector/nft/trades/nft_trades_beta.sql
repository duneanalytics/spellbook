{{ config(
    schema = 'nft',
    tags = ['dunesql'],
    alias = alias('trades_beta'),
    partition_by = ['blockchain','project','block_month'],
    materialized = 'view',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','project_version','tx_hash','sub_tx_trade_id']
    )
}}

-- macros/models/sector/nft
{{ enrich_nft_trades(ref('nft_base_trades'))}}
