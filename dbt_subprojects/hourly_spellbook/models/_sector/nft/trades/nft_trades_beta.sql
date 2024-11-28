{{ config(
    schema = 'nft',
    alias = 'trades_beta',
    partition_by = ['blockchain','project','block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','project_version','tx_hash','sub_tx_trade_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

-- macros/models/sector/nft
{{ enrich_nft_trades(ref('nft_base_trades'))}}
