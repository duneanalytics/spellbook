{{ config(
    schema = 'dex'
    , alias = 'pools_beta'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

select
    *
from
    ref('dex_base_trades')
