{{ config(
    schema = 'dex'
    , alias = 'automated_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'block_month', 'block_number', 'tx_index', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)
}}

with dexs AS (
    {{
        log_decoded_enrich_dex_trades(
            base_trades = ref('dex_automated_base_trades_mapped')
            , tokens_erc20_model = source('tokens', 'erc20')
        )
    }}
)

select *
from dexs