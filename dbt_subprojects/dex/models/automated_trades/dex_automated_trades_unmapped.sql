{{ config(
    schema = 'dex'
    , alias = 'automated_trades_unmapped'
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
        enrich_dex_automated_trades(
            base_trades = ref('dex_automated_base_trades')
            , tokens_erc20_model = source('tokens', 'erc20')
        )
    }}
)

select *
from dexs 
WHERE NOT EXISTS (
    SELECT 1 
    FROM {{ ref('dex_mapping') }} AS dex_map
    WHERE dexs.factory_address = dex_map.factory
    AND dexs.blockchain = dex_map.blockchain
)