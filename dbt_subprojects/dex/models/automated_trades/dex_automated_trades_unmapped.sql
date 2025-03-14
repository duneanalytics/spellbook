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

select 
    blockchain,
    version,
    dex_type,
    concat('unmapped_', blockchain, '_', dex_type, '_', substring( cast(factory_address as varchar), 1, 4)) as project,
    block_month,
    block_date,
    block_time,
    block_number,
    token_bought_symbol,
    token_sold_symbol,
    token_pair,
    token_bought_amount,
    token_sold_amount,
    token_bought_amount_raw,
    token_sold_amount_raw,
    amount_usd,
    token_bought_address,
    token_sold_address,
    taker,
    maker,
    project_contract_address,
    pool_topic0,
    factory_address,
    factory_topic0,
    factory_info,
    tx_hash,
    tx_from,
    tx_to,
    evt_index,
    tx_index
from dexs 
WHERE NOT EXISTS (
    SELECT 1 
    FROM {{ ref('dex_mapping') }} AS dex_map
    WHERE dexs.factory_address = dex_map.factory
    AND dexs.blockchain = dex_map.blockchain
)