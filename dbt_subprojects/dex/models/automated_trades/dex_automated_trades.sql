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
        enrich_dex_automated_trades(
            base_trades = ref('dex_automated_base_trades')
            , tokens_erc20_model = source('tokens', 'erc20')
        )
    }}
)

select 
    dexs.blockchain,
    dexs.version,
    dexs.dex_type,
    dex_map.project,
    dexs.block_month,
    dexs.block_date,
    dexs.block_time,
    dexs.block_number,
    dexs.token_bought_symbol,
    dexs.token_sold_symbol,
    dexs.token_pair,
    dexs.token_bought_amount,
    dexs.token_sold_amount,
    dexs.token_bought_amount_raw,
    dexs.token_sold_amount_raw,
    dexs.amount_usd,
    dexs.token_bought_address,
    dexs.token_sold_address,
    dexs.taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.pool_topic0,
    dexs.factory_address,
    dexs.factory_topic0,
    dexs.factory_info,
    dexs.tx_hash,
    dexs.tx_from,
    dexs.tx_to,
    dexs.evt_index,
    dexs.tx_index
from dexs 
INNER JOIN {{ ref('dex_mapping') }} AS dex_map
    ON dexs.factory_address = dex_map.factory
    AND dexs.blockchain = dex_map.blockchain 