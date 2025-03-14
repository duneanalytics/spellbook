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

with mapped_trades as (
    select
        dexs.*
        , dex_map.project
    from 
        {{ ref('dex_automated_base_trades') }} as dexs
    inner join 
        {{ ref('dex_mapping') }} as dex_map
        on dexs.factory_address = dex_map.factory
        and dexs.blockchain = dex_map.blockchain 
    {% if is_incremental() %}
    where 
        {{ incremental_predicate('dexs.block_time') }}
    {% endif %}
), dexs AS (
    {{
        enrich_dex_automated_trades(
            base_trades = 'mapped_trades'
            , project = True
        )
    }}
)

select 
    dexs.blockchain,
    dexs.version,
    dexs.dex_type,
    dexs.project,
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