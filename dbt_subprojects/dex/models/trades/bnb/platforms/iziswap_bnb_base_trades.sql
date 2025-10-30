{{
    config(
        schema = 'iziswap_bnb',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

with dexs as (
    select
        t.evt_block_number as block_number,
        t.evt_block_time as block_time,
        case when t.sellXEarnY then t.tokenY else t.tokenX end as token_bought_address,
		case when t.sellXEarnY then t.tokenX else t.tokenY end as token_sold_address,
		case when t.sellXEarnY then t.amountY else t.amountX end as token_bought_amount_raw,
		case when t.sellXEarnY then t.amountX else t.amountY end as token_sold_amount_raw,
        cast(null as varbinary) as taker,
        cast(null as varbinary) as maker,
        t.evt_tx_hash as tx_hash,
        t.evt_index,
        t.contract_address as project_contract_address
    from {{ source('izumi_bnb', 'iZiSwapPool_evt_Swap') }} t
    {% if is_incremental() %}
    where {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
)

select
    'bnb' as blockchain,
    'iziswap' as project,
    '1' as version,
    CAST(date_trunc('month', dexs.block_time) as date) as block_month,
    CAST(date_trunc('day', dexs.block_time) as date) as block_date,
    dexs.block_time,
    dexs.block_number,
    dexs.token_bought_amount_raw,
    dexs.token_sold_amount_raw,
    dexs.token_bought_address,
    dexs.token_sold_address,
    dexs.taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    dexs.evt_index
from dexs
