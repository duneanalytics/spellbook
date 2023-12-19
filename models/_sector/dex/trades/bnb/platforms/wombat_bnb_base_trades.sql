{{
    config(
        schema = 'wombat_bnb',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set wombat_bnb_swap_evt_tables = [
    source('wombat_bnb', 'Pool_evt_Swap'),
    source('wombat_bnb', 'HighCovRatioFeePool_evt_Swap'),
    source('wombat_bnb', 'DynamicPool_evt_Swap'),
    source('wombat_bnb', 'mWOM_Pool_evt_Swap'),
    source('wombat_bnb', 'qWOM_WOMPool_evt_Swap'),
    source('wombat_bnb', 'WMX_WOM_Pool_evt_Swap')
] %}

with dexs as (
    {% for swap_evt_table in wombat_bnb_swap_evt_tables %}
        select
            t.evt_block_number as block_number,
            t.evt_block_time as block_time,
            t.toAmount as token_bought_amount_raw,
            t.fromAmount as token_sold_amount_raw,
            t.toToken as token_bought_address,
            t.fromToken as token_sold_address,
            t.evt_tx_hash as tx_hash,
            t.evt_index,
            t.to as taker,
            cast(null as varbinary) as maker,
            t.contract_address as project_contract_address
        from {{ swap_evt_table }} t
        {% if is_incremental() %}
        where {{ incremental_predicate('t.evt_block_time') }}
        {% endif %}

        {% if not loop.last %}
        union all
        {% endif %}
    {% endfor%}
)

select
    'bnb' as blockchain,
    'wombat' as project,
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
