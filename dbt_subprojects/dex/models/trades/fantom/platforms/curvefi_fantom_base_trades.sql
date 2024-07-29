{{
    config(
        schema = 'curvefi_fantom',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

with

exchange_evt_all as (
    select
        t.evt_block_number as block_number,
        t.evt_block_time as block_time,
        coalesce(cast(t.bought_id_uint256 as int256), t.bought_id_int256) as bought_id,
		coalesce(cast(t.sold_id_uint256 as int256), t.sold_id_int256) as sold_id,
		t.tokens_bought AS token_bought_amount_raw,
		t.tokens_sold AS token_sold_amount_raw,
        t.buyer as taker,
        cast(null as varbinary) as maker,
        t.contract_address as project_contract_address,
        t.evt_tx_hash as tx_hash,
        t.evt_index
    from {{ source('curvefi_fantom', 'StableSwap_evt_TokenExchange') }} t
    {% if is_incremental() %}
    where {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
),
 
exchange_und_evt_all as (
    select
        t.evt_block_number as block_number,
        t.evt_block_time as block_time,
        t.bought_id,
		t.sold_id,
		t.tokens_bought AS token_bought_amount_raw,
		t.tokens_sold AS token_sold_amount_raw,
        t.buyer as taker,
        cast(null as varbinary) as maker,
        t.contract_address as project_contract_address,
        t.evt_tx_hash as tx_hash,
        t.evt_index
    from {{ source('curvefi_fantom', 'StableSwap_evt_TokenExchangeUnderlying') }} t
    {% if is_incremental() %}
    where {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
),

dexs as (
    select
        t.block_number,
        t.block_time,
        pt_bought.token_address as token_bought_address,
        pt_sold.token_address as token_sold_address,
		t.token_bought_amount_raw,
		t.token_sold_amount_raw,
        t.taker,
        t.maker,
        t.project_contract_address,
        t.tx_hash,
        t.evt_index
    from exchange_evt_all t
    inner join {{ ref('curvefi_fantom_pool_tokens') }} pt_bought
        on t.bought_id = cast(pt_bought.token_id as int256)
        and t.project_contract_address = pt_bought.pool
        and pt_bought.token_type = 'pool_token'
    inner join {{ ref('curvefi_fantom_pool_tokens') }} pt_sold
        on t.sold_id = cast(pt_sold.token_id as int256)
        and t.project_contract_address = pt_sold.pool
        and pt_sold.token_type = 'pool_token'

    union all

    select
        t.block_number,
        t.block_time,
        pt_bought.token_address as token_bought_address,
        pt_sold.token_address as token_sold_address,
		t.token_bought_amount_raw,
		t.token_sold_amount_raw,
        t.taker,
        t.maker,
        t.project_contract_address,
        t.tx_hash,
        t.evt_index
    from exchange_und_evt_all t
    inner join {{ ref('curvefi_fantom_pool_tokens') }} pt_bought
        on t.bought_id = cast(pt_bought.token_id as int256)
        and t.project_contract_address = pt_bought.pool
        and pt_bought.token_type = 'underlying_token_bought'
    inner join {{ ref('curvefi_fantom_pool_tokens') }} pt_sold
        on t.sold_id = cast(pt_sold.token_id as int256)
        and t.project_contract_address = pt_sold.pool
        and pt_sold.token_type = 'underlying_token_sold'
)

select
    'fantom' as blockchain,
    'curve' as project,
    '2' as version,
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
