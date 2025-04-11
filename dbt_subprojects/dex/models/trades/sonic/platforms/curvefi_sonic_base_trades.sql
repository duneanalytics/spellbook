{{
    config(
        schema = 'curvefi_sonic',
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
        t.bought_id as bought_id,
        t.sold_id as sold_id,
        t.tokens_bought AS token_bought_amount_raw,
        t.tokens_sold AS token_sold_amount_raw,
        t.buyer as taker,
        cast(null as varbinary) as maker,
        t.contract_address as project_contract_address,
        t.evt_tx_hash as tx_hash,
        t.evt_index
    from {{ source('curvefi_sonic', 'CurveStableSwap_evt_TokenExchange') }} t
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
    from {{ source('curvefi_sonic', 'CurveStableSwap_evt_TokenExchangeUnderlying') }} t
    {% if is_incremental() %}
    where {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
),

dexs as (
    select
        t.block_number,
        t.block_time,
        CASE
            WHEN t.bought_id = 0 THEN p.coin0
            WHEN t.bought_id = 1 THEN p.coin1
            WHEN t.bought_id = 2 THEN p.coin2
            WHEN t.bought_id = 3 THEN p.coin3
        END as token_bought_address,
        CASE
            WHEN t.sold_id = 0 THEN p.coin0
            WHEN t.sold_id = 1 THEN p.coin1
            WHEN t.sold_id = 2 THEN p.coin2
            WHEN t.sold_id = 3 THEN p.coin3
        END as token_sold_address,
        t.token_bought_amount_raw,
        t.token_sold_amount_raw,
        t.taker,
        t.maker,
        t.project_contract_address,
        t.tx_hash,
        t.evt_index
    from exchange_evt_all t
    inner join {{ ref('curve_sonic_pools') }} p
        on t.project_contract_address = p.pool_address

    union all

    select
        t.block_number,
        t.block_time,
        CASE
            WHEN t.bought_id = 0 THEN p.undercoin0
            WHEN t.bought_id = 1 THEN p.undercoin1
            WHEN t.bought_id = 2 THEN p.undercoin2
            WHEN t.bought_id = 3 THEN p.undercoin3
        END as token_bought_address,
        CASE
            WHEN t.sold_id = 0 THEN p.undercoin0
            WHEN t.sold_id = 1 THEN p.undercoin1
            WHEN t.sold_id = 2 THEN p.undercoin2
            WHEN t.sold_id = 3 THEN p.undercoin3
        END as token_sold_address,
        t.token_bought_amount_raw,
        t.token_sold_amount_raw,
        t.taker,
        t.maker,
        t.project_contract_address,
        t.tx_hash,
        t.evt_index
    from exchange_und_evt_all t
    inner join {{ ref('curve_sonic_pools') }} p
        on t.project_contract_address = p.pool_address
)

select
    'sonic' as blockchain,
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