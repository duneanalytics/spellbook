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
        bought_id,
		sold_id,
		t.tokens_bought AS token_bought_amount_raw,
		t.tokens_sold AS token_sold_amount_raw,
        t.buyer as taker,
        cast(null as varbinary) as maker,
        t.contract_address as project_contract_address,
        t.evt_tx_hash as tx_hash,
        t.evt_index
    from {{ source('curvefi_sonic', 'curvestableswap_evt_TokenExchange') }} t
    {% if is_incremental() %}
    where {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
),

dexs as (
    select
        t.block_number,
        t.block_time,
        t.bought_id,
        t.sold_id,
        CASE
            WHEN CAST(t.bought_id AS INT) <= 3 AND CAST(t.bought_id AS INT) >= 0 -- Ensure bought_id is 0, 1, 2, or 3
            THEN p.coins[CAST(t.bought_id AS INT) + 1]
            ELSE NULL
        END as token_bought_address,
        CASE
            WHEN CAST(t.sold_id AS INT) <= 3 AND CAST(t.sold_id AS INT) >= 0   -- Ensure sold_id is 0, 1, 2, or 3
            THEN p.coins[CAST(t.sold_id AS INT) + 1]
            ELSE NULL
        END as token_sold_address,
		t.token_bought_amount_raw,
		t.token_sold_amount_raw,
        t.taker,
        t.maker,
        t.project_contract_address,
        t.tx_hash,
        t.evt_index,
        p.version as pool_version
    from exchange_evt_all t
    inner join {{ ref('curve_sonic_pools') }} p
        on t.project_contract_address = p.pool_address
)

select
    'sonic' as blockchain,
    'curve' as project,
    '2' as version,
    CASE WHEN lower(dexs.pool_version) like '%stableswap%' THEN 'stable' ELSE NULL END as pool_type,
    CAST(date_trunc('month', dexs.block_time) as date) as block_month,
    CAST(date_trunc('day', dexs.block_time) as date) as block_date,
    dexs.block_time,
    dexs.block_number,
    dexs.bought_id,
    dexs.sold_id,
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
