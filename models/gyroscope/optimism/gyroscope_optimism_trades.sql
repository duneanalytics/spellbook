{{ config(
    schema = 'gyroscope_optimism',
    alias = 'trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'blockchain', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

with E_CLPs AS (
    SELECT 
        x.pool
        , y.min_block_time
        FROM {{ source('gyroscope_optimism','GyroECLPPoolFactory_evt_PoolCreated') }} x
        LEFT JOIN (
            SELECT min(evt_block_time) AS min_block_time 
            FROM {{ source('gyroscope_optimism','GyroECLPPoolFactory_evt_PoolCreated') }} 
        ) y
        on x.pool is not null
)

SELECT
    blockchain,
    'gyroscope' AS project,
    block_date,
    block_month,
    block_time,
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
    pool_id,
    swap_fee,
    tx_hash,
    tx_from,
    tx_to,
    evt_index
FROM {{ ref('balancer_v2_optimism_trades') }} x 
inner join E_CLPs y
on x.block_time >= y.min_block_time
and x.project_contract_address = y.pool
{% if is_incremental() %}
WHERE 
    {{incremental_predicate('x.block_time')}}
{% endif %}