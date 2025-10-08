{{ config(
    schema = 'thorchain_silver',
    alias = 'prices',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'contract_address', 'symbol'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'prices']
) }}

-- Get RUNE prices
WITH rune_prices AS (
    SELECT
        block_time,
        block_date,
        block_month,
        rune_price_usd as price,
        symbol,
        blockchain,
        contract_address
    FROM {{ ref('thorchain_silver_rune_price') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
),

-- Get external token prices directly from the main prices table
external_prices AS (
    SELECT
        p.minute as block_time,
        date(p.minute) as block_date,
        date_trunc('month', p.minute) as block_month,
        p.price,
        p.symbol,
        'thorchain' as blockchain,
        p.contract_address
    FROM {{ source('prices', 'usd') }} p
    WHERE p.blockchain = 'thorchain'
        AND p.price IS NOT NULL
    {% if is_incremental() %}
    AND {{ incremental_predicate('p.minute') }}
    {% endif %}
)

-- Union RUNE prices with external token prices
SELECT 
    block_time,
    block_date,
    block_month,
    price,
    symbol,
    blockchain,
    contract_address
FROM rune_prices

UNION ALL

SELECT 
    block_time,
    block_date,
    block_month,
    price,
    symbol,
    blockchain,
    contract_address
FROM external_prices
