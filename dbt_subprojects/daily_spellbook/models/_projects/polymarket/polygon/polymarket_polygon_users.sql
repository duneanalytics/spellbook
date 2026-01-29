{{ 
  config(
    schema = 'polymarket_polygon',
    alias = 'users_address_lookup',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['polymarket_wallet'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.created_time')]
    , post_hook='{{ hide_spells() }}'
  )
}}

WITH first_capital_action AS (
    SELECT 
        to_address as proxy,
        MIN(block_time) as first_funded_time,
        MIN_BY(tx_hash, block_time) as first_funded_tx_hash,
        MIN_BY(from_address, block_time) as first_funded_by
    FROM {{ ref('polymarket_polygon_users_capital_actions') }}
    GROUP BY to_address  -- Remove tx_hash from GROUP BY
)

{% if is_incremental() %}
-- get wallets with new capital actions that might need updating
, new_capital_actions AS (
    SELECT DISTINCT to_address as proxy
    FROM {{ ref('polymarket_polygon_users_capital_actions') }}
    WHERE {{ incremental_predicate('block_time') }}
    )
{% endif %}

, wallet_addresses AS (
    SELECT 
        block_time as created_time,
        block_number,
        type_of_wallet,
        owner,
        proxy,
        tx_hash
    FROM {{ ref('polymarket_polygon_users_safe_proxies') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
    
    UNION ALL
    
    SELECT 
        block_time as created_time,
        block_number,
        type_of_wallet,
        owner,
        proxy,
        tx_hash
    FROM {{ ref('polymarket_polygon_users_magic_wallet_proxies') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
    
    {% if is_incremental() %}
    -- Also include existing unfunded wallets that now have new capital actions
    UNION ALL
    
    SELECT 
        created_time,
        block_number,
        wallet_type as type_of_wallet,
        owner,
        polymarket_wallet as proxy,
        created_tx_hash as tx_hash
    FROM {{ this }}
    WHERE polymarket_wallet IN (SELECT proxy FROM new_capital_actions)
    AND has_been_funded = false
    {% endif %}
)

SELECT
    w.created_time,
    w.block_number,
    w.type_of_wallet as wallet_type,
    w.owner,
    w.proxy as polymarket_wallet,
    w.tx_hash as created_tx_hash,
    f.first_funded_time,
    f.first_funded_by,
    f.first_funded_tx_hash,
    CASE 
        WHEN f.first_funded_time IS NOT NULL THEN true 
        ELSE false 
    END as has_been_funded
FROM wallet_addresses w
LEFT JOIN first_capital_action f ON f.proxy = w.proxy
