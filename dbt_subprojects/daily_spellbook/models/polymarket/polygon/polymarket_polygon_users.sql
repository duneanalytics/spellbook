{{ 
  config(
    schema = 'polymarket_polygon',
    alias = 'users_address_lookup',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['polymarket_wallet'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook = '{{ expose_spells(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "polymarket",
                                  contributors = \'["0xBoxer"]\') }}'
  )
}}

WITH first_capital_action AS (
    SELECT 
        to_address as proxy,
        MIN(tx_hash) as first_funded_tx_hash,
        MIN(block_time) as first_funded_time,
        MIN(from_address) as first_funded_by
    FROM {{ ref('polymarket_polygon_users_capital_actions') }}
    GROUP BY to_address  -- Remove tx_hash from GROUP BY
),

wallet_addresses AS (
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
