{{config(
    alias = 'beraswap_pools_berachain',
    post_hook='{{ expose_spells(blockchains = \'["berachain"]\',
                                spell_type = "sector",
                                spell_name = "labels",
                                contributors = \'["hosuke"]\') }}'
)}}

WITH pools AS (
    SELECT
        poolId AS pool_id,
        poolAddress AS pool_address,
        specialization,
        evt_block_time,
        CASE 
            WHEN w.pool IS NOT NULL THEN 'Weighted Pool'
            WHEN specialization = 0 THEN 'General Pool'
            WHEN specialization = 1 THEN 'Minimal Swap Info'
            WHEN specialization = 2 THEN 'Two Token'
            ELSE 'Unknown'
        END AS pool_type
    FROM {{ source('beraswap_berachain', 'vault_evt_poolregistered') }} p
    LEFT JOIN {{ source('beraswap_berachain', 'weightedpoolfactory_evt_poolcreated') }} w
        ON p.poolAddress = w.pool
),

pool_tokens AS (
    SELECT DISTINCT
        p.pool_id,
        p.pool_address,
        p.pool_type,
        p.evt_block_time,
        token AS token_address
    FROM pools p
    LEFT JOIN {{ source('beraswap_berachain', 'vault_evt_swap') }} s
        ON p.pool_id = s.poolId
    CROSS JOIN (
        VALUES
            (tokenIn),
            (tokenOut)
    ) AS t(token)
),

tokens_with_symbols AS (
    SELECT 
        pt.*,
        COALESCE(e.symbol, '?') AS token_symbol
    FROM pool_tokens pt
    LEFT JOIN {{ source('tokens', 'erc20') }} e
        ON pt.token_address = e.contract_address 
        AND e.blockchain = 'berachain'
)

SELECT
    'berachain' AS blockchain,
    pool_address AS address,
    lower(array_join(array_agg(DISTINCT token_symbol ORDER BY token_symbol), '/')) AS name,
    pool_type,
    '1' AS version,
    'beraswap_pool' AS category,
    'hosuke' AS contributor,
    'query' AS source,
    TIMESTAMP'2024-02-19 00:00' AS created_at,
    now() AS updated_at,
    'beraswap_pools_berachain' AS model_name,
    'identifier' AS label_type
FROM tokens_with_symbols
GROUP BY pool_id, pool_address, pool_type, evt_block_time
ORDER BY evt_block_time
