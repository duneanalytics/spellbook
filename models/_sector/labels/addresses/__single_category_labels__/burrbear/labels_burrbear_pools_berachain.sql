{{config(
    alias = 'burrbear_pools_berachain',
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
            WHEN specialization = 0 THEN 'General Pool'
            WHEN specialization = 1 THEN 'Minimal Swap Info'
            WHEN specialization = 2 THEN 'Two Token'
            ELSE 'Unknown'
        END AS pool_type
    FROM {{ source('burrbear_berachain', 'vault_evt_poolregistered') }}
),

pool_tokens AS (
    SELECT 
        p.pool_id,
        p.pool_address,
        p.pool_type,
        p.evt_block_time,
        t.tokens AS token_address,
        COALESCE(e.symbol, '?') AS token_symbol
    FROM pools p
    CROSS JOIN UNNEST(tokens) AS t(tokens)
    LEFT JOIN {{ source('tokens', 'erc20') }} e
        ON t.tokens = e.contract_address 
        AND e.blockchain = 'berachain'
)

SELECT
    'berachain' AS blockchain,
    pool_address AS address,
    lower(array_join(array_agg(token_symbol ORDER BY token_symbol), '/')) AS name,
    pool_type,
    '1' AS version,
    'burrbear_pool' AS category,
    'hosuke' AS contributor,
    'query' AS source,
    TIMESTAMP'2024-02-19 00:00' AS created_at,
    now() AS updated_at,
    'burrbear_pools_berachain' AS model_name,
    'identifier' AS label_type
FROM pool_tokens
GROUP BY pool_id, pool_address, pool_type, evt_block_time
ORDER BY evt_block_time
