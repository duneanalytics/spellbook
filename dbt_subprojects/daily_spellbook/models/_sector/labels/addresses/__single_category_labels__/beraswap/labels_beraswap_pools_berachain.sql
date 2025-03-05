{{config(
    alias = 'beraswap_pools_berachain',
    post_hook='{{ expose_spells(blockchains = \'["berachain"]\',
                                spell_type = "sector",
                                spell_name = "labels",
                                contributors = \'["hosuke"]\') }}'
)}}

WITH pools AS (
    SELECT
        pool_id,
        zip.tokens AS token_address,
        symbol,
        pool_type
    FROM (
        SELECT
            c.poolId AS pool_id,
            t.tokens,
            cc.symbol,
            CASE 
                WHEN specialization = 0 THEN 'General Pool'
                WHEN specialization = 1 THEN 'Minimal Swap Info'
                WHEN specialization = 2 THEN 'Two Token'
                ELSE 'Unknown'
            END AS pool_type
        FROM {{ source('beraswap_berachain', 'vault_evt_poolregistered') }} c
        INNER JOIN {{ source('beraswap_berachain', 'weightedpoolfactory_call_create') }} cc
            ON c.evt_tx_hash = cc.call_tx_hash
            AND bytearray_substring(c.poolId, 1, 20) = cc.output_0
        CROSS JOIN UNNEST(cc.tokens) AS t(tokens)
    ) zip
),

tokens_with_symbols AS (
    SELECT 
        p.*,
        COALESCE(e.symbol, '?') AS token_symbol
    FROM pools p
    LEFT JOIN {{ source('tokens', 'erc20') }} e
        ON p.token_address = e.contract_address 
        AND e.blockchain = 'berachain'
)

SELECT
    'berachain' AS blockchain,
    pool_id AS address,
    lower(array_join(array_agg(token_symbol ORDER BY token_symbol), '/')) AS name,
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
GROUP BY 1,2,4,5,6,7,8,9,10,11,12
