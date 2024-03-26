{% macro 
    balancer_bpt_supply_macro(
        blockchain, version
    ) 
%}

WITH pool_labels AS (
        SELECT * FROM (
            SELECT
                address,
                name,
                pool_type,
                ROW_NUMBER() OVER (PARTITION BY address ORDER BY MAX(updated_at) DESC) AS num
            FROM {{ ref('labels_balancer_v2_pools') }}
            WHERE blockchain = '{{blockchain}}'
            GROUP BY 1, 2, 3) 
        WHERE num = 1
    ),

  -- Extract mints and burns from transfers
    transfers AS (
        SELECT
            block_date AS day,
            contract_address AS token,
            COALESCE(SUM(CASE WHEN t."from" = 0x0000000000000000000000000000000000000000 THEN value / POWER(10, 18) ELSE 0 END), 0) AS mints,
            COALESCE(SUM(CASE WHEN t.to = 0x0000000000000000000000000000000000000000 THEN value / POWER(10, 18) ELSE 0 END), 0) AS burns
        FROM  {{ ref('balancer_transfers_bpt') }} t
        WHERE blockchain = '{{blockchain}}'   
        AND version = '{{version}}'
        GROUP BY 1, 2
    ),

    -- Calculate token balances over time
    balances AS (
        SELECT
            day,
            token,
            LEAD(DAY, 1, NOW()) OVER (PARTITION BY token ORDER BY DAY) AS day_of_next_change,
            SUM(COALESCE(mints, 0) - COALESCE(burns, 0)) OVER (PARTITION BY token ORDER BY DAY ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS supply
        FROM transfers
    ),

    -- Extract preminted BPTs data
    premints_1 AS (
        SELECT
            poolId AS pool_id,
            t.token,
            d.delta,
            ROW_NUMBER() OVER (PARTITION BY poolId ORDER BY evt_block_time ASC) AS rn
        FROM {{ source('balancer_v2_' + blockchain, 'Vault_evt_PoolBalanceChanged') }} pb
        CROSS JOIN UNNEST (pb.deltas) WITH ORDINALITY d(delta, i)
        CROSS JOIN UNNEST (pb.tokens) WITH ORDINALITY t(token, i)
        WHERE d.i = t.i 
        AND BYTEARRAY_SUBSTRING(poolId, 1, 20) = t.token          
        ORDER BY 1 DESC
    ),

    -- Select the first row for each pool, which counts as the preminted tokens
    premints_2 AS (
        SELECT 
            * 
        FROM premints_1
        WHERE rn = 1
    ),

    -- Calculate preminted BPTs based on pool type
    premints AS (
        SELECT 
            p.address AS bpt, 
            CASE WHEN pool_type IN ('linear') THEN CAST('5192296858534827628530496329220095' AS INT256) / POWER(10, 18)
                WHEN pool_type IN ('stable') THEN CAST(m.delta AS INT256) / POWER(10, 18)
                ELSE 0
            END AS preminted_bpts
        FROM pool_labels p 
        LEFT JOIN premints_2 m ON p.address = BYTEARRAY_SUBSTRING(m.pool_id, 1, 20)
    ),

    -- Calculating Joins(mint) and Exits(burn) via Swap
    joins AS (
        SELECT 
            DATE_TRUNC('day', evt_block_time) AS block_date, 
            tokenOut,
            pool_type,
            CASE WHEN pool_type IN ('weighted') 
            THEN 0
            ELSE SUM(amountOut / POWER(10, 18)) 
            END AS ajoins
        FROM {{ source('balancer_v2_' + blockchain, 'Vault_evt_Swap') }} 
        LEFT JOIN pool_labels ON BYTEARRAY_SUBSTRING(poolId, 1, 20) = address
        WHERE tokenOut = BYTEARRAY_SUBSTRING(poolId, 1, 20)       
        GROUP BY 1, 2, 3
    ),

    exits AS (
        SELECT 
            DATE_TRUNC('day', evt_block_time) AS block_date, 
            tokenIn,
            pool_type,
            CASE WHEN pool_type IN ('weighted') 
            THEN 0
            ELSE SUM(amountIn / POWER(10, 18)) 
            END AS aexits
        FROM {{ source('balancer_v2_' + blockchain, 'Vault_evt_Swap') }} 
        LEFT JOIN pool_labels ON BYTEARRAY_SUBSTRING(poolId, 1, 20) = address
        WHERE tokenIn = BYTEARRAY_SUBSTRING(poolId, 1, 20)        
        GROUP BY 1, 2, 3
    ),

    joins_and_exits AS (
        SELECT 
            j.block_date, 
            j.tokenOut AS bpt, 
            SUM(COALESCE(ajoins, 0) - COALESCE(aexits, 0)) OVER (PARTITION BY j.tokenOut ORDER BY j.block_date ASC) AS adelta
        FROM joins j
        FULL OUTER JOIN exits e ON j.block_date = e.block_date AND e.tokenIn = j.tokenOut
    ),

    calendar AS (
        SELECT 
            date_sequence AS day
        FROM unnest(sequence(date('2021-04-21'), date(now()), interval '1' day)) as t(date_sequence)
    )

    SELECT
        c.day,
        l.pool_type,
        '{{version}}' as version,
        '{{blockchain}}' as blockchain,
        b.token AS token_address,
        COALESCE(SUM(b.supply - COALESCE(preminted_bpts, 0) + COALESCE(adelta, 0)),0) AS supply
    FROM calendar c 
    LEFT JOIN balances b ON b.day <= c.day AND c.day < b.day_of_next_change
    LEFT JOIN joins_and_exits j ON c.day = j.block_date AND b.token = j.bpt
    LEFT JOIN premints p ON b.token = p.bpt
    LEFT JOIN pool_labels l ON b.token = l.address
    WHERE l.pool_type IN ('weighted', 'LBP', 'investment', 'stable', 'linear', 'ECLP', 'managed', 'FX')
    GROUP BY 1, 2, 3, 4, 5
    HAVING SUM(b.supply - COALESCE(preminted_bpts, 0) + COALESCE(adelta, 0)) >= 0  --simple filter to remove outliers

{% endmacro %}
