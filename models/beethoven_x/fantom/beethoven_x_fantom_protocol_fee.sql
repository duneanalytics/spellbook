{{
    config(
        schema = 'beethoven_x_fantom',
        alias = 'protocol_fee', 
        materialized = 'table',
        file_format = 'delta'
    )
}}

WITH pool_labels AS (
        SELECT * FROM (
            SELECT
                address,
                name,
                pool_type,
                ROW_NUMBER() OVER (PARTITION BY address ORDER BY MAX(updated_at) DESC) AS num
            FROM {{ ref('labels_beethoven_x_pools_fantom') }}
            GROUP BY 1, 2, 3) 
        WHERE num = 1
    ),

    prices AS (
        SELECT
            date_trunc('day', minute) AS day,
            contract_address AS token,
            decimals,
            AVG(price) AS price
        FROM {{ source('prices', 'usd') }}
        WHERE blockchain = 'fantom'
        GROUP BY 1, 2, 3

    ),

    dex_prices_1 AS (
        SELECT
            date_trunc('day', hour) AS DAY,
            contract_address AS token,
            approx_percentile(median_price, 0.5) AS price,
            sum(sample_size) AS sample_size
        FROM {{ source('dex', 'prices') }}
        GROUP BY 1, 2
        HAVING sum(sample_size) > 3
    ),
 
    dex_prices_2 AS(
        SELECT 
            day,
            token,
            price,
            lag(price) OVER(PARTITION BY token ORDER BY day) AS previous_price
        FROM dex_prices_1
    ),    

    dex_prices AS (
        SELECT
            day,
            token,
            price,
            LEAD(DAY, 1, NOW()) OVER (PARTITION BY token ORDER BY DAY) AS day_of_next_change
        FROM dex_prices_2
        WHERE (price < previous_price * 1e4 AND price > previous_price / 1e4)
    ),

    bpt_prices_1 AS ( --special calculation for this spell, in order to achieve completeness without relying on prices.usd
        SELECT 
            l.day,
            s.token_address AS token,
            18 AS decimals,
            SUM(protocol_liquidity_usd / supply) AS price
        FROM {{ ref('beethoven_x_fantom_liquidity') }} l
        LEFT JOIN {{ ref('beethoven_x_fantom_bpt_supply') }} s ON s.token_address = l.pool_address 
        AND l.blockchain = s.blockchain AND s.day = l.day AND s.supply > 0
        WHERE l.blockchain = 'fantom'
        GROUP BY 1, 2, 3
    ),

    bpt_prices AS (
        SELECT  
            day,
            token,
            decimals,
            price,
            LEAD(DAY, 1, NOW()) OVER (PARTITION BY token ORDER BY DAY) AS day_of_next_change
        FROM bpt_prices_1
    ),

    daily_protocol_fee_collected AS (
        SELECT
            date_trunc('day', evt_block_time) AS day,
            poolId AS pool_id,
            token AS token_address,
            SUM(protocol_fees) AS protocol_fee_amount_raw
        FROM {{ source('beethoven_x_fantom', 'Vault_evt_PoolBalanceChanged') }} b
        CROSS JOIN unnest("protocolFeeAmounts", "tokens") AS t(protocol_fees, token)   
        GROUP BY 1, 2, 3 

        UNION ALL          

        SELECT
            date_trunc('day', t.evt_block_time) AS day,
            poolId AS pool_id,
            b.poolAddress AS token_address,
            sum(value) AS protocol_fee_amount_raw
        FROM {{ source('beethoven_x_fantom', 'Vault_evt_PoolRegistered') }} b
        INNER JOIN {{ source('erc20_fantom', 'evt_transfer') }} t
            ON t.contract_address = b.poolAddress
            AND t."from" = 0x0000000000000000000000000000000000000000
            AND t.to = 0xc6920d3a369e7c8bd1a22dbe385e11d1f7af948f
        GROUP BY 1, 2, 3
    ),

    decorated_protocol_fee AS (
        SELECT 
            d.day, 
            d.pool_id, 
            d.token_address, 
            t.symbol AS token_symbol,
            SUM(d.protocol_fee_amount_raw) AS token_amount_raw, 
            SUM(d.protocol_fee_amount_raw / power(10, COALESCE(t.decimals,p1.decimals, p3.decimals))) AS token_amount,
            SUM(COALESCE(p1.price, p2.price, p3.price) * protocol_fee_amount_raw / POWER(10, COALESCE(t.decimals,p1.decimals, p3.decimals))) AS protocol_fee_collected_usd
        FROM daily_protocol_fee_collected d
        LEFT JOIN prices p1
            ON p1.token = d.token_address
            AND p1.day = d.day
        LEFT JOIN dex_prices p2
            ON p2.token = d.token_address
            AND p2.day = d.day
        LEFT JOIN bpt_prices p3
            ON p3.token = d.token_address
            AND p3.day <= d.day
            AND d.day < p3.day_of_next_change     
        LEFT JOIN {{ source('tokens', 'erc20') }} t 
            ON t.contract_address = d.token_address
            AND t.blockchain = 'fantom'
        GROUP BY 1, 2, 3, 4
    )


    SELECT
        f.day,
        f.pool_id,
        BYTEARRAY_SUBSTRING(f.pool_id,1,20) as pool_address,
        l.name AS pool_symbol,
        'fantom' as blockchain,
        l.pool_type,
        f.token_address,
        f.token_symbol,
        SUM(f.token_amount_raw) as token_amount_raw,
        SUM(f.token_amount) as token_amount,
        SUM(f.protocol_fee_collected_usd) as protocol_fee_collected_usd
    FROM decorated_protocol_fee f
    LEFT JOIN pool_labels l
        ON BYTEARRAY_SUBSTRING(f.pool_id,1,20) = l.address
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8