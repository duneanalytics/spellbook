{% macro 
    balancer_v2_compatible_protocol_fee_macro(
        blockchain, version, project_decoded_as, base_spells_namespace, pool_labels_spell
    ) 
%}

WITH pool_labels AS (
        SELECT * FROM (
            SELECT
                address,
                name,
                pool_type,
                ROW_NUMBER() OVER (PARTITION BY address ORDER BY MAX(updated_at) DESC) AS num
            FROM {{ pool_labels_spell }}
            WHERE blockchain = '{{blockchain}}'
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
        WHERE blockchain = '{{blockchain}}'
        GROUP BY 1, 2, 3

    ),

    dex_prices_1 AS (
        SELECT
            date_trunc('day', HOUR) AS DAY,
            contract_address AS token,
            approx_percentile(median_price, 0.5) AS price,
            sum(sample_size) AS sample_size
        FROM {{ source('dex', 'prices') }}
        WHERE blockchain = '{{blockchain}}'
        AND contract_address NOT IN (0x039e2fb66102314ce7b64ce5ce3e5183bc94ad38, 0xde1e704dae0b4051e80dabb26ab6ad6c12262da0, 0x5ddb92a5340fd0ead3987d3661afcd6104c3b757) 
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
        FROM {{ ref(base_spells_namespace + '_liquidity') }} l
        LEFT JOIN {{ ref(base_spells_namespace + '_bpt_supply') }} s ON s.token_address = l.pool_address 
        AND l.blockchain = s.blockchain AND s.day = l.day AND s.supply > 1e-4
        WHERE l.blockchain = '{{blockchain}}'
        AND l.version = '{{version}}'
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
        -- flashloans are taken from the vault contract, there is no pool involved. 
        SELECT
            date_trunc('day', evt_block_time) AS day,
            0xba12222222228d8ba445958a75a0704d566bf2c8 AS pool_id,
            token AS token_address,
            SUM(feeAmount) AS protocol_fee_amount_raw
        FROM {{ source(project_decoded_as + '_' + blockchain, 'Vault_evt_FlashLoan') }} b
        GROUP BY 1, 2, 3 

        UNION ALL      

        SELECT
            date_trunc('day', evt_block_time) AS day,
            poolId AS pool_id,
            token AS token_address,
            SUM(protocol_fees) AS protocol_fee_amount_raw
        FROM {{ source(project_decoded_as + '_' + blockchain, 'Vault_evt_PoolBalanceChanged') }} b
        CROSS JOIN unnest("protocolFeeAmounts", "tokens") AS t(protocol_fees, token)   
        GROUP BY 1, 2, 3 

        UNION ALL          

        SELECT
            date_trunc('day', t.evt_block_time) AS day,
            poolId AS pool_id,
            b.poolAddress AS token_address,
            sum(value) AS protocol_fee_amount_raw
        FROM {{ source(project_decoded_as + '_' + blockchain, 'Vault_evt_PoolRegistered') }} b
        INNER JOIN {{ source('erc20_' + blockchain, 'evt_transfer') }} t
            ON t.contract_address = b.poolAddress
            AND t."from" = 0x0000000000000000000000000000000000000000
            AND t."to" =
                CASE
                    WHEN '{{blockchain}}' = 'fantom' THEN 0xc6920d3a369e7c8bd1a22dbe385e11d1f7af948f
                    ELSE 0xce88686553686DA562CE7Cea497CE749DA109f9F
                    END --ProtocolFeesCollector address, which is the same across all chains except for fantom   
        GROUP BY 1, 2, 3
    ),

    decorated_protocol_fee AS (
        SELECT 
            d.day, 
            d.pool_id, 
            d.token_address, 
            t.symbol AS token_symbol,
            SUM(d.protocol_fee_amount_raw) AS token_amount_raw, 
            SUM(d.protocol_fee_amount_raw / POWER(10, COALESCE(t.decimals,p1.decimals, p3.decimals))) AS token_amount,
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
            AND t.blockchain = '{{blockchain}}'
        GROUP BY 1, 2, 3, 4
    ),

    revenue_share as(
        SELECT
        day,
        CASE 
            WHEN day < DATE '2022-07-03' THEN 0.25 -- veBAL release
            WHEN day >= DATE '2022-07-03' AND day < DATE '2023-01-23' THEN 0.25 -- BIP 19
            WHEN day >= DATE '2023-01-23' AND day < DATE '2023-07-24' THEN 0.35 -- BIP 161
            WHEN day >= DATE '2023-07-24' THEN 0.175 -- BIP 371 and BIP 734
        END AS treasury_share
    FROM UNNEST(SEQUENCE(DATE '2022-03-01', CURRENT_DATE, INTERVAL '1' DAY)) AS date(day)
    )


    SELECT
        f.day,
        f.pool_id,
        BYTEARRAY_SUBSTRING(f.pool_id,1,20) AS pool_address,
        CASE WHEN f.pool_id = 0xba12222222228d8ba445958a75a0704d566bf2c8 THEN 'flashloan' ELSE l.name END AS pool_symbol,
        '{{version}}' AS version,
        '{{blockchain}}' AS blockchain,
        l.pool_type,
        CASE WHEN f.pool_id = 0xba12222222228d8ba445958a75a0704d566bf2c8 THEN 'flashloan' ELSE 'v2' END AS fee_type,
        f.token_address,
        f.token_symbol,
        SUM(f.token_amount_raw) AS token_amount_raw,
        SUM(f.token_amount) AS token_amount,
        SUM(f.protocol_fee_collected_usd) AS protocol_fee_collected_usd, 
        r.treasury_share,
        SUM(f.protocol_fee_collected_usd) * r.treasury_share AS treasury_fee_usd,
        SUM(f.protocol_fee_collected_usd) AS lp_fee_collected_usd
    FROM decorated_protocol_fee f
    INNER JOIN revenue_share r 
        ON r.day = f.day
    LEFT JOIN pool_labels l
        ON BYTEARRAY_SUBSTRING(f.pool_id,1,20) = l.address
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 14

{% endmacro %}

{# ######################################################################### #}

{% macro 
    balancer_v3_compatible_protocol_fee_macro(
        blockchain, version, project_decoded_as, base_spells_namespace, pool_labels_spell
    ) 
%}

WITH pool_labels AS (
        SELECT * FROM (
            SELECT
                address,
                name,
                pool_type,
                ROW_NUMBER() OVER (PARTITION BY address ORDER BY MAX(updated_at) DESC) AS num
            FROM {{ pool_labels_spell }}
            WHERE blockchain = '{{blockchain}}'
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
        WHERE blockchain = '{{blockchain}}'
        GROUP BY 1, 2, 3

    ),

    dex_prices_1 AS (
        SELECT
            date_trunc('day', HOUR) AS DAY,
            contract_address AS token,
            approx_percentile(median_price, 0.5) AS price,
            sum(sample_size) AS sample_size
        FROM {{ source('dex', 'prices') }}
        WHERE blockchain = '{{blockchain}}'
        AND contract_address NOT IN (0x039e2fb66102314ce7b64ce5ce3e5183bc94ad38, 0xde1e704dae0b4051e80dabb26ab6ad6c12262da0, 0x5ddb92a5340fd0ead3987d3661afcd6104c3b757) 
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
        FROM {{ ref(base_spells_namespace + '_liquidity') }} l
        LEFT JOIN {{ ref(base_spells_namespace + '_bpt_supply') }} s ON s.token_address = l.pool_address 
        AND l.blockchain = s.blockchain AND s.day = l.day AND s.supply > 1e-4
        WHERE l.blockchain = '{{blockchain}}'
        AND l.version = '{{version}}'
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

    erc4626_prices AS (
        SELECT
            DATE_TRUNC('day', minute) AS day,
            wrapped_token AS token,
            decimals,
            APPROX_PERCENTILE(median_price, 0.5) AS price,
            LEAD(DATE_TRUNC('day', minute), 1, CURRENT_DATE + INTERVAL '1' day) OVER (PARTITION BY wrapped_token ORDER BY date_trunc('day', minute)) AS next_change
        FROM {{ source('balancer_v3' , 'erc4626_token_prices') }}
        WHERE blockchain = '{{blockchain}}'
        GROUP BY 1, 2, 3
    ),    

    daily_protocol_fee_collected AS (
        SELECT 
            d.day, 
            d.pool_id, 
            d.token_address, 
            d.fee_type,
            SUM(d.protocol_fee_amount_raw) AS token_amount_raw
        FROM (
            SELECT
                date_trunc('day', evt_block_time) AS day,
                pool AS pool_id,
                token AS token_address,
                'swap_fee' AS fee_type,
                SUM(amount) AS protocol_fee_amount_raw
            FROM {{ source(project_decoded_as + '_' + blockchain, 'ProtocolFeeController_evt_ProtocolSwapFeeCollected') }}
            GROUP BY 1, 2, 3, 4 

            UNION ALL          

            SELECT
                date_trunc('day', evt_block_time) AS day,
                pool AS pool_id,
                token AS token_address,
                'yield_fee' AS fee_type,
                SUM(amount) AS protocol_fee_amount_raw
            FROM {{ source(project_decoded_as + '_' + blockchain, 'ProtocolFeeController_evt_ProtocolYieldFeeCollected') }}
            GROUP BY 1, 2, 3, 4
            ) d
        GROUP BY 1, 2, 3, 4
    ),

    decorated_protocol_fee AS (
        SELECT 
            d.day, 
            d.pool_id, 
            d.token_address, 
            t.symbol AS token_symbol,
            d.fee_type,
            SUM(d.token_amount_raw) AS token_amount_raw, 
            SUM(d.token_amount_raw / POWER(10, COALESCE(t.decimals,p1.decimals, p3.decimals, p4.decimals))) AS token_amount,
            SUM(COALESCE(p1.price, p2.price, p3.price, p4.price) * token_amount_raw / POWER(10, COALESCE(t.decimals,p1.decimals, p3.decimals, p4.decimals))) AS protocol_fee_collected_usd
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
        LEFT JOIN erc4626_prices p4 ON p4.day <= d.day
            AND d.day < p4.next_change
            AND p4.token = d.token_address            
        LEFT JOIN {{ source('tokens', 'erc20') }} t 
            ON t.contract_address = d.token_address
            AND t.blockchain = '{{blockchain}}'
        GROUP BY 1, 2, 3, 4, 5
    ),

    revenue_share as(
        SELECT
        day,
        CASE 
            WHEN day >= DATE '2024-12-01' THEN 0.175 -- BIP 734
        END AS treasury_share
    FROM UNNEST(SEQUENCE(DATE '2024-12-01', CURRENT_DATE, INTERVAL '1' DAY)) AS date(day)
    )


    SELECT
        f.day,
        f.pool_id,
        BYTEARRAY_SUBSTRING(f.pool_id,1,20) as pool_address,
        l.name AS pool_symbol,
        '{{version}}' as version,
        '{{blockchain}}' as blockchain,
        l.pool_type,
        f.fee_type,        
        f.token_address,
        f.token_symbol,
        SUM(f.token_amount_raw) as token_amount_raw,
        SUM(f.token_amount) as token_amount,
        SUM(f.protocol_fee_collected_usd) as protocol_fee_collected_usd, 
        r.treasury_share,
        SUM(f.protocol_fee_collected_usd) * r.treasury_share as treasury_fee_usd,
        SUM(CASE WHEN f.fee_type = 'swap_fee' THEN f.protocol_fee_collected_usd
        WHEN f.fee_type = 'yield_fee' THEN f.protocol_fee_collected_usd * 9 END) 
            AS lp_fee_collected_usd
    FROM decorated_protocol_fee f
    INNER JOIN revenue_share r 
        ON r.day = f.day
    LEFT JOIN pool_labels l
        ON BYTEARRAY_SUBSTRING(f.pool_id,1,20) = l.address
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 14

{% endmacro %}