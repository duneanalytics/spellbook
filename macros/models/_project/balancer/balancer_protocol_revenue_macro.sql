{% macro 
    balancer_protocol_fees_macro(
        blockchain
    ) 
%}

WITH pool_labels AS (
        SELECT * FROM (
            SELECT
                address AS pool_id,
                name AS pool_symbol,
                ROW_NUMBER() OVER (PARTITION BY address ORDER BY MAX(updated_at) DESC) AS num
            FROM {{ ref('labels_balancer_v2_pools') }}
            WHERE blockchain = '{{blockchain}}'
            GROUP BY 1, 2) 
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
            date_trunc('day', hour) AS DAY,
            contract_address AS token,
            approx_percentile(median_price, 0.5) AS price,
            sum(sample_size) AS sample_size
        FROM {{ ref('dex_prices') }}
        GROUP BY 1, 2
        HAVING sum(sample_size) > 3
    ),
 
    dex_prices AS (
        SELECT
            *,
            LEAD(DAY, 1, NOW()) OVER (
                PARTITION BY token
                ORDER BY
                    DAY
            ) AS day_of_next_change
        FROM dex_prices_1
    ),

    bpt_prices AS(
        SELECT 
            date_trunc('day', hour) AS day,
            contract_address AS token,
            approx_percentile(median_price, 0.5) AS bpt_price
        FROM {{ ref('balancer_bpt_prices') }}
        WHERE blockchain = '{{blockchain}}'
        GROUP BY 1, 2
    ),

    daily_protocol_fee_collected AS (
        SELECT
            date_trunc('day', evt_block_time) AS day,
            poolId AS pool_id,
            token AS token_address,
            SUM(protocol_fees) AS protocol_fee_amount_raw
        FROM {{ source('balancer_v2_' + blockchain, 'Vault_evt_PoolBalanceChanged') }} b
        CROSS JOIN unnest("protocolFeeAmounts", "tokens") AS t(protocol_fees, token)
        GROUP BY 1, 2, 3 

        UNION ALL          

        SELECT
            date_trunc('day', t.evt_block_time) AS day,
            poolId AS pool_id,
            b.poolAddress AS token_address,
            sum(value) AS protocol_fee_amount_raw
        FROM {{ source('balancer_v2_' + blockchain, 'Vault_evt_PoolRegistered') }} b
        JOIN {{ source('erc20_' + blockchain, 'evt_transfer') }} t
            ON t.contract_address = b.poolAddress
            AND t."from" = 0x0000000000000000000000000000000000000000
            AND t.to = 0xce88686553686DA562CE7Cea497CE749DA109f9F
        GROUP BY 1, 2, 3
    ),

    decorated_protocol_fee AS (
        SELECT 
            d.day, 
            pool_id, 
            token_address, 
            SUM(protocol_fee_amount_raw) AS token_amount_raw, 
            SUM(protocol_fee_amount_raw / power(10, COALESCE(t.decimals,p1.decimals))) AS token_amount,
            SUM(COALESCE(p1.price, p2.price, p3.price) * protocol_fee_amount_raw / POWER(10, COALESCE(t.decimals,p1.decimals))) AS protocol_fee_collected_usd
        FROM daily_protocol_fee_collected d
        LEFT JOIN prices p1
            ON p1.token = d.token_address
            AND p1.day = d.day
        LEFT JOIN dex_prices p2
            ON p2.token = d.token_address
            AND p2.day = d.day
        LEFT JOIN bpt_prices p3
            ON p3.token = CAST(d.token_address AS VARCHAR)
            AND p3.day = d.day
        LEFT JOIN {{ ref('tokens_erc20') }} t 
            ON t.contract_address = d.token_address
            AND t.blockchain = '{{blockchain}}'
        GROUP BY 1, 2, 3
    ),

    revenue_share as(
        SELECT
        day,
        CASE 
            WHEN day < DATE '2022-07-03' THEN 1
            WHEN day >= DATE '2022-07-03' AND day < DATE '2023-01-23' THEN 0.25
            WHEN day >= DATE '2023-01-23' AND day < DATE '2023-07-24' THEN 0.35
            WHEN day >= DATE '2023-07-24' THEN 0.175
        END AS treasury_share
    FROM UNNEST(SEQUENCE(DATE '2022-01-01', CURRENT_DATE, INTERVAL '1' DAY)) AS date(day)
    )


    SELECT
        f.day,
        f.pool_id,
        BYTEARRAY_SUBSTRING(f.pool_id,1,20) as pool_address,
        l.name AS pool_symbol,
        '{{blockchain}}' as blockchain,
        f.token_address,
        SUM(f.token_amount_raw) as token_amount_raw,
        SUM(f.token_amount) as token_amount,
        SUM(f.protocol_fee_usd) as protocol_fee_collected_usd, 
        r.treasury_share,
        SUM(f.protocol_fee_usd) * r.treasury_share as treasury_revenue
    FROM decorated_protocol_fee f
    LEFT JOIN revenue_share r
        ON r.day = f.day
    LEFT JOIN pool_labels l
        ON BYTEARRAY_SUBSTRING(f.pool_id,1,20) = l.address
    GROUP BY 1, 2, 3, 4, 5, 9

{% endmacro %}
