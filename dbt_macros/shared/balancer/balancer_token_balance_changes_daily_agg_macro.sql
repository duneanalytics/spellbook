{% macro
    balancer_v2_compatible_token_balance_changes_daily_agg_macro(
        blockchain, version, project_decoded_as, base_spells_namespace
    )
%}
WITH
    prices AS (
        SELECT
            date_trunc('day', minute) AS day,
            contract_address AS token,
            decimals,
            AVG(price) AS price
        FROM {{ source('prices', 'usd') }}
        WHERE blockchain = '{{blockchain}}'
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('minute') }}
        {% endif %}
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
        {% if is_incremental() %}
        AND {{ incremental_predicate('hour') }}
        {% endif %}
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

    bpt_prices AS(
        SELECT DISTINCT
            day,
            contract_address AS token,
            decimals,
            bpt_price
        FROM {{ ref(base_spells_namespace + '_bpt_prices') }}
        WHERE blockchain = '{{blockchain}}'
        {% if is_incremental() %}
        AND {{ incremental_predicate('day') }}
        {% endif %}
        AND version = '{{version}}'
    ),

    eth_prices AS (
        SELECT
            DATE_TRUNC('day', minute) as day,
            AVG(price) as eth_price
        FROM {{ source('prices', 'usd') }}
        WHERE symbol = 'ETH'
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('minute') }}
        {% endif %}
        GROUP BY 1
    ),

    gyro_prices AS (
        SELECT
            token_address,
            decimals,
            price
        FROM {{ source('gyroscope','gyro_tokens') }}
        WHERE blockchain = '{{blockchain}}'
    ),

    daily_balance AS (
        SELECT
            block_date,
            pool_id,
            pool_symbol,
            pool_type,
            token_address,
            token_symbol,
            LEAD(block_date, 1, NOW()) OVER (PARTITION BY token_address, pool_id ORDER BY block_date) AS day_of_next_change,
            SUM(delta_amount) AS daily_amount
        FROM {{ ref(base_spells_namespace + '_token_balance_changes') }}
        WHERE blockchain = '{{blockchain}}'
        AND version = '{{version}}'
        GROUP BY 1, 2, 3, 4, 5, 6
    ),

    calendar AS (
        SELECT date_sequence AS day
        FROM unnest(sequence(date('2021-04-21'), date(now()), interval '1' day)) as t(date_sequence)
    ),

   daily_usd_balance AS (
        SELECT
            c.day AS block_date,
            '{{blockchain}}"' as blockchain,
            b.pool_id,
            b.pool_symbol,
            b.pool_type,
            b.token_address,
            b.token_symbol,
            daily_amount,
            daily_amount * COALESCE(p1.price, p2.price, p3.bpt_price, p4.price, 0) AS daily_amount_usd
        FROM calendar c
        LEFT JOIN daily_balance b ON b.block_date <= c.day
        AND c.day < b.day_of_next_change
        LEFT JOIN prices p1 ON p1.day = b.block_date
        AND p1.token = b.token_address
        LEFT JOIN dex_prices p2 ON p2.day <= c.day
        AND c.day < p2.day_of_next_change
        AND p2.token = b.token_address
        LEFT JOIN bpt_prices p3 ON p3.day = b.block_date
        AND p3.token = b.token_address
        LEFT JOIN gyro_prices p4 ON p4.token_address = b.token_address
        WHERE b.token_address != BYTEARRAY_SUBSTRING(b.pool_id, 1, 20)
    ),

    weighted_pool_amount_estimates AS (
        SELECT
            b.block_date,
            b.pool_id,
            q.name,
            pool_type,
            ROW_NUMBER() OVER (PARTITION BY b.block_date, b.pool_id ORDER BY SUM(b.daily_amount_usd) ASC) AS pricing_count, --to avoid double count in pools with multiple pricing assets
            SUM(b.daily_amount_usd) / COALESCE(SUM(w.normalized_weight), 1) AS weighted_daily_amount_usd
        FROM daily_usd_balance b
        LEFT JOIN {{ ref(base_spells_namespace + '_pools_tokens_weights') }} w ON b.pool_id = w.pool_id
        AND b.token_address = w.token_address
        AND b.daily_amount_usd > 0
        LEFT JOIN {{ source('balancer','token_whitelist') }} q ON b.token_address = q.address
        AND b.blockchain = q.chain
        WHERE q.name IS NOT NULL
        AND b.pool_type = 'weighted' -- filters for weighted pools with pricing assets
        AND w.blockchain = '{{blockchain}}'
        AND w.version = '2'
        GROUP BY 1, 2, 3, 4
    ),

    weighted_pool_amount_estimates_2 AS(
    SELECT  e.block_date,
            e.pool_id,
            SUM(e.weighted_daily_amount_usd) / MAX(e.pricing_count) AS weighted_daily_amount_usd
    FROM weighted_pool_amount_estimates e
    GROUP BY 1,2
    )

    SELECT DISTINCT
        c.block_date,
        c.pool_id,
        BYTEARRAY_SUBSTRING(c.pool_id, 1, 20) AS pool_address,
        c.pool_symbol,
        '2' AS version,
        '{{blockchain}}' AS blockchain,
        c.pool_type,
        c.token_address,
        c.token_symbol,
        c.daily_amount AS daily_delta,
        COALESCE(b.weighted_daily_amount_usd * w.normalized_weight, c.daily_amount_usd) AS daily_delta_usd,
        COALESCE(b.weighted_daily_amount_usd * w.normalized_weight, c.daily_amount_usd)/e.eth_price AS daily_delta_eth
    FROM daily_usd_balance c
    FULL OUTER JOIN weighted_pool_amount_estimates_2 b ON c.block_date = b.block_date
    AND c.pool_id = b.pool_id
    LEFT JOIN {{ ref(base_spells_namespace + '_pools_tokens_weights') }} w ON b.pool_id = w.pool_id
    AND w.blockchain = '{{blockchain}}'
    AND w.version = '2'
    AND w.token_address = c.token_address
    LEFT JOIN eth_prices e ON e.day = c.block_date
    {% endmacro %}

{# ######################################################################### #}

{% macro
    balancer_v3_compatible_token_balance_changes_daily_agg_macro(
        blockchain, version, project_decoded_as, base_spells_namespace
    )
%}
WITH
    prices AS (
        SELECT
            date_trunc('day', minute) AS day,
            contract_address AS token,
            decimals,
            AVG(price) AS price
        FROM {{ source('prices', 'usd') }}
        WHERE blockchain = '{{blockchain}}'
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('minute') }}
        {% endif %}
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
        {% if is_incremental() %}
        AND {{ incremental_predicate('hour') }}
        {% endif %}
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

    bpt_prices AS(
        SELECT DISTINCT
            day,
            contract_address AS token,
            decimals,
            bpt_price
        FROM {{ ref(base_spells_namespace + '_bpt_prices') }}
        WHERE blockchain = '{{blockchain}}'
        {% if is_incremental() %}
        AND {{ incremental_predicate('day') }}
        {% endif %}
        AND version = '{{version}}'
    ),

    eth_prices AS (
        SELECT
            DATE_TRUNC('day', minute) as day,
            AVG(price) as eth_price
        FROM {{ source('prices', 'usd') }}
        WHERE symbol = 'ETH'
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('minute') }}
        {% endif %}
        GROUP BY 1
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

    daily_balance AS (
        SELECT
            block_date,
            pool_id,
            pool_symbol,
            pool_type,
            token_address,
            token_symbol,
            LEAD(block_date, 1, NOW()) OVER (PARTITION BY token_address, pool_id ORDER BY block_date) AS day_of_next_change,
            SUM(delta_amount) AS daily_amount
        FROM {{ ref(base_spells_namespace + '_token_balance_changes') }}
        WHERE blockchain = '{{blockchain}}'
        AND version = '{{version}}'
        GROUP BY 1, 2, 3, 4, 5, 6
    ),

    calendar AS (
        SELECT date_sequence AS day
        FROM unnest(sequence(date('2024-12-01'), date(now()), interval '1' day)) as t(date_sequence)
    ),

   daily_usd_balance AS (
        SELECT
            c.day AS block_date,
            '{{blockchain}}"' as blockchain,
            b.pool_id,
            b.pool_symbol,
            b.pool_type,
            b.token_address,
            b.token_symbol,
            daily_amount,
            daily_amount * COALESCE(p1.price, p2.price, p3.bpt_price, p4.price, 0) AS daily_amount_usd
        FROM calendar c
        LEFT JOIN daily_balance b ON b.block_date <= c.day
        AND c.day < b.day_of_next_change
        LEFT JOIN prices p1 ON p1.day = b.block_date
        AND p1.token = b.token_address
        LEFT JOIN dex_prices p2 ON p2.day <= c.day
        AND c.day < p2.day_of_next_change
        AND p2.token = b.token_address
        LEFT JOIN bpt_prices p3 ON p3.day = b.block_date
        AND p3.token = b.token_address
        LEFT JOIN erc4626_prices p4 ON p4.day <= c.day
        AND c.day < p4.next_change
        AND p4.token = b.token_address      
        WHERE b.token_address != BYTEARRAY_SUBSTRING(b.pool_id, 1, 20)
    ),

    weighted_pool_amount_estimates AS (
        SELECT
            b.block_date,
            b.pool_id,
            q.name,
            pool_type,
            ROW_NUMBER() OVER (PARTITION BY b.block_date, b.pool_id ORDER BY SUM(b.daily_amount_usd) ASC) AS pricing_count, --to avoid double count in pools with multiple pricing assets
            SUM(b.daily_amount_usd) / COALESCE(SUM(w.normalized_weight), 1) AS weighted_daily_amount_usd
        FROM daily_usd_balance b
        LEFT JOIN {{ ref(base_spells_namespace + '_pools_tokens_weights') }} w ON b.pool_id = w.pool_id
        AND b.token_address = w.token_address
        AND b.daily_amount_usd > 0
        LEFT JOIN {{ source('balancer','token_whitelist') }} q ON b.token_address = q.address
        AND b.blockchain = q.chain
        WHERE q.name IS NOT NULL
        AND b.pool_type = 'weighted' -- filters for weighted pools with pricing assets
        AND w.blockchain = '{{blockchain}}'
        AND w.version = '2'
        GROUP BY 1, 2, 3, 4
    ),

    weighted_pool_amount_estimates_2 AS(
    SELECT  e.block_date,
            e.pool_id,
            SUM(e.weighted_daily_amount_usd) / MAX(e.pricing_count) AS weighted_daily_amount_usd
    FROM weighted_pool_amount_estimates e
    GROUP BY 1,2
    )

    SELECT DISTINCT
        c.block_date,
        c.pool_id,
        BYTEARRAY_SUBSTRING(c.pool_id, 1, 20) AS pool_address,
        c.pool_symbol,
        '2' AS version,
        '{{blockchain}}' AS blockchain,
        c.pool_type,
        c.token_address,
        c.token_symbol,
        c.daily_amount AS daily_delta,
        COALESCE(b.weighted_daily_amount_usd * w.normalized_weight, c.daily_amount_usd) AS daily_delta_usd,
        COALESCE(b.weighted_daily_amount_usd * w.normalized_weight, c.daily_amount_usd)/e.eth_price AS daily_delta_eth
    FROM daily_usd_balance c
    FULL OUTER JOIN weighted_pool_amount_estimates_2 b ON c.block_date = b.block_date
    AND c.pool_id = b.pool_id
    LEFT JOIN {{ ref(base_spells_namespace + '_pools_tokens_weights') }} w ON b.pool_id = w.pool_id
    AND w.blockchain = '{{blockchain}}'
    AND w.version = '2'
    AND w.token_address = c.token_address
    LEFT JOIN eth_prices e ON e.day = c.block_date
    {% endmacro %}

{# ######################################################################### #}
