{{ config(
    materialized = 'incremental',
    schema = 'desyn',
    unique_key = ['day', 'pool_address'],
    incremental_strategy = 'merge',
    file_format = 'delta',
    tags = ['desyn']
) }}

WITH pools AS (
    SELECT DISTINCT
      '0x' || LOWER(SUBSTRING(SUBSTRING(CAST(topic2 AS VARCHAR), 27, 40), 1, 40)) AS pool_address
    FROM {{ source('ethereum', 'logs') }}
    WHERE contract_address = 0x01a38b39beddcd6bfeedba14057e053cbf529cd2
      AND topic0 = 0x0ca525a414e11c32284272215f33c3c4d119f75876d0dcf9fcf573768ff4baa1
      {% if is_incremental() %}
      AND block_time >= date_trunc('day', now() - interval '7' day)
      {% endif %}
),

joins AS (
    SELECT 
        p.pool_address AS pool, 
        DATE_TRUNC('day', e.evt_block_time) AS day, 
        e.contract_address AS token, 
        SUM(CAST(e.value AS uint256)) AS amount
    FROM {{ source('erc20_ethereum', 'evt_Transfer') }} e
    INNER JOIN pools p ON e."to" = CAST(p.pool_address AS varbinary)
    {% if is_incremental() %}
    WHERE e.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    GROUP BY 1, 2, 3
),

exits AS (
    SELECT 
        p.pool_address AS pool, 
        DATE_TRUNC('day', e.evt_block_time) AS day, 
        e.contract_address AS token, 
        -SUM(CAST(e.value AS uint256)) AS amount
    FROM {{ source('erc20_ethereum', 'evt_Transfer') }} e
    INNER JOIN pools p ON e."from" = CAST(p.pool_address AS varbinary)
    {% if is_incremental() %}
    WHERE e.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    GROUP BY 1, 2, 3
),

daily_delta_balance_by_token AS (
    SELECT 
        pool, 
        day, 
        token, 
        SUM(COALESCE(amount, CAST(0 AS uint256))) AS amount 
    FROM 
        (SELECT * FROM joins
        UNION ALL
        SELECT * FROM exits) foo
    GROUP BY 1, 2, 3
),

cumulative_balance_by_token AS (
    SELECT
        pool, 
        token, 
        day, 
        LEAD(day, 1, now()) OVER (PARTITION BY pool, token ORDER BY day) AS day_of_next_change,
        SUM(amount) OVER (PARTITION BY pool, token ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
    FROM daily_delta_balance_by_token
),

calendar AS (
    SELECT 
        date_sequence AS day
    FROM unnest(sequence(date('2021-01-01'), date(now()), interval '1' day)) AS t(date_sequence)
),

daily_balances AS (
    SELECT
        c.day, 
        b.pool AS pool_address, 
        b.token AS token_address, 
        b.cumulative_amount AS amount_raw
    FROM calendar c
    LEFT JOIN cumulative_balance_by_token b ON b.day <= c.day AND c.day < b.day_of_next_change
    WHERE b.pool IS NOT NULL
      {% if is_incremental() %}
      AND c.day >= date_trunc('day', now() - interval '7' day)
      {% endif %}
)

SELECT
    b.day,
    'ethereum' AS blockchain,
    CAST(b.pool_address AS varchar) AS pool_address,
    SUM((b.amount_raw / POWER(10, COALESCE(t.decimals, 18))) * COALESCE(p.price, 0)) AS protocol_liquidity_usd,
    'v1' AS version
FROM daily_balances b
LEFT JOIN {{ ref('tokens_erc20') }} t ON t.contract_address = b.token_address AND t.blockchain = 'ethereum'
LEFT JOIN {{ source('prices', 'usd') }} p
  ON p.contract_address = b.token_address
 AND p.blockchain = 'ethereum'
 AND date_trunc('day', p.minute) = b.day
WHERE b.amount_raw > 0
GROUP BY 1,2,3,5