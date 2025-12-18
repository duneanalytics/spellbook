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
    FROM {{ source('plume', 'logs') }}
    WHERE contract_address = 0x770c9d0851b21df8A84943EdE4f487D30d9741ba
      AND topic0 = 0x0ca525a414e11c32284272215f33c3c4d119f75876d0dcf9fcf573768ff4baa1
      {% if is_incremental() %}
      AND block_time >= date_trunc('day', now() - interval '7' day)
      {% endif %}
),

transfers AS (
    SELECT
        date_trunc('day', block_time) AS day,
        contract_address AS token_address,
        bytearray_substring(topic1, 13, 20) AS "from",
        bytearray_substring(topic2, 13, 20) AS "to",
        bytearray_to_uint256(data) AS value
    FROM {{ source('plume', 'logs') }}
    WHERE topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
      AND (bytearray_substring(topic2, 13, 20) IN (SELECT CAST(pool_address AS varbinary) FROM pools) 
           OR bytearray_substring(topic1, 13, 20) IN (SELECT CAST(pool_address AS varbinary) FROM pools))
      {% if is_incremental() %}
      AND block_time >= date_trunc('day', now() - interval '7' day)
      {% endif %}
),

daily_net AS (
    SELECT
        day,
        token_address,
        CAST(pool_address AS varbinary) AS pool_address,
        SUM(CASE WHEN "to" = CAST(pool_address AS varbinary) THEN value ELSE 0 END) - 
        SUM(CASE WHEN "from" = CAST(pool_address AS varbinary) THEN value ELSE 0 END) AS net_amount
    FROM transfers
    CROSS JOIN pools
    GROUP BY 1, 2, 3
),

cumulative_balances AS (
    SELECT
        day,
        token_address,
        pool_address,
        SUM(net_amount) OVER (PARTITION BY token_address, pool_address ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS amount_raw
    FROM daily_net
)

SELECT
    b.day,
    'plume' AS blockchain,
    CAST(b.pool_address AS varchar) AS pool_address,
    SUM((b.amount_raw / POWER(10, COALESCE(t.decimals, 18))) * COALESCE(p.price, 0)) AS protocol_liquidity_usd,
    'v1' AS version
FROM cumulative_balances b
LEFT JOIN {{ ref('tokens_erc20') }} t ON t.contract_address = b.token_address AND t.blockchain = 'plume'
LEFT JOIN {{ source('prices', 'usd') }} p
  ON p.contract_address = b.token_address
 AND p.blockchain = 'plume'
 AND date_trunc('day', p.minute) = b.day
WHERE b.amount_raw > 0
  {% if is_incremental() %}
  AND b.day >= date_trunc('day', now() - interval '7' day)
  {% endif %}
GROUP BY 1,2,3,5