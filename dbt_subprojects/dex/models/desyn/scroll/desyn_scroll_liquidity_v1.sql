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
        '0x' || LOWER(SUBSTRING(to_hex(bytearray_substring(topic2, 13, 20)), 25, 40)) AS pool_address
    FROM {{ source('scroll', 'logs') }}
    WHERE contract_address = 0x09eFC8C8F08B810F1F76B0c926D6dCeb37409665
      AND topic0 = 0x0ca525a414e11c32284272215f33c3c4d119f75876d0dcf9fcf573768ff4baa1
      {% if is_incremental() %}
      AND block_time >= date_trunc('day', now() - interval '7' day)
      {% endif %}
)

SELECT
    b.day,
    'scroll' AS blockchain,
    b.wallet_address AS pool_address,
    SUM(b.amount * COALESCE(p.price, 0)) AS protocol_liquidity_usd,
    'v1' AS version
FROM {{ ref('tokens_erc20_daily_balances') }} b
LEFT JOIN {{ ref('prices_usd') }} p
  ON p.contract_address = b.token_address
 AND p.blockchain = 'scroll'
 AND date_trunc('day', p.minute) = b.day
WHERE b.wallet_address IN (SELECT pool_address FROM pools)
  AND b.blockchain = 'scroll'
  {% if is_incremental() %}
  AND b.day >= date_trunc('day', now() - interval '7' day)
  {% endif %}
GROUP BY 1,2,3,5