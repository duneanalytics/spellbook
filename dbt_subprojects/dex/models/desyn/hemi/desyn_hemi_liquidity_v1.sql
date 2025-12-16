{{ config(
    materialized = 'incremental',
    alias = 'desyn_hemi_liquidity_v1',
    unique_key = ['day', 'pool_address'],
    incremental_strategy = 'merge',
    file_format = 'delta',
    tags = ['desyn']
) }}

WITH pools AS (
    SELECT DISTINCT
        '0x' || encode(bytearray_substring(topic2, 13, 20), 'hex') AS pool_address
    FROM {{ source('hemi', 'logs') }}
    WHERE contract_address = 0xdE6b117384452b21F5a643E56952593B88110e78
      AND topic0 = 0x0ca525a414e11c32284272215f33c3c4d119f75876d0dcf9fcf573768ff4baa1
      {% if is_incremental() %}
      AND block_time >= date_trunc('day', now() - interval '7' day)
      {% endif %}
)

SELECT
    b.day,
    'hemi' AS blockchain,
    b.wallet_address AS pool_address,
    SUM(b.amount * COALESCE(p.price, 0)) AS protocol_liquidity_usd,
    'v1' AS version
FROM erc20."view_token_balances_daily" b 
LEFT JOIN prices.usd p
  ON p.contract_address = b.token_address
 AND p.blockchain = 'hemi'
 AND date_trunc('day', p.minute) = b.day
WHERE b.wallet_address IN (SELECT pool_address FROM pools)
  AND b.blockchain = 'hemi'
  {% if is_incremental() %}
  AND b.day >= date_trunc('day', now() - interval '7' day)
  {% endif %}
GROUP BY 1,2,3,5