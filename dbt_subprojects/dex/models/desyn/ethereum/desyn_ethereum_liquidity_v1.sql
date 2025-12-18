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
)

SELECT
    date_trunc('day', b.block_day) AS day,
    'ethereum' AS blockchain,
    b.wallet_address AS pool_address,
    SUM(b.amount * COALESCE(p.price, 0)) AS protocol_liquidity_usd,
    'v1' AS version
FROM {{ ref('balances_ethereum_erc20_day') }} b
LEFT JOIN {{ source('prices', 'usd') }} p
  ON p.contract_address = b.token_address
 AND p.blockchain = 'ethereum'
 AND date_trunc('day', p.minute) = date_trunc('day', b.block_day)
WHERE b.wallet_address IN (SELECT pool_address FROM pools)
  AND b.blockchain = 'ethereum'
  {% if is_incremental() %}
  AND date_trunc('day', b.block_day) >= date_trunc('day', now() - interval '7' day)
  {% endif %}
GROUP BY 1,2,3,5