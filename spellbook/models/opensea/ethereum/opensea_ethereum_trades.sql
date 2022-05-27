 {{
  config(
        alias='trades',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge'
  )
}}

SELECT
'ethereum' as blockchain,
evt_tx_hash as tx_hash,
evt_block_time as block_time,
om.price / power(10, decimals) * p.price AS amount_usd,
om.price / power(10, decimals) AS amount,
om.price AS amount_raw,
terc20.symbol as token_symbol,
wam.token_address as token_address,
maker,
taker
FROM
  {{ source('opensea_ethereum','wyvernexchange_evt_ordersmatched') }} om
  INNER JOIN {{ ref('opensea_ethereum_wyvern_atomic_match') }} wam ON wam.tx_hash = om.evt_tx_hash
  INNER JOIN tokens_ethereum.erc20 terc20 ON terc20.contract_address = wam.token_address
  INNER JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', evt_block_time)
      AND p.blockchain = 'ethereum'
      AND p.contract_address = wam.token_address
  AND maker != taker
  AND date_trunc('day', p.minute) >= '2018-06-01'
  AND evt_tx_hash not in (
    SELECT
      *
    FROM
      {{ ref('opensea_ethereum_excluded_txns') }}
  )
  {% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  WHERE evt_block_time > (select max(block_time) from {{ this }})
  {% endif %} 