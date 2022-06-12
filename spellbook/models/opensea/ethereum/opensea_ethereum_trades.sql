 {{
  config(
        alias='trades'
  )
}}

SELECT
evt_tx_hash || evt_index::string as unique_id,
'ethereum' as blockchain,
evt_tx_hash as tx_hash,
evt_block_time as block_time,
om.price / power(10, decimals) * p.price AS amount_usd,
om.price / power(10, decimals) AS amount,
om.price AS amount_raw,
terc20.symbol as token_symbol,
wam.token_address as token_address,
maker,
taker,
evt_index as trade_id
FROM
  {{ source('opensea_ethereum','wyvernexchange_evt_ordersmatched') }} om
  LEFT JOIN {{ ref('opensea_ethereum_wyvern_atomic_match') }} wam ON wam.tx_hash = om.evt_tx_hash
  LEFT JOIN {{ ref('tokens_ethereum_erc20') }} terc20 ON terc20.contract_address = wam.token_address
  LEFT JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', evt_block_time)
      AND p.blockchain = 'ethereum'
      AND p.contract_address = wam.token_address
  WHERE maker != taker
  AND date_trunc('day', p.minute) >= '2018-06-01'
  AND evt_tx_hash not in (
    SELECT
      *
    FROM
      {{ ref('opensea_ethereum_excluded_txns') }}
  )
  {% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  AND evt_block_time > now() - interval 2 days
  {% endif %} 