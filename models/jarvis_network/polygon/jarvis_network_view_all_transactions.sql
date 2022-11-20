{{
  config(
    alias='view_all_transactions',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    post_hook='{{ expose_spells(\'["polygon"]\',
                                      "project",
                                      "jarvis_network",
                                    \'["0xroll"]\') }}')
}}

{% set project_start_date = '2021-08-16' %}


SELECT
'polygon' as blockchain,
evt_block_time,
action,
user,
recipient,
jfiat_token_symbol,
jfiat_token_amount,
collateral_symbol,
collateral_token_amount,
net_collateral_amount,
fee_amount,
collateral_token_amount_usd
net_collateral_amount_usd,
fee_amount_usd
evt_tx_hash,
evt_index
FROM (
SELECT evt_block_time,
       action,
       user,
       recipient,
       am.jfiat_symbol as jfiat_token_symbol,
       jfiat_token_amount/POWER(10,am.decimals) as jfiat_token_amount,
       jfiat_collateral_symbol as collateral_symbol,
       collateral_token_amount/POWER(10,cm.decimals) as collateral_token_amount,
       net_collateral_amount/POWER(10,cm.decimals) as net_collateral_amount,
       fee_amount/POWER(10,cm.decimals) as fee_amount,
       collateral_token_amount/POWER(10,cm.decimals) * price as collateral_token_amount_usd,
       net_collateral_amount/POWER(10,cm.decimals) * price as net_collateral_amount_usd,
       fee_amount/POWER(10,cm.decimals) * price as fee_amount_usd,
       evt_tx_hash,
       evt_index
        FROM (
SELECT
    evt_block_time,
    'Mint' as action,
    contract_address,
    user,
    recipient,
    mintvalues:numTokens as jfiat_token_amount,
    mintvalues:totalCollateral as collateral_token_amount,
    mintvalues:exchangeAmount  as net_collateral_amount,
    mintvalues:feeAmount as fee_amount,
    evt_tx_hash,
    evt_index
FROM {{ source('jarvis_network_polygon','SynthereumMultiLpLiquidityPool_evt_Minted') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% else %}
WHERE evt_block_time >= '{{ project_start_date }}'
{% endif %}

UNION ALL

SELECT
  evt_block_time,
  'Redeem' as action,
  contract_address,
  user as sender,
  recipient,
  redeemvalues:numTokens as jfiat_token_amount,
  redeemvalues:collateralAmount  as collateral_token_amount,
  redeemvalues:exchangeAmount as net_collateral_amount,
  redeemvalues:feeAmount as fee_amount,
  evt_tx_hash,
  evt_index
FROM {{ source('jarvis_network_polygon','SynthereumMultiLpLiquidityPool_evt_Redeemed') }}

UNION ALL

SELECT
    evt_block_time,
    'Mint' as action,
    contract_address,
    account as user,
    recipient,
    numTokensReceived as jfiat_token_amount,
    collateralSent as collateral_token_amount,
    (collateralSent - feePaid) as net_collateral_amount,
    feePaid as fee_amount,
    evt_tx_hash,
    evt_index
FROM {{ source('jarvis_network_polygon','SynthereumPoolOnChainPriceFeed_evt_Mint') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% else %}
WHERE evt_block_time >= '{{ project_start_date }}'
{% endif %}

UNION ALL

    SELECT
    evt_block_time,
    'Redeem' as action,
    contract_address,
    account as user,
    recipient,
    numTokensSent as jfiat_token_amount,
    collateralReceived + feePaid as collateral_token_amount,
    collateralReceived  as net_collateral_amount,
    feePaid as fee_amount,
    evt_tx_hash,
    evt_index
FROM {{ source('jarvis_network_polygon','SynthereumPoolOnChainPriceFeed_evt_Redeem') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% else %}
WHERE evt_block_time >= '{{ project_start_date }}'
{% endif %}

UNION ALL

    SELECT
    evt_block_time,
    'Exchange' as action,
    contract_address,
    account as sender,
    recipient,
    numTokensSent as jfiat_token_amount,
    (feePaid * 1000) as collateral_token_amount,
    ((feePaid * 1000) - feePaid) as net_collateral_amount,
    feePaid as fee_amount,
    evt_tx_hash,
    evt_index
FROM {{ source('jarvis_network_polygon','SynthereumPoolOnChainPriceFeed_evt_Exchange') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% else %}
WHERE evt_block_time >= '{{ project_start_date }}'
{% endif %}
) x
INNER JOIN {{ ref('jarvis_network_polygon_jfiat_address_mapping') }} am ON (contract_address = jfiat_collateral_pool_address)
LEFT JOIN  {{ ref('jarvis_network_polygon_jfiat_collateral_mapping') }} cm ON (am.jfiat_collateral_pool_address = cm.jfiat_collateral_pool_address)
LEFT JOIN  {{ source('prices', 'usd') }} pu ON (am.blockchain = pu.blockchain AND cm.jfiat_collateral_symbol = pu.symbol AND date_trunc('minute',x.evt_block_time) = date_trunc('minute',pu.minute))
) p





