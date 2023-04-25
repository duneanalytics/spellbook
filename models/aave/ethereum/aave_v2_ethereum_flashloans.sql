{{ config(
    schema = 'aave_v2_ethereum'
    , alias='borrow'
    , post_hook='{{ expose_spells(\'["ethereum"]\',
                                  "project",
                                  "aave_v2",
                                  \'["hildobby"]\') }}'
  )
}}

{% set aave_mock_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' %}
{% set weth_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' %}


WITH flashloans AS (
    SELECT flash.evt_block_time AS block_time
    , flash.evt_block_number AS block_number
    , flash.amount AS amount_raw
    , flash.evt_tx_hash AS tx_hash
    , flash.evt_index
    , flash.premium AS fee
    , CASE WHEN flash.asset='{{aave_mock_address}}' THEN '{{weth_address}}' ELSE flash.asset END AS currency_contract
    , CASE WHEN flash.asset='{{aave_mock_address}}' THEN 'ETH' ELSE erc20.symbol END AS currency_symbol
    , CASE WHEN flash.asset='{{aave_mock_address}}' THEN 18 ELSE erc20.decimals END AS currency_decimals
    , flash.target AS contract_address
    , flash.contract_address AS router_contract
    FROM {{ source('aave_v2_ethereum','LendingPool_evt_FlashLoan') }} flash
    LEFT JOIN {{ ref('tokens_ethereum_erc20') }} erc20 ON flash.asset = erc20.contract_address
    )
    
SELECT 'ethereum' AS blockchain
, 'Aave' AS project
, 'v2' AS version
, flash.block_time
, flash.block_number
, flash.amount_raw/POWER(10, flash.currency_decimals) AS amount
, pu.price*flash.amount_raw/POWER(10, flash.currency_decimals) AS amount_usd
, flash.tx_hash
, flash.evt_index
, flash.fee/POWER(10, flash.currency_decimals) AS fee
, flash.currency_contract
, flash.currency_symbol
, flash.contract_address
, flash.router_contract
FROM flashloans flash
LEFT JOIN {{ source('prices','usd') }} pu ON pu.blockchain = 'ethereum'  
    AND pu.contract_address = flash.currency_contract
    AND pu.minute = date_trunc('minute', flash.block_time)