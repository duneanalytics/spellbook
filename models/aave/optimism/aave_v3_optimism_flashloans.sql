{{ config(
    schema = 'aave_v3_optimism'
    , alias='flashloans'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , post_hook='{{ expose_spells(\'["optimism"]\',
                                  "project",
                                  "aave_v3",
                                  \'["hildobby"]\') }}'
  )
}}

{% set aave_mock_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' %}
{% set weth_address = '0x4200000000000000000000000000000000000006' %}


WITH flashloans AS (
    SELECT flash.evt_block_time AS block_time
    , flash.evt_block_number AS block_number
    , CAST(flash.amount AS double) AS amount_raw
    , flash.evt_tx_hash AS tx_hash
    , flash.evt_index
    , CAST(flash.premium AS double) AS fee
    , CASE WHEN flash.asset='{{aave_mock_address}}' THEN '{{weth_address}}' ELSE flash.asset END AS currency_contract
    , CASE WHEN flash.asset='{{aave_mock_address}}' THEN 'ETH' ELSE erc20.symbol END AS currency_symbol
    , CASE WHEN flash.asset='{{aave_mock_address}}' THEN 18 ELSE erc20.decimals END AS currency_decimals
    , flash.target AS recipient
    , flash.contract_address
    FROM {{ source('aave_v3_optimism','Pool_evt_FlashLoan') }} flash
    LEFT JOIN {{ ref('tokens_optimism_erc20') }} erc20 ON flash.asset = erc20.contract_address
    WHERE CAST(flash.amount AS double) > 0
        {% if is_incremental() %}
        AND flash.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    )
    
SELECT 'optimism' AS blockchain
, 'Aave' AS project
, '3' AS version
, flash.block_time
, flash.block_number
, flash.amount_raw/POWER(10, flash.currency_decimals) AS amount
, pu.price*flash.amount_raw/POWER(10, flash.currency_decimals) AS amount_usd
, flash.tx_hash
, flash.evt_index
, flash.fee/POWER(10, flash.currency_decimals) AS fee
, flash.currency_contract
, flash.currency_symbol
, flash.recipient
, flash.contract_address
FROM flashloans flash
LEFT JOIN {{ source('prices','usd') }} pu ON pu.blockchain = 'optimism'  
    AND pu.contract_address = flash.currency_contract
    AND pu.minute = date_trunc('minute', flash.block_time)