{{ config(
      alias='flashloans'
      , materialized = 'incremental'
      , file_format = 'delta'
      , incremental_strategy = 'merge'
      , unique_key = ['tx_hash', 'evt_index']
      , post_hook='{{ expose_spells(\'["ethereum"]\',
                                  "project",
                                  "dydx",
                                  \'["hildobby"]\') }}'
  )
}}

WITH flashloans AS (
    SELECT d.evt_block_time AS block_time
        , d.evt_block_number AS block_number
        , CAST(get_json_object(get_json_object(d.update, '$.deltaWei'), '$.value') AS DECIMAL(38,0)) AS amount_raw
        , d.evt_tx_hash AS tx_hash
        , d.evt_index
        , CASE WHEN MIN(CAST(get_json_object(get_json_object(d.update, '$.deltaWei'), '$.value') AS DECIMAL(38,0)) - CAST(get_json_object(get_json_object(w.update, '$.deltaWei'), '$.value') AS DECIMAL(38,0))) < 0 THEN 0
            ELSE MIN(CAST(get_json_object(get_json_object(d.update, '$.deltaWei'), '$.value') AS DECIMAL(38,0)) - CAST(get_json_object(get_json_object(w.update, '$.deltaWei'), '$.value') AS DECIMAL(38,0))) END AS fee
        , CASE WHEN d.market = 0 THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            WHEN d.market = 1 THEN '0x89d24a6b4ccb1b6faa2625fe562bdd9a23260359'
            WHEN d.market = 2 THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
            WHEN d.market = 3 THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
            END AS currency_contract
        , CASE WHEN d.market = 0 THEN 'ETH'
            WHEN d.market = 1 THEN 'SAI'
            WHEN d.market = 2 THEN 'USDC'
            WHEN d.market = 3 THEN 'DAI'
            END AS currency_symbol
        , CASE WHEN d.market IN (0, 1, 3) THEN 18
            WHEN d.market = 2 THEN 6
            END AS currency_decimals
        , d.contract_address
    FROM {{ source('dydx_ethereum','SoloMargin_evt_LogDeposit') }} d
    INNER JOIN {{ source('dydx_ethereum','SoloMargin_evt_LogWithdraw') }} w
        ON w.evt_block_number = d.evt_block_number
        AND w.evt_tx_hash = d.evt_tx_hash
        AND w.market = d.market
        AND d.accountOwner = w.accountOwner
        AND w.evt_index < d.evt_index
        {% if is_incremental() %}
        AND w.evt_block_time >= date_trunc("day", now() - interval '1 week')
        AND d.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    GROUP BY 1, 2, 3, 4, 5, 7, 8, 9, 10
    )

SELECT 'ethereum'                                                       AS blockchain
     , 'dYdX'                                                           AS project
     , '1'                                                              AS version
     , flash.block_time
     , flash.block_number
     , flash.amount_raw / POWER(10, flash.currency_decimals)            AS amount
     , pu.price * flash.amount_raw / POWER(10, flash.currency_decimals) AS amount_usd
     , flash.tx_hash
     , flash.evt_index
     , flash.fee / POWER(10, flash.currency_decimals)                   AS fee
     , flash.currency_contract
     , flash.currency_symbol
     , flash.contract_address
FROM flashloans flash
LEFT JOIN {{ source('prices','usd') }} pu
    ON pu.blockchain = 'ethereum'
    AND pu.contract_address = flash.currency_contract
    AND pu.minute = date_trunc('minute', flash.block_time)