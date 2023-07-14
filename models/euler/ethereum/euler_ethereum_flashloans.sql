{{ config(
      alias = alias('flashloans')
      , materialized = 'incremental'
      , file_format = 'delta'
      , incremental_strategy = 'merge'
      , unique_key = ['tx_hash', 'evt_index']
      , post_hook='{{ expose_spells(\'["ethereum"]\',
                                  "project",
                                  "euler",
                                  \'["hildobby"]\') }}'
  )
}}

SELECT 'ethereum' AS blockchain
    , 'Euler' AS project
    , 1 AS version
    , b.evt_block_time AS block_time
    , b.evt_block_number AS block_number
    , b.amount/POWER(10, 18) AS amount
    , pu.price*(b.amount/POWER(10, 18)) AS amount_usd
    , b.evt_tx_hash AS tx_hash
    , b.evt_index
    , 0 AS fee
    , b.underlying AS currency_contract
    , tok.symbol AS currency_symbol
    , b.account AS recipient
    , b.contract_address
    FROM {{ source('euler_ethereum','Euler_evt_Borrow') }} b
    INNER JOIN {{ source('euler_ethereum','Euler_evt_Repay') }} r ON r.evt_block_number = b.evt_block_number
        AND r.evt_tx_hash = b.evt_tx_hash
        AND r.account = b.account
        AND r.underlying = b.underlying
        AND r.amount = b.amount
        AND r.evt_index > b.evt_index
        {% if is_incremental() %}
        AND r.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN {{ ref('tokens_ethereum_erc20_legacy') }} tok ON tok.contract_address=b.underlying
    LEFT JOIN {{ source('prices','usd') }} pu
        ON pu.blockchain = 'ethereum'
        AND pu.contract_address = b.underlying
        AND pu.minute = date_trunc('minute', b.evt_block_time)
        {% if is_incremental() %}
        AND pu.minute >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    {% if is_incremental() %}
    WHERE b.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}