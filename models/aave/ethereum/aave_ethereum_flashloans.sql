{{ config(
      alias='flashloans'
      , post_hook='{{ expose_spells(\'["ethereum"]\',
                                  "project",
                                  "aave",
                                  \'["hildobby"]\') }}'
  )
}}

SELECT *
FROM (
      SELECT blockchain
      , project
      , version
      , block_time
      , block_number
      , amount
      , amount_usd
      , tx_hash
      , evt_index
      , fee
      , currency_contract
      , currency_symbol
      , recipient
      , contract_address
      FROM {{ ref('aave_v1_ethereum_flashloans') }}

      UNION ALL

      SELECT blockchain
      , project
      , version
      , block_time
      , block_number
      , amount
      , amount_usd
      , tx_hash
      , evt_index
      , fee
      , currency_contract
      , currency_symbol
      , recipient
      , contract_address
      FROM {{ ref('aave_v2_ethereum_flashloans') }} 

      UNION ALL

      SELECT blockchain
      , project
      , version
      , block_time
      , block_number
      , amount
      , amount_usd
      , tx_hash
      , evt_index
      , fee
      , currency_contract
      , currency_symbol
      , recipient
      , contract_address
      FROM {{ ref('aave_v3_ethereum_flashloans') }} 
)