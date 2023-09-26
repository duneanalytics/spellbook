{{ config(
      tags = ['dunesql']
      , alias = alias('borrow')
      , post_hook='{{ expose_spells(\'["ethereum"]\',
                                  "project",
                                  "aave",
                                  \'["batwayne", "chuxin"]\') }}'
  )
}}

SELECT *
FROM 
(
      SELECT
            version,
            transaction_type,
            loan_type,
            symbol,
            token_address,
            borrower,
            repayer,
            liquidator,
            amount,
            usd_amount,
            evt_tx_hash,
            evt_index,
            evt_block_time,
            evt_block_number  
      FROM {{ ref('aave_v1_ethereum_borrow') }}
      UNION
      SELECT
            version,
            transaction_type,
            loan_type,
            symbol,
            token_address,
            borrower,
            repayer,
            liquidator,
            amount,
            usd_amount,
            evt_tx_hash,
            evt_index,
            evt_block_time,
            evt_block_number  
      FROM {{ ref('aave_v2_ethereum_borrow') }} 
)