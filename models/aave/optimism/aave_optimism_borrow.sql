{{ config(
      alias='borrow'
      , post_hook='{{ expose_spells(\'["optimism"]\',
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
      FROM {{ ref('aave_v3_optimism_borrow') }}
      /*
      UNION ALL
      < add new version as needed
      */
)
