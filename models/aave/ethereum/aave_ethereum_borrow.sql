{{ config(
      alias='borrow'
      , post_hook='{{ expose_spells(\'["ethereum"]\',
                                  "project",
                                  "aave",
                                  \'["batwayne", "chuxin"]\') }}'
  )
}}
{{ trino_comment("aave", "borrow", [{"name": "version", "type": "uint256"}]) }}
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
      FROM hello
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
      FROM world
)
;