{{ config(
	tags=['legacy'],
	
      alias = alias('supply', legacy_model=True)
      , post_hook='{{ expose_spells(\'["ethereum"]\',
                                  "project",
                                  "aave",
                                  \'["batwayne", "chuxin", "hildobby"]\') }}'
  )
}}

SELECT *
FROM 
(
      SELECT 
            version,
            transaction_type,
            symbol,
            token_address, 
            depositor,
            withdrawn_to,
            liquidator,
            amount,
            usd_amount,
            evt_tx_hash,
            evt_index,
            evt_block_time,
            evt_block_number 
      FROM {{ ref('aave_v1_ethereum_supply_legacy') }}
      UNION ALL
      SELECT 
            version,
            transaction_type,
            symbol,
            token_address, 
            depositor,
            withdrawn_to,
            liquidator,
            amount,
            usd_amount,
            evt_tx_hash,
            evt_index,
            evt_block_time,
            evt_block_number 
      FROM {{ ref('aave_v2_ethereum_supply_legacy') }}
)
;