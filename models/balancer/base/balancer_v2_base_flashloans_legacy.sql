{{ config(
	tags=['legacy'],
	
      schema = 'balancer_v2_base'
      , alias = alias('flashloans', legacy_model=True)
      , materialized = 'incremental'
      , file_format = 'delta'
      , incremental_strategy = 'merge'
      , unique_key = ['tx_hash', 'evt_index']
      , post_hook='{{ expose_spells(\'["base"]\',
                                  "project",
                                  "balancer_v2",
                                  \'["hildobby"]\') }}'
  )
}}

SELECT 1 as id