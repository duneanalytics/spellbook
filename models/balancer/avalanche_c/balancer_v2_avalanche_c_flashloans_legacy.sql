{{ config(
	tags=['legacy'],
      schema = 'balancer_v2_avalanche_c'
      , alias = alias('flashloans', legacy_model=True)
  )
}}

SELECT 1
