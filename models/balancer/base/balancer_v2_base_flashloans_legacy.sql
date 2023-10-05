{{ config(
	tags=['legacy'],
      schema = 'balancer_v2_base'
      , alias = alias('flashloans', legacy_model=True)
  )
}}

SELECT 1
