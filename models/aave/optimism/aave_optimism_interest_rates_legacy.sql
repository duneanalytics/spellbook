{{ config(
	tags=['legacy'],
	
      alias = alias('interest', legacy_model=True)
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
            reserve,
            symbol,
            hour,
            deposit_apy,
            stable_borrow_apy,
            variable_borrow_apy
      FROM {{ ref('aave_v3_optimism_interest_rates_legacy') }}
      /*
      UNION ALL
      < add new version as needed
      */
)
