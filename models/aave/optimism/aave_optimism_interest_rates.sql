{{ config(
      alias='interest'
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
      FROM {{ ref('aave_v3_optimism_interest_rates') }}
      /*
      UNION ALL
      < add new version as needed
      */
)
