{{ config(
    schema = 'aave_base'
    , alias = 'interest_rates'
    , post_hook='{{ expose_spells(\'["base"]\',
                                  "project",
                                  "aave",
                                  \'["mikeghen1","batwayne", "chuxin"]\') }}'
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
      FROM {{ ref('aave_v3_base_interest_rates') }}
      /*
      UNION ALL
      < add new version as needed
      */
)
