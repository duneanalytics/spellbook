 {{
  config(
	tags=['legacy'],
	
        alias = alias('latest_balances', legacy_model=True),
        materialized='table',
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "solana_utils",
                                    \'["ilemi"]\') }}')
}}

WITH 
      updated_balances as (
            SELECT
                  address 
                  , day
                  , sol_balance
                  , token_mint_address
                  , token_balance
                  , token_balance_owner
                  , row_number() OVER (partition by address order by day desc) as latest_balance
            FROM {{ ref('solana_utils_daily_balances_legacy') }}
            WHERE sol_balance is not null OR (token_balance is not null AND token_mint_address is not null) --sometimes the daily balance has empty values for some reason.
      )

SELECT 
      address
      , sol_balance
      , token_balance
      , token_mint_address
      , token_balance_owner
      , now() as updated_at 
FROM updated_balances
WHERE latest_balance = 1
