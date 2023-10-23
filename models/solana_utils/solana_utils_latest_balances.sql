 {{
  config(
        schema = 'solana_utils',
        alias = alias('latest_balances'),
        materialized='table',
        tags = ['dunesql'],
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
            FROM {{ ref('solana_utils_daily_balances') }}
      )

SELECT
      ub.address
      , ub.sol_balance
      , ub.token_balance
      , coalesce(ub.token_mint_address, tk.token_mint_address) as token_mint_address
      , coalesce(ub.token_balance_owner, tk.token_balance_owner) as token_balance_owner
      , now() as updated_at
FROM updated_balances ub
LEFT JOIN {{ ref('solana_utils_token_accounts')}} tk ON tk.address = ub.address
WHERE latest_balance = 1