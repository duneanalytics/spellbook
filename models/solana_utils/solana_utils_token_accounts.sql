 {{
  config(
        schema = 'solana_utils',
        tags = ['dunesql'],
        alias = alias('token_accounts'),
        materialized='table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "solana_utils",
                                    \'["ilemi"]\') }}')
}}

WITH 
      distinct_accounts as (
            SELECT
                  token_mint_address
                  , address 
                  , max_by(token_balance_owner, block_time) as token_balance_owner --some account created before and then again after token owner schema change, so then it created dupes.
                  , min(block_time) as created_at 
            FROM {{ source('solana','account_activity') }}
            WHERE token_mint_address is not null
            group by 1,2
      )
      
SELECT * FROM distinct_accounts