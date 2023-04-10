 {{
  config(
        alias='daily_balances',
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy='merge',
        unique_key = ['token_mint_address', 'address','day'],
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "solana_utils",
                                    \'["ilemi"]\') }}')
}}

WITH 
      updated_balances as (
            SELECT
                  address 
                  , date_trunc('day', block_time) as day
                  , token_mint_address
                  , post_balance as sol_balance
                  , post_token_balance as token_balance
                  , row_number() OVER (partition by address, date_trunc('day', block_time) order by block_slot desc) as latest_balance
            FROM {{ source('solana','account_activity') }}
            WHERE tx_success 
            {% if is_incremental() %}
            AND block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
      )

--is there a way to then only keep the most recent "updated_at" for each address?
SELECT 
      *
      , now() as updated_at 
FROM updated_balances
WHERE latest_balance = 1