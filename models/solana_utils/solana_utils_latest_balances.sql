 {{
  config(
        alias='latest_balances',
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy='merge',
        unique_key = ['token_mint_address', 'address'],
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
                  , sol_balance
                  , token_balance
                  , row_number() OVER (partition by address order by day desc) as latest_balance
            FROM {{ ref('solana_utils_daily_balances') }}
            {% if is_incremental() %}
            WHERE block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
      )

SELECT 
      address
      , token_mint_address
      , sol_balance
      , token_balance
      , now() as updated_at 
FROM updated_balances
WHERE latest_balance = 1