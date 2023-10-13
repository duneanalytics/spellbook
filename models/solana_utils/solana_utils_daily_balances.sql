 {{
  config(
        tags=['dunesql'],
        schema = 'solana_utils',
        alias = alias('daily_balances'),
        partition_by = ['month'],
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy='merge',
        incremental_predicates = ['DBT_INTERNAL_DEST.day >= date_trunc(\'day\', now() - interval \'1\' day)'],
        unique_key = ['token_mint_address', 'address','day'],
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "solana_utils",
                                    \'["ilemi"]\') }}')
}}

WITH
      updated_balances as (
            SELECT
                  date_trunc('day', block_time) as day
                  , cast(date_trunc('month', block_time) as date) as month
                  , address
                  , coalesce(token_mint_address,original_token_mint_address) as token_mint_address
                  , cast(post_balance as double)/1e9 as sol_balance --lamport -> sol
                  , coalesce(post_token_balance,0) as token_balance --tokens are already correct decimals in this table
                  , coalesce(token_balance_owner,original_token_balance_owner) as token_balance_owner
                  , block_time
                  , block_slot
                  --could be one token mint closing and another opening on the same day. edge case but it exists.
                  , row_number() OVER (partition by address, coalesce(token_mint_address,original_token_mint_address), date_trunc('day', block_time) order by block_slot desc) as latest_balance
            FROM (
                  SELECT
                        aa.token_mint_address
                        , aa.block_time
                        , aa.block_slot
                        , aa.address
                        , aa.post_balance
                        , aa.post_token_balance
                        , aa.token_balance_owner
                        , max_by(tk.token_mint_address, tk.created_at) as original_token_mint_address
                        , max_by(tk.token_balance_owner, tk.created_at) as original_token_balance_owner
                        -- , row_number() over (partition by aa.address, aa.block_slot, aa.tx_id, aa.tx_index order by tk.created_at desc) as latest_tk
                  FROM {{ source('solana','account_activity') }}  aa
                  LEFT JOIN {{ ref('solana_utils_token_accounts')}} tk ON tk.address = aa.address
                        AND aa.token_mint_address is null --only join on the empty token mints
                        AND tk.created_at <= aa.block_time --only get token mints that were created at or before this account activity
                  WHERE tx_success
                  {% if is_incremental() %}
                  AND block_time >= date_trunc('day', now() - interval '1' day)
                  {% endif %}
                  group by 1,2,3,4,5,6,7
            )
            -- WHERE latest_tk = 1
      )

SELECT
      day
      , month
      , address
      , sol_balance
      , token_mint_address
      , token_balance
      , token_balance_owner
      , now() as updated_at
FROM updated_balances
WHERE latest_balance = 1