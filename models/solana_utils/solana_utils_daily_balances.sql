 {{
  config(
        
        schema = 'solana_utils',
        alias = 'daily_balances',
        partition_by = ['month'],
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy='merge',
        incremental_predicates = ['DBT_INTERNAL_DEST.day >= date_trunc(\'day\', now() - interval \'1\' day)'],
        unique_key = ['unique_address_key'],
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
                  , token_mint_address
                  , cast(post_balance as double)/1e9 as sol_balance --lamport -> sol
                  , coalesce(post_token_balance,0) as token_balance --tokens are already correct decimals in this table
                  , token_balance_owner
                  , block_time
                  , block_slot
                  , row_number() OVER (partition by address, date_trunc('day', block_time) order by block_slot desc, tx_index desc) as latest_balance
            FROM {{ source('solana','account_activity') }}  aa
            WHERE tx_success
            --many writable accounts are included in account activity but don't see any state changes. So this shrinks the table by 70%.
            AND (aa.balance_change != 0 or aa.token_balance_change != 0)
            {% if is_incremental() %}
            AND block_time >= date_trunc('day', now() - interval '1' day)
            {% endif %}
      )

SELECT
      day
      , month
      , address
      , sol_balance
      , token_mint_address
      , token_balance
      , token_balance_owner
      , {{ dbt_utils.generate_surrogate_key(['address', 'token_mint_address', 'day']) }} as unique_address_key
      , now() as updated_at
FROM updated_balances
WHERE latest_balance = 1
