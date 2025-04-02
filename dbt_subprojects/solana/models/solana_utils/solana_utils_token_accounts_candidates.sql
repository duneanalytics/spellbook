{{
  config(
        schema = 'solana_utils',
        alias = 'token_accounts_candidates',
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "solana_utils",
                                    \'["ilemi"]\') }}')
}}

-- This model identifies addresses that have multiple token_balance_owner or token_mint_address values
-- These are the only addresses that need the expensive time-window processing in token_accounts_timed

WITH address_counts AS (
    SELECT 
        address,
        COUNT(DISTINCT token_balance_owner) AS unique_owner_count,
        COUNT(DISTINCT token_mint_address) AS unique_mint_count
    FROM {{ source('solana', 'account_activity') }}
    WHERE 
        writable = true
        and token_mint_address is not null
        and token_balance_owner is not null
    GROUP BY address
    HAVING COUNT(DISTINCT token_balance_owner) > 1 OR COUNT(DISTINCT token_mint_address) > 1
)

SELECT
    address,
    unique_owner_count,
    unique_mint_count
FROM address_counts 