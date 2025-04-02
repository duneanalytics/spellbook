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

-- This model identifies addresses with multiple owners or mint addresses
-- Uses standard SQL that's compatible with TrinoSQL

WITH filtered_activity AS (
    -- Apply all filters once to reduce data volume
    SELECT 
        address,
        token_balance_owner,
        token_mint_address
    FROM {{ source('solana', 'account_activity') }}
    WHERE 
        writable = true
        AND token_mint_address IS NOT NULL
        AND token_balance_owner IS NOT NULL
        AND block_time >= DATE('2025-04-01') -- Test run with future date
),

-- Group and count distinct values
address_counts AS (
    SELECT 
        address,
        COUNT(DISTINCT token_balance_owner) AS unique_owner_count,
        COUNT(DISTINCT token_mint_address) AS unique_mint_count
    FROM filtered_activity
    GROUP BY address
    -- Skip HAVING to avoid another GROUP BY scan, we'll filter later
),

-- Get only the addresses that have multiple values
candidate_addresses AS (
    SELECT address
    FROM address_counts
    WHERE unique_owner_count > 1 OR unique_mint_count > 1
)

-- Final output with just the addresses we need
SELECT address
FROM candidate_addresses 