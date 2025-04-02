{{
  config(
        schema = 'solana_utils',
        tags = ['prod_exclude'],
        alias = 'token_accounts_static',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['address', 'token_balance_owner', 'token_mint_address'],
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "solana_utils",
                                    \'["ilemi"]\') }}')
}}

-- This model processes addresses with only one owner/mint (the majority)
-- Using TrinoSQL compatible approaches

{% if is_incremental() %}
-- For incremental runs, find recently modified addresses
WITH addresses_to_process AS (
    SELECT DISTINCT act.address
    FROM {{ source('solana', 'account_activity') }} act
    LEFT JOIN {{ ref('solana_utils_token_accounts_candidates') }} cand
        ON act.address = cand.address
    WHERE {{incremental_predicate('act.block_time')}}
    AND act.writable = true
    AND act.block_time >= DATE('2025-04-01') -- Test run with future date
    AND cand.address IS NULL -- Only addresses NOT in candidates
),
{% else %}
WITH addresses_to_process AS (
    -- For full runs, get all addresses not in candidates
    SELECT DISTINCT act.address
    FROM {{ source('solana', 'account_activity') }} act
    LEFT JOIN {{ ref('solana_utils_token_accounts_candidates') }} cand
        ON act.address = cand.address
    WHERE act.writable = true
    AND act.token_mint_address IS NOT NULL
    AND act.token_balance_owner IS NOT NULL
    AND act.block_time >= DATE('2025-04-01') -- Test run with future date
    AND cand.address IS NULL -- Only addresses NOT in candidates
),
{% endif %}

-- Get the earliest transaction for each address
earliest_tx AS (
    SELECT 
        a.address,
        MIN(a.block_time) AS min_time
    FROM {{ source('solana', 'account_activity') }} a
    WHERE writable = true
    AND a.token_mint_address IS NOT NULL
    AND a.token_balance_owner IS NOT NULL
    AND a.block_time >= DATE('2025-04-01') -- Test run with future date
    AND a.address IN (SELECT address FROM addresses_to_process)
    GROUP BY a.address
),

-- Get a representative record (using MAX to get a single value)
-- Since these are static accounts, all records have the same owner/mint
representative_record AS (
    SELECT 
        a.address,
        -- Use aggregates to get one value per address
        MAX(a.token_balance_owner) AS token_balance_owner,
        MAX(a.token_mint_address) AS token_mint_address
    FROM {{ source('solana', 'account_activity') }} a
    WHERE a.writable = true
    AND a.token_mint_address IS NOT NULL
    AND a.token_balance_owner IS NOT NULL
    AND a.block_time >= DATE('2025-04-01') -- Test run with future date
    AND a.address IN (SELECT address FROM addresses_to_process)
    GROUP BY a.address
),

-- Combine both to get complete static account data
static_accounts AS (
    SELECT
        r.address,
        r.token_balance_owner,
        r.token_mint_address,
        e.min_time AS valid_from
    FROM representative_record r
    INNER JOIN earliest_tx e
        ON r.address = e.address
)

-- Join with NFT data for classification
SELECT
    sa.address,
    sa.token_balance_owner,
    sa.token_mint_address,
    sa.valid_from,
    CAST(NOW() AS TIMESTAMP) AS valid_to,
    CASE
        WHEN nft.account_mint IS NOT NULL THEN 'nft'
        ELSE 'fungible'
    END AS account_type
FROM static_accounts sa
LEFT JOIN {{ ref('tokens_solana_nft') }} nft
    ON sa.token_mint_address = nft.account_mint
    AND nft.token_standard NOT IN ('Fungible', 'FungibleAsset') 