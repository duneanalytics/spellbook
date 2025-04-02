{{
  config(
        schema = 'solana_utils',
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

-- Simplified model for better Trino compatibility

{% if is_incremental() %}
-- For incremental runs
WITH base_data AS (
    SELECT 
        act.address,
        act.token_balance_owner,
        act.token_mint_address,
        MIN(act.block_time) AS valid_from
    FROM {{ source('solana', 'account_activity') }} act
    WHERE 
        act.writable = true
        AND act.token_mint_address IS NOT NULL
        AND act.token_balance_owner IS NOT NULL
        AND act.block_time >= DATE('2025-04-01')
        AND {{incremental_predicate('act.block_time')}}
        AND NOT EXISTS (
            SELECT 1 
            FROM {{ ref('solana_utils_token_accounts_candidates') }} cand
            WHERE act.address = cand.address
        )
    GROUP BY 
        act.address,
        act.token_balance_owner,
        act.token_mint_address
)
{% else %}
-- For full runs
WITH base_data AS (
    SELECT 
        act.address,
        act.token_balance_owner,
        act.token_mint_address,
        MIN(act.block_time) AS valid_from
    FROM {{ source('solana', 'account_activity') }} act
    WHERE 
        act.writable = true
        AND act.token_mint_address IS NOT NULL
        AND act.token_balance_owner IS NOT NULL
        AND act.block_time >= DATE('2025-04-01')
        AND NOT EXISTS (
            SELECT 1 
            FROM {{ ref('solana_utils_token_accounts_candidates') }} cand
            WHERE act.address = cand.address
        )
    GROUP BY 
        act.address,
        act.token_balance_owner,
        act.token_mint_address
)
{% endif %}

-- Final output with NFT classification
SELECT
    bd.address,
    bd.token_balance_owner,
    bd.token_mint_address,
    bd.valid_from,
    current_timestamp AS valid_to,
    CASE
        WHEN nft.account_mint IS NOT NULL THEN 'nft'
        ELSE 'fungible'
    END AS account_type
FROM base_data bd
LEFT JOIN {{ ref('tokens_solana_nft') }} nft
    ON bd.token_mint_address = nft.account_mint
    AND nft.token_standard NOT IN ('Fungible', 'FungibleAsset') 