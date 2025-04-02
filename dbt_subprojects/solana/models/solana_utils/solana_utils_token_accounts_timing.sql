{{
  config(
        schema = 'solana_utils',
        tags = ['prod_exclude'],
        alias = 'token_accounts_timed',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['address', 'token_balance_owner', 'token_mint_address', 'valid_from', 'valid_to'],
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "solana_utils",
                                    \'["ilemi"]\') }}')
}}

-- This model creates time periods for addresses with multiple owners/mints

{% if is_incremental() %}
--limiting data to only the partitions that have changed for incremental runs
WITH addresses_to_process AS (
    SELECT DISTINCT act.address
    FROM {{ source('solana', 'account_activity') }} act
    INNER JOIN {{ ref('solana_utils_token_accounts_candidates') }} cand
        ON act.address = cand.address
    WHERE {{incremental_predicate('act.block_time')}}
    AND act.block_time >= DATE('2025-04-01') -- Test run with future date
),
{% else %}
WITH addresses_to_process AS (
    -- For full runs, get all candidate addresses
    SELECT address
    FROM {{ ref('solana_utils_token_accounts_candidates') }}
),
{% endif %}

-- Only process activity for candidate addresses
activity_for_processing AS (
    SELECT 
        act.address,
        act.block_time,
        act.token_balance_owner,
        act.token_mint_address
    FROM {{ source('solana','account_activity') }} act
    INNER JOIN addresses_to_process proc
        ON act.address = proc.address
    WHERE 
        act.writable = true
        AND act.token_mint_address IS NOT NULL
        AND act.token_balance_owner IS NOT NULL
        AND act.block_time >= DATE('2025-04-01') -- Test run with future date
),

-- Number activity chronologically per address for LAG calculations
numbered_activity AS (
    SELECT
        address,
        block_time,
        token_balance_owner,
        token_mint_address,
        -- Add row number to allow manual LAG calculation
        ROW_NUMBER() OVER (PARTITION BY address ORDER BY block_time ASC) AS row_num
    FROM activity_for_processing
),

-- Join with itself to simulate LAG function in a way compatible with all SQL dialects
change_detection AS (
    SELECT
        current.address,
        current.block_time,
        current.token_balance_owner,
        current.token_mint_address,
        prev.token_balance_owner AS prev_owner,
        prev.token_mint_address AS prev_mint,
        -- Detect changes
        CASE
            WHEN prev.address IS NULL THEN 1 -- First record
            WHEN current.token_balance_owner != prev.token_balance_owner THEN 1
            WHEN current.token_mint_address != prev.token_mint_address THEN 1
            ELSE 0
        END AS is_change_point
    FROM numbered_activity current
    LEFT JOIN numbered_activity prev
        ON current.address = prev.address
        AND current.row_num = prev.row_num + 1
),

-- Only keep rows where changes happened
change_points AS (
    SELECT
        address,
        block_time,
        token_balance_owner,
        token_mint_address 
    FROM change_detection
    WHERE is_change_point = 1
),

-- Number the change points chronologically
numbered_changes AS (
    SELECT
        address,
        block_time,
        token_balance_owner,
        token_mint_address,
        ROW_NUMBER() OVER (PARTITION BY address ORDER BY block_time ASC) AS change_num
    FROM change_points
),

-- Join with the next change to get valid_to
time_periods AS (
    SELECT
        current.address,
        current.token_balance_owner,
        current.token_mint_address,
        current.block_time AS valid_from,
        next_change.block_time AS valid_to
    FROM numbered_changes current
    LEFT JOIN numbered_changes next_change
        ON current.address = next_change.address
        AND current.change_num = next_change.change_num - 1
)

-- Join with NFT data directly, without creating a separate subquery
SELECT
    tp.address,
    tp.token_balance_owner,
    tp.token_mint_address,
    tp.valid_from,
    -- For the last period of each address (where valid_to is NULL), set to current time
    COALESCE(tp.valid_to, CAST(NOW() AS TIMESTAMP)) AS valid_to,
    CASE
        WHEN nft.account_mint IS NOT NULL THEN 'nft'
        ELSE 'fungible'
    END AS account_type
FROM time_periods tp
LEFT JOIN {{ ref('tokens_solana_nft') }} nft
    ON tp.token_mint_address = nft.account_mint
    AND nft.token_standard NOT IN ('Fungible', 'FungibleAsset')



