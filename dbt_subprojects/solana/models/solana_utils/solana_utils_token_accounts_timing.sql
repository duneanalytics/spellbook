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

-- account_activity table is updated by a streaming service
-- it's partitioned by address
-- it contains a row for every transaction that has modified an account



{% if is_incremental() %}

--attemting to limit data read to only the partitions that have changed for incremental runs
WITH affected_partitions AS (
    SELECT DISTINCT address
    FROM {{ source('solana', 'account_activity') }}
    WHERE {{incremental_predicate('block_time')}}
    
),
{% else %}
WITH affected_partitions AS (
    SELECT 1
),
{% endif %}

      activity_for_processing AS (
        SELECT act.*
        FROM {{ source('solana','account_activity') }} act
        {% if is_incremental() %}
        INNER JOIN affected_partitions ap
            ON act.address = ap.address
        {% endif %}
        where act.writable = true
        and act.block_time >= DATE ('2025-03-25')
      ),

      state_offsetter AS (
      -- adds helper columns to identify when an address's owner OR mint changes
            SELECT
                  *
                  , LAG(token_balance_owner) OVER (PARTITION BY address ORDER BY block_time ASC) AS prev_owner
                  , LAG(token_mint_address) OVER (PARTITION BY address ORDER BY block_time ASC) AS prev_mint
            FROM activity_for_processing
      )

      , change_periods AS (
      -- identifies the ordering/rank of each contiguous period based on owner OR mint changes
            SELECT
                  *
                  , SUM(
                        CASE
                        -- Increment when owner changes, or mint changes, or it's the first record for the address
                        WHEN token_balance_owner != prev_owner 
                             OR token_mint_address != prev_mint 
                             OR (prev_owner IS NULL AND prev_mint IS NULL) 
                        THEN 1 
                        ELSE 0
                        END
                  ) OVER (PARTITION BY address ORDER BY block_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS change_period_rank
            FROM state_offsetter
      )

      , period_starts AS (
      -- Determine the start time for each distinct period
            SELECT
                  address
                  , token_balance_owner
                  , token_mint_address
                  , change_period_rank
                  , MIN(block_time) AS period_start_time -- This is the valid_from
            FROM change_periods
            GROUP BY address, token_balance_owner, token_mint_address, change_period_rank
      )

      , period_intervals AS (
      -- Calculate valid_from and valid_to for each period using LEAD
            SELECT
                  address
                  , token_balance_owner
                  , token_mint_address
                  , period_start_time AS valid_from
                  , LEAD(period_start_time) OVER (PARTITION BY address ORDER BY period_start_time ASC) AS valid_to
            FROM period_starts
      )

      , nft_addresses AS (
      -- updated nft logic to exclude fungible token_standard types from nft classification
            SELECT
                  account_mint
            FROM {{ ref('tokens_solana_nft') }}
            WHERE
                  account_mint IS NOT NULL
                  AND token_standard NOT IN ('Fungible', 'FungibleAsset')
            GROUP BY 1
      )

-- final table retains existing solana.account_activity columns with additional valid_from/valid_to columns
SELECT
    aa.address
    , aa.token_balance_owner
    , aa.token_mint_address
    , aa.valid_from
    , aa.valid_to
    , CASE
            WHEN nft.account_mint IS NOT NULL THEN 'nft'
            ELSE 'fungible'
      END AS account_type
FROM period_intervals aa
LEFT JOIN nft_addresses nft
    ON aa.token_mint_address = nft.account_mint



