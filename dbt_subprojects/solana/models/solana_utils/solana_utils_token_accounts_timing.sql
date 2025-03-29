{{
  config(
        schema = 'solana_utils',
        alias = 'token_accounts_timed',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['address'],
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "solana_utils",
                                    \'["ilemi"]\') }}')
}}

WITH
      token_offsetter AS (
      -- adds helper column to identify when an address was assigned a new token
            SELECT
                  *
                  , LAG(token_mint_address) OVER (PARTITION BY address, token_balance_owner ORDER BY block_time ASC) AS prev_token
            FROM {{ source('solana','account_activity') }}
            {% if is_incremental() %}
            WHERE {{incremental_predicate('block_time')}}
            {% endif %}
      )

      , pair_orderings AS (
      -- identifies the ordering of each address-mint pairing with support for repeated pairings
            SELECT
                  *
                  , SUM(
                        CASE
                        WHEN token_mint_address != prev_token THEN 1
                        ELSE 0
                        END
                  ) OVER (PARTITION BY address, token_balance_owner ORDER BY block_time ASC) AS token_pairing_rank
            FROM token_offsetter
      )

      , account_activity_base AS (
      -- flattens data to each address-mint pairing with start/end timestamps
            SELECT
                  address
                  , token_balance_owner
                  , token_mint_address
                  , CAST(MIN(block_time) AS TIMESTAMP) AS activity_start
                  , CAST(MAX(block_time) AS TIMESTAMP) AS activity_end
            FROM pair_orderings
            GROUP BY 1, 2, 3, token_pairing_rank
      )

      , nft_addresses AS (
      -- updated nft logic to exclude fungible token_standard types from nft classification
            SELECT
                  account_mint
            FROM {{ ref('tokens_solana_nft') }}
            WHERE
                  account_mint IS NOT NULL
                  -- without this filter the old table classified many fungible tokens (e.g. JUP,DRIFT) as account_type=nft
                  AND token_standard NOT IN ('Fungible', 'FungibleAsset')
            GROUP BY 1
      )

-- final table retains existing solana.account_activity columns with additional start/end columns
SELECT
    aa.address
    , aa.token_balance_owner
    , aa.token_mint_address
    , aa.activity_start
    , aa.activity_end
    , CASE
            WHEN nft.account_mint IS NOT NULL THEN 'nft'
            ELSE 'fungible'
      END AS account_type
FROM account_activity_base aa
LEFT JOIN nft_addresses nft
    ON aa.token_mint_address = nft.account_mint
ORDER BY address, activity_start