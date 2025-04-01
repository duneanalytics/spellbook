{{
  config(
        schema = 'solana_utils',
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

{% if is_incremental() %}

--attemting to limit data read to only the partitions that have changed
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
        where act.address = 'YgiU6QrKidVFS6PhwoeTeHZXiSc8Av2UykCk7umyddo'  
      ),

      token_offsetter AS (
      -- adds helper column to identify when an address was assigned a new token
            SELECT
                  *
                  , LAG(token_mint_address) OVER (PARTITION BY address, token_balance_owner ORDER BY block_time ASC) AS prev_token
            FROM activity_for_processing
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
                  ) OVER (PARTITION BY address, token_balance_owner ORDER BY block_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS token_pairing_rank
            FROM token_offsetter
      )

      , account_activity_base AS (
      -- flattens data to each address-mint pairing with valid_from/valid_to timestamps
            SELECT
                  address
                  , token_balance_owner
                  , token_mint_address
                  , CAST(MIN(block_time) AS TIMESTAMP) AS valid_from
                  , CAST(MAX(block_time) AS TIMESTAMP) AS valid_to
            FROM pair_orderings
            GROUP BY address, token_balance_owner, token_mint_address, token_pairing_rank
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
FROM account_activity_base aa
LEFT JOIN nft_addresses nft
    ON aa.token_mint_address = nft.account_mint