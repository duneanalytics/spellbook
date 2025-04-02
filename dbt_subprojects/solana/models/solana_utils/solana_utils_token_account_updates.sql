{{
  config(
        schema = 'solana_utils',
        alias = 'token_account_updates',
        materialized = 'incremental',
        file_format = 'delta',
        partition_by = ['partition_key'],
        incremental_strategy = 'merge',
        unique_key = ['address', 'valid_from'],
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "solana_utils",
                                    \'["ilemi"]\') }}')
}}


{% set start_date = '2025-03-25' %}

WITH
{% if is_incremental() %}
relevant_historical_data AS (
      select old.* from {{this}} old
      inner join  (
            SELECT distinct address
            FROM {{ source('solana', 'account_activity') }}
            WHERE {{incremental_predicate('block_time')}}
                  and writable = true
                  and token_mint_address is not null
      ) new
      on old.address = new.address  -- only include historical data for addresses that have been updated
      where old.valid_to is null    -- this includes only the latest record for each address
      and not {{incremental_predicate('old.valid_from')}}   -- ignore records that are already in the current interval
),
{% endif %}

activity_for_processing AS (
    SELECT 
      act.*
      , LAG(token_balance_owner) OVER (PARTITION BY address ORDER BY block_time ASC, tx_index ASC) AS prev_owner
      , LAG(token_mint_address) OVER (PARTITION BY address ORDER BY block_time ASC, tx_index ASC) AS prev_mint
    FROM (
      select * 
      from (
        select 
          address
          , token_balance_owner
          , token_mint_address
          , block_time
          , tx_index
        from {{ source('solana','account_activity') }} act
        where act.writable = true
        and act.token_mint_address is not null
        and act.block_time >= timestamp '{{start_date}}'
      ) 
      {% if is_incremental() %}
      -- Include any relevant historical data as if they are part of the current interval
      -- this makes logic for the window functions applicable to both full refresh and incremental runs
      union all
      select 
            address
            , token_balance_owner
            , token_mint_address
            , valid_from as block_time
            , 0 as tx_index
      from relevant_historical_data
      {% endif %}
    ) act
  ),

periods as (
-- Determine the start time and end time for each period
      SELECT *
            , LEAD(valid_from) OVER (PARTITION BY address ORDER BY valid_from ASC, tx_index ASC) as valid_to
      FROM(
            SELECT
                  address
                  , token_balance_owner
                  , token_mint_address
                  , block_time AS valid_from
                  , tx_index
            FROM activity_for_processing
            WHERE prev_owner != token_balance_owner 
                  OR prev_mint != token_mint_address
                  OR prev_owner is null 
                  OR prev_mint is null
      )
),

nft_addresses AS (
      SELECT distinct account_mint
      FROM {{ ref('tokens_solana_nft') }}
      WHERE
            account_mint IS NOT NULL
            AND token_standard NOT IN ('Fungible', 'FungibleAsset')
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
    , substring(aa.address, 1, 4) AS partition_key
FROM periods aa
LEFT JOIN nft_addresses nft
ON aa.token_mint_address = nft.account_mint
WHERE aa.valid_from IS DISTINCT FROM aa.valid_to -- ignore changes that happen within the same block
{% if is_incremental() %}
-- only update records outside of the current interval if they have a valid_to date
AND not(aa.valid_to is null and not {{incremental_predicate('aa.valid_from')}})
{% endif %}