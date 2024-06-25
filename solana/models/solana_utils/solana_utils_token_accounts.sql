 {{
  config(
        schema = 'solana_utils',
        alias = 'token_accounts',
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
      distinct_accounts as (
            SELECT
                  aa.address
                  , max_by(aa.token_balance_owner, aa.block_time) as token_balance_owner
                  , max_by(aa.token_mint_address, aa.block_time) as token_mint_address
            FROM {{ source('solana','account_activity') }} aa
            WHERE aa.token_mint_address is not null
            {% if is_incremental() %}
            AND {{incremental_predicate('aa.block_time')}}
            {% endif %}
            group by 1
      )
SELECT
da.*
, case when nft.account_mint is not null then 'nft'
      else 'fungible'
      end as account_type
FROM distinct_accounts da
LEFT JOIN {{ ref('tokens_solana_nft')}} nft ON da.token_mint_address = nft.account_mint
