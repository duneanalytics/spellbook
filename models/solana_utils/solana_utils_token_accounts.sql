 {{
  config(
        schema = 'solana_utils',
        alias = 'token_accounts',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.created_at')],
        unique_key = ['token_mint_address','address'],
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "solana_utils",
                                    \'["ilemi"]\') }}')
}}

WITH 
      distinct_accounts as (
            SELECT
                  aa.token_mint_address
                  , aa.address 
                  , max_by(aa.token_balance_owner, aa.block_time) as token_balance_owner --some account created before and then again after token owner schema change, so then it created dupes.
                  , min(aa.block_time) as created_at 
            FROM {{ source('solana','account_activity') }} aa
            WHERE aa.token_mint_address is not null
            {% if is_incremental() %}
            AND {{incremental_predicate('aa.block_time')}}
            {% endif %}
            group by 1,2
      )
      
SELECT * FROM distinct_accounts