 {{
  config(
        schema = 'solana_utils',
        tags = ['dunesql'],
        alias = alias('token_accounts'),
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy='merge',
        unique_key = ['token_mint_address', 'address'],
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "solana_utils",
                                    \'["ilemi"]\') }}')
}}

WITH 
      distinct_accounts as (
            SELECT
                  distinct 
                  token_mint_address
                  , address 
            FROM {{ source('solana','account_activity') }}
            WHERE token_mint_address is not null
            {% if is_incremental() %}
            AND block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
      )
      
SELECT *, now() as updated_at FROM distinct_accounts