 {{
  config(
        alias='solana_utils',
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
            {% if is_incremental() %}
            WHERE block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
      )
      
SELECT *, now() as updated_at FROM distinct_accounts