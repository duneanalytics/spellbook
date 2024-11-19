 {{
  config(
        schema = 'stablecoins',
        alias = 'solana_balances',
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['token_mint_address', 'token_balance_owner', 'day'],
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "solana_utils",
                                    "stablecoins",
                                    \'["synthquest"]\') }}')
}}

with
      stablecoin_tokens as (
            select
                  blockchain, symbol, address, decimals
            from 
                  {{ source('tokens_solana', 'stablecoins')}}
)

, updated_balances as (
            select 
                  day
                  , bal.token_balance_owner
                  , sd.symbol
                  , sum(coalesce(token_balance,0)) as token_balance 
                  , token_mint_address
            from {{ ref('solana_utils_daily_balances') }}
            inner join (select address, symbol from stable_definitions) sd on sd.address = bal.token_mint_address
            where day > cast('2024-10-26' as timestamp)
            group by 1,2,3,5
      )

, time_table as (
    select distinct day, token_balance_owner, token_mint_address, symbol
    FROM unnest(SEQUENCE(CAST('2021-01-30 00:00' AS TIMESTAMP), current_date, INTERVAL '1' day)) AS t(day)
    cross join (select distinct token_balance_owner, token_mint_address, symbol from balance_base)
      )

, final as (

      select tt.day, tt.token_balance_owner
            , tt.symbol
            , COALESCE(nf.token_balance, last_value(nf.token_balance) IGNORE NULLS OVER (PARTITION BY tt.token_balance_owner, tt.token_mint_address ORDER BY nf.block_time)) AS token_balance
            , tt.token_mint_address
      
      from time_table tt
      left join balance_base nf on nf.day = tt.day and nf.token_balance_owner = tt.token_balance_owner and nf.token_mint_address = tt.token_mint_address
      {% if is_incremental() %}
      WHERE {{incremental_predicate('tt.day')}} and tt.day >= cast('2024-10-26' as timestamp)
      {% endif %}
      order by tt.day desc, token_balance desc
)

select day, token_balance_owner, symbol, token_balance, token_mint_address
from final
order by day desc