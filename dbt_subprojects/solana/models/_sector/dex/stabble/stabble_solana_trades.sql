 {{
  config(
        schema = 'stabble_solana',
        alias = 'trades',
        materialized = 'view')
}}


select
  blockchain
  , project
  , version
  , block_month
  , block_date
  , block_time
  , block_slot
  , trade_source
  , token_bought_symbol
  , token_sold_symbol
  , token_pair
  , token_bought_amount
  , token_sold_amount
  , token_bought_amount_raw
  , token_sold_amount_raw
  , amount_usd
  , fee_tier
  , fee_usd
  , token_bought_mint_address
  , token_sold_mint_address
  , token_bought_vault
  , token_sold_vault
  , project_program_id
  , project_main_id
  , trader_id
  , tx_id
  , outer_instruction_index
  , inner_instruction_index
  , tx_index
from {{ref('dex_solana_trades')}}
where project = 'stabble'

--trigger run