{{
  config(
        schema = 'solfi_solana',
        alias = 'trades',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["solana"]\', "project", "solfi", \'["ilemi"]\') }}')
}}
select
      blockchain
      , project
      , version
      , block_month
      , block_time
      , block_slot
      , trade_source
      , null as token_bought_symbol
      , null as token_sold_symbol
      , null as token_pair
      , null as token_bought_amount
      , null as token_sold_amount
      , token_bought_amount_raw
      , token_sold_amount_raw
      , null as amount_usd
      , fee_tier
      , null as fee_usd
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
from {{ ref('solfi_solana_base_trades') }}
