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
      , cast(null as varchar) as token_bought_symbol
      , cast(null as varchar) as token_sold_symbol
      , cast(null as varchar) as token_pair
      , cast(null as double) as token_bought_amount
      , cast(null as double) as token_sold_amount
      , token_bought_amount_raw
      , token_sold_amount_raw
      , cast(null as double) as amount_usd
      , fee_tier
      , cast(null as double) as fee_usd
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
from {{ ref('solfi_base_trades') }}
