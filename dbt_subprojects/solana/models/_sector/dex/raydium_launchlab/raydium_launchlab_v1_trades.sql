{{
  config(
        schema = 'raydium_launchlab_v1',
        alias = 'trades',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "project",
                                    "raydium_launchlab_v1",
                                    \'["krishhh"]\') }}')
}}

select
      dex_trades.blockchain
      , dex_trades.project
      , dex_trades.version
      , dex_trades.block_month
      , dex_trades.block_date
      , dex_trades.block_time
      , dex_trades.block_slot
      , dex_trades.trade_source
      , dex_trades.token_bought_symbol
      , dex_trades.token_sold_symbol
      , dex_trades.token_pair
      , dex_trades.token_bought_amount
      , dex_trades.token_sold_amount
      , dex_trades.token_bought_amount_raw
      , dex_trades.token_sold_amount_raw
      , dex_trades.amount_usd
      , dex_trades.fee_tier
      , dex_trades.fee_usd
      , dex_trades.token_bought_mint_address
      , dex_trades.token_sold_mint_address
      , dex_trades.token_bought_vault
      , dex_trades.token_sold_vault
      , dex_trades.project_program_id
      , dex_trades.project_main_id
      , dex_trades.trader_id
      , dex_trades.tx_id
      , dex_trades.outer_instruction_index
      , dex_trades.inner_instruction_index
      , dex_trades.tx_index
      , base.platform_name
      , base.platform_params
from {{ref('dex_solana_trades')}} as dex_trades
left join {{ref('raydium_launchlab_v1_base_trades')}} as base
      on dex_trades.tx_id = base.tx_id 
      and dex_trades.outer_instruction_index = base.outer_instruction_index
      and COALESCE(dex_trades.inner_instruction_index, 0) = COALESCE(base.inner_instruction_index, 0)
      and dex_trades.tx_index = base.tx_index
      and dex_trades.block_slot = base.block_slot
      and dex_trades.block_time = base.block_time
where dex_trades.project = 'raydium_launchlab' and dex_trades.version = 1
