{{
  config(
        schema = 'raydium_launchlab_v1',
        alias = 'trades',
        materialized = 'incremental',
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "project",
                                    "raydium_launchlab_v1",
                                    \'["krishhh"]\') }}')
}}

{% set project_start_date = '2025-03-17' %}

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
      , account_platform_config
from {{ref('dex_solana_trades')}} as dex_trades
left join {{ref('raydium_launchlab_v1_base_trades')}} as base
      on dex_trades.tx_id = base.tx_id 
      and dex_trades.outer_instruction_index = base.outer_instruction_index
      and COALESCE(dex_trades.inner_instruction_index, 0) = COALESCE(base.inner_instruction_index, 0)
      and dex_trades.tx_index = base.tx_index
      and dex_trades.block_slot = base.block_slot
      and dex_trades.block_time = base.block_time
where dex_trades.project = 'raydium_launchlab' and dex_trades.version = 1
and dex_trades.block_time >= TIMESTAMP '{{project_start_date}}'
{% if is_incremental() -%}
 and {{incremental_predicate('dex_trades.block_time')}}
{% endif -%}