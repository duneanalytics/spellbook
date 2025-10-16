{{
  config(
    schema = 'meteora_version_amm',
    alias = 'base_trades',
    materialized = 'view'
  )
}}

SELECT
    blockchain,
    project,
    version,
    'amm' as version_name,
    block_month,
    block_time,
    block_slot,
    trade_source,
    token_bought_amount_raw,
    token_sold_amount_raw,
    fee_tier,
    token_sold_mint_address,
    token_bought_mint_address,
    token_sold_vault,
    token_bought_vault,
    project_program_id,
    project_main_id,
    trader_id,
    tx_id,
    outer_instruction_index,
    inner_instruction_index,
    tx_index
FROM {{ ref('meteora_v1_solana_base_trades') }}
