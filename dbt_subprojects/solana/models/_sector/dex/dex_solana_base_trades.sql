{{
  config(
        schema = 'dex_solana',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        partition_by = ['block_month'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_id', 'outer_instruction_index', 'inner_instruction_index', 'tx_index','block_month']
  )
}}

{% set solana_dexes = [

   ref('raydium_v3_base_trades')
  , ref('raydium_v4_base_trades')
  , ref('raydium_v5_base_trades')
  , ref('raydium_launchlab_v1_base_trades')
  , ref('orca_whirlpool_base_trades')
  , ref('orca_whirlpool_v2_base_trades')
  , ref('phoenix_v1_base_trades')
  , ref('lifinity_v1_base_trades')
  , ref('lifinity_v2_base_trades')
  , ref('meteora_v1_solana_base_trades')
  , ref('meteora_v2_solana_base_trades')
  , ref('goosefx_ssl_v2_solana_base_trades')
  , ref('pumpdotfun_solana_base_trades')
  , ref('pumpswap_solana_base_trades') 
  , ref('stabble_solana_base_trades')
] %}

/*
 intentionally excluded:    , ref('sanctum_router_base_trades')
*/

{% for dex in solana_dexes %}
SELECT
      blockchain
      , project
      , version
      , CAST(date_trunc('month', block_time) AS DATE) as block_month
      , block_time
      , block_slot
      , trade_source
      , token_bought_amount_raw
      , token_sold_amount_raw
      {% if dex == ref('phoenix_v1_base_trades') %}
      , token_bought_decimal_project_specific
      , token_sold_decimal_project_specific
      {% else %}
      , CAST(NULL AS BIGINT) as token_bought_decimal_project_specific
      , CAST(NULL AS BIGINT) as token_sold_decimal_project_specific
      {% endif %}
      , fee_tier
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
FROM
      {{ dex }}
{% if is_incremental() %}
      WHERE {{incremental_predicate('block_time')}}
{% endif %}
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %} 