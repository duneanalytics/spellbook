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
      ref('raydium_version_clmm_base_trades')
      , ref('raydium_v4_base_trades')
      , ref('raydium_v5_base_trades')
      , ref('raydium_launchlab_version_1_base_trades')
      , ref('orca_whirlpool_version_v1_base_trades')
      , ref('orca_whirlpool_version_v2_base_trades')
      , ref('phoenix_version_1_base_trades')
      , ref('lifinity_version_v1_base_trades')
      , ref('lifinity_version_v2_base_trades')
      , ref('meteora_version_amm_base_trades')
      , ref('meteora_version_dlmm_base_trades')
      , ref('meteora_version_cpamm_base_trades')
      , ref('meteora_v4_solana_base_trades')
      , ref('goosefx_version_ssl_base_trades')
      , ref('pumpdotfun_version_1_base_trades')
      , ref('pumpswap_version_1_base_trades')
      , ref('pancakeswap_version_v3_base_trades') 
      , ref('stabble_version_1_base_trades')
      , ref('solfi_version_1_base_trades') 
      , ref('zerofi_solana_base_trades')
      , ref('humidifi_solana_base_trades')  
      , ref('tessera_solana_base_trades')
      , ref('goonfi_solana_base_trades')
      , ref('obric_solana_base_trades')
      , ref('aquifer_solana_base_trades')
      , ref('goonfi_v2_solana_base_trades')
      ]
%}

/*
 intentionally excluded:    , ref('sanctum_router_base_trades')
*/

{% for dex in solana_dexes %}
SELECT
      blockchain
      , project
      , version
      , version_name
      , CAST(date_trunc('month', block_time) AS DATE) as block_month
      , CAST(date_trunc('day', block_time) AS DATE) as block_date
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
WHERE
      1=1
      {% if is_incremental() -%}
      AND {{incremental_predicate('block_time')}}
      {% endif -%}
{% if not loop.last -%}
UNION ALL
{% endif -%}
{% endfor %}