 {{
  config(
        
        schema = 'dex_solana',
        alias = 'trades',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "project",
                                    "dex",
                                    \'["ilemi"]\') }}')
}}

{% set solana_dexes = [
    ref('orca_whirlpool_trades')
    , ref('raydium_v3_trades')
    , ref('raydium_v4_trades')
    , ref('phoenix_v1_trades')
    , ref('lifinity_v1_trades')
    , ref('lifinity_v2_trades')
    , ref('meteora_v1_solana_trades')
    , ref('meteora_v2_solana_trades')
    , ref('goosefx_ssl_v2_solana_trades')   
] %}

{% for dex in solana_dexes %}
SELECT
      blockchain
      , project
      , version
      , block_time
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
      , trader_id
      , tx_id
      , outer_instruction_index
      , inner_instruction_index
      , tx_index
FROM {{ dex }}

{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}