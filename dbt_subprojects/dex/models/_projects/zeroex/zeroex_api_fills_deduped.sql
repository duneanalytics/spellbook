{{ config(
     schema = 'zeroex'
        , alias = 'api_fills_deduped'
        , post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "bnb", "celo", "ethereum", "fantom", "optimism", "polygon","scroll", "linea","blast","mantle","mode"]\',
                                "project",
                                "zeroex",
                                \'["rantum","bakabhai993"]\') }}'
        )
}}


{% set v1_models = [  
  ref('zeroex_arbitrum_api_fills_deduped')
  ,ref('zeroex_avalanche_c_api_fills_deduped')
  ,ref('zeroex_base_api_fills_deduped')
  ,ref('zeroex_celo_api_fills_deduped')
  ,ref('zeroex_ethereum_api_fills_deduped')
  ,ref('zeroex_fantom_api_fills_deduped')
  ,ref('zeroex_optimism_api_fills_deduped')
  ,ref('zeroex_polygon_api_fills_deduped')
  ,ref('zeroex_bnb_api_fills_deduped')
] %}

{% set v2_models = [  
  ref('zeroex_v2_ethereum_trades')
  ,ref('zeroex_v2_base_trades')
  ,ref('zeroex_v2_polygon_trades')
  ,ref('zeroex_v2_optimism_trades')
  ,ref('zeroex_v2_bnb_trades')
  ,ref('zeroex_v2_avalanche_c_trades')
  ,ref('zeroex_v2_arbitrum_trades')
  ,ref('zeroex_v2_scroll_trades')
  ,ref('zeroex_v2_linea_trades')
  ,ref('zeroex_v2_blast_trades')
  ,ref('zeroex_v2_mantle_trades')
  ,ref('zeroex_v2_mode_trades')
] %}


SELECT *
FROM (
    {% for model in v1_models %}
    SELECT
      blockchain
      ,version
      ,block_month
      ,block_date
      ,block_time
      ,maker_symbol
      ,taker_symbol
      ,token_pair
      ,maker_token_amount
      ,taker_token_amount
      ,maker_token_amount_raw
      ,taker_token_amount_raw
      ,volume_usd
      ,maker_token
      ,taker_token
      ,taker
      ,maker
      ,contract_address
      ,tx_hash
      ,tx_from
      ,tx_to
      ,trace_address
      ,evt_index
      ,affiliate_address
      ,null as zid 
      ,type 
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)

UNION ALL 

SELECT *
FROM (
    {% for model in v2_models %}
    SELECT
      blockchain
      ,version
      ,block_month
      ,block_date
      ,block_time
      ,maker_symbol
      ,taker_symbol
      ,token_pair
      ,maker_token_amount
      ,taker_token_amount
      ,maker_token_amount_raw
      ,taker_token_amount_raw
      ,volume_usd
      ,maker_token
      ,taker_token
      ,taker
      ,maker
      ,contract_address
      ,tx_hash
      ,tx_from
      ,tx_to
      ,trace_address
      ,evt_index
      ,tag as affiliate_address 
      ,zid
      ,type
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)