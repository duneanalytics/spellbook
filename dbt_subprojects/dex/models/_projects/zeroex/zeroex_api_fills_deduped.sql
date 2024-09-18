{{ config(
     schema = 'zeroex'
        , alias = 'api_fills_deduped'
        , post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "bnb", "celo", "ethereum", "fantom", "optimism", "polygon","scroll", "linea"]\',
                                "project",
                                "zeroex",
                                \'["rantum","bakabhai993"]\') }}'
        )
}}


{% set zeroex_models = [  
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

{% set settler_models = [  
  ref('zeroex_ethereum_settler_trades')
  ,ref('zeroex_base_settler_trades')
  ,ref('zeroex_polygon_settler_trades')
  ,ref('zeroex_optimism_settler_trades')
  ,ref('zeroex_bnb_settler_trades')
  ,ref('zeroex_avalanche_c_settler_trades')
  ,ref('zeroex_arbitrum_settler_trades')
  ,ref('zeroex_scroll_settler_trades')
  ,ref('zeroex_linea_settler_trades')
] %}


SELECT *
FROM (
    {% for model in zeroex_models %}
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
    {% for model in settler_models %}
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