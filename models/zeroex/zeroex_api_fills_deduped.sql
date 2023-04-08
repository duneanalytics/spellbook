{{ config(
        alias ='api_fills_deduped',
        post_hook='{{ expose_spells(\'["ethereum","arbitrum", "optimism", "polygon","fantom","avalanche_c"]\',
                                "project",
                                "zeroex",
                                \'["rantum","bakabhai993"]\') }}'
        )
}}

{% set zeroex_models = [  
ref('zeroex_arbitrum_api_fills_deduped')
,ref('zeroex_avalanche_c_api_fills_deduped')
,ref('zeroex_ethereum_api_fills_deduped')
,ref('zeroex_fantom_api_fills_deduped')
,ref('zeroex_optimism_api_fills_deduped')
,ref('zeroex_polygon_api_fills_deduped')
] %}


SELECT *
FROM (
    {% for model in zeroex_models %}
    SELECT
    volume_usd  as amount_usd,
      block_date  as block_date,
      block_time  as block_time,
      blockchain  as blockchain,
      evt_index  as evt_index, 
      maker  as maker, 
      '0x API'  as project,
      contract_address  as project_contract_address,
      taker  as taker, 
      maker_symbol as  token_bought_symbol, 
      token_pair  as token_pair,
      taker_token as token_sold_address,
      taker_token_amount  as token_sold_amount,
      taker_symbol  as  token_sold_symbol,
      CAST(ARRAY() as array<bigint>) as trace_address,
      tx_from  as tx_from,
      tx_hash  as tx_hash,
      tx_to  as tx_to,
      taker_token_amount_raw  as token_sold_amount_raw,
      maker_token  as token_bought_address, 
      maker_token_amount  as token_bought_amount, 
      cast(null as varchar(10)) version,
      maker_token_amount_raw  as token_bought_amount_raw
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;
