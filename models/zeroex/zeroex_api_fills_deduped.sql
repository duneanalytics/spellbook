{{ config(
        alias ='api_fills_deduped',
        post_hook='{{ expose_spells(\'["ethereum","arbitrum", "optimism", "polygon","fantom","avalanche_c"]\',
                                "project",
                                "zeroex",
                                \'["rantumBits","bakabhai993"]\') }}'
        )
}}

{% set zeroex_models = [
'zeroex_arbitrum_api_fills_deduped'
,'zeroex_avalanche_c_api_fills_deduped'
,'zeroex_ethereum_api_fills_deduped'
,'zeroex_fantom_api_fills_deduped'
,'zeroex_optimism_api_fills_deduped'
,'zeroex_polygon_api_fills_deduped'
] %}


SELECT *
FROM (
    {% for dex_model in zeroex_models %}
    SELECT
    volume_usd  as amount_usd,
      block_date  as block_date,
      block_time  as block_time,
      blockchain  as blockchain,
      evt_index  as evt_index, 
      maker  as maker, 
      '0x API'  as project,
      --contract_address  as project_contract_address,
      taker  as taker, 
      cast(maker_symbol as varbinary)  as  token_bought_symbol, 
      token_pair  as token_pair,
      cast(taker_token as varbinary) as token_sold_address,
      taker_token_amount  as token_sold_amount,
      cast(taker_symbol as varbinary)  as  token_sold_symbol,
      null  as trace_address,
      tx_from  as tx_from,
      tx_hash  as tx_hash,
      tx_to  as tx_to,
      cast(taker_token_amount_raw as double)  as token_sold_amount_raw,
      cast(maker_token as varbinary) as token_bought_address, 
      maker_token_amount  as token_bought_amount, 
      null  as version,
      cast(maker_token_amount_raw as decimal)  as token_bought_amount_raw
    FROM {{ ref(dex_model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;