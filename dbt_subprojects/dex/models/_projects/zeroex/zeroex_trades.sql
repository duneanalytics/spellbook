{{ config(
    schema = 'zeroex'
        ,alias = 'trades'
        ,post_hook='{{ expose_spells(\'["ethereum","arbitrum", "optimism", "polygon","fantom","avalanche_c","bnb","base","scroll","linea"]\',
                                "project",
                                "zeroex",
                                \'["rantum","bakabhai993"]\') }}'
        )
}}

-- sample dune query for this model
-- https://dune.com/queries/2329953

{% set zeroex_models = [
  ref('zeroex_api_fills_deduped')
] %}


SELECT *
FROM (
    {% for model in zeroex_models %}
    SELECT
      blockchain  as blockchain,
      '0x API'  as project,
      version as version,
      block_month as block_month,
      block_date  as block_date,
      block_time  as block_time,
      maker_symbol as  token_bought_symbol,
      taker_symbol  as  token_sold_symbol,
      token_pair  as token_pair,
      maker_token_amount  as token_bought_amount,
      taker_token_amount  as token_sold_amount,
      maker_token_amount_raw  as token_bought_amount_raw,
      taker_token_amount_raw  as token_sold_amount_raw,
      volume_usd  as amount_usd,
      maker_token  as token_bought_address,
      taker_token as token_sold_address,
      taker  as taker,
      maker  as maker,
      contract_address  as project_contract_address,
      tx_hash  as tx_hash,
      tx_from  as tx_from,
      tx_to  as tx_to,
      trace_address,
      evt_index  as evt_index
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)