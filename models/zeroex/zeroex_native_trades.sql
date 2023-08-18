{{ config(tags=['dunesql'],
        alias = alias('native_trades'),
        post_hook='{{ expose_spells(\'["ethereum","arbitrum", "optimism", "polygon","bnb"]\',
                                "project",
                                "zeroex",
                                \'["rantum","bakabhai993"]\') }}'
        )
}}

-- sample dune query for this model


{% set zeroex_models = [
ref('zeroex_native_fills')
] %}


SELECT *
FROM (
    {% for model in zeroex_models %}
    SELECT
      blockchain,
      '0x API'  as project,
      version,
      block_month,
      block_date,
      block_time,
      token_bought_symbol,
      token_sold_symbol,
      token_pair,
      token_bought_amount,
      token_sold_amount,
      token_bought_amount_raw,
      token_sold_amount_raw,
      amount_usd,
      token_bought_address,
      token_sold_address,
      taker,
      maker,
      project_contract_address,
      tx_hash,
      tx_from,
      tx_to,
      evt_index

    FROM {{ model }}
    {% if not loop.last %}

    UNION ALL

    {% endif %}
    {% endfor %}
)
