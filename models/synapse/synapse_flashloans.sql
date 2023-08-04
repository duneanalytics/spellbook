{{ config(
      alias = alias('flashloans')
      , materialized = 'incremental'
      , file_format = 'delta'
      , incremental_strategy = 'merge'
      , unique_key = ['blockchain', 'tx_hash', 'evt_index']
      , post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "bnb", "ethereum", "fantom", "optimism", "polygon"]\',
                                  "project",
                                  "synapse",
                                  \'["hildobby"]\') }}'
  )
}}

{% set synapse_models = [
ref('synapse_arbitrum_flashloans')
, ref('synapse_avalanche_c_flashloans')
, ref('synapse_bnb_flashloans')
, ref('synapse_ethereum_flashloans')
, ref('synapse_fantom_flashloans')
, ref('synapse_optimism_flashloans')
, ref('synapse_polygon_flashloans')
] %}

SELECT *
FROM (
    {% for synapse_model in synapse_models %}
      SELECT blockchain
      , project
      , version
      , block_time
      , block_number
      , amount
      , amount_usd
      , tx_hash
      , evt_index
      , fee
      , currency_contract
      , currency_symbol
      , recipient
      , contract_address
    FROM {{ synapse_model }}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %} 
)