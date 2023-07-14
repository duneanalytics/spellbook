{{ config(
	tags=['legacy'],
	
      alias = alias('flashloans', legacy_model=True)
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
ref('synapse_arbitrum_flashloans_legacy')
, ref('synapse_avalanche_c_flashloans_legacy')
, ref('synapse_bnb_flashloans_legacy')
, ref('synapse_ethereum_flashloans_legacy')
, ref('synapse_fantom_flashloans_legacy')
, ref('synapse_optimism_flashloans_legacy')
, ref('synapse_polygon_flashloans_legacy')
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