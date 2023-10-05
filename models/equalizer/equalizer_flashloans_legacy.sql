{{ config(
	tags=['legacy'],
	
      alias = alias('flashloans', legacy_model=True)
      , materialized = 'incremental'
      , file_format = 'delta'
      , incremental_strategy = 'merge'
      , unique_key = ['blockchain', 'tx_hash', 'evt_index']
      , post_hook='{{ expose_spells(\'["bnb", "ethereum", "optimism", "polygon"]\',
                                  "project",
                                  "equalizer",
                                  \'["hildobby"]\') }}'
  )
}}

{% set equalizer_models = [
ref('equalizer_bnb_flashloans_legacy')
, ref('equalizer_ethereum_flashloans_legacy')
, ref('equalizer_optimism_flashloans_legacy')
, ref('equalizer_polygon_flashloans_legacy')
] %}

SELECT *
FROM (
    {% for equalizer_model in equalizer_models %}
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
    FROM {{ equalizer_model }}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %} 
)