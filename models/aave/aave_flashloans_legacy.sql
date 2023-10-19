{{ config(
	tags=['legacy'],
	
      alias = alias('flashloans', legacy_model=True)
      , materialized = 'incremental'
      , file_format = 'delta'
      , incremental_strategy = 'merge'
      , unique_key = ['blockchain', 'tx_hash', 'evt_index']
      , post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "optimism", "ethereum", "polygon", "fantom"]\',
                                  "project",
                                  "aave",
                                  \'["hildobby"]\') }}'
  )
}}

{% set aave_models = [
ref('aave_arbitrum_flashloans_legacy')
, ref('aave_avalanche_c_flashloans_legacy')
, ref('aave_optimism_flashloans_legacy')
, ref('aave_ethereum_flashloans_legacy')
, ref('aave_polygon_flashloans_legacy')
, ref('aave_fantom_flashloans_legacy')
] %}

SELECT *
FROM (
    {% for aave_model in aave_models %}
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
    FROM {{ aave_model }}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %} 
)