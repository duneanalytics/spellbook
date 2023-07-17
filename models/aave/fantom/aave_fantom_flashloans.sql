{{ config(
      alias = alias('flashloans')
      , materialized = 'incremental'
      , file_format = 'delta'
      , incremental_strategy = 'merge'
      , unique_key = ['tx_hash', 'evt_index']
      , post_hook='{{ expose_spells(\'["fantom"]\',
                                  "project",
                                  "aave",
                                  \'["hildobby"]\') }}'
  )
}}

{% set aave_models = [
ref('aave_v3_fantom_flashloans')
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