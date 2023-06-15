{{ config(
      alias='flashloans'
      , materialized = 'incremental'
      , file_format = 'delta'
      , incremental_strategy = 'merge'
      , unique_key = ['blockchain', 'tx_hash', 'evt_index']
      , post_hook='{{ expose_spells(\'["ethereum", "optimism", "arbitrum", "fantom"]\',
                                  "project",
                                  "saddle_finance",
                                  \'["hildobby"]\') }}'
  )
}}

{% set saddle_finance_models = [
ref('saddle_finance_arbitrum_flashloans')
, ref('saddle_finance_ethereum_flashloans')
, ref('saddle_finance_optimism_flashloans')
, ref('saddle_finance_fantom_flashloans')
] %}

SELECT *
FROM (
    {% for saddle_finance_model in saddle_finance_models %}
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
    FROM {{ saddle_finance_model }}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %} 
)