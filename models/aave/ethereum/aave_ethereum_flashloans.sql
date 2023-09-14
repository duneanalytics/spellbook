{{ config(
      tags = ['dunesql']
      , partition_by = ['block_month']
      , alias = alias('flashloans')
      , materialized = 'incremental'
      , file_format = 'delta'
      , incremental_strategy = 'merge'
      , unique_key = ['tx_hash', 'evt_index']
      , post_hook='{{ expose_spells(\'["ethereum"]\',
                                  "project",
                                  "aave",
                                  \'["hildobby"]\') }}'
  )
}}

{% set aave_models = [
ref('aave_v1_ethereum_flashloans')
, ref('aave_v2_ethereum_flashloans')
, ref('aave_v3_ethereum_flashloans')
] %}

SELECT *
FROM (
    {% for aave_model in aave_models %}
      SELECT blockchain
      , project
      , version
      , block_month
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
    WHERE block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %} 
)