{{
  config(
    tags=['dunesql'],
    alias=alias('fm_gas_submission_logs'),
    post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","bnb","ethereum","fantom","gnosis","optimism","polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_arbitrum_fm_gas_submission_logs',
  'chainlink_avalanche_c_fm_gas_submission_logs',
  'chainlink_bnb_fm_gas_submission_logs',
  'chainlink_ethereum_fm_gas_submission_logs',
  'chainlink_fantom_fm_gas_submission_logs',
  'chainlink_gnosis_fm_gas_submission_logs',
  'chainlink_optimism_fm_gas_submission_logs',
  'chainlink_polygon_fm_gas_submission_logs'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
      blockchain,
      block_hash,
      contract_address,
      data,
      topic0,
      topic1,
      topic2,
      topic3,
      tx_hash,
      block_number,
      block_time,
      index,
      tx_index
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)