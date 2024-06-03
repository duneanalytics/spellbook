{{
  config(
    
    alias='ccip_send_requested_logs_v1_2',
    post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "bnb", "ethereum", "optimism", "polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_arbitrum_ccip_send_requested_logs_v1_2',
  'chainlink_avalanche_c_ccip_send_requested_logs_v1_2',
  'chainlink_base_ccip_send_requested_logs_v1_2',
  'chainlink_bnb_ccip_send_requested_logs_v1_2',
  'chainlink_ethereum_ccip_send_requested_logs_v1_2',
  'chainlink_optimism_ccip_send_requested_logs_v1_2',
  'chainlink_polygon_ccip_send_requested_logs_v1_2'
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
        tx_index,
        tx_from,
        fee_token_amount,
        origin_selector,
        fee_token,
        destination_selector,
        destination_blockchain
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
