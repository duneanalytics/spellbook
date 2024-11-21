{{
  config(
    
    alias='ccip_send_traces',
    post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "bnb", "ethereum", "optimism", "polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_arbitrum_ccip_send_traces',
  'chainlink_avalanche_c_ccip_send_traces',
  'chainlink_base_ccip_send_traces',
  'chainlink_bnb_ccip_send_traces',
  'chainlink_ethereum_ccip_send_traces',
  'chainlink_optimism_ccip_send_traces',
  'chainlink_polygon_ccip_send_traces'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
        blockchain,
        block_hash,
        block_number,
        block_time,
        tx_hash,
        "from",
        "to",
        input,
        "output",
        tx_success,
        value,
        chain_selector,
        destination       
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
