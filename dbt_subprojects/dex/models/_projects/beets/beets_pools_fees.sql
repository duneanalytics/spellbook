{{ config(
    schema = 'beets',
    alias = 'pools_fees',
    post_hook='{{ expose_spells(blockchains = \'["sonic"]\',
                                spell_type = "project",
                                spell_name = "beets",
                                contributors = \'["viniabussafi"]\') }}'
    )
}}

{% set balancer_models = [
    ref('beethoven_x_v2_sonic_pools_fees'),
    ref('beethoven_x_v3_sonic_pools_fees')
] %}


SELECT *
FROM (
    {% for model in balancer_models %}
    SELECT
      blockchain
      , version
      , contract_address
      , tx_hash
      , index
      , tx_index
      , block_time
      , block_number
      , swap_fee_percentage
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)