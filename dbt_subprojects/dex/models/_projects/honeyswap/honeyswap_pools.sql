{{ config(
    schema = 'honeyswap',
    alias = 'pools',
    post_hook='{{ expose_spells(\'["gnosis"]\',
                                "project",
                                "honeyswap",
                                \'["mlaegn"]\') }}'
) }}

{% set honeyswap_models = [
  ref('honeyswap_gnosis_pools')
] %}

SELECT *
FROM (
    {% for pool_model in honeyswap_models %}
      SELECT
          blockchain,
          project,
          version,
          pool,
          fee,
          token0,
          token1,
          creation_block_time,
          creation_block_number,
          contract_address
      FROM {{ pool_model }}
      {% if not loop.last %}
         UNION ALL
      {% endif %}
    {% endfor %}
)
;