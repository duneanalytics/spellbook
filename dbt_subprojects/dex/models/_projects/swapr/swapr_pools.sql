{{ config(
    schema = 'swapr',
    alias = 'pools',
    post_hook='{{ expose_spells(\'["gnosis"]\', "project", "swapr", \'["mlaegn"]\') }}'
) }}

{% set swapr_models = [
  ref('swapr_gnosis_pools'),
  ref('swapr_v3_gnosis_pools')
] %}

SELECT *
FROM (
    {% for model in swapr_models %}
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
      FROM {{ model }}
      {% if not loop.last %}
         UNION ALL
      {% endif %}
    {% endfor %}
)
;