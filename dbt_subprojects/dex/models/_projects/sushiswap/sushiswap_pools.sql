{{ config(
    schema = 'sushiswap',
    alias = 'pools'
) }}

{% set sushiswap_models = [
  ref('sushiswap_gnosis_pools')
] %}

SELECT *
FROM (
    {% for pool_model in sushiswap_models %}
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