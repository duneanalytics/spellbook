{{ config(
    schema = 'elk_finance',
    alias = 'pools'
) }}

{% set elk_finance_models = [
  ref('elk_finance_gnosis_pools')
] %}

SELECT *
FROM (
    {% for pool_model in elk_finance_models %}
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