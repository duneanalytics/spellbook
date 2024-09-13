{{config(
    schema = 'labels',
    alias = 'balancer_cowswap_amm_pools',
    materialized = 'view'
    )
}}

{% set b_cow_amm_models = [
    ref('labels_balancer_cowswap_amm_pools_arbitrum'),
    ref('labels_balancer_cowswap_amm_pools_ethereum'),
    ref('labels_balancer_cowswap_amm_pools_gnosis')
] %}

SELECT *
FROM (
    {% for model in b_cow_amm_models %}
    SELECT
      blockchain,
      address,
      name,
      category,
      contributor,
      source,
      created_at,
      updated_at,
      model_name,
      label_type
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)