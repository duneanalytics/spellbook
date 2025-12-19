{{ 
  config(
        schema = 'jupiter_solana',
        alias = 'aggregator_swaps',
        materialized='view',
        post_hook='{{ expose_spells(\'["jupiter"]\',
                                    "project",
                                    "jupiter_solana",
                                    \'["ilemi"]\') }}'
    )
}}

{%- set models = [
    'jupiter_v4_solana_aggregator_swaps'
    , 'jupiter_v5_solana_aggregator_swaps'
    , 'jupiter_v6_solana_aggregator_swaps'
] -%}

{%- for model in models %}
SELECT
    *
FROM
    {{ ref(model) }}
{%- if not loop.last %}
UNION ALL
{%- endif %}
{%- endfor %}