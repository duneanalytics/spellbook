{{ 
  config(
        schema = 'dex_aggregator_solana',
        alias = 'trades',
        materialized='view',
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "dex_aggregator",
                                    \'["ilemi", "jeff-dude"]\') }}'
    )
}}

{%- set models = [
    'jupiter_solana_aggregator_swaps'
] -%}

{%- for model in models %}
SELECT
    , 
FROM
    {{ ref(model) }}
{%- if not loop.last %}
UNION ALL
{%- endif %}
{%- endfor %}