{{ config(
        schema='gmx_v2',
        alias = 'order_cancelled',
        post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c"]\',
                                    "project",
                                    "gmx",
                                    \'["ai_data_master","gmx-io"]\') }}'
        )
}}

{%- set chains = [
    'arbitrum',
    'avalanche_c',
] -%}

{%- for chain in chains -%}
SELECT *
FROM {{ ref('gmx_v2_' ~ chain ~ '_order_cancelled') }}
{% if not loop.last %}
UNION ALL
{% endif %}
{%- endfor -%}