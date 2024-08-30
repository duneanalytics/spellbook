{{ config(
        schema='gmx_v2',
        alias = 'erc20',
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
FROM {{ ref('gmx_v2_' ~ chain ~ '_erc20') }}
{% if not loop.last %}
UNION ALL
{% endif %}
{%- endfor -%}