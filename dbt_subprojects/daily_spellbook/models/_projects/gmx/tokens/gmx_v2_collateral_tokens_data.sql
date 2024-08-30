{{ config(
        schema='gmx_v2',
        alias = 'collateral_tokens_data',
        )
}}

{%- set chains = [
    'arbitrum',
    'avalanche_c',
] -%}

{%- for chain in chains -%}
SELECT 
    '{{ chain }}' AS "blockchain",
    *
FROM {{ ref('gmx_v2_' ~ chain ~ '_collateral_tokens_data') }}
{% if not loop.last %}
UNION ALL
{% endif %}
{%- endfor -%}