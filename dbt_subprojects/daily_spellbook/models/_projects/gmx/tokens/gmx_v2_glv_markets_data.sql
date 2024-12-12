{{ config(
        schema='gmx_v2',
        alias = 'glv_markets_data',
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
SELECT
    '{{ chain }}' AS "blockchain",
    glv,
    glv_market_name,
    market_token_symbol,
    market_token_decimals,
    long_token_symbol,
    long_token_decimals,
    short_token_symbol,
    short_token_decimals  
FROM {{ ref('gmx_v2_' ~ chain ~ '_glv_markets_data') }}
{% if not loop.last %}
UNION ALL
{% endif %}
{%- endfor -%}

