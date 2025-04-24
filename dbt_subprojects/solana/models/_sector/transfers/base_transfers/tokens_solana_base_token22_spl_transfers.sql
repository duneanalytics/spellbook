{{ config(
    schema = 'tokens_solana',
    alias = 'base_token22_spl_transfers',
    materialized = 'view'
) }}

{%- set models = [
    'tokens_solana_token22_spl_transfers_current'
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

