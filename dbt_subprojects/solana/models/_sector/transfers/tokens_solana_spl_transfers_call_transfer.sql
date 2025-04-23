{{ config(
    schema = 'tokens_solana',
    alias = 'spl_transfers_call_transfer',
    materialized = 'view'
) }}

{%- set models = [
    'tokens_solana_spl_transfers_call_transfer_current'
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
