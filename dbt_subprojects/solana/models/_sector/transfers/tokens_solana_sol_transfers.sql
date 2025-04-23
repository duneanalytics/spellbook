{{ config(
    schema = 'tokens_solana',
    alias = 'sol_transfers',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["solana"]\', "sector", "tokens", \'["0xBoxer"]\') }}'
) }}

{%- set models = [
    'tokens_solana_sol_transfers_current'
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

