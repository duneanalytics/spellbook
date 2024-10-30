{{ config(
    schema = 'tokens_solana',
    alias = 'token22_spl_transfers',
    materialized = 'view'
) }}

{%- set models = [
    'tokens_solana_token22_spl_transfers_2020_q4',
    'tokens_solana_token22_spl_transfers_2021_q1',
    'tokens_solana_token22_spl_transfers_2021_q2',
    'tokens_solana_token22_spl_transfers_2021_q3',
    'tokens_solana_token22_spl_transfers_2021_q4',
    'tokens_solana_token22_spl_transfers_2022_q1',
    'tokens_solana_token22_spl_transfers_2022_q2',
    'tokens_solana_token22_spl_transfers_2022_q3',
    'tokens_solana_token22_spl_transfers_2022_q4',
    'tokens_solana_token22_spl_transfers_2023_q1',
    'tokens_solana_token22_spl_transfers_2023_q2',
    'tokens_solana_token22_spl_transfers_2023_q3',
    'tokens_solana_token22_spl_transfers_2023_q4',
    'tokens_solana_token22_spl_transfers_2024_q1',
    'tokens_solana_token22_spl_transfers_2024_q2',
    'tokens_solana_token22_spl_transfers_2024_q3',
    'tokens_solana_token22_spl_transfers_current'
] -%}

{%- for model in models %}
    SELECT *
    FROM {{ ref(model) }}
    {%- if not loop.last %}
    UNION ALL
    {%- endif %}
{%- endfor %}

