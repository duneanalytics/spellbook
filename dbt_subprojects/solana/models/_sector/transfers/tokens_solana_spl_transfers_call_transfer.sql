{{ config(
    schema = 'tokens_solana',
    alias = 'spl_transfers_call_transfer',
    materialized = 'view'
) }}

{%- set models = [
    'tokens_solana_spl_transfers_call_transfer_2020_q4',
    'tokens_solana_spl_transfers_call_transfer_2021_q1',
    'tokens_solana_spl_transfers_call_transfer_2021_q2',
    'tokens_solana_spl_transfers_call_transfer_2021_q3',
    'tokens_solana_spl_transfers_call_transfer_2021_q4',
    'tokens_solana_spl_transfers_call_transfer_2022_q1',
    'tokens_solana_spl_transfers_call_transfer_2022_q2',
    'tokens_solana_spl_transfers_call_transfer_2022_q3',
    'tokens_solana_spl_transfers_call_transfer_2022_q4',
    'tokens_solana_spl_transfers_call_transfer_2023_q1',
    'tokens_solana_spl_transfers_call_transfer_2023_q2',
    'tokens_solana_spl_transfers_call_transfer_2023_q3',
    'tokens_solana_spl_transfers_call_transfer_2023_q4',
    'tokens_solana_spl_transfers_call_transfer_2024_q1',
    'tokens_solana_spl_transfers_call_transfer_2024_q2',
    'tokens_solana_spl_transfers_call_transfer_2024_q3',
    'tokens_solana_spl_transfers_call_transfer_current'
] -%}

{%- for model in models %}
    SELECT *
    FROM {{ ref(model) }}
    {%- if not loop.last %}
    UNION ALL
    {%- endif %}
{%- endfor %}
