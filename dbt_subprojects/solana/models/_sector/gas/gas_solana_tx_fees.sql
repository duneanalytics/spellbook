{{ config(
    schema = 'gas_solana',
    alias = 'tx_fees',
    materialized = 'view'
    )
}}

{%- set models = [
    'gas_solana_tx_fees_2020_q4'
    , 'gas_solana_tx_fees_2021_q1'
    , 'gas_solana_tx_fees_2021_q2'
    , 'gas_solana_tx_fees_2021_q3'
    , 'gas_solana_tx_fees_2021_q4'
    , 'gas_solana_tx_fees_2022_q1'
    , 'gas_solana_tx_fees_2022_q2'
    , 'gas_solana_tx_fees_2022_q3' 
    , 'gas_solana_tx_fees_2022_q4'
    , 'gas_solana_tx_fees_2023_q1'
    , 'gas_solana_tx_fees_2023_q2'
    , 'gas_solana_tx_fees_2023_q3'
    , 'gas_solana_tx_fees_2023_q4'
    , 'gas_solana_tx_fees_2024_q1'
    , 'gas_solana_tx_fees_2024_q2'
    , 'gas_solana_tx_fees_2024_q3'
    , 'gas_solana_tx_fees_current'
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