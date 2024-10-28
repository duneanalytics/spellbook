{{ config(
    schema = 'gas_solana',
    alias = 'tx_fees',
    tags = ['prod_exclude'],
    partition_by = ['block_date', 'block_hour'],
    materialized = 'static',
    file_format = 'delta',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_date', 'block_slot', 'tx_index']
) }}


{% if is_incremental() %}
    {{ solana_tx_fees_macro() }}
{% else %}
    -- For backfill, reference all quarterly tables
    SELECT * FROM {{ ref('gas_solana_tx_fees_q4_2020') }}
    UNION ALL
    SELECT * FROM {{ ref('gas_solana_tx_fees_q1_2021') }}
{% endif %}
