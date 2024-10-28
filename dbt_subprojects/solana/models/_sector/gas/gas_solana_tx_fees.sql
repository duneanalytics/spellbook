{{ config(
    schema = 'gas_solana',
    alias = 'tx_fees',
    partition_by = ['block_date', 'block_hour'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_date', 'block_slot', 'tx_index']
) }}

SELECT * FROM 
{% if is_incremental() %}
    ({{ solana_tx_fees_macro() }})
{% else %}
    {{ ref('tx_fees_backfill') }}
{% endif %}
