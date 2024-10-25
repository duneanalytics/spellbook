{{ config(
    schema = 'gas_solana',
    alias = 'vote_fees',
    partition_by = ['block_date', 'block_hour'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_date', 'block_slot', 'tx_index']
) }}

{% if is_incremental() %}
    {{ solana_vote_fees_template(
        "'" ~ var('DBT_INCREMENTAL_FROM') ~ "'", 
        "'" ~ var('DBT_INCREMENTAL_TO') ~ "'"
    ) }}
{% else %}
    SELECT * FROM {{ ref('gas_solana_vote_fees_q1_2020') }}
{% endif %}
