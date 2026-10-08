{{
    config(
        materialized='incremental',
        schema='safe_ink',
        alias= 'transactions',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'tx_hash', 'trace_address'],
        file_format ='delta',
        incremental_strategy='merge'
        , post_hook='{{ hide_spells() }}'
    )
}}

{% if target.name == 'ci' %}
{{ safe_transactions('ink', (run_started_at - modules.datetime.timedelta(days=7)).strftime('%Y-%m-%d')) }}
{% else %}
{{ safe_transactions('ink', '2023-07-01') }}
{% endif %}
