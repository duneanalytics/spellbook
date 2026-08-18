{{ config(
    schema = 'banana_gun_solana',
    alias = 'fee_payments_raw',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'tx_id', 'fee_receiver']
   )
}}

{% set project_start_date = '2024-01-08' %}
{% set ci_start_date = '2026-08-11' %}
{% set fee_receiver_1 = '8r2hZoDfk5hDWJ1sDujAi2Qr45ZyZw5EQxAXiMZWLKh2' %}
{% set fee_receiver_2 = 'Cj297UauzMX64FU9dKJZRUBWszJ7tEWpVheasq4CfATV' %}
{% set fee_receiver_3 = 'HKMh8nV3ysSofRi23LsfVGLGQKB415QAEfZT96kCcVj4' %}
{% set fee_receiver_4 = '7tQiiBdKoScWQkB1RmVuML7DBGnR31cuKPEtMM7Vy5SA' %}
{% set fee_receiver_5 = '4BBNEVRgrxVKv9f7pMNE788XM1tt379X9vNjpDH2KCL7' %}
{% set fee_receiver_6 = '47hEzz83VFR23rLTEeVm9A7eFzjJwjvdupPPmX3cePqF' %}
{% set fee_receiver_7 = 'EMbqD9Y9jLXEa3RbCR8AsEW1kVa3EiJgDLVgvKh4qNFP' %}
{% set fee_receiver_8 = 'Lk693UiTzQC4vobasRS1QGcYA9D6RGYLjHp1bWreQtM' %}
{% set wsol_token = 'So11111111111111111111111111111111111111112' %}

select
    block_time,
    cast(date_trunc('month', block_time) as date) as block_month,
    'Banana Gun' as bot,
    'solana' as blockchain,
    balance_change / 1e9 as amount,
    '{{ wsol_token }}' as token_address,
    address as fee_receiver,
    signed as fee_receiver_signed,
    tx_id
from {{ source('solana', 'account_activity') }}
where
    tx_success
    and balance_change > 0
    and address in (
        '{{ fee_receiver_1 }}',
        '{{ fee_receiver_2 }}',
        '{{ fee_receiver_3 }}',
        '{{ fee_receiver_4 }}',
        '{{ fee_receiver_5 }}',
        '{{ fee_receiver_6 }}',
        '{{ fee_receiver_7 }}',
        '{{ fee_receiver_8 }}'
    )
    and address_prefix in (
        '{{ fee_receiver_1[:2] }}',
        '{{ fee_receiver_2[:2] }}',
        '{{ fee_receiver_3[:2] }}',
        '{{ fee_receiver_4[:2] }}',
        '{{ fee_receiver_5[:2] }}',
        '{{ fee_receiver_6[:2] }}',
        '{{ fee_receiver_7[:2] }}',
        '{{ fee_receiver_8[:2] }}'
    )
    and tx_id !=
        'AT915GhHaLdGsdFkywx2uE6jqSXeyTauveYH2BQqWMyptGhUtjE6dcdr74ErELg79VY9apZ9Egiyc1VtA6Ddykb'
    {% if is_incremental() %}
        and {{ incremental_predicate('block_time') }}
    {% else %}
        {# Temporary 7-day CI window; restore project_start_date for full-history validation. #}
        and block_time >= timestamp '{{ ci_start_date }}'
    {% endif %}
