{% set blockchain = 'base' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'across_v2_withdrawals',
    materialized = 'view',
    )
}}

{{across_v2_withdrawals(blockchain = blockchain)}}