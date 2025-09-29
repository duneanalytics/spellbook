{% set blockchain = 'base' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'celer_v1_withdrawals',
    materialized = 'view',
    )
}}

{{celer_v1_withdrawals(blockchain = blockchain)}}