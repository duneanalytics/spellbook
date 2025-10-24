{% set blockchain = 'ronin' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'ronin_native_v1_withdrawals',
    materialized = 'view',
    )
}}

