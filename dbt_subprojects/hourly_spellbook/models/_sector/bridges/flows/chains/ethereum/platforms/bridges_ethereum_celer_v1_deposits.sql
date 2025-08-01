{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'celer_v1_deposits',
    materialized = 'view',
    )
}}

{{celer_v1_deposits(blockchain = blockchain)}}