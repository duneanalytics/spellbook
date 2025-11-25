{% set blockchain = 'polygon' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'allbridge_classic_deposits',
    materialized = 'view',
    )
}}

{{allbridge_classic_deposits(blockchain = blockchain)}}
