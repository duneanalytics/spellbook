{% set blockchain = 'linea' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'symbiosis_v1_deposits',
    materialized = 'view',
    )
}}

{{symbiosis_v1_deposits(
    blockchain = blockchain
    , events = source('symbiosis_' + blockchain, 'portal_evt_synthesizerequest')
    )}}