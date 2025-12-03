{% set blockchain = 'scroll' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'axelar_gateway_deposits',
    materialized = 'view',
    )
}}

{{axelar_gateway_deposits(
    blockchain = blockchain
    , events = source('axelar_' + blockchain, 'axelargateway_evt_tokensent')
    )}}

