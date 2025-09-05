{% set blockchain = 'base' %}
{% set l1_standard_bridge = '0x3154Cf16ccdb4C6d922629664174b904d80F2C35' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'ovm_v2_eth_deposits',
    materialized = 'view',
    )
}}

{{ovm_v2_deposits(blockchain = blockchain, l1_standard_bridge = l1_standard_bridge)}}
