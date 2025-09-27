{% set blockchain = 'base' %}
{% set l1_portal = '0x49048044D57e1C92A77f79988d21Fa8fAF74E97e' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'ovm_v2_eth_withdrawals',
    materialized = 'view',
    )
}}

{{ovm_v2_withdrawals(blockchain = blockchain, l1_portal = l1_portal)}}
