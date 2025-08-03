{% set blockchain = 'avalanche_c' %}
{% set event_name = 'TokensSent' %}

{{
    config(
        schema = 'tesseract_' + blockchain,
        alias = 'ictt_evt_' + event_name | lower,
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['evt_block_number', 'evt_index']
    )
}}

{# topic0filter in the below is keccak(to_utf8('TokensSent(bytes32,address,(bytes32,address,address,address,uint256,uint256,uint256,address),uint256)')) #}

{{
    tesseract_ictt_events(
        blockchain = blockchain,
        event_name = event_name,
        topic0_filter = '0x93f19bf1ec58a15dc643b37e7e18a1c13e85e06cd11929e283154691ace9fb52'
    )
}}
