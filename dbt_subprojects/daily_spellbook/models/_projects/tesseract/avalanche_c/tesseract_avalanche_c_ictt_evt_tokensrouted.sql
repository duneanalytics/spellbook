{% set blockchain = 'avalanche_c' %}
{% set event_name = 'TokensRouted' %}

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

{# topic0filter in the below is keccak(to_utf8('TokensRouted(bytes32,(bytes32,address,address,address,uint256,uint256,uint256,address),uint256)')) #}

{{
    tesseract_ictt_events(
        blockchain = blockchain,
        event_name = event_name,
        topic0_filter = '0x825080857c76cef4a1629c0705a7f8b4ef0282ddcafde0b6715c4fb34b68aaf0'
    )
}}
