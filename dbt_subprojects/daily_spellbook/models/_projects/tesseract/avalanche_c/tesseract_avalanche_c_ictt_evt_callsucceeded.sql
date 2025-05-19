{% set blockchain = 'avalanche_c' %}
{% set event_name = 'CallSucceeded' %}

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

{# topic0filter in the below is keccak(to_utf8('CallSucceeded(address,uint256)')) #}

{{
    tesseract_ictt_events(
        blockchain = blockchain,
        event_name = event_name,
        topic0_filter = '0x104deb555f67e63782bb817bc26c39050894645f9b9f29c4be8ae68d0e8b7ff4',
        topic1_name = 'recipientContract',
        topic1_type = 'address'
    )
}}
