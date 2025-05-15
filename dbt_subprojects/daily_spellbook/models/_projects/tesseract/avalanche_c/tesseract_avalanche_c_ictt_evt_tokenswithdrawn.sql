{% set blockchain = 'avalanche_c' %}
{% set event_name = 'TokensWithdrawn' %}

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

{# topic0filter in the below is keccak(to_utf8('TokensWithdrawn(address,uint256)')) #}

{{
    tesseract_ictt_events(
        blockchain = blockchain,
        event_name = event_name,
        topic0_filter = '0x6352c5382c4a4578e712449ca65e83cdb392d045dfcf1cad9615189db2da244b',
        topic1_name = 'recipient',
        topic1_type = 'address'
    )
}}
