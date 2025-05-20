{% set blockchain = 'avalanche_c' %}
{% set event_name = 'TokensAndCallRouted' %}

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

{# topic0filter in the below is keccak(to_utf8('TokensAndCallRouted(bytes32,(bytes32,address,address,bytes,uint256,uint256,address,address,address,uint256,uint256),uint256)')) #}

{{
    tesseract_ictt_events(
        blockchain = blockchain,
        event_name = event_name,
        topic0_filter = '0x42eff9005856e3c586b096d67211a566dc926052119fd7cc08023c70937ecb30'
    )
}}
