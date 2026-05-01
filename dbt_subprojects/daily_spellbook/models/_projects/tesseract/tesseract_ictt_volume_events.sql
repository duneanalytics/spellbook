{%- set alias = 'ictt_volume_events' -%}

{{
    config(
	    schema = 'tesseract',
        alias = alias,
        materialized = 'view'
        , post_hook='{{ hide_spells() }}'
        )
}}

{%- set tesseract_models = [
    ref('tesseract_avalanche_c_' + alias)
] -%}

SELECT *
FROM (
    {%- for model in tesseract_models %}
    SELECT
        blockchain
        , contract_address
        , evt_tx_hash
        , evt_index
        , evt_block_time
        , evt_block_number
        , evt_block_date
        , evt_name
        , amount
        , used_tesseract
        , source_blockchain_id
        , destination_blockchain_id
    FROM {{ model }}
    {%- if not loop.last %}
    UNION ALL
    {%- endif -%}
    {%- endfor %}
)