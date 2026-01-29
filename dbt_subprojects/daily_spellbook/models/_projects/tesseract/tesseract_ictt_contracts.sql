{%- set alias = 'ictt_contracts' -%}

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
        , is_token_home
        , creation_block_time
        , creation_block_number
        , creation_tx_hash
    FROM {{ model }}
    {%- if not loop.last %}
    UNION ALL
    {%- endif -%}
    {%- endfor %}
)