{%- set alias = 'ictt_pairs' -%}

{{
    config(
	    schema = 'tesseract',
        alias = alias,
        materialized = 'view',
        post_hook='{{ expose_spells(
                      blockchains = \'["avalanche_c"]\',
                      spell_type = "project",
                      spell_name = "tesseract",
                      contributors = \'["angus_1"]\') }}'
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
        , token_home_blockchain_id
        , token_home_address
        , token_remote_blockchain_id
        , token_remote_address
        , block_time
        , block_number
    FROM {{ model }}
    {%- if not loop.last %}
    UNION ALL
    {%- endif -%}
    {%- endfor %}
)