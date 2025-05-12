{%- set alias = 'icm_blockchains' -%}

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
        , blockchain_id
        , blockchain_id_base58
        , earliest_icm_interaction
        , latest_icm_interaction
        , sample_message_id
    FROM {{ model }}
    {%- if not loop.last %}
    UNION ALL
    {%- endif -%}
    {%- endfor %}
)