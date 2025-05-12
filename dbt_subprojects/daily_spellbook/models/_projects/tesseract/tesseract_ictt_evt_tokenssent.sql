{%- set alias = 'ictt_evt_tokenssent' -%}

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
        , contract_address
        , evt_tx_hash
        , evt_index
        , evt_block_time
        , evt_block_number
        , evt_block_date
        , teleporterMessageID
        , sender
        , destinationBlockchainID
        , destinationTokenTransferrerAddress
        , recipient
        , primaryFeeTokenAddress
        , primaryFee
        , secondaryFee
        , requiredGasLimit
        , multiHopFallback
        , amount
    FROM {{ model }}
    {%- if not loop.last %}
    UNION ALL
    {%- endif -%}
    {%- endfor %}
)