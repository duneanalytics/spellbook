{%- set alias = 'cell_routed' -%}

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
        , cell_type
        , tesseractID
        , messageID
        , action
        , transferrer
        , destinationBlockchainID
        , destinationCell
        , destinationTransferrer
        , tokenIn
        , amountIn
        , tokenOut
        , amountOut
    FROM {{ model }}
    {%- if not loop.last %}
    UNION ALL
    {%- endif -%}
    {%- endfor %}
)