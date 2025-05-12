{%- macro tesseract_cell_routed(
        blockchain = null
    )
-%}

{%- set namespace_blockchain = 'tesseract_' + blockchain -%}

SELECT
    '{{ blockchain }}' AS blockchain
    , *
FROM (
    {%- for cell_type in tesseract_cell_types(blockchain) %}
    SELECT
        contract_address
        , evt_tx_hash
        , evt_index
        , evt_block_time
        , evt_block_number
        , evt_block_date
        , '{{ cell_type }}' AS cell_type
        , tesseractID
        , messageID
        , CASE action
            WHEN 1 THEN 'Hop'
            WHEN 2 THEN 'HopAndCall'
            WHEN 3 THEN 'SwapAndHop'
            WHEN 4 THEN 'SwapAndTransfer'
            ELSE NULL END AS action
        , transferrer
        , destinationBlockchainID
        , destinationCell
        , destinationTransferrer
        , tokenIn
        , amountIn
        , tokenOut
        , amountOut
    FROM {{ source(namespace_blockchain, cell_type + '_evt_CellRouted') }}
    {%- if not loop.last %}
    UNION ALL
    {%- endif -%}
    {%- endfor %}
)

{%- endmacro -%}