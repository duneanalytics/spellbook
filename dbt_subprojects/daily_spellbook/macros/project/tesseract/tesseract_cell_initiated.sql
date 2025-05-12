{%- macro tesseract_cell_initiated(
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
        , tesseractId AS tesseractID
        , sourceId AS sourceID
        , origin
        , sender
        , receiver
        , token
        , amount
        , nativeFeeAmount
        , baseFeeAmount
    FROM {{ source(namespace_blockchain, cell_type + '_evt_Initiated') }}
    {%- if not loop.last %}
    UNION ALL
    {%- endif -%}
    {%- endfor %}
)

{%- endmacro -%}