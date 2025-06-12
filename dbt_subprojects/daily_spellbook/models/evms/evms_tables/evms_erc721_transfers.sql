{{ config(
        schema='evms',
        alias = 'erc721_transfers',
        materialized = 'view',
        post_hook='{{ expose_spells(evms_structured_blockchains_list() | tojson, "sector", "evms", \'[]\') }}'
        )
}}

{% set structured_blockchains = evms_structured_blockchains_list() %}

SELECT *
FROM (
        {% for blockchain in structured_blockchains %}
        SELECT
        '{{ blockchain }}' AS blockchain
        , contract_address
        , evt_tx_hash
        , evt_index
        , evt_block_time
        , evt_block_number
        , evt_block_date
        , evt_tx_from
        , evt_tx_to
        , evt_tx_index
        , "from"
        , to
        , tokenId
        FROM {{ source('erc721_' + blockchain, 'evt_Transfer') }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );