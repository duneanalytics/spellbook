{{ config(
        schema='evms',
        alias = 'creation_traces',
        materialized = 'view',
        post_hook='{{ expose_spells(evms_structured_blockchains_list() | tojson, "sector", "evms", \'[]\') }}'
        )
}}

{% set structured_blockchains = evms_structured_blockchains_list() %}

SELECT
        *
FROM 
(
        {% for blockchain in structured_blockchains %}
        SELECT
                '{{ blockchain }}' AS blockchain
                , block_time
                , block_number
                , tx_hash
                , address
                , "from"
                , code
                --, tx_from
                --, tx_to
        FROM {{ source(blockchain, 'creation_traces') }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
)