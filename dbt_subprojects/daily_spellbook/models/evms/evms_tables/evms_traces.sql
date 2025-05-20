{{ config(
        schema='evms',
        alias = 'traces',
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
        , block_time
        , block_number
        , value
        , gas
        , gas_used
        , block_hash
        , success
        , tx_index
        , error
        , tx_success
        , tx_hash
        , "from"
        , to
        , trace_address
        , type
        , address
        , code
        , call_type
        , input
        , output
        , refund_address
        FROM {{ source(blockchain, 'traces') }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );