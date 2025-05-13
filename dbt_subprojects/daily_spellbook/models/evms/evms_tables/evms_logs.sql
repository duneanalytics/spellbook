{{ config(
        schema='evms',
        alias = 'logs',
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
        , block_hash
        , contract_address
        , topic0
        , topic1
        , topic2
        , topic3
        , data
        , tx_hash
        , index
        , tx_index
        , block_date
        , tx_from
        , tx_to
        FROM {{ source(blockchain, 'logs') }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );