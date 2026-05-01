{{ config(
        schema='evms',
        alias = 'traces_decoded',
        materialized = 'view'
        , post_hook='{{ hide_spells() }}'
        )
}}

{% set structured_blockchains = evms_structured_blockchains_list() %}

SELECT *
FROM (
        {% for blockchain in structured_blockchains %}
        SELECT
        '{{ blockchain }}' AS blockchain
        , block_date
        , block_time
        , block_number
        , contract_name
        , function_name
        , namespace
        , signature
        , to
        , trace_address
        , tx_hash
        FROM {{ source(blockchain, 'traces_decoded') }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );