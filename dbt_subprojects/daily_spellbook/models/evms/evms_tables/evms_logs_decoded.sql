{{ config(
        schema='evms',
        alias = 'logs_decoded',
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
        , index
        , contract_address
        , event_name
        , namespace
        , signature
        , tx_hash
        FROM {{ source(blockchain, 'logs_decoded') }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );