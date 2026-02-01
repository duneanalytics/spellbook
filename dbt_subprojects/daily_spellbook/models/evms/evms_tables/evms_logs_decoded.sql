{{ config(
        schema='evms',
        alias = 'logs_decoded',
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
        , block_time
        , block_number
        , block_date
        , index
        , contract_address
        , contract_name
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