{{ config(
        schema='evms',
        alias = 'creation_traces',
        materialized = 'view'
        , post_hook='{{ hide_spells() }}'
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
                , block_month
                , cast(date_trunc('day', block_time) as date) as block_date
        FROM {{ source(blockchain, 'creation_traces') }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
)