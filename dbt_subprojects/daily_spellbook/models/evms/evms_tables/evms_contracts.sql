{{ config(
        schema='evms',
        alias = 'contracts',
        materialized = 'view',
        post_hook='{{ expose_spells(evms_structured_blockchains_list() | tojson, "sector", "evms", \'[]\') }}'
        )
}}

{% set structured_blockchains = evms_structured_blockchains_list() %}

SELECT *
    FROM
    (
        {% for blockchain in structured_blockchains %}
        SELECT
            '{{ blockchain }}' AS blockchain
            , abi_id
            , abi
            , address
            , "from"
            , code
            , name
            , namespace
            , dynamic
            , base
            , factory
            , detection_source
            , created_at
        FROM {{ source(blockchain, 'contracts') }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
