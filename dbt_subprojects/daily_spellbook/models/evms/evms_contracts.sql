{{ config(
        schema='evms',
        alias = 'contracts',
        unique_key=['blockchain', 'address'],
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum", "celo", "base", "zksync", "zora", "scroll", "linea", "zkevm", "blast", "mantle", "ronin"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

{% set contracts_models = [
     ('ethereum', source('ethereum', 'contracts'))
     , ('polygon', source('polygon', 'contracts'))
     , ('bnb', source('bnb', 'contracts'))
     , ('avalanche_c', source('avalanche_c', 'contracts'))
     , ('gnosis', source('gnosis', 'contracts'))
     , ('fantom', source('fantom', 'contracts'))
     , ('optimism', source('optimism', 'contracts'))
     , ('arbitrum', source('arbitrum', 'contracts'))
     , ('celo', source('celo', 'contracts'))
     , ('base', source('base', 'contracts'))
     , ('zksync', source('zksync', 'contracts'))
     , ('zora', source('zora', 'contracts'))
     , ('scroll', source('scroll', 'contracts'))
     , ('linea', source('linea', 'contracts'))
     , ('zkevm', source('zkevm', 'contracts'))
     , ('blast', source('blast', 'contracts'))
     , ('mantle', source('mantle', 'contracts'))
     , ('sei', source('sei', 'contracts'))
     , ('ronin', source('ronin', 'contracts'))
] %}

SELECT *
    FROM
    (
        {% for contracts_model in contracts_models %}
        SELECT
            '{{ contracts_model[0] }}' AS blockchain
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
            , row_number() over (partition by address order by created_at desc) as duplicates_rank
        FROM {{ contracts_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
    WHERE
        duplicates_rank = 1
