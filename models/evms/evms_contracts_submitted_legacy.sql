{{ config(
        tags = ['dunesql'],
        alias = alias('contracts_submitted'),
        unique_key=['blockchain', 'address', 'created_at'],
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum", "celo"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby", "thetroyharris"]\') }}'
        )
}}

{% set contracts_models = [
     ('ethereum', source('ethereum', 'contracts_submitted'))
     , ('polygon', source('polygon', 'contracts_submitted'))
     , ('bnb', source('bnb', 'contracts_submitted'))
     , ('avalanche_c', source('avalanche_c', 'contracts_submitted'))
     , ('gnosis', source('gnosis', 'contracts_submitted'))
     , ('fantom', source('fantom', 'contracts_submitted'))
     , ('optimism', source('optimism', 'contracts_submitted'))
     , ('arbitrum', source('arbitrum', 'contracts_submitted'))
     , ('celo', source('celo', 'contracts_submitted'))
] %}

SELECT *
FROM (
        {% for contracts_model in contracts_models %}
        SELECT
        '{{ contracts_model[0] }}' AS blockchain
        , abi
      	, address
        , "from"
      	, code
        , name
      	, namespace
        , dynamic
      	, factory
      	, created_at
        FROM {{ contracts_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
  );
