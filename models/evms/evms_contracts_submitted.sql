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
     ('ethereum', ref('ethereum', 'contracts_submitted'))
     , ('polygon', ref('polygon', 'contracts_submitted'))
     , ('bnb', ref('bnb', 'contracts_submitted'))
     , ('avalanche_c', ref('avalanche_c', 'contracts_submitted'))
     , ('gnosis', ref('gnosis', 'contracts_submitted'))
     , ('fantom', ref('fantom', 'contracts_submitted'))
     , ('optimism', ref('optimism', 'contracts_submitted'))
     , ('arbitrum', ref('arbitrum', 'contracts_submitted'))
     , ('celo', ref('celo', 'contracts_submitted'))
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
