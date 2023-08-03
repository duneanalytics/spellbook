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
     ('ethereum', ref('ethereum_contracts_submitted'))
     , ('polygon', ref('polygon_contracts_submitted'))
     , ('bnb', ref('bnb_contracts_submitted'))
     , ('avalanche_c', ref('avalanche_c_contracts_submitted'))
     , ('gnosis', ref('gnosis_contracts_submitted'))
     , ('fantom', ref('fantom_contracts_submitted'))
     , ('optimism', ref('optimism_contracts_submitted'))
     , ('arbitrum', ref('arbitrum_contracts_submitted'))
     , ('celo', ref('celo_contracts_submitted'))
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
