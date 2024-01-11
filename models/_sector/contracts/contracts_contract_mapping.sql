 {{
  config(     
        schema = 'contracts',
        alias = 'contract_mapping',
        post_hook='{{ expose_spells(\'["ethereum", "base", "optimism", "zora", "arbitrum", "celo", "polygon", "bnb"]\',
                                    "sector",
                                    "contracts",
                                    \'["msilb7", "chuxin", "tomfutago"]\') }}'
  )
}}

{% set chain_models = [

   ref('contracts_ethereum_contract_mapping')
 , ref('contracts_base_contract_mapping')
 , ref('contracts_optimism_contract_mapping')
 , ref('contracts_zora_contract_mapping')
 , ref('contracts_polygon_contract_mapping')
 , ref('contracts_arbitrum_contract_mapping')
 , ref('contracts_bnb_contract_mapping_dynamic')
 , ref('contracts_celo_contract_mapping')
] %}
-- todo: add chains for all EVMs in Dune

SELECT *
FROM (
    {% for chain_model in chain_models %}
    SELECT
          *
    FROM {{ chain_model }}
    
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)