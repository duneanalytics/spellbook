 {{
  config(   
        schema = 'contracts',
        alias = 'contract_mapping',
        materialized ='table',
        unique_key=['blockchain','contract_address'],
        partition_by=['blockchain'],
        post_hook='{{ expose_spells(\'["ethereum", "base"]\',
                                    "sector",
                                    "contracts",
                                    \'["msilb7", "chuxin"]\') }}'
  )
}}

{% set chain_models = [

   ref('contracts_ethereum_contract_creator_project_mapping')
 , ref('contracts_base_contract_creator_project_mapping')

] %}
--  ('contracts_arbitrum_contract_creator_project_mapping')
-- ,('contracts_avalanche_c_contract_creator_project_mapping')
--
-- ,('contracts_bnb_contract_creator_project_mapping')
-- ,('contracts_celo_contract_creator_project_mapping')
-- ,('contracts_fantom_contract_creator_project_mapping')
-- ,('contracts_gnosis_contract_creator_project_mapping')
-- ,('contracts_goerli_contract_creator_project_mapping')
-- ,('contracts_optimism_contract_creator_project_mapping')
-- ,('contracts_polygon_contract_creator_project_mapping')

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