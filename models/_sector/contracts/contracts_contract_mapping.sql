 {{
  config(     
        schema = 'contracts',
        alias = 'contract_mapping',
        post_hook='{{ expose_spells(\'["ethereum", "base", "optimism"]\',
                                    "sector",
                                    "contracts",
                                    \'["msilb7", "chuxin"]\') }}'
  )
}}

{% set chain_models = [

   ref('contracts_ethereum_contract_mapping')
 , ref('contracts_base_contract_mapping')
 , ref('contracts_optimism_contract_mapping')

] %}
--  ('contracts_arbitrum_contract_mapping')
-- ,('contracts_avalanche_c_contract_mapping')
--
-- ,('contracts_bnb_contract_mapping')
-- ,('contracts_celo_contract_mapping')
-- ,('contracts_fantom_contract_mapping')
-- ,('contracts_gnosis_contract_mapping')
-- ,('contracts_goerli_contract_mapping')
-- 
-- ,('contracts_polygon_contract_mapping')

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