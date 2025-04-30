{{
  config(     
        schema = 'contracts',
        alias = 'contract_mapping',
        post_hook='{{ expose_spells(\'["ethereum", "base", "optimism", "zora", "arbitrum", "celo", "polygon", "bnb", "avalanche_c", "fantom", "gnosis", "goerli","zksync", "scroll"]\',
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
 , ref('contracts_avalanche_c_contract_mapping')
 , ref('contracts_fantom_contract_mapping_dynamic')
 , ref('contracts_gnosis_contract_mapping')
 , ref('contracts_goerli_contract_mapping')
 , ref('contracts_zksync_contract_mapping')
 , ref('contracts_scroll_contract_mapping')
] %}
-- TODO: add support for additional EVMs in Dune
-- The following chains should be added (each requires its own directory and contract_mapping file):
-- linea, blast, mantle, mode, sei, bob, nova, degen, abstract, berachain, sonic, kaia, etc.
-- For each chain, create:
-- 1. A directory: dbt_subprojects/daily_spellbook/models/contracts/{chain_name}/
-- 2. A contract mapping file: contracts_{chain_name}_contract_mapping.sql
-- 3. Add ref('contracts_{chain_name}_contract_mapping') to the chain_models list above
-- 4. Add the chain to the expose_spells list in the config

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
