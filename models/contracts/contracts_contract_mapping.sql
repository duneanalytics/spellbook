 {{
  config(
        tags = ['dunesql'],
        schema = 'contracts',
        alias = alias('contract_mapping'),
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key=['blockchain','contract_address'],
        partition_by=['blockchain'],
        post_hook='{{ expose_spells(\'["ethereum", "optimism", "arbitrum", "avalanche_c", "polygon", "bnb", "gnosis", "fantom", "base", "goerli"]\',
                                    "sector",
                                    "contracts",
                                    \'["msilb7", "chuxin"]\') }}'
  )
}}

{% set chain_models = [

 ref('contracts_ethereum_contract_creator_project_mapping')

] %}
--  ('contracts_arbitrum_contract_creator_project_mapping')
-- ,('contracts_avalanche_c_contract_creator_project_mapping')
-- ,('contracts_base_contract_creator_project_mapping')
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
    {% if is_incremental() %}
    WHERE created_time >= date_trunc('day', now() - interval '7' day)
    OR is_updated_in_last_run = 1 --flag we use to see if contract metadata is new
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)