{{ 
  config(
    tags = ['static'],
    schema = 'contracts',
    alias = 'project_name_mappings',
    unique_key='dune_name',
    post_hook='{{ expose_spells(\'["ethereum", "optimism", "arbitrum", "avalanche_c", "polygon", "bnb", "gnosis", "fantom", "base", "goerli", "zksync"]\',
                              "sector",
                              "contracts",
                              \'["msilb7", "chuxin", "lgingerich"]\') }}'
    )  
}}

{% set contracts_project_mapping_models = [
  ref('contracts_optimism_project_name_mappings')
 ,ref('contracts_zksync_project_name_mappings')
] %}

SELECT *
FROM (
    {% for contracts_project_mapping_model in contracts_project_mapping_models %}
    SELECT 
      dune_name
     ,mapped_name
    FROM {{ contracts_project_mapping_model }}
    {% if not loop.last %}
    UNION
    {% endif %}
    {% endfor %}
)
