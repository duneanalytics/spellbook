 {{
  config(
        
        schema = 'contracts',
        alias = 'self_destruct_contracts',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['blockchain', 'contract_address'],
        partition_by=['blockchain'],
        post_hook='{{ expose_spells(\'["ethereum", "optimism", "base", "zora"]\',
                                    "sector",
                                    "contracts",
                                    \'["msilb7", "chuxin"]\') }}'
  )
}}
{% set chain_models = [

    ref('contracts_ethereum_find_self_destruct_contracts')
  , ref('contracts_base_find_self_destruct_contracts')
  , ref('contracts_optimism_find_self_destruct_contracts')
  , ref('contracts_zora_find_self_destruct_contracts')

] %}
--  ('contracts_arbitrum_find_self_destruct_contracts')
-- ,('contracts_avalanche_c_find_self_destruct_contracts')
-- ,('contracts_bnb_find_self_destruct_contracts')
-- ,('contracts_celo_find_self_destruct_contracts')
-- ,('contracts_fantom_find_self_destruct_contracts')
-- ,('contracts_gnosis_find_self_destruct_contracts')
-- ,('contracts_goerli_find_self_destruct_contracts')
-- ,('contracts_polygon_find_self_destruct_contracts')

SELECT *
FROM (
    {% for chain_model in chain_models %}
    SELECT * FROM {{ chain_model }}
      {% if is_incremental() %}
      WHERE {{ incremental_predicate('destructed_time') }}
      {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)