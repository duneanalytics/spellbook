 {{
  config(
        alias='predeploys',
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum","goerli"]\',
                                    "sector",
                                    "contracts",
                                    \'["msilb7", "chuxin"]\') }}'
  )
}}

{% set pre_models = [
    ( 'optimism', ref('contracts_optimism_predeploys') )
] %}

SELECT * FROM
    (
    {% for pre in pre_models %}
    SELECT
    '{{ pre[0] }}' AS blockchain
    ,trace_creator_address
    ,contract_address
    ,contract_project
    ,contract_name
    ,creator_address
    ,created_time
    ,contract_creator_if_factory
    ,is_self_destruct
    ,creation_tx_hash
    ,source
    FROM {{ pre[1] }}
    {% if not loop.last %}
    {% if is_incremental() %}
    {% endif %}
    UNION ALL
    {% endif %}
    {% endfor %}
    ) a