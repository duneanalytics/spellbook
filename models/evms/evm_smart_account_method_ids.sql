
{{ config(
        schema = 'evms',
        tags = ['static'],
        alias = 'evm_smart_account_method_ids',
        post_hook='{{ expose_spells(\'["ethereum","optimism","arbitrum","polygon","gnosis","avalanche_c","fantom","goerli","bnb","base","celo","zora"]\',
                                "sector",
                                "method_ids",
                                \'["msilb7"]\') }}'
        )
}}


{% set all_chains_array = all_evm_mainnets_testnets_chains() %}


SELECT *
FROM (
    {% for chain in all_chains_array %}
    SELECT '{{chain}}' AS blockchain, method_id, method_descriptor, contract_project
    FROM {{ ref('base_evm_smart_account_method_ids') }} r
    WHERE
        r.blockchain IS NULL --If Null, make an entry for all chains
        OR r.blockchain = '{{chain}}'
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)