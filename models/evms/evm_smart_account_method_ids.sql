
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

WITH aggrregate_methods AS (
-- Generic EVM methods, we read null array as "all"
SELECT NULL as blockchains, method_id, method_descriptor, contract_project
    FROM (values
         (0x6a761202, 'execTransaction', 'Gnosis Safe')
        ,(0x1fad948c, 'handleOps', 'Erc4337')
        ) a (method_id, method_descriptor, contract_project)
)


SELECT *
FROM (
    {% for chain in all_chains_array %}
    SELECT '{{chain}}' AS blockchain, method_id, method_descriptor, contract_project
    FROM aggrregate_methods
    WHERE
        blockchains IS NULL --If Null, make an entry for all chains
        OR blockchains = '{{chain}}'
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)