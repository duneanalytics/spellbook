{{config(
    alias = 'system_addresses',
    post_hook='{{ expose_spells(\'["arbitrum","optimism","base","zora","bnb"]\',
                                "sector",
                                "labels",
                                \'["msilb7"]\') }}'
)}}


WITH curated_addresses AS (
        
    SELECT blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type
    FROM
    (VALUES
         ('arbitrum', 0x00000000000000000000000000000000000a4b05,   'Arbitrum - ArbOS L1 Data Oracle',        'infrastructure',   'msilb7',   'static',   TIMESTAMP '2022-12-02' ,    now(), 'system_addresses', 'identifier')
        {% for chain in all_op_chains() %}
        ,('{{chain}}', 0xdeaddeaddeaddeaddeaddeaddeaddeaddead0001,   'Optimism - L1 Attributes Depositor Contract',        'infrastructure',   'msilb7',   'static',   TIMESTAMP '2022-12-28' ,    now(), 'system_addresses', 'identifier')
        {% endfor %}
    --     ,('solana',   'Vote111111111111111111111111111111111111111',  'Solana - Voting Address',          'infrastructure',   'msilb7',   'static',   TIMESTAMP '2022-12-02' ,    now(), 'system_addresses', 'identifier')
    ) AS temp_table (blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type)

)


SELECT blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type
FROM curated_addresses

UNION ALL

SELECT pdp.blockchain, contract_address AS address, contract_name as name, 'infrastructure' as category, 'contracts_system_predeploys' as contributor, 'static' as source, NOW() AS created_at, NOW() AS updated_at, 'system_addresses' AS model_name, 'identifier' as label_type
FROM {{ ref('contracts_system_predeploys') }} pdp
LEFT JOIN curated_addresses c 
    ON c.blockchain = pdp.blockchain 
    AND c.address = pdp.contract_address
WHERE c.address IS NULL

