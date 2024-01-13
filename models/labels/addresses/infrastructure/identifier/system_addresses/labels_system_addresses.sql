{{config(
    alias = 'system_addresses',
    post_hook='{{ expose_spells(\'["arbitrum","optimism","solana"]\',
                                "sector",
                                "labels",
                                \'["msilb7"]\') }}'
)}}



SELECT blockchain, address as address, name, category, contributor, source, created_at, updated_at, model_name, label_type
FROM
(
    VALUES
--     ,('solana',   'Vote111111111111111111111111111111111111111',  'Solana - Voting Address',          'infrastructure',   'msilb7',   'static',   TIMESTAMP '2022-12-02' ,    now(), 'system_addresses', 'identifier')
    ('optimism', 0x420000000000000000000000000000000000000f,   'Optimism - L1 Gas Price Oracle',   'infrastructure',   'msilb7',   'static',   TIMESTAMP '2022-12-02' ,    now(), 'system_addresses', 'identifier')
    ,('arbitrum', 0x00000000000000000000000000000000000a4b05,   'Arbitrum - ArbOS L1 Data Oracle',        'infrastructure',   'msilb7',   'static',   TIMESTAMP '2022-12-02' ,    now(), 'system_addresses', 'identifier')
    {% for chain in all_op_chains() %}
    ,('{{chain}}', 0xdeaddeaddeaddeaddeaddeaddeaddeaddead0001,   'Optimism - L1 Attributes Depositor Contract',        'infrastructure',   'msilb7',   'static',   TIMESTAMP '2022-12-28' ,    now(), 'system_addresses', 'identifier')
    ,('{{chain}}', 0x4200000000000000000000000000000000000015,   'Optimism - L1 Attributes Predeployed Contract',        'infrastructure',   'msilb7',   'static',   TIMESTAMP '2022-12-28' ,    now(), 'system_addresses', 'identifier')
    {% endfor %}
    ,('bnb', 0x0000000000000000000000000000000000001007, 'BSC: Governance Hub',   'msilb7',   'static',   TIMESTAMP '2024-01-13' ,    now(), 'system_addresses', 'identifier')
    ,('bnb', 0x0000000000000000000000000000000000001006, 'BSC: Relayer Hub',   'msilb7',   'static',   TIMESTAMP '2024-01-13' ,    now(), 'system_addresses', 'identifier')
    ,('bnb', 0x0000000000000000000000000000000000001005, 'BSC: Relayer Incentivize',   'msilb7',   'static',   TIMESTAMP '2024-01-13' ,    now(), 'system_addresses', 'identifier')
    ,('bnb', 0x0000000000000000000000000000000000001001, 'BSC: SlashIndicator',   'msilb7',   'static',   TIMESTAMP '2024-01-13' ,    now(), 'system_addresses', 'identifier')
    ,('bnb', 0x0000000000000000000000000000000000001002, 'BSC: System Reward',   'msilb7',   'static',   TIMESTAMP '2024-01-13' ,    now(), 'system_addresses', 'identifier')
    ,('bnb', 0x0000000000000000000000000000000000001003, 'BSC: Tendermint Light Client',   'msilb7',   'static',   TIMESTAMP '2024-01-13' ,    now(), 'system_addresses', 'identifier')
    ,('bnb', 0x0000000000000000000000000000000000001004, 'BSC: Token Hub',   'msilb7',   'static',   TIMESTAMP '2024-01-13' ,    now(), 'system_addresses', 'identifier')
    ,('bnb', 0x0000000000000000000000000000000000001008, 'BSC: Token Manager',   'msilb7',   'static',   TIMESTAMP '2024-01-13' ,    now(), 'system_addresses', 'identifier')
    ,('bnb', 0x0000000000000000000000000000000000001000, 'BSC: Validator Set',   'msilb7',   'static',   TIMESTAMP '2024-01-13' ,    now(), 'system_addresses', 'identifier')

) AS temp_table (blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type)

