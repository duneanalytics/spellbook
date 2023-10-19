{{config(
	tags=['legacy'],
	
    alias = alias('system_addresses', legacy_model=True),
    post_hook='{{ expose_spells(\'["arbitrum","optimism","solana"]\',
                                "sector",
                                "labels",
                                \'["msilb7"]\') }}'
)}}

SELECT blockchain, lower(address) as address, name, category, contributor, source, created_at, updated_at, model_name, label_type
FROM
(
    VALUES
     ('optimism', '0x420000000000000000000000000000000000000f',   'Optimism - L1 Gas Price Oracle',   'infrastructure',   'msilb7',   'static',   timestamp('2022-12-02'),    now(), 'system_addresses', 'identifier')
    ,('arbitrum', '0x00000000000000000000000000000000000a4b05',   'Arbitrum - ArbOS L1 Data Oracle',        'infrastructure',   'msilb7',   'static',   timestamp('2022-12-02'),    now(), 'system_addresses', 'identifier')
    ,('solana',   'Vote111111111111111111111111111111111111111',  'Solana - Voting Address',          'infrastructure',   'msilb7',   'static',   timestamp('2022-12-02'),    now(), 'system_addresses', 'identifier')
    ,('optimism', '0xdeaddeaddeaddeaddeaddeaddeaddeaddead0001',   'Optimism - L1 Attributes Depositor Contract',        'infrastructure',   'msilb7',   'static',   timestamp('2022-12-28'),    now(), 'system_addresses', 'identifier')
    ,('optimism', '0x4200000000000000000000000000000000000015',   'Optimism - L1 Attributes Predeployed Contract',        'infrastructure',   'msilb7',   'static',   timestamp('2022-12-28'),    now(), 'system_addresses', 'identifier')
) AS temp_table (blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type)
