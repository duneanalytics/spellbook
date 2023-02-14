{{config(
    alias='system_addresses',
    post_hook='{{ expose_spells(\'["arbitrum","optimism","solana"]\',
                                "sector",
                                "labels",
                                \'["msilb7"]\') }}'
)}}

SELECT blockchain, lower(address) as address, name, category, contributor, source, created_at, updated_at
FROM
(
    VALUES
     (array('optimism'), '0x420000000000000000000000000000000000000f',   'Optimism - L1 Gas Price Oracle',   'system',   'msilb7',   'static',   timestamp('2022-12-02'),    now())
    ,(array('arbitrum'), '0x00000000000000000000000000000000000a4b05',   'Arbitrum - ArbOS L1 Data Oracle',        'system',   'msilb7',   'static',   timestamp('2022-12-02'),    now())
    ,(array('solana'),   'Vote111111111111111111111111111111111111111',  'Solana - Voting Address',          'system',   'msilb7',   'static',   timestamp('2022-12-02'),    now())
    ,(array('optimism'), '0xdeaddeaddeaddeaddeaddeaddeaddeaddead0001',   'Optimism - L1 Attributes Depositor Contract',        'system',   'msilb7',   'static',   timestamp('2022-12-28'),    now())
    ,(array('optimism'), '0x4200000000000000000000000000000000000015',   'Optimism - L1 Attributes Predeployed Contract',        'system',   'msilb7',   'static',   timestamp('2022-12-28'),    now())
) AS temp_table (blockchain, address, name, category, contributor, source, created_at, updated_at)
;
