{{ config (tags=['dunesql', 'static'],
    schema='tokemak_ethereum',
    alias = alias('tokemak_addresses'),
    post_hook = '{{ expose_spells(\'["ethereum"]\',
        "project", 
            "Tokemak",
             \'["addmorebass"]\') }}'
) }}

SELECT tokemak_address
FROM (VALUES
(0x9e0bcE7ec474B481492610eB9dd5D69EB03718D5) /*deployer*/,
(0x90b6C61B102eA260131aB48377E143D6EB3A9d4B) /*coordinator*/,
(0xA86e412109f77c45a3BC1c5870b880492Fb86A14) /*manager*/,
(0x8b4334d4812c530574bd4f2763fcd22de94a969b) /*treasury*/
) AS temp_table (tokemak_address)