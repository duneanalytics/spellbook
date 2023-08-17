{{ config (tags=['dunesql'],
    schema='tokemak_ethereum',
    alias = alias('view_tokemak_addresses'),
    post_hook = '{{ expose_spells(\'["ethereum"]\',
        "project", 
            "Tokemak",
             \'["addmorebass"]\') }}'
) }}

WITH tokemak_ethereum_view_tokemak_addresses AS (
    SELECT '0x9e0bcE7ec474B481492610eB9dd5D69EB03718D5' AS address /*deployer*/
    UNION 
    SELECT '0x90b6C61B102eA260131aB48377E143D6EB3A9d4B' AS address/*coordinator*/
    UNION 
    SELECT '0xA86e412109f77c45a3BC1c5870b880492Fb86A14' AS address/*manager*/
    UNION 
    SELECT '0x8b4334d4812c530574bd4f2763fcd22de94a969b' as address /*treasury*/
)

SELECT address FROM tokemak_ethereum_view_tokemak_addresses