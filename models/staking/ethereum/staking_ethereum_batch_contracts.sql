{{ config(
    schema = 'staking_ethereum',
    alias = 'batch_contracts',
    unique_key = ['address'])
}}

SELECT address, entity, entity_unique_name
FROM (VALUES
    (0x1e68238ce926dec62b3fbc99ab06eb1d85ce0270, 'Kiln', 'Kiln 1')
    , (0x9b8c989ff27e948f55b53bb19b3cc1947852e394, 'Kiln', 'Kiln 2')
    , (0xf2be95116845252a28bd43661651917dc183dab1, 'Figment', 'Figment 1')
    , (0x37ab162ab59e106d6072eb7a7bd4c4c2973455a7, 'Figment', 'Figment 2')
    , (0xb4e2e925d75793c33f5f94cd652f6c464665c76b, 'Figment', 'Figment 3')
    , (0xf0075b3cf8953d3e23b0ef65960913fd97eb5227, 'Figment', 'Figment 4')
    , (0x3f124c700fb5e741f128e28985267d44f56b242f, 'Blockdaemon', 'Blockdaemon 1')
    , (0xfa5f9eaa65ffb2a75de092eb7f3fc84fc86b5b18, 'Abyss Finance', 'Abyss Finance 2')
    , (0xbF1556A7D625654E3D64d1f0466A60A697FAc178, 'InfStones', 'InfStones 1')
    , (0x4befa2aa9c305238aa3e0b5d17eb20c045269e9d, 'RockX', 'RockX 1')
    , (0xe8239B17034c372CDF8A5F8d3cCb7Cf1795c4572, 'RockX', 'RockX 2')
    , (0x1BDc639EaBF1c5EbC020Bb79E2dD069A8b6fe865, 'Unidentified 0x1bdc63', 'Unidentified 0x1bdc63 1')
    , (0x7ef5d450be41fcbc6191cb09789c5783da53f3fc, 'Unidentified 0x7ef5d4', 'Unidentified 0x7ef5d4 1')
    , (0xfc3e2b3003859e28f6efd8321615731f9b02c81f, 'Unidentified 0xfc3e2b', 'Unidentified 0xfc3e2b 2')
    ) AS temp_table (address, entity, entity_unique_name)