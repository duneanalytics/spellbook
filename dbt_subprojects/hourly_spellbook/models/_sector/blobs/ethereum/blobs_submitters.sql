{{ config(
    schema = 'blobs',
    alias = 'submitters',
    materialized = 'table',
    file_format = 'delta'
)}}

-- @hildobby https://dune.com/queries/3521610
SELECT address
    , entity
    FROM (
        values
        (0x0d3250c3d5facb74ac15834096397a3ef790ec99, 'zkSync Era')
        , (0x2c169dfe5fbba12957bdd0ba47d9cedbfe260ca7, 'StarkNet')
        , (0x6887246668a3b87f54deb3b94ba47a6f63f32985, 'OP Mainnet')
        , (0x5050f69a9786f081509234f1a7f4684b5e5b76c9, 'Base')
        , (0xc1b634853cb333d3ad8663715b08f41a3aec47cc, 'Arbitrum')
        , (0x625726c858dbf78c0125436c943bf4b4be9d9033, 'Zora')
        , (0x889e21d7ba3d6dd62e75d4980a4ad1349c61599d, 'Aevo')
        , (0x41b8cd6791de4d8f9e0eaf7861ac506822adce12, 'Kroma')
        , (0x14e4e97bdc195d399ad8e7fc14451c279fe04c8e, 'Lyra')
        , (0x99199a22125034c808ff20f377d91187e8050f2e, 'Mode')
        , (0x415c8893d514f9bc5211d36eeda4183226b84aa7, 'Blast')
        , (0x6017f75108f251a488b045a7ce2a7c15b179d1f2, 'Fraxtal')
        , (0x99526b0e49a95833e734eb556a6abaffab0ee167, 'PGN')
        , (0xc70ae19b5feaa5c19f576e621d2bad9771864fe2, 'Paradex')
        , (0xa9268341831eFa4937537bc3e9EB36DbecE83C7e, 'Linea')
        , (0xc94c243f8fb37223f3eb2f7961f7072602a51b8b, 'Metal')
        , (0xe1b64045351b0b6e9821f19b39f81bc4711d2230, 'Boba Network')
        , (0x08f9f14ff43e112b18c96f0986f28cb1878f1d11, 'Camp Network')
        , (0x5ead389b57d533a94a0eacd570dc1cc59c25f2d4, 'Parallel')
        , (0xcf2898225ed05be911d3709d9417e86e0b4cfc8f, 'Scroll')
        , (0xa6ea2f3299b63c53143c993d2d5e60a69cd6fe24, 'Lisk')
        , (0x3d0bf26e60a689a7da5ea3ddad7371f27f7671a5, 'Optopia')
        , (0x5c53f2ff1030c7fbc0616fd5b8fc6be97aa27e00, 'Lumio')
        , (0x1fd6a75cc72f39147756a663f3ef1fc95ef89495, 'opBNB')
        , (0xa76e31d8471d569efdd3d95d1b11ce6710f4533f, 'Manta')
        , (0x84bdfb21ed7c8b332a42bfd595744a84f3101e4e, 'Karak')
        , (0x994c288de8418c8d3c5a4d21a69f35bf9641781c, 'Hypr')
        , (0x6079e9c37b87fe06d0bde2431a0fa309826c9b67, 'Ancient8')
        , (0x2f6afe2e3fea041b892a6e240fd1a0e5b51e8376, 'Mantle')
        , (0xcdf02971871b7736874e20b8487c019d28090019, 'Metis')
        , (0xf8db8aba597ff36ccd16fecfbb1b816b3236e9b8, 'Orderly')
        , (0xdec273bf31ad79ad00d619c52662f724176a12fb, 'Lambda')
        , (0x68bdfece01535090c8f3c27ec3b1ae97e83fa4aa, 'Mint')
        , (0x000000633b68f5d8d3a86593ebb815b4663bcbe0, 'Taiko')
        , (0x52ee324F2bCD0c5363d713eb9f62D1eE47266ac1, 'Rari')
        , (0x7ab7da0c3117d7dfe0abfaa8d8d33883f8477c74, 'Debank Chain') 
        , (0xe27f3f6db6824def1738b2aace2672ac59046a39, 'Kinto')
        , (0xb1b676357de100c5bd846299cf6c85436803e839, 'Nal')
        , (0x90680f0f6d63060fb7a16bdc722a85b992dd5047, 'XGA')
        , (0xaf1e4f6a47af647f87c0ec814d8032c4a4bff145, 'Zircuit')
        ) AS x(address, entity)
