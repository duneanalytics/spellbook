{{ config(
    schema = 'blobs',
    alias = 'submitters',
    materialized = 'table',
    file_format = 'delta'
)}}

-- @hildobby https://dune.com/queries/3521610
SELECT address
    , entity
    , x_username
    FROM (
        values
        (0x0d3250c3d5facb74ac15834096397a3ef790ec99, 'zkSync Era', 'zksync')
        , (0x2c169dfe5fbba12957bdd0ba47d9cedbfe260ca7, 'StarkNet', 'Starknet')
        , (0x6887246668a3b87f54deb3b94ba47a6f63f32985, 'OP Mainnet', 'Optimism')
        , (0x5050f69a9786f081509234f1a7f4684b5e5b76c9, 'Base', 'base')
        , (0xc1b634853cb333d3ad8663715b08f41a3aec47cc, 'Arbitrum', 'arbitrum')
        , (0x625726c858dbf78c0125436c943bf4b4be9d9033, 'Zora', 'ourZORA')
        , (0x889e21d7ba3d6dd62e75d4980a4ad1349c61599d, 'Aevo', 'aevoxyz')
        , (0x41b8cd6791de4d8f9e0eaf7861ac506822adce12, 'Kroma', 'kroma_network')
        , (0x14e4e97bdc195d399ad8e7fc14451c279fe04c8e, 'Lyra', 'lyrafinance')
        , (0x99199a22125034c808ff20f377d91187e8050f2e, 'Mode', 'modenetwork')
        , (0x415c8893d514f9bc5211d36eeda4183226b84aa7, 'Blast', 'Blast_L2')
        , (0x6017f75108f251a488b045a7ce2a7c15b179d1f2, 'Fraxtal', 'fraxfinance')
        , (0x99526b0e49a95833e734eb556a6abaffab0ee167, 'PGN', 'pgn_eth')
        , (0xc70ae19b5feaa5c19f576e621d2bad9771864fe2, 'Paradex', 'tradeparadex')
        , (0xa9268341831eFa4937537bc3e9EB36DbecE83C7e, 'Linea', 'LineaBuild')
        , (0xc94c243f8fb37223f3eb2f7961f7072602a51b8b, 'Metal', 'Metal_L2')
        , (0xe1b64045351b0b6e9821f19b39f81bc4711d2230, 'Boba Network', 'bobanetwork')
        , (0x08f9f14ff43e112b18c96f0986f28cb1878f1d11, 'Camp Network', 'Camp_L2')
        , (0x5ead389b57d533a94a0eacd570dc1cc59c25f2d4, 'Parallel', 'ParallelFi')
        , (0x40acdc94a00b33151b40763b3fed7c46ff639df4, 'Parallel', 'ParallelFi')
        , (0xcf2898225ed05be911d3709d9417e86e0b4cfc8f, 'Scroll', 'Scroll_ZKP')
        , (0xa6ea2f3299b63c53143c993d2d5e60a69cd6fe24, 'Lisk', 'LiskHQ')
        , (0x3d0bf26e60a689a7da5ea3ddad7371f27f7671a5, 'Optopia', 'Optopia_AI')
        , (0x5c53f2ff1030c7fbc0616fd5b8fc6be97aa27e00, 'Lumio', 'PontemNetwork')
        , (0x1fd6a75cc72f39147756a663f3ef1fc95ef89495, 'opBNB', 'BNBCHAIN')
        , (0xa76e31d8471d569efdd3d95d1b11ce6710f4533f, 'Manta', 'MantaNetwork')
        , (0x84bdfb21ed7c8b332a42bfd595744a84f3101e4e, 'Karak', 'Karak_Network')
        , (0x994c288de8418c8d3c5a4d21a69f35bf9641781c, 'Hypr', 'hypr_network')
        , (0x6079e9c37b87fe06d0bde2431a0fa309826c9b67, 'Ancient8', 'Ancient8_gg')
        , (0x2f6afe2e3fea041b892a6e240fd1a0e5b51e8376, 'Mantle', '0xMantle')
        , (0xcdf02971871b7736874e20b8487c019d28090019, 'Metis', 'MetisL2')
        , (0xf8db8aba597ff36ccd16fecfbb1b816b3236e9b8, 'Orderly', 'OrderlyNetwork')
        , (0xdec273bf31ad79ad00d619c52662f724176a12fb, 'Lambda', 'Lambdaim')
        , (0x68bdfece01535090c8f3c27ec3b1ae97e83fa4aa, 'Mint', 'Mint_Blockchain')
        , (0x000000633b68f5d8d3a86593ebb815b4663bcbe0, 'Taiko', 'taikoxyz')
        , (0x52ee324F2bCD0c5363d713eb9f62D1eE47266ac1, 'Rari', 'rarichain')
        , (0x7ab7da0c3117d7dfe0abfaa8d8d33883f8477c74, 'Debank Chain', 'DeBankDeFi') 
        , (0xe27f3f6db6824def1738b2aace2672ac59046a39, 'Kinto', 'KintoXYZ')
        , (0xb1b676357de100c5bd846299cf6c85436803e839, 'Nal', 'nal_network')
        , (0x90680f0f6d63060fb7a16bdc722a85b992dd5047, 'XGA', 'foldfinance')
        , (0xaf1e4f6a47af647f87c0ec814d8032c4a4bff145, 'Zircuit', 'ZircuitL2')
        , (0xdbbe3d8c2d2b22a2611c5a94a9a12c2fcd49eb29, 'World Chain', 'worldcoin')
        , (0x8cda8351236199af7532bad53d683ddd9b275d89, 'RACE', 'RACEecosystem')
        , (0x7f9d9c1bce1062e1077845ea39a0303429600a06, 'Binary', 'thebinaryhldgs')
        , (0x4d875acfd836eb3d8a2f25ba03de16c97ec9fc0f, 'PandaSea', 'pandaseaweb3')
        , (0xf7ca543d652e38692fd12f989eb55b5327ec9a20, 'Shape', 'Shape_L2')
        , (0xde794bec196832474f2f218135bfd0f7ca7fb038, 'Swan Chain', 'swan_chain')
        , (0x67a44ce38627f46f20b1293960559ed85dd194f1, 'Polynomial', 'PolynomialFi')
        , (0x060b915ca4904b56ada63565626b9c97f6cad212, 'SNAXchain', 'synthetix_io')
        , (0x65115c6d23274e0a29a63b69130efe901aa52e7a, 'Hemi Network', 'hemi_xyz')
        , (0x43ca061ea80fbb4a2b5515f4be4e953b191147af, 'Ethernity', 'EthernityChain')
        , (0xee12c640b0793cf514e42ea1c428bd5399545d4d, 'MetaMail', 'MetaMailInk')
        ) AS x(address, entity, x_username)