{{ 
  config(
    tags = ['dunesql','static'],
    alias = alias('contract_overrides'),
    unique_key='contract_address',
    post_hook='{{ expose_spells(\'["optimism"]\',
                              "sector",
                              "contracts",
                              \'["msilb7", "chuxin"]\') }}'
    ) 
}}

select 
  contract_address
  ,cast(contract_project as varchar) AS contract_project
  ,contract_name
from 
    (values 
     (0xc30141B657f4216252dc59Af2e7CdB9D8792e1B0, 'Socket', 'Socket Registry')
    ,(0x81b30ff521D1fEB67EDE32db726D95714eb00637, 'Optimistic Explorer', 'OptimisticExplorerNFT')
    ,(0x998EF16Ea4111094EB5eE72fC2c6f4e6E8647666, 'Quix', 'Seaport')
    ,(0x9dDA6Ef3D919c9bC8885D5560999A3640431e8e6, 'Metamask', 'Metamask Swaps')
    ,(0x74A002D13f5F8AF7f9A971f006B9a46c9b31DaBD, 'Rabbithole', 'RabbitHoleExplorerNFT')
    ,(0xcD487Bbd5F6f9AFD3CEa637A1803b6E8d71C958A, 'BitKeep', 'SwapRouter')
    ,(0x15DdA60616Ffca20371ED1659dBB78E888f65556, 'RetroPGF Receiver', 'AssetReceiver')
    ,(0x92D932aBBC7885999c4347880Eb069F854982eDD, 'OKX NFT', 'OKX NFT')
    ,(0x86Bb63148d17d445Ed5398ef26Aa05Bf76dD5b59, 'Layer Zero', 'TheAptosBridge')
    ,(0x00000000000076a84fef008cdabe6409d2fe638b, 'DelegateCash', 'delegationRegistry')
    ,(0x82E0b8cDD80Af5930c4452c684E71c861148Ec8A, 'Metamask', 'Metamask BridgeRouter')
    ,(0x81E792e5a9003CC1C8BF5569A00f34b65d75b017, 'Layer Zero', 'Relayer v2')
    ,(0xA0Cc33Dd6f4819D473226257792AFe230EC3c67f, 'Layer Zero', 'LayerZero Oracle')
    ,(0xEE36eaaD94d1Cc1d0eccaDb55C38bFfB6Be06C77, 'OP AttestationStation','AttestationStation')
    ,(0xbed744818e96aad8a51324291a6f6cb20a0c22be, 'OP AttestationStation', 'AttestationStation')
    ,(0x2335022c740d17c2837f9c884bfe4ffdbf0a95d5, 'Optimist NFT', 'Optimist NFT')
    ,(0x225B0747C4062d98E2e957752Ac5d73C7DaCff90, 'Seaport', 'IOperatorFilterRegistry')
    ,(0xc40F949F8a4e094D1b49a23ea9241D289B7b2819, 'Liquity', 'LUSD')
    ,(0x1B36291fF8F503CfB4E3baBe198a40398BCF54AD, 'Manifold', 'ERC1155CreatorImplementation')
    ,(0x000000000000AAeB6D7670E522A718067333cd4E, 'OpenSea', 'OperatorFilterRegistry')
    ,(0x3cc6CddA760b79bAfa08dF41ECFA224f810dCeB6, 'OpenSea', 'OperatorFilterRegistry')
    ,(0xca11bde05977b3631167028862be2a173976ca11, 'Multicall3', 'Multicall3')
    --Non-Contract Labels
    ,(0x80C67432656d59144cEFf962E8fAF8926599bCF8, 'Orbiter Finance', 'Bridge')
    ,(0xf332761c673b59B21fF6dfa8adA44d78c12dEF09, 'OKX', 'OKX DEX')
    
    ) as temp_table(contract_address, contract_project, contract_name)
