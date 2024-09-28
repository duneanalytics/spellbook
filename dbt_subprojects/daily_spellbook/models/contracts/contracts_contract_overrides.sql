{{ 
  config(
    tags = ['static'],
    schema = 'contracts',
    alias = 'contract_overrides',
    unique_key='contract_address',
    post_hook='{{ expose_spells(\'["ethereum", "base", "optimism", "zora"]\',
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
    ,(0x81b30ff521D1fEB67EDE32db726D95714eb00637, 'Optimistic Explorer', 'OptimisticExplorerNFT') -- OP Mainnet
    ,(0x998EF16Ea4111094EB5eE72fC2c6f4e6E8647666, 'Quix', 'Seaport') -- OP Mainnet
    ,(0x9dDA6Ef3D919c9bC8885D5560999A3640431e8e6, 'Metamask', 'Metamask Swaps') -- OP Mainnet
    ,(0x74A002D13f5F8AF7f9A971f006B9a46c9b31DaBD, 'Rabbithole', 'RabbitHoleExplorerNFT') -- OP Mainnet
    ,(0xcD487Bbd5F6f9AFD3CEa637A1803b6E8d71C958A, 'BitKeep', 'SwapRouter')
    ,(0x15DdA60616Ffca20371ED1659dBB78E888f65556, 'RetroPGF Receiver', 'AssetReceiver') -- OP Mainnet
    ,(0x92D932aBBC7885999c4347880Eb069F854982eDD, 'OKX NFT', 'OKX NFT')
    ,(0x86Bb63148d17d445Ed5398ef26Aa05Bf76dD5b59, 'Layer Zero', 'TheAptosBridge')
    ,(0x00000000000076a84fef008cdabe6409d2fe638b, 'DelegateCash', 'delegationRegistry')
    ,(0x82E0b8cDD80Af5930c4452c684E71c861148Ec8A, 'Metamask', 'Metamask BridgeRouter') -- OP Mainnet
    ,(0x81E792e5a9003CC1C8BF5569A00f34b65d75b017, 'Layer Zero', 'Relayer v2')
    ,(0xA0Cc33Dd6f4819D473226257792AFe230EC3c67f, 'Layer Zero', 'LayerZero Oracle')
    ,(0xEE36eaaD94d1Cc1d0eccaDb55C38bFfB6Be06C77, 'AttestationStation','AttestationStation') -- OP Mainnet
    ,(0xbed744818e96aad8a51324291a6f6cb20a0c22be, 'AttestationStation', 'AttestationStation') -- OP Mainnet
    ,(0x2335022c740d17c2837f9c884bfe4ffdbf0a95d5, 'Optimist NFT', 'Optimist NFT') -- OP Mainnet
    ,(0x225B0747C4062d98E2e957752Ac5d73C7DaCff90, 'Seaport', 'IOperatorFilterRegistry')
    ,(0xc40F949F8a4e094D1b49a23ea9241D289B7b2819, 'Liquity', 'LUSD')
    ,(0x1B36291fF8F503CfB4E3baBe198a40398BCF54AD, 'Manifold', 'ERC1155CreatorImplementation')
    ,(0x000000000000AAeB6D7670E522A718067333cd4E, 'OpenSea', 'OperatorFilterRegistry')
    ,(0x3cc6CddA760b79bAfa08dF41ECFA224f810dCeB6, 'OpenSea', 'OperatorFilterRegistry')
    ,(0xca11bde05977b3631167028862be2a173976ca11, 'Multicall3', 'Multicall3')
    ,(0xfc2d34a2a545dbe210ad0d8cc0e0e943aacff621, 'first.lol', 'first.lol') --Zora
    ,(0x914d7Fec6aaC8cd542e72Bca78B30650d45643d7, 'Gnosis Safe', 'Safe Singleton Factory')
    ,(0x00005EA00Ac477B1030CE78506496e8C2dE24bf5, 'OpenSea', 'SeaDrop 1.0')
    ,(0x74de5d4FCbf63E00296fd95d33236B9794016631, 'Metamask', 'Metamask Swaps') -- Ethereum L1
    ,(0x881d40237659c251811cec9c364ef91dc08d300c, 'Metamask', 'Metamask Swaps') -- Ethereum L1
    ,(0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA, 'Circle', 'USDbC') --Base
    ,(0x1833c6171e0a3389b156eaedb301cffbf328b463, 'Circle', 'UpgradeableOptimismMintableERC20') --Base
    --Non-Contract Labels
    ,(0x80C67432656d59144cEFf962E8fAF8926599bCF8, 'Orbiter Finance', 'Bridge')
    ,(0xf332761c673b59B21fF6dfa8adA44d78c12dEF09, 'OKX', 'OKX DEX')
    --WETH
    ,(0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2, 'WETH', 'WETH')
    
    ) as temp_table(contract_address, contract_project, contract_name)
