{{ 
  config(
    alias='contract_overrides',
    unique_key='contract_address',
    post_hook='{{ expose_spells(\'["optimism"]\',
                              "sector",
                              "contracts",
                              \'["msilb7", "chuxin"]\') }}'
    ) 
}}

select 
  lower(contract_address) as contract_address
  ,cast(contract_project as varchar(250)) AS contract_project
  ,contract_name
from 
    (values 
    ('0xc30141B657f4216252dc59Af2e7CdB9D8792e1B0', 'Socket', 'Socket Registry')
    ,('0x81b30ff521D1fEB67EDE32db726D95714eb00637', 'Optimistic Explorer', 'OptimisticExplorerNFT')
    ,('0x998EF16Ea4111094EB5eE72fC2c6f4e6E8647666', 'Quix', 'Seaport')
    ,('0xEE36eaaD94d1Cc1d0eccaDb55C38bFfB6Be06C77', 'AttestationStation','AttestationStation')
    ,('0x9dDA6Ef3D919c9bC8885D5560999A3640431e8e6', 'Metamask Swaps', 'MetamaskSwaps')
    ,('0x74A002D13f5F8AF7f9A971f006B9a46c9b31DaBD', 'Rabbithole', 'RabbitHoleExplorerNFT')
    ,('0xcD487Bbd5F6f9AFD3CEa637A1803b6E8d71C958A', 'BitKeep', 'SwapRouter')
    ,('0x15DdA60616Ffca20371ED1659dBB78E888f65556', 'RetroPGF Receiver', 'AssetReceiver')
    ,('0x92D932aBBC7885999c4347880Eb069F854982eDD', 'OKX NFT', NULL)
    ,('0x86Bb63148d17d445Ed5398ef26Aa05Bf76dD5b59', 'Layer Zero', 'TheAptosBridge')
    ,('0x00000000000076a84fef008cdabe6409d2fe638b', 'DelegateCash', 'delegationRegistry')
    ) as temp_table(contract_address, contract_project, contract_name)
