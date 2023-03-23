{{ config(
    alias = 'project_wallets',
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "op_token_distributions",
                                \'["msilb7"]\') }}'
    )
}}
-- Derived from: https://dune.com/queries/1857798


-- Pull intermediate project wallets

--wallets we identified as internal transfers within a project (i.e. not going to users)
with intermediate_wallets AS (
SELECT address, proposal_name, address_descriptor
FROM (
SELECT LOWER(address) AS address, cast(proposal_name as varchar(100)) AS proposal_name, cast(address_descriptor as varchar(100)) AS address_descriptor
        , ROW_NUMBER() OVER (PARTITION BY address ORDER BY address_descriptor) AS rnk
FROM (values
     --suspected internal transfer addresses
     ('0xd4c00fe7657791c2a43025de483f05e49a5f76a6', 'Lyra','Intermediate') --holds velo venft
    ,('0xb074ec6c37659525EEf2Fb44478077901F878012', 'Velodrome','Intermediate')
    ,('0xe3f6b34445f499383025ca054028a3ad9693ff67', 'xToken Terminal / Gamma Strategies','Intermediate') --guessing
    ,('0x1d702651ed22736eeb261ac9e2b72e7f79ed9ea9', 'Zeroex (0x)','Intermediate') --guessing
    ,('0xFd0Bd19e849493F77D8f77eD026520C1368102Bd', 'Layer2dao','Contract Deplpoyer')
    ,('0xdb583b636f995ef1ef28ac96b9ba235916bd1583', 'Beefy Finance','Intermediate')
    ,('0x2c3ee61296e89612994e7a1d336fb623e5138411', 'Quix','CEX Deposit')
    ,('0xf1c9750C166329636B0A832dbd598d960fCE6893', 'Revert Finance','Intermediate')
    ,('0xa28390A0eb676c1C40dAa794ECc2336740701BD1', 'WardenSwap','Intermediate')
    ,('0xd245678e417aee2d91763f6f4efe570ff52fd080', 'Angle','Intermediate')
    ,('0x763b9dba40c3d03507df454823fe03517f84a5ab', 'WePiggy','Intermediate')
    ,('0x10d5cc9593cf749a93c61319aa4e36acfd71a26a', 'LiFi','Intermediate')
    ,('0x5b77873C10BB116d87E1C11A36a6491c80D33784', 'LiFi','Intermediate')
    ,('0xAd95A5fE898679B927C266eB2eDfAbC7fe268C27', 'QiDao','Intermediate')
    ,('0x80ab3817C0026D31e5ECaC7675450f510f016EfB', 'dForce','Intermediate')
    ,('0x79946eac000c85c83b6ba3adfa9ed7f4e2314e84', 'OptiChads','Intermediate')
    ,('0x18b7ee080db33c314c74973fcfbc3fd257416162', 'Beefy Finance','Intermediate')
    ,('0x19B6584cA17D3B50E298327dA83Ff36C6EFb71E5', 'dHedge','Intermediate')
    ,('0x253956aedc059947e700071bc6d74bd8e34fe2ab', 'dHedge','Intermediate')
    ,('0x5891BE896ed4a79ed928C55B17FbbEcDb46f8A00', 'QiDao', 'Intermediate')
    ,('0xa6200c2bf2ce83b32c4c1a7345888e75ab64f0e3', 'LiFi', 'Intermediate')
    ,('0xf508311867EFdC50cf36B06EC95E0EEdb2212599', 'Candide Wallet','Intermediate')
    ,('0x8636600a864797aa7ac8807a065c5d8bd9ba3ccb', 'Arrakis Finance', 'Uniswap Grants')
    ,('0xA7EEb1b719ef7b40F79D2056900ee3Ca904F28F9', 'DefiEdge', 'Uniswap Grants')
    ,('0xfda462548ce04282f4b6d6619823a7c64fdc0185', 'Angle', 'Contract Deplopyer')
    ,('0x20f3880A281092dBC6699E5D5a0FF5FEB3D3db1A', 'Celer','Intermediate')
    --quix - should come from CB
    ,('0x5Ad4A019F77e82940f6Dd15A5215362AF061A742','Quix','Distributor')
    
    --suspected grants multisigs
    ,('0x5a06d52f38965904cf15c3f55286263ab9a237d7', 'Perpetual Protocol','Grants Wallet') --guessing
    ,('0xC69a2d7e3De31542aB9ba1e80F9F5d68e49f78e6', 'Lyra','Lyra Grants DAO')
    ,('0xf56dd30d6ab0ebdae3a2892597ec5c8ee03df099', 'Perpetual Protocol','Grants Wallet')
    ,('0x2f837d64858d0867f8d22683b341f754d8258bc3', 'Synthetix','Multisig - Maybe Grants')
    ,('0x246d38588b16dd877c558b245e6d5a711c649fcf', 'Synthetix','Multisig - Maybe Grants')
    
    ) a (address, proposal_name, address_descriptor)
    ) b
    WHERE rnk = 1 --check to prvent duplicates
    AND address NOT IN (SELECT address FROM {{ref('addresses_optimism_cex')}} ) --make sure we don't accidently catch a CEX
)


-- wallets where we consider tokens deployed, but unclaimed
, distributor_wallets AS (
SELECT address, proposal_name, address_descriptor
FROM (
SELECT LOWER(address) AS address, cast(proposal_name as varchar(100)) AS proposal_name, cast(address_descriptor as varchar(100)) AS address_descriptor
    , ROW_NUMBER() OVER (PARTITION BY address ORDER BY cast(address_descriptor as varchar(100)) ) AS rnk
FROM (values
     ('0xeA1e11E3D448F31C565d685115899A11Fd98E40E','1inch','Distributor')
    ,('0xc9e53bb96a8923051326b189bbf93ee9ed87888b','WePiggy','LockDrop')
    ,('0x4f09b919d969b58a96e8bd7673f12372d09395e8','Velodrome','Distributor')
    ,('0x1470c87e2db5247a36c60de3d65d7c972c62ea0f','PoolTogether','Distributor')
    ,('0xf07108249edd2f59abd1d091a0778d58ecedbc49','Pika Protocol','Distributor')
    ,('0x6b473a82c4199dfaa4c31e69f07fc6f5eb73188e','Velodrome','Distributor')
    ,('0x75760bdbf7b71d9e68146684ef0a0c06701e6309','Rubicon','Distributor')
    ,('0xfd6fd41bea9fd489ffdf05cd8118a69bf98caa5d','Rubicon','Distributor LM')
    ,('0xf882defd9d5d988d05c6bca9061fc6f817f491c0','Rubicon','Distributor LM')
    ,('0xd528e1c99b0bdf1caf14f968f31adab81c59dcc8','Rubicon','Distributor LM')
    ,('0x06292de88adb3b1557b034ebb1c367e65ab93e4c','Celer','Distributor')
    ,('0x9f6b09fc2ea2ef9f4454ac6875829a7a89c9cd92','Perpetual Protocol','Distributor')
    ,('0x407da3e66095e28852774d5b88a575d75fdc6af4','Slingshot','Distributor')
    ,('0xdffdbb54b9968fee543a8d2bd3ce7a80d66cd49f','Rubicon','Distributor')
    ,('0x5fafd12ead4234270db300352104632187ed763a','Rubicon','Distributor')
    ,('0x78136ef4bdcbdabb8d7aa09a33c3c16ca6381910','Pika Protocol','Distributor')
    ,('0x019f0233c0277b9422fcdb1213b09c86f5f27d87','Lyra','Distributor')
    ,('0x45269f59aa76bb491d0fc4c26f468d8e1ee26b73','Hop Protocol','Distributor')
    ,('0x30f5fe161da1cb92ac09e10b734de07d5c120fdd','Rubicon','Distributor')
    ,('0x505fb5d94c3cf68e13b5ba2ca1868f2b580007cc','Rubicon','Distributor')
    ,('0xd317fc3fbead8e95f5b75145f9dc5e1c7b815856','WePiggy','Distributor')
    ,('0x5f1e8dc1c296a26188e1e04ed4bb6d1432226650','WePiggy','Distributor') --seems like airdropper
    ,('0x307c3487e0165A6cFc384165d2D914A034AC8c90','Slingshot','Distributor')
    ,('0x3dea6da7cdad789e6d947c3e983ab4f996a7bbc1','Slingshot','Distributor')
    ,('0xa46fd59672434d1917972f1469565baeb57ed204','Slingshot','Distributor')
    ,('0x31a20e5b7b1b067705419d57ab4f72e81cc1f6bf','Thales','Distributor')
    ,('0x1777c6d588fd931751762836811529c0073d6376','Thales','Distributor')
    ,('0x10e7449c75dbbe2d18e9f4cceec7ab6c7d1f8a30','Thales','Distributor')
    ,('0x5027cE356C375A934B4d1DE9240bA789072A5Af1','Thales','Distributor')
    ,('0x4dea9e918c6289a52cd469cac652727b7b412cd2','Stargate Finance','LP Staking Time')
    ,('0xc5ae4b5f86332e70f3205a8151ee9ed9f71e0797','Synthetix','sUSD-3Crv: Curve')
    ,('0xcb8883d1d8c560003489df43b30612aabb8013bb','Synthetix','sETH-ETH: Curve')
    ,('0xF510a2Ff7e9DD7e18629137adA4eb56B9c13E885','Kwenta','Distributor') --airdropper
    ,('0x4a8ff08af7f229b0d032ac182e2abb47ad3094e5','Kwenta','Distributor') --airdropper
    ,('0x2c8d267abd311e411793ffc3aca2d5206af59a08','QiDao','sUSDC/MAI: Velodrome')
    ,('0xd1b1f5b294432aaa399f9eb3069af13a8d327f45','QiDao','sUSDC/MAI: Velodrome')
    ,('0x26d9a248c4ebe777adb37813254e6aa59c1fe301','QiDao','vUSDC/MAI: Velodrome')
    ,('0xecc205dfa8300ced05955c9aea930f1a7ab8daeb','Beethoven X','Pool')
    ,('0xc529fa26588932e15eac04c971ad9350bc8dea32','Beethoven X','Pool')
    ,('0xb99b0b41cc107aea462119253ed3f241e9487abc','Beethoven X','Pool')
    ,('0xe2cf52c1f8fd5d593ff7a03a8b7efc44539caa9f','Beethoven X','Pool')
    ,('0xe039f8102319af854fe11489a19d6b5d2799ada7','Pickle Finance','Distributor')
    ,('0x86690b9Dbb979850AE4622347aF81232bAa3C967','Revert Finance','Distributor')
    ,('0x35bA8C41CeEEA24F7c826015844F2b58aF3058a6','Revert Finance','Distributor')
    -- ,('0x40a58B5B735Ba6596d04c88E7b262f6E79100EBb','Angle','Distributor-velo bribe') --bribe used by multiple parties
    ,('0x0Cb199aF5F402506963A4df08B11053687e09802', 'Polynomial Protocol','Distributor')
    ,('0x7432A3A1545B2764367dA16a207A16475D4221bD', 'WePiggy','Distributor')
    ,('0xfCdb1A1AFAaB60230bBc55D8B3de27F47fB7053f','dForce','Distributor')
    ,('0xebaa48d1c4129e93a1d286b01b56cc4981c30004','dForce','Distributor')
    ,('0x49478499dd1ba1b8a763ddc747661898c0f7c269','Beethoven X','Distributor')
    ,('0x5734bb74cfac69f1c34ba66ea6608ccdee6b81f2','Hundred Finance','pool')
    ,('0x1db11cf7c332e797ac912e11b8762e0a4b24a836','Hundred Finance','pool')
    ,('0x73280c390da5c6fe05ad2d1e6837e8e8c05e4b32','Hundred Finance','pool')
    ,('0x198618d2aa6cbc89ea24550fe896d4afa28cd635','Hundred Finance','pool')
    ,('0xe4e919a0289c66cb7d971268671fb529d88aad46','Beefy Finance','BIFI/WETH: Velodrome')
    ,('0xf276b01b74b410f85c10aaa0e7091164fec7d93d','Beefy Finance','BIFI/OP: Velodrome')
    ,('0xa3795274e173866b64afd1123fce62813076af82','Beefy Finance','BIFI/OP: Velodrome')
    ,('0x340e8ce762327392d430bbda16318ed734d58ef0','Layer2dao','L2DAO/OP: Velodrome')
    ,('0xebe1e96e67a516c2f8549edbd48e6fdc7b50c9ae','Thales','Distributor')
    ,('0x4022e57784b2fF8DceE839c16161F283223aE87B','Synthetix','USDC/SNX: Velodrome')
    ,('0xBee1E4C4276687A8350C2E44eCBe79d676637f86','Synthetix','USDC/sUSD: Velodrome')
    ,('0x68a1d9a49b82c5A0a3431aAE6178F89Ad5214730','Synthetix','ETH/sETH: Velodrome')
    ,('0x1A3E5557039763425B00a2e1B0eB767B01d64756','Beefy Finance','Beefy Launchpool')
    ,('0x65F8a09A1C3581a02C8788a6959652E32a87FC77','Beefy Finance','Beefy Launchpool')
    ,('0xda62d109064138c14d45085b6e49568e1c0b4e23','xToken Terminal / Gamma Strategies','Rewards Program')
    ,('0xf099FA1Bd92f8AAF4886e8927D7bd3c15bA0BbFd','xToken Terminal / Gamma Strategies','Rewarder')

    ,('0xAde63D643564AaA8C2A86F2244f43B5eB00ed5e6','Clipper','Distributor')
    ,('0x9024d0C5d4709b98856CDaE02B955890A69f8007','Kwenta','Distributor')
    ,('0x06292DE88aDb3b1557B034ebB1c367E65Ab93E4C','Celer','Liquidity Mining Distributor')
    ,('0xf6C5d7DA1654d9BbDe0D25A5fd6776B37a2aD881','Celer','Incentive Events Reward')
    
    ,('0x3ee85ac7c0e1799af6f4e582de485fcdfb12855a', 'Rocket Pool', 'WETH/rETH: Beethoven X')
    ,('0xdd5bfe292e377308abb58a211a572bd9732b62b7', 'Rocket Pool', 'sWETH/rETH: Velodrome')
    ,('0x4bae082f810fa888364600efda0bf9f5c6e5e315', 'Rocket Pool', 'vWETH/rETH: Velodrome')
    ,('0xE01A297289f0aE9e745DdDC61F139537ab733710', 'Overnight', 'USD+/USDC: Velodrome') 
    ,('0x8801b45390095f7632C02392C4489985e0607E82', 'Overnight', 'BPT-USD+: Beethoven X')
    ,('0xB66D278b843dBE76ee73Da61182fF97100f97920', 'Overnight', 'USD+/DOLA: Velodrome')
    ,('0x41a7540ec8cb3afafe16a834abe0863f22016ec0', 'Overnight', 'USD+/LUSD: Velodrome')
    ,('0x97a7E9726df22D6f28BB86679a0e5512A8c0E8A2', 'dHedge', 'Distributor')
    ,('0xC792980F2F3016F60bEd35926d21A43E140b99cC', 'dHedge', 'DHT/OP: Velodrome')
    ,('0xfCC293db3b7396a1c2477C9F24F5F948431EF6eC', 'Pika Protocol', 'Distributor')
    ,('0x8b50d942bb4effe0db32393a141ea083db0c4e7f', 'Alchemix', 'alETH/WETH: Velodrome')
    ,('0x4ea2e39626de031a011c3ca7360878202454ce75', 'Angle', 'OP/agEUR: Velodrome')
    ,('0x6333000289bc27a090b3d51e734c2f13e3edc4fd', 'Hundred Finance', 'Token Treasury')
    
    ,('0xeeab81526c9addb75ffffde0cd3f6f018cc39ac2','Synthetix','Multisig - Distributor')
    ,('0x09992dd7b32f7b35d347de9bdaf1919a57d38e82','Synthetix','SNX Bridge: Hop')

    ,('0xf42279467D821bCDf40b50E9A5d2cACCc4Cf5b30','Quests on Coinbase Wallet','Quest 1 - DEX')
    ,('0x9F4F2B8BdA8D2d3832021b3119747470ea86A183','Quests on Coinbase Wallet','Quest 2 - Delegation')

    --governance delegation
    ,('0x6a1e22c82be29eb96850158011b40fafbce1340c','Synthetix','SNXAmbassadors delegation')
    
    
    ) a (address, proposal_name, address_descriptor)
    ) b
    WHERE rnk = 1 --check to prvent duplicates
    AND address NOT IN (SELECT address FROM {{ref('addresses_optimism_cex')}}) --make sure we don't accidently catch a CEX
)

SELECT
        fin.address, fin.label, fin.proposal_name, fin.address_descriptor,
        COALESCE(pnm.project_name, fin.proposal_name) AS project_name
FROM (
        SELECT address, label, proposal_name, address_descriptor, ROW_NUMBER() OVER(PARTITION BY address ORDER BY rnk ASC) AS choice_rank 
                FROM (
                -- Pull known project wallets
                SELECT address, category AS label, proposal_name, funding_source AS address_descriptor, 1 as rnk
                FROM {{ref('addresses_optimism_grants_funding')}}

                UNION ALL

                SELECT address, 'Project' AS label, proposal_name, address_descriptor, 2 as rnk
                FROM intermediate_wallets

                UNION ALL

                SELECT address, 'Deployed' AS label, proposal_name, address_descriptor, 3 as rnk
                FROM distributor_wallets
                ) do_choice_rank
        ) fin
LEFT JOIN {{ ref('op_token_distributions_optimism_project_name_mapping') }} pnm 
        ON pnm.proposal_name = fin.proposal_name

WHERE choice_rank = 1 --remove dupes in preferred order
