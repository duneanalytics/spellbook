-- Derived from: https://dune.com/queries/1857798

-- Pull known project wallets
SELECT address, category AS label, proposal_name, proposal_source AS address_descriptor, project_name AS address_name
FROM {{ref('addresses_optimism_grants_funding')}}

UNION ALL

-- Pull intermediate project wallets
SELECT 

--wallets we identified as internal transfers within a project (i.e. not going to users)
, intermediate_wallets AS (
SELECT address, 'Project' AS label, project, description
FROM (
SELECT LOWER(address) AS address, proposal_name, address_descriptor, project, 
        , ROW_NUMBER() OVER (PARTITION BY address ORDER BY description) AS rnk
FROM (values
     --suspected internal transfer addresses
     ('0xd4c00fe7657791c2a43025de483f05e49a5f76a6','Lyra','intermediate') --holds velo venft
    ,('0xb074ec6c37659525EEf2Fb44478077901F878012','Velodrome','intermediate')
    ,('0xe3f6b34445f499383025ca054028a3ad9693ff67','xToken Terminal / Gamma Strategies','intermediate') --guessing
    ,('0x1d702651ed22736eeb261ac9e2b72e7f79ed9ea9','0x','intermediate?') --guessing
    ,('0xFd0Bd19e849493F77D8f77eD026520C1368102Bd','Layer2dao','deplpoyer')
    ,('0xdb583b636f995ef1ef28ac96b9ba235916bd1583','Beefy Finance','intermediate')
    ,('0x2c3ee61296e89612994e7a1d336fb623e5138411','Quix','intermediate - cb deposit')
    ,('0xf1c9750C166329636B0A832dbd598d960fCE6893','Revert Finance','intermediate')
    ,('0xa28390A0eb676c1C40dAa794ECc2336740701BD1','WardenSwap','intermediate')
    ,('0xd245678e417aee2d91763f6f4efe570ff52fd080','Angle','intermediate')
    ,('0x763b9dba40c3d03507df454823fe03517f84a5ab', 'WePiggy','intermediate')
    ,('0x10d5cc9593cf749a93c61319aa4e36acfd71a26a','LiFi','intermediate')
    ,('0x5b77873C10BB116d87E1C11A36a6491c80D33784','LiFi','intermediate')
    ,('0xAd95A5fE898679B927C266eB2eDfAbC7fe268C27','QiDao','intermediate')
    ,('0x80ab3817C0026D31e5ECaC7675450f510f016EfB','dForce','intermediate')
    ,('0x79946eac000c85c83b6ba3adfa9ed7f4e2314e84','OptiChads','multisig')
    ,('0x18b7ee080db33c314c74973fcfbc3fd257416162','Beefy Finance','intermediate')
    ,('0x19B6584cA17D3B50E298327dA83Ff36C6EFb71E5','dHedge','intermediate')
    ,('0x253956aedc059947e700071bc6d74bd8e34fe2ab','dHedge','intermediate')
    ,('0x5891BE896ed4a79ed928C55B17FbbEcDb46f8A00', 'QiDao', 'intermediate')
    ,('0xa6200c2bf2ce83b32c4c1a7345888e75ab64f0e3', 'LiFi', 'intermediate')
    ,('0xf508311867EFdC50cf36B06EC95E0EEdb2212599', 'Candide Wallet','intermediate')
    ,('0x8636600a864797aa7ac8807a065c5d8bd9ba3ccb', 'Arrakis Finaince', 'Uni Grants')
    --suspected grants multisigs
    ,('0x5a06d52f38965904cf15c3f55286263ab9a237d7','Perpetual Protocol','intermediate/grants?') --guessing
    ,('0xC69a2d7e3De31542aB9ba1e80F9F5d68e49f78e6','Lyra','intermediary - seems like grants') --guess
    ,('0x20f3880A281092dBC6699E5D5a0FF5FEB3D3db1A','Celer','intermediate/grants?') --guessing
    ,('0xf56dd30d6ab0ebdae3a2892597ec5c8ee03df099','Perpetual Protocol','perp grants')
    ,('0x2f837d64858d0867f8d22683b341f754d8258bc3','Synthetix','Multisig - Maybe Grants?')
    ,('0x246d38588b16dd877c558b245e6d5a711c649fcf','Synthetix','Multisig - Maybe Grants?')
    
    ) a (address, proposal_name, address_descriptor)
    ) b
    WHERE rnk = 1 --check to prvent duplicates
    AND address NOT IN (SELECT address FROM {{ref('addresses_optimism_cex')}} ) --make sure we don't accidently catch a CEX
)