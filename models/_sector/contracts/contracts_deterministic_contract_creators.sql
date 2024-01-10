{{ 
  config(
    tags = ['static'],
    schema = 'contracts',
    alias = 'deterministic_contract_creators',
    unique_key='creator_address',
    post_hook='{{ expose_spells(\'["ethereum", "base", "optimism", "zora"]\',
                              "sector",
                              "contracts",
                              \'["msilb7"]\') }}'
    )  
}}


SELECT

    creator_address
     , creator_name

FROM (values
   (0xbb6e024b9cffacb947a71991e386681b1cd1477d,	'Singleton Factory')
  ,(0xce0042B868300000d44A59004Da54A005ffdcf9f,	'Singleton Factory')
  ,(0x3fAB184622Dc19b6109349B94811493BF2a45362,	'Deterministic Deployment Factory')
  ,(0x4c8D290a1B368ac4728d83a9e8321fC3af2b39b1,	'Opensea KEYLESS_CREATE2_DEPLOYER_ADDRESS')
  ,(0x7A0D94F55792C434d74a40883C6ed8545E406D12,	'Opensea KEYLESS_CREATE2_ADDRESS')
  ,(0xcfA3A7637547094fF06246817a35B8333C315196,	'INEFFICIENT_IMMUTABLE_CREATE2_FACTORY_ADDRESS')
  ,(0x0000000000ffe8b47b3e2130213b802212439497,	'IMMUTABLE_CREATE2_FACTORY_ADDRESS')
  ,(0x4200000000000000000000000000000000000012,	'L2StandardTokenFactory') -- OP Chains
  ,(0x2e985AcD6C8Fa033A4c5209b0140940E24da7C5C, 'OVM_L2StandardTokenFactory') -- OP Chains
  ,(0xeedA95f4513f950957Ae84E4da221ee260Fa2f40,	'Deterministic Factory')
  ,(0x4e59b44847b379578588920cA78FbF26c0B4956C, 'CREATE2 Factory')
  ,(0xE21f6b2A09bB27149E1afec31D05675bcf581FEd, 'CREATE3Factory')
  ,(0x93FEC2C00BfE902F733B57c5a6CeeD7CD1384AE1, 'CREATE3Factory')
  ,(0xdeaddeaddeaddeaddeaddeaddeaddeaddead0006, 'Genesis Contract')
  ,(0xe1cb04a0fa36ddd16a06ea828007e35e1a3cbc37, 'Singleton Factory')
  ,(0x488E1A80133870CB71EE2b08f926CE329d56B084, 'Deployer')
  ,(0x0000000000000000000000000000000000008006, 'zkSync Contract Deployer') --zkSync
  ,(0x13b0D85CcB8bf860b6b79AF3029fCA081AE9beF2, 'xdeployer')
  ,(0x6df7bf308ABaf673f38Db316ECc97b988CE1Ca78, 'Create3Factory')
  ,(0x914d7Fec6aaC8cd542e72Bca78B30650d45643d7, 'Safe Singleton Factory')
  ,(0x7506248eC2A111121912B972fadF8405989f8afb, 'Create2Deployer')
  -- excluded creators
  ,(0x36BDE71C97B33Cc4729cf772aE268934f7AB70B2, 'Optimism: CDM Relay')

) a (creator_address, creator_name)