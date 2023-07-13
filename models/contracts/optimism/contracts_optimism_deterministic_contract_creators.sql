{{ 
  config(
    tags = ['dunesql','static'],
    alias = alias('deterministic_contract_creators'),
    unique_key='creator_address',
    post_hook='{{ expose_spells(\'["optimism"]\',
                              "sector",
                              "contracts",
                              \'["msilb7"]\') }}'
    )  
}}


SELECT

creator_address, creator_name

FROM (values
   (0xbb6e024b9cffacb947a71991e386681b1cd1477d,	'Singleton Factory')
  ,(0xce0042B868300000d44A59004Da54A005ffdcf9f,	'Singleton Factory')
  ,(0x3fAB184622Dc19b6109349B94811493BF2a45362,	'Deterministic Deployment Factory')
  ,(0x11f11121df7256c40339393b0fb045321022ce44,	'Create3 Factory')
  ,(0x4c8D290a1B368ac4728d83a9e8321fC3af2b39b1,	'Opensea KEYLESS_CREATE2_DEPLOYER_ADDRESS')
  ,(0x7A0D94F55792C434d74a40883C6ed8545E406D12,	'Opensea KEYLESS_CREATE2_ADDRESS')
  ,(0xcfA3A7637547094fF06246817a35B8333C315196,	'INEFFICIENT_IMMUTABLE_CREATE2_FACTORY_ADDRESS')
  ,(0x0000000000ffe8b47b3e2130213b802212439497,	'IMMUTABLE_CREATE2_FACTORY_ADDRESS')
  ,(0x4200000000000000000000000000000000000012,	'L2StandardTokenFactory')
  ,(0x2e985AcD6C8Fa033A4c5209b0140940E24da7C5C, 'OVM_L2StandardTokenFactory')
  ,(0xeedA95f4513f950957Ae84E4da221ee260Fa2f40,	'Deterministic Factory')
  ,(0x4e59b44847b379578588920cA78FbF26c0B4956C, 'CREATE2 Factory')
  ,(0xE21f6b2A09bB27149E1afec31D05675bcf581FEd, 'CREATE3Factory')
  ,(0xdeaddeaddeaddeaddeaddeaddeaddeaddead0006, 'Genesis Contract')
  ,(0xe1cb04a0fa36ddd16a06ea828007e35e1a3cbc37, 'Singleton Factory')

) a (creator_address, creator_name)