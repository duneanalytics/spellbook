{{ 
  config(
	tags=['legacy'],
	
    alias = alias('deterministic_contract_creators', legacy_model=True),
    unique_key='creator_address',
    post_hook='{{ expose_spells(\'["optimism"]\',
                              "sector",
                              "contracts",
                              \'["msilb7"]\') }}'
    )  
}}


SELECT

LOWER(creator_address) AS creator_address, cast(creator_name as varchar(250)) AS creator_name

FROM (values
   ('0xbb6e024b9cffacb947a71991e386681b1cd1477d',	'singleton factory')
  ,('0xce0042B868300000d44A59004Da54A005ffdcf9f',	'singleton factory')
  ,('0x3fAB184622Dc19b6109349B94811493BF2a45362',	'Deterministic Deployment Factory')
  ,('0x11f11121df7256c40339393b0fb045321022ce44',	'create3 factory')
  ,('0x4c8D290a1B368ac4728d83a9e8321fC3af2b39b1',	'Opensea KEYLESS_CREATE2_DEPLOYER_ADDRESS')
  ,('0x7A0D94F55792C434d74a40883C6ed8545E406D12',	'Opensea KEYLESS_CREATE2_ADDRESS')
  ,('0xcfA3A7637547094fF06246817a35B8333C315196',	'INEFFICIENT_IMMUTABLE_CREATE2_FACTORY_ADDRESS')
  ,('0x0000000000ffe8b47b3e2130213b802212439497',	'IMMUTABLE_CREATE2_FACTORY_ADDRESS')
  ,('0x4200000000000000000000000000000000000012',	'L2StandardTokenFactory')
  ,('0x2e985AcD6C8Fa033A4c5209b0140940E24da7C5C', 'OVM_L2StandardTokenFactory')
  ,('0xeedA95f4513f950957Ae84E4da221ee260Fa2f40',	'Deterministic Factory')
  ,('0x4e59b44847b379578588920cA78FbF26c0B4956C', 'CREATE2 Factory')
  ,('0xE21f6b2A09bB27149E1afec31D05675bcf581FEd', 'CREATE3Factory')
  ,('0x93FEC2C00BfE902F733B57c5a6CeeD7CD1384AE1', 'CREATE3Factory')
  ,('0xdeaddeaddeaddeaddeaddeaddeaddeaddead0006', 'Genesis Contract')
  ,('0x488E1A80133870CB71EE2b08f926CE329d56B084', 'Deployer')
  -- excluded creators
  ,('0x36BDE71C97B33Cc4729cf772aE268934f7AB70B2', 'Optimism: CDM Relay')

) a (creator_address, creator_name)