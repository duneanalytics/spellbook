{{config(
        tags = ['static'],
        schema = 'pyth',
        alias = 'price_feed_contracts',
        post_hook='{{ expose_spells(\'["ethereum", "arbitrum", "avalanche_c", "bnb", "fantom", "optimism", "polygon", "zksync", "zora", "celo", "base"]\',
                                    "project",
                                    "pyth",
                                    \'["synthquest"]\') }}')}}

with definitions as (
-- https://docs.pyth.network/price-feeds/contract-addresses/evm
    select 
        chain, cast(contract_address as varbinary) as contract_address, chain_type 
    from ( VALUES
        ('arbitrum',0xff1a0f4744e8582df1ae09d5611b887b6a12925c,'evm'),
        ('astar zkevm',0xa2aa501b19aff244d90cc15a4cf739d2725b5729,'evm'),
        ('aurora',0xf89c7b475821ec3fdc2dc8099032c05c6c0c9ab9,'evm'),
        ('avalanche_c',0x4305fb66699c3b2702d4d05cf36551390a4c69c6,'evm'),
        ('blast',0xa2aa501b19aff244d90cc15a4cf739d2725b5729,'evm'),
        ('bnb',0x4d7e825f80bdf85e913e0dd2a2d54927e9de1594,'evm'),
        ('bttc',0xa2aa501b19aff244d90cc15a4cf739d2725b5729,'evm'),
        ('base',0x8250f4af4b972684f7b336503e2d6dfedeb1487a,'evm'),
        ('boba',0x4374e5a8b9c22271e9eb878a2aa31de97df15daf,'evm'),
        ('canto',0x98046bd286715d3b0bc227dd7a956b83d8978603,'evm'),
        ('celo',0xff1a0f4744e8582df1ae09d5611b887b6a12925c,'evm'),
        ('chiliz',0xa2aa501b19aff244d90cc15a4cf739d2725b5729,'evm'),
        ('conflux espace',0xe9d69cdd6fe41e7b621b4a688c5d1a68cb5c8adc,'evm'),
        ('core dao',0xa2aa501b19aff244d90cc15a4cf739d2725b5729,'evm'),
        ('cronos',0xe0d0e68297772dd5a1f1d99897c581e2082dba5b,'evm'),
        ('eos',0xa2aa501b19aff244d90cc15a4cf739d2725b5729,'evm'),
        ('evmos',0x354bf866a4b006c9af9d9e06d9364217a8616e12,'evm'),
        ('ethereum',0x4305fb66699c3b2702d4d05cf36551390a4c69c6,'evm'),
        ('fantom',0xff1a0f4744e8582df1ae09d5611b887b6a12925c,'evm'),
        ('filecoin',0xa2aa501b19aff244d90cc15a4cf739d2725b5729,'evm'),
        ('gnosis',0x2880ab155794e7179c9ee2e38200202908c17b43,'evm'),
        ('gravity',0x2880ab155794e7179c9ee2e38200202908c17b43,'evm'),
        ('hedera',0xa2aa501b19aff244d90cc15a4cf739d2725b5729,'evm'),
        ('horizen eon',0xa2aa501b19aff244d90cc15a4cf739d2725b5729,'evm'),
        ('injective inevm',0xa2aa501b19aff244d90cc15a4cf739d2725b5729,'evm'),
        ('iota',0x8d254a21b3c86d32f7179855531ce99164721933,'evm'),
        ('kcc',0xe0d0e68297772dd5a1f1d99897c581e2082dba5b,'evm'),
        ('kava',0xa2aa501b19aff244d90cc15a4cf739d2725b5729,'evm'),
        ('lightlink phoenix',0xa2aa501b19aff244d90cc15a4cf739d2725b5729,'evm'),
        ('linea',0xa2aa501b19aff244d90cc15a4cf739d2725b5729,'evm'),
        ('manta',0xa2aa501b19aff244d90cc15a4cf739d2725b5729,'evm'),
        ('mantle',0xa2aa501b19aff244d90cc15a4cf739d2725b5729,'evm'),
        ('merlin',0xa2aa501b19aff244d90cc15a4cf739d2725b5729,'evm'),
        ('meter',0xbfe3f445653f2136b2fd1e6dddb5676392e3af16,'evm'),
        ('mode',0xa2aa501b19aff244d90cc15a4cf739d2725b5729,'evm'),
        ('neon',0x7f2db085efc3560aff33865dd727225d91b4f9a5,'evm'),
        ('opbnb',0x2880ab155794e7179c9ee2e38200202908c17b43,'evm'),
        ('optimism',0xff1a0f4744e8582df1ae09d5611b887b6a12925c,'evm'),
        ('parallel',0xa2aa501b19aff244d90cc15a4cf739d2725b5729,'evm'),
        ('polygon',0xff1a0f4744e8582df1ae09d5611b887b6a12925c,'evm'),
        ('zkevm',0xc5e56d6b40f3e3b5fbfa266bcd35c37426537c65,'evm'),
        ('ronin',0x2880ab155794e7179c9ee2e38200202908c17b43,'evm'),
        ('scroll',0xa2aa501b19aff244d90cc15a4cf739d2725b5729,'evm'),
        ('sei',0x2880ab155794e7179c9ee2e38200202908c17b43,'evm'),
        ('shimmer',0xa2aa501b19aff244d90cc15a4cf739d2725b5729,'evm'),
        ('taiko',0x2880ab155794e7179c9ee2e38200202908c17b43,'evm'),
        ('viction',0xa2aa501b19aff244d90cc15a4cf739d2725b5729,'evm'),
        ('wemix',0xa2aa501b19aff244d90cc15a4cf739d2725b5729,'evm'),
        ('zkfair',0xa2aa501b19aff244d90cc15a4cf739d2725b5729,'evm'),
        ('zksync',0xf087c864aeccfb6a2bf1af6a0382b0d0f6c5d834,'evm'),
        ('zetachain',0x2880ab155794e7179c9ee2e38200202908c17b43,'evm')
    ) as t(chain, contract_address, chain_type)
)

select 
      chain
    , contract_address
    , chain_type
from definitions
