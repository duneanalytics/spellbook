{{
    config(
        schema = 'oneinch',
        alias = alias('fusion_settlements'),
        materialized = 'view',
        unique_key = ['contract_address', 'blockchain'],
        tags = ['dunesql']
    )
}}



with

settlements(contract_address, blockchain) as (values
    (0xa88800cd213da5ae406ce248380802bd53b47647, 'ethereum')
    , (0x1d0ae300eec4093cee4367c00b228d10a5c7ac63, 'bnb')
    , (0x1e8ae092651e7b14e4d0f93611267c5be19b8b9f, 'polygon')
    , (0x4bc3e539aaa5b18a82f6cd88dc9ab0e113c63377, 'arbitrum')
    , (0x7731f8df999a9441ae10519617c24568dc82f697, 'avalanche_c')
    , (0xd89adc20c400b6c45086a7f6ab2dca19745b89c2, 'optimism')
    , (0xa218543cc21ee9388fa1e509f950fd127ca82155, 'fantom')
    , (0xcbdb7490968d4dbf183c60fc899c2e9fbd445308, 'gnosis')
    , (0x7F069df72b7A39bCE9806e3AfaF579E54D8CF2b9, 'base')
)

select *
from settlements