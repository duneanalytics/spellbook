{{config(
    alias = alias('burn_addresses', legacy_model=True),
    tags=['legacy', 'static'],
    post_hook='{{ expose_spells(\'["ethereum", "bnb", "polygon", "solana", "arbitrum", "optimism", "fantom", "avalanche_c", "gnosis"]\',
                                "sector",
                                "labels",
                                \'["hildobby"]\') }}'
)}}

SELECT blockchain, trim(lower(address)) as address, name, category, contributor, source, created_at, updated_at, model_name, label_type
FROM
(   
    -- Source:
    -- https://etherscan.io/accounts/label/burn

    VALUES
    ('ethereum', '0x0000000000000000000000000000000000000000', 'Burn Address', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-26'), now(), 'burn_addresses', 'identifier')
    , ('ethereum', '0x0000000000000000000000000000000000000001', 'Burn Address', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-26'), now(), 'burn_addresses', 'identifier')
    , ('ethereum', '0x0000000000000000000000000000000000000002', 'Burn Address', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-26'), now(), 'burn_addresses', 'identifier')
    , ('ethereum', '0x0000000000000000000000000000000000000003', 'Burn Address', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-26'), now(), 'burn_addresses', 'identifier')
    , ('ethereum', '0x0000000000000000000000000000000000000004', 'Burn Address', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-26'), now(), 'burn_addresses', 'identifier')
    , ('ethereum', '0x0000000000000000000000000000000000000005', 'Burn Address', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-26'), now(), 'burn_addresses', 'identifier')
    , ('ethereum', '0x0000000000000000000000000000000000000006', 'Burn Address', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-26'), now(), 'burn_addresses', 'identifier')
    , ('ethereum', '0x0000000000000000000000000000000000000007', 'Burn Address', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-26'), now(), 'burn_addresses', 'identifier')
    , ('ethereum', '0x0000000000000000000000000000000000000008', 'Burn Address', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-26'), now(), 'burn_addresses', 'identifier')
    , ('ethereum', '0x0000000000000000000000000000000000000009', 'Burn Address', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-26'), now(), 'burn_addresses', 'identifier')
    , ('ethereum', '0x1111111111111111111111111111111111111111', 'Burn Address', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-26'), now(), 'burn_addresses', 'identifier')
    , ('ethereum', '0x2222222222222222222222222222222222222222', 'Burn Address', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-26'), now(), 'burn_addresses', 'identifier')
    , ('ethereum', '0x3333333333333333333333333333333333333333', 'Burn Address', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-26'), now(), 'burn_addresses', 'identifier')
    , ('ethereum', '0x4444444444444444444444444444444444444444', 'Burn Address', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-26'), now(), 'burn_addresses', 'identifier')
    , ('ethereum', '0x5555555555555555555555555555555555555555', 'Burn Address', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-26'), now(), 'burn_addresses', 'identifier')
    , ('ethereum', '0x6666666666666666666666666666666666666666', 'Burn Address', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-26'), now(), 'burn_addresses', 'identifier')
    , ('ethereum', '0x7777777777777777777777777777777777777777', 'Burn Address', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-26'), now(), 'burn_addresses', 'identifier')
    , ('ethereum', '0x8888888888888888888888888888888888888888', 'Burn Address', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-26'), now(), 'burn_addresses', 'identifier')
    , ('ethereum', '0x9999999999999999999999999999999999999999', 'Burn Address', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-26'), now(), 'burn_addresses', 'identifier')
    , ('ethereum', '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'Burn Address', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-26'), now(), 'burn_addresses', 'identifier')
    , ('ethereum', '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 'Burn Address', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-26'), now(), 'burn_addresses', 'identifier')
    , ('ethereum', '0xffffffffffffffffffffffffffffffffffffffff', 'Burn Address', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-26'), now(), 'burn_addresses', 'identifier')
    , ('ethereum', '0x000000000000000000000000000000000000dead', 'Burn Address', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-26'), now(), 'burn_addresses', 'identifier')
    , ('ethereum', '0x00000000000000000000045261d4ee77acdb3286', 'Burn Address', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-26'), now(), 'burn_addresses', 'identifier')
    , ('ethereum', '0xdead000000000000000042069420694206942069', 'Burn Address', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-26'), now(), 'burn_addresses', 'identifier')
    , ('ethereum', '0x0123456789012345678901234567890123456789', 'Burn Address', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-26'), now(), 'burn_addresses', 'identifier')
    , ('ethereum', '0x1234567890123456789012345678901234567890', 'Burn Address', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-26'), now(), 'burn_addresses', 'identifier')
    ) AS temp_table (blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type)
;