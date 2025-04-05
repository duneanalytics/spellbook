{{
    config(
        schema = 'tokens_bob'
        , alias = 'erc20_stablecoins'
        , tags=['static']
        , post_hook='{{
        expose_spells(
            \'["bob"]\',
            "sector",
            "tokens_bob",
            \'["mastilver"]\'
        )
        }}'
        , unique_key = ['contract_address']
    )
}}

SELECT blockchain, contract_address, backing, symbol, decimals, name
FROM (VALUES
    ('bob', 0x6c851f501a3f24e29a8e39a29591cddf09369080, 'Crypto-backed stablecoin', 'DAI', 18, 'Dai Stablecoin'),
    ('bob', 0xf3107eec1e6f067552c035fd87199e1a5169cb20, 'Crypto-backed stablecoin','DLLR', 18, 'Sovryn DLLR'),
    ('bob', 0xc4a20a608616f18aa631316eeda9fb62d089361e, 'Hybrid stablecoin', 'FRAX', 18, 'FRAX'),
    ('bob', 0xe75d0fb2c24a55ca1e3f96781a2bcc7bdba058f0, 'Fiat-backed stablecoin', 'USDC', 6, 'USD Coin'),
    ('bob', 0x05d032ac25d322df992303dca074ee7392c117b9, 'Fiat-backed stablecoin', 'USDT', 6, 'Tether USD')
) AS temp_table (blockchain, contract_address, backing, symbol, decimals, name)
