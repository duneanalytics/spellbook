{{config(alias='cex_avalanche_c',
        post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby"]\') }}')}}

SELECT blockchain, lower(address) as address, name, category, contributor, source, created_at, updated_at, model_name, label_type
FROM (VALUES
    -- Source: https://snowtrace.io/accounts/label/exchange
    ('avalanche_c', '0x14aa1ad09664c33679ae5689d93085b8f7c84bd3', 'Coinsquare', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_avalanche_c', 'identifier')
    , ('avalanche_c', '0xffb3118124cdaebd9095fa9a479895042018cac2', 'MEXC', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_avalanche_c', 'identifier')
    -- Source: https://snowtrace.io/accounts/label/binance
    , ('avalanche_c', '0x9f8c163cba728e99993abe7495f06c0a3c8ac8b9', 'Binance', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_avalanche_c', 'identifier')
    -- Source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/bitfinex_address.txt
    , ('avalanche_c', '0x77134cbc06cb00b66f4c7e623d5fdbf6777635ec', 'Bitfinex 1', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_avalanche_c', 'identifier')
    , ('avalanche_c', '0x742d35cc6634c0532925a3b844bc454e4438f44e', 'Bitfinex 2', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_avalanche_c', 'identifier')
    -- Bitget, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/bitget_address.txt
    , ('avalanche_c', '0x0639556f03714a74a5feeaf5736a4a64ff70d206', 'Bitget 1', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_avalanche_c', 'identifier')
    , ('avalanche_c', '0x5bdf85216ec1e38d6458c870992a69e38e03f7ef', 'Bitget 2', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_avalanche_c', 'identifier')
    -- Bybit, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/bybit_address.txt
    , ('avalanche_c', '0xf89d7b9c864f589bbf53a82105107622b35eaa40', 'Bybit 1', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_avalanche_c', 'identifier')
    -- Gate.io, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/gate-io_address.txt
    , ('avalanche_c', '0x0d0707963952f2fba59dd06f2b425ace40b492fe', 'Gate.io 1', 'institution', 'hildobby', 'static', timestamp('2022-11-14'), now(), 'cex_avalanche_c', 'identifier')
    , ('avalanche_c', '0x1c4b70a3968436b9a0a9cf5205c787eb81bb558c', 'Gate.io 2', 'institution', 'hildobby', 'static', timestamp('2022-11-14'), now(), 'cex_avalanche_c', 'identifier')
    , ('avalanche_c', '0xd793281182a0e3e023116004778f45c29fc14f19', 'Gate.io 3', 'institution', 'hildobby', 'static', timestamp('2022-11-14'), now(), 'cex_avalanche_c', 'identifier')
    , ('avalanche_c', '0xc882b111a75c0c657fc507c04fbfcd2cc984f071', 'Gate.io 4', 'institution', 'hildobby', 'static', timestamp('2022-11-14'), now(), 'cex_avalanche_c', 'identifier')
    -- Huobi, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/huobi_address.txt
    , ('avalanche_c', '0xe195b82df6a797551eb1acd506e892531824af27', 'Huobi', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_avalanche_c', 'identifier')
    -- OKX, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/okx_address.txt
    , ('avalanche_c', '0x7e4aa755550152a522d9578621ea22edab204308', 'OKX', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_avalanche_c', 'identifier')
    ) AS x (blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type)