{{config(alias='cex_arbitrum',
        post_hook='{{ expose_spells(\'["arbitrum"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby"]\') }}')}}

SELECT blockchain, lower(address) as address, name, category, contributor, source, created_at, updated_at, model_name, label_type
FROM (VALUES
    -- Bitget, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/bitget_address.txt
    ('arbitrum', '0x0639556f03714a74a5feeaf5736a4a64ff70d206', 'Bitget 1', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_arbitrum', 'identifier')
    , ('arbitrum', '0x97b9d2102a9a65a26e1ee82d59e42d1b73b68689', 'Bitget 2', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_arbitrum', 'identifier')
    -- Bybit, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/bitget_address.txt
    , ('arbitrum', '0xf89d7b9c864f589bbf53a82105107622b35eaa40', 'Bybit 1', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_arbitrum', 'identifier')
    -- Gate.io, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/gate-io_address.txt
    ('arbitrum', '0x0d0707963952f2fba59dd06f2b425ace40b492fe', 'Gate.io 1', 'institution', 'hildobby', 'static', timestamp('2022-11-14'), now(), 'cex_arbitrum', 'identifier')
    ('arbitrum', '0x1c4b70a3968436b9a0a9cf5205c787eb81bb558c', 'Gate.io 2', 'institution', 'hildobby', 'static', timestamp('2022-11-14'), now(), 'cex_arbitrum', 'identifier')
    ('arbitrum', '0xd793281182a0e3e023116004778f45c29fc14f19', 'Gate.io 3', 'institution', 'hildobby', 'static', timestamp('2022-11-14'), now(), 'cex_arbitrum', 'identifier')
    ('arbitrum', '0xc882b111a75c0c657fc507c04fbfcd2cc984f071', 'Gate.io 4', 'institution', 'hildobby', 'static', timestamp('2022-11-14'), now(), 'cex_arbitrum', 'identifier')
    -- KuCoin, source: https://github.com/js-kingdata/indicators_factory/blob/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/kucoin_address.txt
    ('arbitrum', '0xd6216fc19db775df9774a6e33526131da7d19a2c', 'KuCoin 1', 'institution', 'hildobby', 'static', timestamp('2022-11-14'), now(), 'cex_arbitrum', 'identifier')
    ('arbitrum', '0x03e6fa590cadcf15a38e86158e9b3d06ff3399ba', 'KuCoin 2', 'institution', 'hildobby', 'static', timestamp('2022-11-14'), now(), 'cex_arbitrum', 'identifier')
    ('arbitrum', '0xf3f094484ec6901ffc9681bcb808b96bafd0b8a8', 'KuCoin 3', 'institution', 'hildobby', 'static', timestamp('2022-11-14'), now(), 'cex_arbitrum', 'identifier')
    -- OKX, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/okx_address.txt
    ('arbitrum', '0x62383739d68dd0f844103db8dfb05a7eded5bbe6', 'OKX 1', 'institution', 'hildobby', 'static', timestamp('2022-11-14'), now(), 'cex_arbitrum', 'identifier')
    ) AS x (blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type)