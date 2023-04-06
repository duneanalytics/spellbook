{{config(alias='cex_fantom',
        post_hook='{{ expose_spells(\'["fantom"]\',
                                    "sector",
                                    "labels",
                                    \'["Henrystats"]\') }}')}}

SELECT blockchain, lower(address) as address, name, category, contributor, source, created_at, updated_at, model_name, label_type
FROM (VALUES
    -- Source https://ftmscan.com/accounts/label/exchange
    ('fantom', '0x8e1701cfd85258ddb8dfe89bc4c7350822b9601d', 'MEXC: Hot Wallet', 'institution', 'Henrystats', 'static', timestamp('2023-01-27'), now(), 'cex_fantom', 'identifier')
    -- Bitget, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/bitget_address.txt
    , ('fantom', '0x5bdf85216ec1e38d6458c870992a69e38e03f7ef', 'Bitget', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_fantom', 'identifier')
    -- Gate.io, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/gate-io_address.txt
    , ('fantom', '0x0d0707963952f2fba59dd06f2b425ace40b492fe', 'Gate.io 1', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_fantom', 'identifier')
    , ('fantom', '0x1c4b70a3968436b9a0a9cf5205c787eb81bb558c', 'Gate.io 2', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_fantom', 'identifier')
    ) AS x (blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type)