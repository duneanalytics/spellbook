{{config(alias='cex_fantom',
        post_hook='{{ expose_spells(\'["fantom"]\',
                                    "sector",
                                    "labels",
                                    \'["Henrystats"]\') }}')}}

SELECT blockchain, lower(address) as address, name, category, contributor, source, created_at, updated_at
FROM (VALUES
    -- Source https://ftmscan.com/accounts/label/exchange
    (array('fantom'), '0x8e1701cfd85258ddb8dfe89bc4c7350822b9601d', 'MEXC: Hot Wallet', 'cex', 'Henrystats', 'static', timestamp('2023-01-27'), now())
    ) AS x (blockchain, address, name, category, contributor, source, created_at, updated_at)