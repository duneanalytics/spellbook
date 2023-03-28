{{config(alias='cex_fantom',
        post_hook='{{ expose_spells(\'["fantom"]\',
                                    "sector",
                                    "labels",
                                    \'["Henrystats"]\') }}')}}

SELECT blockchain, lower(address) as address, name, category, contributor, source, created_at, updated_at, model_name, label_type
FROM (VALUES
    -- Source https://ftmscan.com/accounts/label/exchange
    ('fantom', '0x8e1701cfd85258ddb8dfe89bc4c7350822b9601d', 'MEXC: Hot Wallet', 'institution', 'Henrystats', 'static', timestamp('2023-01-27'), now(), 'cex_fantom', 'identifier')
    ) AS x (blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type)