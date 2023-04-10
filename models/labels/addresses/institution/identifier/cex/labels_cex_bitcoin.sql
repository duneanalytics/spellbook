{{config(alias='cex_bitcoin',
        post_hook='{{ expose_spells(\'["bitcoin"]\',
                                    "sector",
                                    "labels",
                                    \'["ilemi"]\') }}')}}

SELECT blockchain, lower(address) as address, name, category, contributor, source, created_at, updated_at, model_name, label_type
FROM (VALUES
    -- Binance, Source: https://etherscan.io/accounts/label/binance
    ('bitcoin','34xp4vRoCGJym3xR7yCVPFHoCNxv4Twseo', 'Binance 1', 'institution', 'ilemi', 'static', timestamp('2023-03-28'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '38ztxG7GL1LEEbC9gKpqEKEh7WZ3KDTLMi', 'Binance 2', 'institution', 'ilemi', 'static', timestamp('2023-03-28'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '399QCnqVzAt4HGU1SV7PfVPYovb1BX3u9Y', 'Binance 3', 'institution', 'ilemi', 'static', timestamp('2023-03-28'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '3HdGoUTbcztBnS7UzY4vSPYhwr424CiWAA', 'Binance 4', 'institution', 'ilemi', 'static', timestamp('2023-03-28'), now(), 'cex_bitcoin', 'identifier')
     ) AS x (blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type)