{{config(alias='cex_ethereum',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby"]\') }}')}}

SELECT blockchain, address, name, category, contributor, source, created_at, updated_at
FROM (VALUES
    -- Binance, Source: https://etherscan.io/accounts/label/binance
    (array('ethereum'),'0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be', 'Binance 1', 'cex', 'hildobby', 'static', timestamp('2022-08-28'), now())
    , (array('ethereum'), '0xd551234ae421e3bcba99a0da6d736074f22192ff', 'Binance 2', 'cex', 'hildobby', 'static', timestamp('2022-08-28'), now())
   ) AS x (blockchain, address, name, category, contributor, source, created_at, updated_at)