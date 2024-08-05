{{config(
        tags = ['static'],
        schema = 'cex_injective',
        alias = 'addresses',
        post_hook='{{ expose_spells(\'["injective"]\',
                                    "sector",
                                    "cex",
                                    \'["hildobby"]\') }}')}}

SELECT blockchain, address, cex_name, distinct_name, added_by, added_date
FROM (VALUES
    ('injective', 'inj1uyc234cek2ja9ru7a870cmx2lcavt5um2nk6hh', 'LAToken', 'LAToken 1', 'hildobby', date '2024-04-20')
    ) AS x (blockchain, address, cex_name, distinct_name, added_by, added_date)
