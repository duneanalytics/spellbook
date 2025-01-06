{{ config(
        schema='dune',
        alias = 'blockchains',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["dune"]\',
                                    "sector",
                                    "dune",
                                    \'["0xRob"]\') }}')
}}

-- because the graphql endpoint often returns http error 403 when querying through http_get(),
-- we store the results in a matview instead and expose that one here with a view on top.
-- matview: https://dune.com/queries/4467416
select * from
{{ source("dune", "result_blockchains", database="dune") }}
