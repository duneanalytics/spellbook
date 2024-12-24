{{ config(
        schema='dune',
        alias = 'blockchains',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["dune"]\',
                                    "sector",
                                    "dune",
                                    \'["0xRob"]\') }}')
}}
select * from
{{ source("dune", "result_blockchains", database="dune") }}
