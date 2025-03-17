{{ config(
        schema='dune',
        alias = 'blockchains',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["dune"]\',
                                    "sector",
                                    "dune",
                                    \'["0xRob"]\') }}')
}}


select 
        name
        ,display_name
        ,chain_id
        ,protocol
        ,token_address
        ,token_symbol
        ,token_decimals
from {{ source("dune", "dataset_core_blockchains", database="dune") }}