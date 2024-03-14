{{ config(
        schema='tokens',
        alias = 'days',
        materialized = 'table',
        file_format = 'delta',
        post_hook = '{{ expose_spells(\'["ethereum", "solana", "arbitrum", "base", "gnosis", "optimism", "bnb", "avalanche_c", "polygon", "scroll", "zksync"]\',
                                    "sector",
                                    "prices",
                                    \'["aalan3"]\') }}'
        )
}}

select *
from unnest(
        sequence(cast('2010-01-01' as date)
        , date(date_trunc('day',now()))
        , interval '1' day
        )
        ) as foo(day)
