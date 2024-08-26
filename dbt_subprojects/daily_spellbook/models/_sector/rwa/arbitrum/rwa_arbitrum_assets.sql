
{{
    config(
        schema = 'rwa_arbitrum'
        ,alias = 'assets'
        ,materialized = 'table'
        ,post_hook='{{ expose_spells(\'["arbitrum"]\',
                        "sector",
                        "rwa",
                        \'["maybeYonas"]\') }}'
    )
}}

select  
    token_contract,
    symbol,
    protocol,
    type
from (values
    (0x59D9356E565Ab3A36dD77763Fc0d87fEaf85508C, 'USDM'     , 'Mountain Protocol'   , 'RWA'         ),
    (0xfc90518D5136585ba45e34ED5E1D108BD3950CFa, 'USD+'     , 'Dinari'              , 'RWA'         ),
    (0x9d2f299715d94d8a7e6f5eaa8e654e8c74a988a7, 'FXS'      , 'Frax Finance'        , 'Governance'  )
) as t(
    token_contract,
    symbol,
    protocol,
    type
)