{{ config(
        schema='lido_lrt_liquidity_base',
        alias = 'tokens',
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["base"]\',
                                "project",
                                "lido_lrt_liquidity",
                                \'["pipistrella"]\') }}'
        )
}}

SELECT * FROM (
values    (0x04C0599Ae5A44757c0af6F9eC3b93da8976c150A, 'weETH', 'base', 'ether.fi')
        , (0x2416092f143378750bb29b79eD961ab195CcEea5, 'ezETH', 'base', 'renzo')
        , (0xEDfa23602D0EC14714057867A78d01e94176BEA0, 'rsETH',  'base', 'kelp')
     
)x(address, symbol, blockchain, project)