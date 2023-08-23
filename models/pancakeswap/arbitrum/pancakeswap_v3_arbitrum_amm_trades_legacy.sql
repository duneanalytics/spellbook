{{ config(
    schema = 'pancakeswap_v3_arbitrum',
    alias = alias('amm_trades', legacy_model=True),
    tags=['legacy']
    )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 
    1