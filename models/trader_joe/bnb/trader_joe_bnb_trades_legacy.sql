{{ 
    config(
        alias = alias('trades', legacy_model=True),
        tags=['legacy']
    )
}}

{% set trader_joe_models = [
    ref('trader_joe_v2_bnb_trades_legacy')
,   ref('trader_joe_v2_1_bnb_trades_legacy')  
] %}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 
    1