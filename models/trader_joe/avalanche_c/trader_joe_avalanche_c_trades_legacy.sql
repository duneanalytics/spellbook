{{ config(
	tags=['legacy'],
        alias = alias('trades', legacy_model=True)
        )
}}

{% set trader_joe_models = [
ref('trader_joe_v1_avalanche_c_trades_legacy')
,ref('trader_joe_v2_avalanche_c_trades_legacy')
] %}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 
    1

