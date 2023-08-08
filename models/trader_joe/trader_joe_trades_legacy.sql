{{ config(
	tags=['legacy'],
        alias = alias('trades', legacy_model=True),
        post_hook='{{ expose_spells(\'["avalanche_c","bnb"]\',
                                "project",
                                "trader_joe",
                                \'["jeff-dude","mtitus6","Henrystats","hsrvc"]\') }}'
        )
}}

{% set trader_joe_models = [
    ref('trader_joe_avalanche_c_trades_legacy')
,   ref('trader_joe_bnb_trades_legacy')
] %}


-- DUMMY TABLE, WILL BE REMOVED SOON
select 
    1