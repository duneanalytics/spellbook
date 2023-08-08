{{ config(
    tags=['legacy'],
    alias = alias('trades', legacy_model=True),
    post_hook='{{ expose_spells(\'["bnb","ethereum"]\',
                                "project",
                                "maverick",
                                \'["gte620v", "chef_seaweed"]\') }}'
    )
}}

{% set maverick_models = [
    ref('maverick_v1_ethereum_trades_legacy')
,   ref('maverick_v1_bnb_trades_legacy')    
] %}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 
    1
