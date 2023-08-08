
{{ config(
    tags=['legacy'],
        alias = alias('trades', legacy_model=True),
        post_hook='{{ expose_spells(\'["bnb"]\',
                                "project",
                                "biswap",
                                \'["codingsh", "chef_seaweed"]\') }}'
        )
}}

{% set biswap_models = [
    ref('biswap_v2_bnb_trades_legacy')
,   ref('biswap_v3_bnb_trades_legacy')
] %}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 
    1