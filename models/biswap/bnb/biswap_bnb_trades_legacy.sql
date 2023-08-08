
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
    ref('biswap_v2_bnb_trades')
,   ref('biswap_v3_bnb_trades')
] %}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 
    1