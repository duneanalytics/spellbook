
{{ config(
    tags=['legacy'],
        alias = alias('trades', legacy_model=True),
        post_hook='{{ expose_spells(\'["bnb"]\',
                                "project",
                                "biswap",
                                \'["codingsh", "chef_seaweed"]\') }}'
        )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 
    1