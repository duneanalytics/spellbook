{{ config(
    tags=['legacy'],
    alias = alias('trades', legacy_model=True),
    post_hook='{{ expose_spells(\'["bnb","ethereum"]\',
                                "project",
                                "maverick",
                                \'["gte620v", "chef_seaweed"]\') }}'
    )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 
    1
