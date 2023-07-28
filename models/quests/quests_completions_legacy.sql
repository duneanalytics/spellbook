{{ config( 
    schema = 'quests',
    alias = alias('completions', legacy_model=True),
    tags = ['legacy']
    )
}}


-- DUMMY TABLE, WILL BE REMOVED SOON
select 
    1