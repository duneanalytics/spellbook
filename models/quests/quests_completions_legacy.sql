{{ config( 
    schema = 'quests',
    alias = alias('completions', legacy_model=True),
    tags = ['legacy']
    )
}}


-- DUMMY TABLE, WILL BE REMOVED SOON
-- fitting the same schema because of downstream tables (yolo)
select 
    '1' as blockchain,
    '1' as platform, 
    '0x' as quester_address, 
    1 as block_number, 
    timestamp '2023-01-01' as block_time, 
    '1' as quest_name, 
    '0x' as token_address,
    '1' as token_id
