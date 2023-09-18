{{ 
    config( 
        schema = 'oneinch_ethereum',
        alias = alias('ar_calls_transfers', legacy_model=True),
        tags = ['legacy']
    )
}}


-- DUMMY TABLE, WILL BE REMOVED SOON
select 
    1