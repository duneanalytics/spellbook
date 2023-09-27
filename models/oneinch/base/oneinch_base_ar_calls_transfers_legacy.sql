{{ 
    config( 
        schema = 'oneinch_base',
        alias = alias('ar_calls_transfers', legacy_model=True),
        tags = ['legacy']
    )
}}


-- DUMMY TABLE, WILL BE REMOVED SOON
select 
    1