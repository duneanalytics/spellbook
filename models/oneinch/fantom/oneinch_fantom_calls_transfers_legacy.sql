{{ 
    config( 
        schema = 'oneinch_fantom',
        alias = alias('calls_transfers', legacy_model=True),
        tags = ['legacy']
    )
}}


-- DUMMY TABLE, WILL BE REMOVED SOON
select 
    1