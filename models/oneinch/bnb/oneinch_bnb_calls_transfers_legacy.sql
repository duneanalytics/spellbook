{{ config( 
    schema = 'oneinch_bnb',
    alias = alias('calls_transfers', legacy_model=True),
    tags = ['legacy']
    )
}}


-- DUMMY TABLE, WILL BE REMOVED SOON
select 
    1