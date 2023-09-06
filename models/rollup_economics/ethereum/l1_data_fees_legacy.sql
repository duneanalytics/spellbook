{{ config( 
    schema = 'rollup_economics_ethereum',
    alias = alias('l1_data_fees', legacy_model=True),
    tags = ['legacy']
    )
}}


-- DUMMY TABLE, WILL BE REMOVED SOON
select 
    1