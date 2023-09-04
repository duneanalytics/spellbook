{{ config( 
  schema = 'tokemak_ethereum',
  alias = alias('lookup_tokens', legacy_model=True),
  tags = ['legacy']
  )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 
  1 as dummy