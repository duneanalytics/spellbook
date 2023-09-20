{{ config( 
  schema = 'transfers_bnb_bnb',
  alias = alias('bnb', legacy_model=True),
  tags = ['legacy']
  )
}}

SELECT 
1 as dummy 