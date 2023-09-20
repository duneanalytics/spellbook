{{ config( 
  schema = 'transfers_bnb_bnb',
  alias = alias('bnb_agg_hour', legacy_model=True),
  tags = ['legacy']
  )
}}

SELECT 
1 as dummy 