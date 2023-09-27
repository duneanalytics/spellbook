{{ config( 
  schema = 'transfers_bnb',
  alias = alias('bnb_rolling_hour', legacy_model=True),
  tags = ['legacy']
  )
}}

SELECT 
1 as dummy 