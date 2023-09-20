{{ config( 
  schema = 'transfers_bnb_bnb',
  alias = alias('bnb_rolling_day', legacy_model=True),
  tags = ['legacy']
  )
}}

SELECT 
1 as dummy 