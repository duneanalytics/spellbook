{{ config( 
  alias = alias('ocr_gas_daily', legacy_model=True),
  tags = ['legacy']
  )
}}

-- TODO: Remove Dummy Table
SELECT
  1
