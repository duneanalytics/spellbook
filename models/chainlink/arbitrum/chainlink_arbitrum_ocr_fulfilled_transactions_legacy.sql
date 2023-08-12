{{ config( 
  alias = alias('ocr_fulfilled_transactions', legacy_model=True),
  tags = ['legacy']
  )
}}

-- TODO: Remove Dummy Table
SELECT
  1