{{ config( 
  alias = alias('ocr_request_daily', legacy_model=True),
  tags = ['legacy']
  )
}}

-- TODO: Remove Dummy Table
SELECT
  1
