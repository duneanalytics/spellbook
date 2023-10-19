{{ config( 
  alias = alias('ocr_operator_node_meta', legacy_model=True),
  tags = ['legacy']
  )
}}

-- TODO: Remove Dummy Table
SELECT
  1
