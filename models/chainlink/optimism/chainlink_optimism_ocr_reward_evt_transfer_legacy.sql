{{ config( 
  alias = alias('ocr_reward_evt_transfer', legacy_model=True),
  tags = ['legacy']
  )
}}

-- TODO: Remove Dummy Table
SELECT
  1
