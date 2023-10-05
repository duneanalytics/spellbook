{{ config( 
    schema='prices_celo',
    alias = alias('tokens', legacy_model=True),
    tags = ['legacy']
  )
}}

-- TODO: Remove Dummy Table
SELECT
  1
