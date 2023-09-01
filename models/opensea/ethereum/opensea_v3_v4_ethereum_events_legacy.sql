 {{
  config(
        tags = ['legacy'],
        schema = 'opensea_v3_v4_ethereum',
        alias = alias('events',legacy_model=True)
  )
}}

SELECT 1