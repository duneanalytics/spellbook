 {{
  config(
        tags = ['legacy'],
        schema = 'contracts',
        alias = alias('daily_aggregated_usage',legacy_model=True)
  )
}}

SELECT 1