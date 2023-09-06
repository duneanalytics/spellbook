 {{
  config(
        tags = ['legacy'],
        schema = 'contracts_optimism',
        alias = alias('find_self_destruct_contracts', legacy_model=True)
  )
}}

SELECT 1