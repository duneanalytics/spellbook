 {{
  config(
        schema = 'contracts_optimism',
        alias = 'find_self_destruct_contracts',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge'
  )
}}

{{find_self_destruct_contracts(
    chain='optimism'
)}}