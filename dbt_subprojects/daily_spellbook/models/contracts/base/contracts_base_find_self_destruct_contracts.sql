 {{
  config(
        schema = 'contracts_base',
        alias = 'find_self_destruct_contracts',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['blockchain', 'contract_address']
  )
}}

{{find_self_destruct_contracts(
    chain='base'
)}}