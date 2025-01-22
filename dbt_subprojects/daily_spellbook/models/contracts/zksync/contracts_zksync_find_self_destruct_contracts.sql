 {{
  config(
        schema = 'contracts_zksync',
        alias = 'find_self_destruct_contracts',
        materialized ='incremental',
        file_format ='delta',
        unique_key = ['blockchain', 'contract_address'],
        incremental_strategy='merge'
  )
}}

{{find_self_destruct_contracts(
    chain='zksync', days_forward=183
)}}