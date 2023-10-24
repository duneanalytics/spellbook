 {{
  config(
        
        schema = 'contracts_ethereum',
        alias = 'find_self_destruct_contracts',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='contract_address'
  )
}}

{{find_self_destruct_contracts_by_chain(
    chain='ethereum'
)}}