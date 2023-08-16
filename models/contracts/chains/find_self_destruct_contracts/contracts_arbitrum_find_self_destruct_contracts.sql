 {{
  config(
        tags = ['dunesql'],
        alias = alias('find_self_destruct_contracts_arbitrum'),
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='contract_address'
  )
}}

{{find_self_destruct_contracts_by_chain(
    chain='arbitrum'
)}}