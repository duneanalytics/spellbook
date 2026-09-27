 {{
  config(
        tags = ['prod_exclude', 'static'],
        schema = 'contracts_zora',
        alias = 'find_self_destruct_contracts',
        materialized ='table',
        file_format ='delta',
        unique_key = ['blockchain', 'contract_address'],
        incremental_strategy='merge'
  )
}}

{{find_self_destruct_contracts(
    chain='zora'
)}}