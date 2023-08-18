 {{
  config(
        tags = ['dunesql'],
        alias = alias('contract_goerli_creator_project_mapping'),
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='contract_address',
        partition_by = ['block_month']
  )
}}

{{contract_creator_project_mapping_by_chain(
    chain='goerli'
)}}