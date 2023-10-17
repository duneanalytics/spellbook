 {{
  config(
        tags = ['dunesql', 'prod_daily'],
        schema = 'contracts_ethereum',
        alias = alias('find_self_destruct_contracts'),
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.created_time')],
        unique_key='contract_address'
  )
}}

{{find_self_destruct_contracts_by_chain(
    chain='ethereum'
)}}