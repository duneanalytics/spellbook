 {{
  config(
        alias = alias('predeploys',legacy_model=True),
        tags = ['legacy']
  )
}}


select 
   1 as trace_creator_address
  ,1 as contract_address
  ,1 as contract_project
  ,1 as contract_name
  ,1 as creator_address
  ,1 as created_time
  ,1 as  contract_creator_if_factory
  ,1 as is_self_destruct
  ,1 as creation_tx_hash
  ,1 as source
