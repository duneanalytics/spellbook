 {{
  config(
	tags=['legacy'],
	
        alias = alias('self_destruct_contracts', legacy_model=True)
  )
}}


SELECT
1 as created_time
, 1 as created_block_number
, 1 as creation_tx_hash
, 1 as contract_address
, 1 as trace_element