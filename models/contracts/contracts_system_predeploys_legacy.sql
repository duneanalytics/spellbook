 {{
  config(
        alias = alias('system_predeploys',legacy_model=True),
		tags = ['legacy']
  )
}}

-- https://github.com/ethereum-optimism/optimism/blob/c93958755b4f6ab7f95cc0b2459f39ca95c06684/specs/predeploys.md?plain=1#L48
SELECT
	1 as contract_name
	, 1 as contract_address