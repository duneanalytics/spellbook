{{ config(alias = alias('nft_curated', legacy_model=True), tags=['static', 'legacy']) }}

SELECT
  LOWER(contract_address) AS contract_address, name, '' as symbol
FROM
  (VALUES
 ('0xdb46d1dc155634fbc732f92e853b10b288ad5a1d',	'Lens Protocol Profiles')
,('0x9d305a42a3975ee4c1c57555bed5919889dce63f',	'Sandboxs LANDs')


) as temp_table (contract_address, name)
