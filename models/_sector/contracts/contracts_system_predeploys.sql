 {{
  config(
	tags = ['static'],
	schema = 'contracts',
        alias = 'system_predeploys',
        post_hook='{{ expose_spells(\'["ethereum", "base", "optimism", "zora"]\',
                                    "sector",
                                    "contracts",
                                    \'["msilb7", "chuxin"]\') }}'
  )
}}

{% set op_chains = all_op_chains() %} --macro: all_op_chains.sql

-- https://github.com/ethereum-optimism/optimism/blob/c93958755b4f6ab7f95cc0b2459f39ca95c06684/specs/predeploys.md?plain=1#L48
WITH op_stack_predeploys AS (
	SELECT contract_project, contract_name, contract_address
	FROM (values
		 ('OVM',	'LegacyMessagePasser',			0x4200000000000000000000000000000000000000)
		,('OVM',	'DeployerWhitelist',				0x4200000000000000000000000000000000000002)
		,('OVM',	'LegacyERC20ETH',				0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000)
		,('OVM',	'WETH9',					0x4200000000000000000000000000000000000006)
		,('OVM',	'L2CrossDomainMessenger',			0x4200000000000000000000000000000000000007)
		,('OVM',	'L2StandardBridge',				0x4200000000000000000000000000000000000010)
		,('OVM',	'SequencerFeeVault',				0x4200000000000000000000000000000000000011)
		,('OVM',	'OptimismMintableERC20Factory', 		0x4200000000000000000000000000000000000012)
		,('OVM',	'L1BlockNumber',				0x4200000000000000000000000000000000000013)
		,('OVM',	'GasPriceOracle',				0x420000000000000000000000000000000000000F)
		,('OVM',	'GovernanceToken',				0x4200000000000000000000000000000000000042)
		,('OVM',	'L1Block',					0x4200000000000000000000000000000000000015)
		,('OVM',	'L2ToL1MessagePasser',			0x4200000000000000000000000000000000000016)
		,('OVM',	'L2ERC721Bridge',				0x4200000000000000000000000000000000000014)
		,('OVM',	'OptimismMintableERC721Factory',  		0x4200000000000000000000000000000000000017)
		,('OVM',	'ProxyAdmin',					0x4200000000000000000000000000000000000018)
		,('OVM',	'BaseFeeVault',				0x4200000000000000000000000000000000000019)
		,('OVM',	'L1FeeVault',					0x420000000000000000000000000000000000001a)
		---
		,('EAS',	'EAS',					0x4200000000000000000000000000000000000021)
		,('EAS',	'SchemaRegistry',				0x4200000000000000000000000000000000000020)

	) a (contract_project, contract_name, contract_address)

)

{% for chain in op_chains %} --op chain predeploys

	select 
		'{{chain}}' as blockchain
		, contract_project
		, contract_name
		, contract_address
		FROM op_stack_predeploys
	{% if not loop.last %}
	UNION ALL
	{% endif %}

{% endfor %}