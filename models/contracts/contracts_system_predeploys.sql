 {{
  config(
	tags = ['static'],
	schema = 'contracts',
        alias = 'system_predeploys',
        post_hook='{{ expose_spells(\'["ethereum", "optimism", "arbitrum", "avalanche_c", "polygon", "bnb", "gnosis", "fantom", "base", "goerli"]\',
                                    "sector",
                                    "contracts",
                                    \'["msilb7", "chuxin"]\') }}'
  )
}}

-- https://github.com/ethereum-optimism/optimism/blob/c93958755b4f6ab7f95cc0b2459f39ca95c06684/specs/predeploys.md?plain=1#L48
SELECT contract_name, contract_address, 'optimism' AS blockchain
FROM (values
	 ('LegacyMessagePasser',			0x4200000000000000000000000000000000000000)
	,('DeployerWhitelist',				0x4200000000000000000000000000000000000002)
	,('LegacyERC20ETH',				0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000)
	,('WETH9',					0x4200000000000000000000000000000000000006)
	,('L2CrossDomainMessenger',			0x4200000000000000000000000000000000000007)
	,('L2StandardBridge',				0x4200000000000000000000000000000000000010)
	,('SequencerFeeVault',				0x4200000000000000000000000000000000000011)
	,('OptimismMintableERC20Factory', 		0x4200000000000000000000000000000000000012)
	,('L1BlockNumber',				0x4200000000000000000000000000000000000013)
	,('GasPriceOracle',				0x420000000000000000000000000000000000000F)
	,('GovernanceToken',				0x4200000000000000000000000000000000000042)
	,('L1Block',					0x4200000000000000000000000000000000000015)
	,('L2ToL1MessagePasser',			0x4200000000000000000000000000000000000016)
	,('L2ERC721Bridge',				0x4200000000000000000000000000000000000014)
	,('OptimismMintableERC721Factory',  		0x4200000000000000000000000000000000000017)
	,('ProxyAdmin',					0x4200000000000000000000000000000000000018)
	,('BaseFeeVault',				0x4200000000000000000000000000000000000019)
	,('L1FeeVault',					0x420000000000000000000000000000000000001a)

) a (contract_name, contract_address)

-- UNION ALL other chains