{{ config(
	schema='lido_accounting_ethereum',
	alias='other_income',
	materialized='incremental',
	file_format='delta',
	incremental_strategy='merge',
	unique_key=['blockchain', 'period', 'evt_tx_hash', 'token', 'amount_token'],
	incremental_predicates=[incremental_predicate('DBT_INTERNAL_DEST.period')],
) }}

{% set project_start_date = '2020-12-17' %}

with tokens as (
	select
		*
	from
		(values
		(0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32)--LDO
		, (0x6B175474E89094C44Da98b954EedeAC495271d0F)--DAI
		, (0xdC035D45d973E3EC169d2276DDab16f1e407384F)--USDS
		, (0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD)--Savings USDS (sUSDS)
		, (0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48)--USDC
		, (0xdAC17F958D2ee523a2206206994597C13D831ec7)--USDT
		, (0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2)--WETH
		, (0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0)--MATIC
		, (0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84)--stETH
		, (0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0)--wstETH
		) as tokens(address)
)
, multisigs_list as (
	select
		*
	from
		(values
		(0x3e40d73eb977dc6a537af587d48316fee66e9c8c, 'Ethereum', 'Aragon')
		, (0x48F300bD3C52c7dA6aAbDE4B683dEB27d38B9ABb, 'Ethereum', 'FinanceOpsMsig')
		, (0x87D93d9B2C672bf9c9642d853a8682546a5012B5, 'Ethereum', 'LiquidityRewardsMsig')
		, (0x753D5167C31fBEB5b49624314d74A957Eb271709, 'Ethereum', 'LiquidityRewardMngr')--Curve Rewards Manager
		, (0x1dD909cDdF3dbe61aC08112dC0Fdf2Ab949f79D8, 'Ethereum', 'LiquidityRewardMngr')--Balancer Rewards Manager V1
		, (0x55c8De1Ac17C1A937293416C9BCe5789CbBf61d1, 'Ethereum', 'LiquidityRewardMngr')--Balancer Rewards Manager V2
		, (0x86F6c353A0965eB069cD7f4f91C1aFEf8C725551, 'Ethereum', 'LiquidityRewardMngr')--Balancer Rewards Manager V3
		, (0xf5436129Cf9d8fa2a1cb6e591347155276550635, 'Ethereum', 'LiquidityRewardMngr')--1inch Reward Manager
		, (0xE5576eB1dD4aA524D67Cf9a32C8742540252b6F4, 'Ethereum', 'LiquidityRewardMngr')--Sushi Reward Manager
		, (0x87D93d9B2C672bf9c9642d853a8682546a5012B5, 'Polygon', 'LiquidityRewardsMsig')
		, (0x9cd7477521B7d7E7F9e2F091D2eA0084e8AaA290, 'Ethereum', 'PolygonTeamRewardsMsig')
		, (0x5033823f27c5f977707b58f0351adcd732c955dd, 'Optimism', 'LiquidityRewardsMsig')
		, (0x8c2b8595ea1b627427efe4f29a64b145df439d16, 'Arbitrum', 'LiquidityRewardsMsig')
		, (0x13c6ef8d45afbccf15ec0701567cc9fad2b63ce8, 'Ethereum', 'ReferralRewardsMsig')--Solana Ref Prog Msig
		, (0x12a43b049A7D330cB8aEAB5113032D18AE9a9030, 'Ethereum', 'LegoMsig')
		, (0x9B1cebF7616f2BC73b47D226f90b01a7c9F86956, 'Ethereum', 'ATCMsig')
		, (0x17F6b2C738a63a8D3A113a228cfd0b373244633D, 'Ethereum', 'PMLMsig')
		, (0xde06d17db9295fa8c4082d4f73ff81592a3ac437, 'Ethereum', 'RCCMsig')
		, (0x834560f580764bc2e0b16925f8bf229bb00cb759, 'Ethereum', 'TRPMsig')
		, (0x606f77BF3dd6Ed9790D9771C7003f269a385D942, 'Ethereum', 'AllianceMsig')
		, (0x55897893c19e4B0c52731a3b7C689eC417005Ad6, 'Ethereum', 'EcosystemBORGMsig')
		, (0x95B521B4F55a447DB89f6a27f951713fC2035f3F, 'Ethereum', 'LabsBORGMsig')
		) as list(address, chain, name)
)
, diversifications_addresses as (
	select
		*
	from
		(values
    (0x489f04eeff0ba8441d42736549a1f1d6cca74775, '1round_1'),
    (0x689e03565e36b034eccf12d182c3dc38b2bb7d33, '1round_2'),
    (0xA9b2F5ce3aAE7374a62313473a74C98baa7fa70E, '2round')
    ) as list(address, name)
)

, intermediate_addresses as (
    select * from  (values
    (0xe3224542066d3bbc02bc3d70b641be4bc6f40e36, 'Jumpgate(Solana)'),
    (0x40ec5b33f54e0e8a33a975908c5ba1c14e5bbbdf, 'Polygon bridge'),
    (0xa3a7b6f88361f48403514059f1f16c8e78d60eec, 'Arbitrum bridge'),
    (0x99c9fc46f92e8a1c0dec1b1747d010903e884be1, 'Optimism bridge'),
    (0x9de443adc5a411e83f1878ef24c3f52c61571e72, 'Base bridge'),
    (0x41527B2d03844dB6b0945f25702cB958b6d55989, 'zkSync bridge'),
    (0xb948a93827d68a82F6513Ad178964Da487fe2BD9, 'BnB bridge'),
    (0x051F1D88f0aF5763fB888eC4378b4D8B29ea3319, 'Linea bridge'),
    (0x2D001d79E5aF5F65a939781FE228B267a8Ed468B, 'Mantle bridge'),
    (0x6625C6332c9F91F2D27c304E729B86db87A3f504, 'Scroll bridge'),
    (0x0914d4ccc4154ca864637b0b653bc5fd5e1d3ecf, 'AnySwap bridge (Polkadot, Kusama)'),
    (0x3ee18b2214aff97000d974cf647e7c347e8fa585, 'Wormhole bridge'), --Solana, Terra
    (0x9ee91F9f426fA633d227f7a9b000E28b9dfd8599, 'stMatic Contract'),

    (0xd0A61F2963622e992e6534bde4D52fd0a89F39E0, 'Spark PSM'),
    (0x37305B1cD40574E4C5Ce33f8e8306Be057fD7341, 'Sky PSM')

    ) as list(address, name)
)

, ldo_referral_payments_addr as (
    select * from  (values
    (0x558247e365be655f9144e1a0140d793984372ef3),
    (0x6DC9657C2D90D57cADfFB64239242d06e6103E43),
    (0xDB2364dD1b1A733A690Bf6fA44d7Dd48ad6707Cd),
    (0x586b9b2F8010b284A0197f392156f1A7Eb5e86e9),
    (0xC976903918A0AF01366B31d97234C524130fc8B1),
    (0x53773e034d9784153471813dacaff53dbbb78e8c),
    (0x883f91D6F3090EA26E96211423905F160A9CA01d),
    (0xf6502Ea7E9B341702609730583F2BcAB3c1dC041),
    (0x82AF9d2Ea81810582657f6DC04B1d7d0D573F616),
    (0x351806B55e93A8Bcb47Be3ACAF71584deDEaB324),
    (0x9e2b6378ee8ad2A4A95Fe481d63CAba8FB0EBBF9),
    (0xaf8aE6955d07776aB690e565Ba6Fbc79B8dE3a5d) --rhino
    ) as list(address)
)


, dai_referral_payments_addr as (
	select
		_recipient as address
	from
		{{ source('lido_ethereum', 'AllowedRecipientsRegistry_evt_RecipientAdded') }}
	union all
	select
		0xaf8aE6955d07776aB690e565Ba6Fbc79B8dE3a5d
)

, steth_referral_payments_addr as (
	select
		_recipient as address
	from
		{{ source('lido_ethereum', 'AllowedRecipientsRegistry_RevShare_evt_RecipientAdded') }}
)

, stonks as (
    select * from (values 
    ('STETH→DAI', 0x3e2D251275A92a8169A3B17A2C49016e2de492a7, 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84, 0x6B175474E89094C44Da98b954EedeAC495271d0F),
    ('STETH→USDC', 0xf4F6A03E3dbf0aA22083be80fDD340943d275Ea5, 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84, 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48),
    ('STETH→USDT', 0x7C2a1E25cA6D778eCaEBC8549371062487846aAF, 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84, 0xdAC17F958D2ee523a2206206994597C13D831ec7),
    ('DAI→USDC', 0x79f5E20996abE9f6a48AF6f9b13f1E55AED6f06D, 0x6B175474E89094C44Da98b954EedeAC495271d0F, 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48),
    ('DAI→USDT', 0x8Ba6D367D15Ebc52f3eBBdb4a8710948C0918d42, 0x6B175474E89094C44Da98b954EedeAC495271d0F, 0xdAC17F958D2ee523a2206206994597C13D831ec7),
    ('USDT→USDC', 0x281e6BB6F26A94250aCEb24396a8E4190726C97e, 0xdAC17F958D2ee523a2206206994597C13D831ec7, 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48),
    ('USDT→DAI', 0x64B6aF9A108dCdF470E48e4c0147127F26221A7C, 0xdAC17F958D2ee523a2206206994597C13D831ec7, 0x6B175474E89094C44Da98b954EedeAC495271d0F),
    ('USDC→USDT', 0x278f7B6CBB3Cc37374e6a40bDFEBfff08f65A5C7, 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48, 0xdAC17F958D2ee523a2206206994597C13D831ec7),
    ('USDC→DAI', 0x2B5a3944A654439379B206DE999639508bA2e850, 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48, 0x6B175474E89094C44Da98b954EedeAC495271d0F)
    ) as list(namespace, address, token_out, token_in)
)


, cow_settlement as (
    select * from (values 
    (0x9008D19f58AAbD9eD0D60971565AA8510560ab41)
    ) as list(address)    
)

, stonks_orders as (
	select
		cast(replace(l.topic1, 0x000000000000000000000000, 0x) as varbinary) as order_addr
		, s.token_out
		, s.token_in
		, s.namespace
		, s.address as stonk_contract
	from
		{{ source('ethereum', 'logs') }} as l
	inner join stonks as s
		on l.contract_address = s.address
		and l.topic0 = 0x96a6d5477fba36522dca4102be8b3785435baf902ef6c4edebcb99850630c75f
	where
		l.block_time >= timestamp '{{ project_start_date }}'
)

, stonks_orders_txns as (
	select
		tr.evt_block_time
		, tr.evt_tx_hash
		, s.token_out
		, s.token_in
		, s.namespace
		, s.stonk_contract
	from
		{{ source('erc20_ethereum', 'evt_Transfer') }} as tr
	inner join stonks_orders as s
		on tr."from" = s.order_addr
		and tr.contract_address = s.token_out
	where
		{% if not is_incremental() -%}
		tr.evt_block_time >= timestamp '{{ project_start_date }}'
		{% else -%}
		{{ incremental_predicate('tr.evt_block_time') }}
		{% endif -%}
		and exists (select 1 from cow_settlement as c where c.address = tr.to)
)

, stonks_to_treasury as (
	select
		tr.evt_tx_hash
	from
		{{ source('erc20_ethereum', 'evt_Transfer') }} as tr
	inner join stonks_orders_txns as s
		on tr.evt_tx_hash = s.evt_tx_hash
		and tr.contract_address = s.token_in
		and exists (select 1 from tokens as tok where tok.address = tr.contract_address)
	where
		{% if not is_incremental() -%}
		tr.evt_block_time >= timestamp '{{ project_start_date }}'
		{% else -%}
		{{ incremental_predicate('tr.evt_block_time') }}
		{% endif -%}
		and exists (
			select
				1
			from
				multisigs_list as m
			where
				m.address = tr.to
				and m.name = 'Aragon'
				and m.chain = 'Ethereum'
		)
		and exists (select 1 from cow_settlement as c where c.address = tr."from")
)


, other_income_txns as (
	-- ERC20 inflows to Aragon/FinanceOpsMsig (excl. stonks)
	select
		t.evt_block_time
		, cast(t.value as double) as value
		, t.evt_tx_hash
		, t.contract_address
		, 'ethereum' as blockchain
	from
		{{ source('erc20_ethereum', 'evt_Transfer') }} as t
	where
		{% if not is_incremental() -%}
		t.evt_block_time >= timestamp '{{ project_start_date }}'
		{% else -%}
		{{ incremental_predicate('t.evt_block_time') }}
		{% endif -%}
		and exists (select 1 from tokens as tok where tok.address = t.contract_address)
		and exists (
			select
				1
			from
				multisigs_list as m
			where
				m.address = t.to
				and m.name in ('Aragon', 'FinanceOpsMsig')
				and m.chain = 'Ethereum'
		)
		and not exists (select 1 from multisigs_list as m where m.address = t."from")
		and not exists (select 1 from ldo_referral_payments_addr as l where l.address = t."from")
		and not exists (select 1 from dai_referral_payments_addr as d where d.address = t."from")
		and not exists (select 1 from steth_referral_payments_addr as s where s.address = t."from")
		and t."from" != 0x0000000000000000000000000000000000000000
		and not exists (select 1 from diversifications_addresses as d where d.address = t."from")
		and not exists (select 1 from stonks_to_treasury as st where st.evt_tx_hash = t.evt_tx_hash)

	union all

	-- stETH staked by DAO (from null address)
	select
		t.evt_block_time
		, cast(t.value as double) as value
		, t.evt_tx_hash
		, t.contract_address
		, 'ethereum' as blockchain
	from
		{{ source('erc20_ethereum', 'evt_Transfer') }} as t
	inner join {{ source('lido_ethereum', 'steth_evt_Submitted') }} as s
		on t.evt_tx_hash = s.evt_tx_hash
		and t.evt_block_time = s.evt_block_time
		and t.evt_block_number = s.evt_block_number
	where
		{% if not is_incremental() -%}
		t.evt_block_time >= timestamp '{{ project_start_date }}'
		{% else -%}
		{{ incremental_predicate('t.evt_block_time') }}
		{% endif -%}
		and t.contract_address = 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84
		and exists (select 1 from multisigs_list as m where m.address = t.to and m.chain = 'Ethereum' and m.name = 'Aragon')
		and t."from" = 0x0000000000000000000000000000000000000000
)

-- Solana stSOL income
, stsol_income_txs as (
	select
		i.tx_id
		, i.block_time as period
		, i.block_slot
		, i.pre_token_balance
		, i.post_token_balance
		, i.token_balance_change as delta
	from
		{{ source('solana', 'account_activity') }} as i
	where
		{% if not is_incremental() -%}
		i.block_time >= timestamp '2021-11-01'
		{% else -%}
		{{ incremental_predicate('i.block_time') }}
		{% endif -%}
		and i.pre_token_balance is not null
		and i.token_mint_address = '7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj'
		and i.address = 'CYpYPtwY9QVmZsjCmguAud1ctQjXWKpWD7xeL5mnpcXk'
		and i.token_balance_change > 0
)

, stsol_income as (
	select
		i.period
		, '7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj' as token
		, coalesce(i.delta, 0) as amount_token
		, i.tx_id as evt_tx_hash
		, 'solana' as blockchain
	from
		stsol_income_txs as i
)

select
	base.blockchain
	, base.period
	, base.evt_tx_hash
	, base.token
	, base.amount_token
from
	(
		select
			o.evt_block_time as period
			, o.evt_tx_hash
			, o.blockchain
			, o.contract_address as token
			, o.value as amount_token
		from
			other_income_txns as o

		union all

		-- ETH inflow
		select
			tr.block_time as period
			, tr.tx_hash as evt_tx_hash
			, 'ethereum' as blockchain
			, 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 as token
			, cast(tr.value as double) as amount_token
		from
			{{ source('ethereum', 'traces') }} as tr
		where
			{% if not is_incremental() -%}
			tr.block_time >= timestamp '{{ project_start_date }}'
			{% else -%}
			{{ incremental_predicate('tr.block_time') }}
			{% endif -%}
			and tr.success = true
			and exists (
				select
					1
				from
					multisigs_list as m
				where
					m.address = tr.to
					and m.name in ('Aragon', 'FinanceOpsMsig')
					and m.chain = 'Ethereum'
			)
			and not exists (select 1 from multisigs_list as m where m.address = tr."from")
			and not exists (select 1 from diversifications_addresses as d where d.address = tr."from")
			and tr.type = 'call'
			and (tr.call_type not in ('delegatecall', 'callcode', 'staticcall') or tr.call_type is null)

		union all

		-- stSOL to Solana treasury
		select
			s.period
			, from_base64(s.evt_tx_hash) as evt_tx_hash
			, s.blockchain
			, from_base64('7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj') as token
			, s.amount_token
		from
			stsol_income as s
	) as base
where
	base.amount_token != 0
group by
	base.blockchain
	, base.period
	, base.evt_tx_hash
	, base.token
    , base.amount_token