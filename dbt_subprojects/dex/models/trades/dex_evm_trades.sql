{% set chains = [
	'abstract'
	, 'apechain'
	, 'arbitrum'
	, 'avalanche_c'
	, 'base'
	, 'berachain'
	, 'blast'
	, 'bnb'
	, 'boba'
	, 'celo'
	, 'corn'
	, 'cronos'
	, 'ethereum'
	, 'fantom'
	, 'flare'
	, 'flow'
	, 'gnosis'
	, 'hemi'
	, 'hyperevm'
	, 'ink'
	, 'kaia'
	, 'katana'
	, 'linea'
	, 'mantle'
	, 'megaeth'
	, 'mezo'
	, 'monad'
	, 'morph'
	, 'nova'
	, 'opbnb'
	, 'optimism'
	, 'peaq'
	, 'plasma'
	, 'plume'
	, 'polygon'
	, 'robinhood'
	, 'ronin'
	, 'scroll'
	, 'sei'
	, 'shape'
	, 'somnia'
	, 'sonic'
	, 'sophon'
	, 'story'
	, 'superseed'
	, 'tac'
	, 'taiko'
	, 'tempo'
	, 'tezos_evm'
	, 'unichain'
	, 'worldchain'
	, 'xlayer'
	, 'zkevm'
	, 'zksync'
	, 'zora'
] %}

{{ config(
	schema = 'dex_evm'
	, alias = 'trades'
	, materialized = 'view'
	, post_hook = '{{ expose_spells(blockchains = \'["' + chains | join('","') + '"]\',
									spell_type = "sector",
									spell_name = "dex_evm",
									contributors = \'["hosuke", "0xrob", "jeff-dude", "tomfutago", "viniabussafi", "krishhh", "kryptaki"]\') }}'
	)
}}

select
	blockchain
	, project
	, version
	, block_month
	, block_date
	, block_time
	, block_number
	, token_bought_symbol
	, token_sold_symbol
	, token_pair
	, token_bought_amount
	, token_sold_amount
	, token_bought_amount_raw
	, token_sold_amount_raw
	, amount_usd
	, token_bought_address
	, token_sold_address
	, taker
	, maker
	, project_contract_address
	, tx_hash
	, tx_from
	, tx_to
	, evt_index
	, _updated_at
from {{ ref('dex_trades') }}
