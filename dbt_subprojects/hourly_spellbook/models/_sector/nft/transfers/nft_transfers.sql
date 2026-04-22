{% set chains = [
	'ethereum',
	'bnb',
	'avalanche_c',
	'gnosis',
	'optimism',
	'arbitrum',
	'polygon',
	'fantom',
	'goerli',
	'base',
	'zksync',
	'zora',
	'celo',
	'scroll',
	'linea',
	'blast',
	'mantle',
	'sei',
	'ronin',
	'worldchain',
	'kaia',
] %}

{{ config(
	schema='nft',
	alias='transfers',
	materialized='view',
	post_hook='{{ expose_spells(blockchains = \'["' ~ chains | join('","') ~ '"]\',
		spell_type = "sector",
		spell_name = "nft",
		contributors = \'["hildobby", "0xRob", "rantum", "petertherock"]\') }}',
) }}

-- ci-stamp: 3
select
	*
from (
	{% for chain in chains -%}
	select
		blockchain
		, block_time
		, block_month
		, block_date
		, block_number
		, token_standard
		, transfer_type
		, evt_index
		, contract_address
		, token_id
		, amount
		, "from"
		, to
		, executed_by
		, tx_hash
		, unique_transfer_id
	from
		{{ ref('nft_' ~ chain ~ '_transfers') }}
	{% if not loop.last -%}
	union all
	{% endif -%}
	{% endfor -%}
)
