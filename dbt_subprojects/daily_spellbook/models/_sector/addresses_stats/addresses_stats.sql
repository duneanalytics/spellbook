{% set chains = [
	'arbitrum',
	'avalanche_c',
	'base',
	'bnb',
	'ethereum',
	'hyperevm',
	'linea',
	'monad',
	'optimism',
	'polygon',
	'sei',
	'zksync',
] %}

{{ config(
	schema='addresses',
	alias='stats',
	materialized='view',
	post_hook='{{ expose_spells(blockchains = \'["' ~ chains | join('","') ~ '"]\',
		spell_type = "sector",
		spell_name = "addresses",
		contributors = \'["kryptaki"]\') }}',
) }}

select
	*
from (
	{% for chain in chains -%}
	select
		blockchain
		, address
		, first_funded_by
		, first_funded_at
		, is_smart_contract
		, is_eoa
		, first_deployment_date
		, _updated_at
	from
		{{ ref('addresses_' ~ chain ~ '_stats') }}
	{% if not loop.last -%}
	union all
	{% endif -%}
	{% endfor -%}
)
