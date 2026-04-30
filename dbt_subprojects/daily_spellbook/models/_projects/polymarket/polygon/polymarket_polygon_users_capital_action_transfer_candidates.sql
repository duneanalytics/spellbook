{{ config(
	schema='polymarket_polygon',
	alias='users_capital_action_transfer_candidates',
	materialized='view',
) }}

select
	*
from
	{{ ref('polymarket_polygon_users_capital_action_inbound_transfer_candidates') }}
union all
select
	*
from
	{{ ref('polymarket_polygon_users_capital_action_outbound_transfer_candidates') }}
