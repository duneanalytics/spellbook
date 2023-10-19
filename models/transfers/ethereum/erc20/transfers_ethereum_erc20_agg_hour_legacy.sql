{{ config(
	    tags=['legacy'],
        alias = alias('erc20_agg_hour', legacy_model=True),
        file_format ='delta',
        unique_key='unique_transfer_id'
        )
}}

-- removed from config:
-- materialized ='incremental',

select
    'ethereum' as blockchain,
    date_trunc('hour', tr.evt_block_time) as hour,
    tr.wallet_address,
    tr.token_address,
    t.symbol,
    tr.wallet_address || '-' || tr.token_address || '-' || date_trunc('hour', tr.evt_block_time) as unique_transfer_id,
    sum(tr.amount_raw) as amount_raw,
    sum(tr.amount_raw / power(10, t.decimals)) as amount
from {{ ref('transfers_ethereum_erc20_legacy') }} tr
left join {{ ref('tokens_ethereum_erc20_legacy') }} t on t.contract_address = tr.token_address
group by 1, 2, 3, 4, 5, 6
