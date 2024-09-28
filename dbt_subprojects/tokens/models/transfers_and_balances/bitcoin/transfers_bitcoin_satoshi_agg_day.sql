{{ config(
        schema = 'transfers_bitcoin',
        alias = 'satoshi_agg_day',

        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
        unique_key = ['day', 'wallet_address']
        )
}}

select
    'bitcoin' as blockchain,
    tr.block_date as day,
    tr.wallet_address,
    sum(tr.amount_raw) as amount_raw,
    sum(tr.amount_transfer_usd) as amount_transfer_usd
from {{ ref('transfers_bitcoin_satoshi') }} tr
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where {{ incremental_predicate('tr.block_date ') }}
{% endif %}
group by 1, 2, 3
