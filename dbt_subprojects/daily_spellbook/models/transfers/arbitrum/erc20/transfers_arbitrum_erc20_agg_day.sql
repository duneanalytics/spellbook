{{ config(

        alias = 'erc20_agg_day',
        materialized ='incremental',
        partition_by = ['block_month'],
        file_format ='delta',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_day')],
        unique_key = ['block_day', 'wallet_address', 'token_address']
        )
}}

select
    tr.blockchain,
    date_trunc('day', tr.evt_block_time) as block_day,
    block_month,
    tr.wallet_address,
    tr.token_address,
    t.symbol,
    sum(tr.amount_raw) as amount_raw,
    sum(tr.amount_raw / power(10, t.decimals)) as amount
FROM
{{ ref('transfers_arbitrum_erc20') }} tr
LEFT JOIN
{{ source('tokens_arbitrum', 'erc20') }} t on t.contract_address = tr.token_address
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE {{ incremental_predicate('tr.evt_block_time') }}
{% endif %}
GROUP BY 1, 2, 3, 4, 5, 6