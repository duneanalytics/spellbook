{{ config(
        
        alias = 'erc20_agg_day',
        partition_by = ['block_month'],
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['block_day', 'wallet_address', 'counterparty', 'token_address']
        )
}}


SELECT
    tr.blockchain,
    date_trunc('day', tr.evt_block_time) as block_hour,
    tr.block_month,
    tr.wallet_address,
    tr.counterparty,
    tr.token_address,
    t.symbol,
    sum(tr.amount_raw) as amount_raw,
    sum(tr.amount_raw / power(10, t.decimals)) as amount
FROM 
{{ ref('transfers_gnosis_erc20') }} tr
LEFT JOIN 
{{ source('tokens_gnosis', 'erc20') }} t on t.contract_address = tr.token_address
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE tr.evt_block_time >= date_trunc('day', now() - interval '3' Day)
{% endif %}
GROUP BY 1, 2, 3, 4, 5, 6, 7