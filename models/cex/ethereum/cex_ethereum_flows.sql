{{config(
        schema = 'cex_ethereum',
        alias = 'flows',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_number', 'tx_index', 'evt_index']
        )}}

SELECT t.blockchain
, date_trunc('month', block_time) AS block_month
, block_time
, block_number
, a.cex_name
, a.distinct_name
, t.contract_address AS token_address
, t.symbol AS token_symbol
, t.token_standard
, CASE WHEN a.address=t."from" THEN 'Outflow' ELSE 'Inflow' END AS flow_type
, CASE WHEN a.address=t."from" THEN -t.amount ELSE t.amount END AS amount
, t.amount_raw
, CASE WHEN a.address=t."from" THEN -t.usd_amount ELSE t.usd_amount END AS usd_amount
, t."from"
, t.to
, t.tx_from
, t.tx_to
, t.tx_index
, t.tx_hash
, t.evt_index
FROM {{ ref('tokens_ethereum_transfers')}} t
INNER JOIN {{ ref('cex_ethereum_addresses')}} a ON a.address IN (t."from", t.to)
{% if is_incremental() %}
WHERE t.block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}