{{ config(
	alias = 'aggregated_traces',
	partition_by = ['block_date'],
	materialized = 'incremental',
	file_format = 'delta',
	incremental_strategy = 'merge',
	unique_key = ['unique_id']
	)
}}

SELECT
DATE_TRUNC('day', block_time) AS block_date
, block_time, block_number, tx_hash
, tx_from, tx_to, tx_method, tx_value, tx_gas_used
, trace_from, trace_to, trace_method, trace_type
, tx_l1_gas_used, tx_l1_gas_price, tx_l1_fee
, tx_l2_gas_price, tx_l2_base_fee_price, tx_l2_priority_fee_price, tx_gas_fee_eth
, trace_success, tx_success

, SUM(trace_value) AS trace_value
, SUM(gas_used_original) AS gas_used_original
, SUM(gas_used_trace) AS gas_used_trace
, cast(SUM(gas_used_trace) as double) / cast(tx_gas_used AS double) AS pct_tx_trace_gas_used
, COUNT(*) AS num_traces

, 'txh-' || coalesce(cast(tx_hash as varchar(100)), 'null_tx_hash')
|| '-trf-' || coalesce(cast(trace_from as varchar(100)),'null_trace_from')
|| '-trf-' || coalesce(cast(trace_to as varchar(100)),'null_trace_to')
|| '-trm-' || coalesce(cast(trace_method as varchar(100)),'null_trace_method')
|| '-trt-' || coalesce(cast(trace_type as varchar(100)),'null_trace_type')
|| '-trs-' || coalesce(cast(trace_success as varchar(100)),'null_trace_success')
|| '-txs-' || coalesce(cast(tx_success as varchar(100)),'null_tx_success')

as unique_id

FROM (

	SELECT

          r.block_time
        , r.block_number
        , r.tx_hash
    
	, r.tx_from
	, r.tx_to
	, r.tx_method
	, r.tx_gas_used
	, t.value AS tx_value

	, trace_from
	, trace_to
	, trace_method
	, r.trace_value
	, r.trace_type
	, r.gas_used_original
	, r.gas_used_trace --if no sub traces, then it's the lowest level trace

	, t.l1_gas_used AS tx_l1_gas_used
	, t.l1_gas_price AS tx_l1_gas_price
	, t.l1_fee AS tx_l1_fee
	, t.gas_price AS tx_l2_gas_price
	, COALESCE( cast(b.base_fee_per_gas as double), cast(t.gas_price as double) ) AS tx_l2_base_fee_price --use gas price pre-Bedrock
	, CASE WHEN b.base_fee_per_gas IS NULL THEN 0 ELSE
		cast(t.gas_price as double) - cast(b.base_fee_per_gas as double)
		END AS tx_l2_priority_fee_price
	, case when t.gas_price = 0 THEN 0 ELSE
		cast(t.l1_fee + (t.gas_used * cast(t.gas_price as double)) as double)
		END AS tx_gas_fee_eth
	
	, r.trace_success
	, r.tx_success

	FROM {{ ref('gas_optimism_fees_traces') }} r
    
        INNER JOIN {{ source('optimism', 'transactions') }} t
        	ON t.hash = r.tx_hash
		AND t.block_time = r.block_time
		AND t.block_number = r.block_number
		{% if is_incremental() %}
        	AND t.block_time >= date_trunc('day', now() - interval '1 week')
        	{% endif %}

        INNER JOIN {{ source('optimism', 'blocks') }} b
		ON  t.block_time = b.time
		AND t.block_number = b.number
		{% if is_incremental() %}
        	AND b.time >= date_trunc('day', now() - interval '1 week')
        	{% endif %}
        
	{% if is_incremental() %}
	WHERE r.block_time >= date_trunc('day', now() - interval '1 week')
	{% endif %}

) a

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22, 28