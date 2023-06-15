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
, tx_from, tx_to, tx_method_id, tx_value, tx_l2_gas_used
, trace_from, trace_to, trace_method_id, call_type
, tx_l1_gas_used, tx_l1_gas_price, tx_l1_fee
, tx_l2_gas_price, tx_l2_base_fee_price, tx_l2_priority_fee_price, tx_gas_fee_eth

, SUM(trace_value) AS trace_value
, SUM(top_level_trace_gas_used) AS top_level_trace_gas_used
, SUM(trace_gas_used) AS trace_gas_used
, cast(SUM(trace_gas_used) as double) / cast(tx_l2_gas_used AS double) AS pct_tx_trace_gas_used
, COUNT(*) AS num_traces

, cast(tx_hash as varchar(100))
	|| '-' || coalesce(cast(trace_from as varchar(100)),'null_trace_from')
	|| '-' || coalesce(cast(trace_to as varchar(100)),'null_trace_to')
	|| '-' || coalesce(cast(trace_method_id as varchar(100)),'null_trace_method_id')
	|| '-' || coalesce(cast(call_type as varchar(100)),'null_call_type')
	|| '-' || coalesce(cast(trace_success as varchar(100)),'null_trace_success')
	|| '-' || coalesce(cast(tx_success as varchar(100)),'null_tx_success')
as unique_id

FROM (

	SELECT

          t.block_time
        , t.block_number
        , t.hash AS tx_hash
    
	, t.from AS tx_from
	, t.to AS tx_to
	, substring(t.data,1,10) AS tx_method_id --dunesql: bytearray_substring(t.data,1,4)
	, t.value AS tx_value
	, t.gas_used AS tx_l2_gas_used

	, r.from AS trace_from
	, r.to AS trace_to
	, substring(r.input,1,10) AS trace_method_id --dunesql: bytearray_substring(r.input,1,4)
	, r.value AS trace_value
	, r.type AS call_type
	, r.gas_used AS top_level_trace_gas_used
	, COALESCE( r.gas_used - sub_tr.gas_used , r.gas_used)	AS trace_gas_used --if no sub traces, then it's the lowest level trace

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
	
	, r.success AS trace_success
	, r.tx_success

	, ROW_NUMBER() OVER (PARTITION BY tx_hash ORDER BY r.success desc nulls last, r.tx_success DESC nulls last)
    
	FROM {{ source('optimism', 'traces') }} r
    
        INNER JOIN {{ source('optimism', 'transactions') }} t
        	ON t.hash = r.tx_hash
		AND t.block_time = r.block_time
		AND t.block_number = r.block_number
		{% if is_incremental() %}
        	AND r.block_time >= date_trunc('day', now() - interval '1 week')
        	{% endif %}

        INNER JOIN {{ source('optimism', 'blocks') }} b
		ON  t.block_time = b.time
		AND t.block_number = b.number
		{% if is_incremental() %}
        	AND b.time >= date_trunc('day', now() - interval '1 week')
        	{% endif %}
            
        LEFT JOIN {{ source('optimism', 'traces') }} sub_tr
		ON r.tx_hash = sub_tr.tx_hash
		AND r.block_number = sub_tr.block_number
		AND r.block_time = sub_tr.block_time
		--dunesql: AND r.trace_address = (CASE WHEN cardinality(sub_tr.trace_address) =0 THEN array[-1] ELSE slice(sub_tr.trace_address, 1, cardinality(sub_tr.trace_address) - 1) END)
		AND r.trace_address = (CASE WHEN cardinality(sub_tr.trace_address) =0 THEN array(-1) ELSE slice(sub_tr.trace_address, 1, cardinality(sub_tr.trace_address) - 1) END)
		{% if is_incremental() %}
        	AND sub_tr.block_time >= date_trunc('day', now() - interval '1 week')
        	{% endif %}
        
	{% if is_incremental() %}
	WHERE t.block_time >= date_trunc('day', now() - interval '1 week')
	{% endif %}

) a

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20, 26