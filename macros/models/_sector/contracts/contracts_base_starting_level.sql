{% macro contracts_base_starting_level( chain ) %}

SELECT *
FROM (
  select 
    blockchain
    , trace_creator_address
    , contract_address
    , created_time
    , created_block_number
    , created_tx_hash
    , top_level_time
    , top_level_block_number
    , top_level_tx_hash
    , top_level_tx_from
    , top_level_tx_to
    , top_level_tx_method_id
    , created_tx_from
    , created_tx_to
    , created_tx_method_id
    , created_tx_index
    , code
    , code_bytelength
    --handle for OP allowlist issue
    , ROW_NUMBER() OVER (PARTITION BY contract_address ORDER BY created_block_number DESC, created_tx_index DESC) AS reinitialize_rank
    , cast( DATE_TRUNC('month',created_time) as date) AS created_month
  from (
        select 
            '{{chain}}' AS blockchain
            ,ct."from" as trace_creator_address
            ,ct.address as contract_address
            ,ct.block_time as created_time
            ,ct.block_number as created_block_number
            ,ct.tx_hash as created_tx_hash
            ,t.block_time as top_level_time
            ,t.block_number as top_level_block_number
            ,t.hash as top_level_tx_hash
            ,t."from" AS top_level_tx_from
            ,t.to AS top_level_tx_to
            ,bytearray_substring(t.data,1,4) AS top_level_tx_method_id
            ,t."from" AS created_tx_from
            ,t.to AS created_tx_to
            ,bytearray_substring(t.data,1,4) AS created_tx_method_id
            ,t.index as created_tx_index
            ,ct.code
            ,bytearray_length(ct.code) AS code_bytelength
            
          from {{ source( chain , 'transactions') }} as t 
          inner join  {{ source( chain , 'creation_traces') }} as ct 
            ON t.hash = ct.tx_hash
            AND t.block_time = ct.block_time
            AND t.block_number = ct.block_number
            {% if is_incremental() %}
            AND {{ incremental_predicate('ct.block_time') }}
            AND {{ incremental_predicate('t.block_time') }}
            {% endif %}
          where 
            1=1
            {% if is_incremental() %}
            AND {{ incremental_predicate('ct.block_time') }}
            AND {{ incremental_predicate('t.block_time') }}
            {% endif %}
        ) x
) y
  WHERE reinitialize_rank = 1 --ensures one row per contract

{% endmacro %}