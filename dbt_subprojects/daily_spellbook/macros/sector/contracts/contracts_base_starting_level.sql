{% macro contracts_base_starting_level( chain, days_forward=365 ) %}


WITH check_date AS (
  SELECT
  {% if is_incremental() %}
    MAX(created_time) AS base_time FROM {{this}}
  {% else %}
    MIN(time) AS base_time FROM {{ source( chain , 'blocks') }}
  {% endif %}
)

SELECT *
FROM (
  select 
    blockchain
    , trace_creator_address
    , contract_address
    , created_time
    , created_block_number
    , created_tx_hash
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
            ,t."from" AS created_tx_from
            ,t.to AS created_tx_to
            ,bytearray_substring(t.data,1,4) AS created_tx_method_id
            ,t.index as created_tx_index
            ,ct.code
            ,bytearray_length(ct.code) AS code_bytelength
            
          from {{ source( chain , 'transactions') }} as t 
          cross join check_date cd
          inner join  {{ source( chain , 'creation_traces') }} as ct 
            ON t.hash = ct.tx_hash
            AND t.block_time = ct.block_time
            AND t.block_number = ct.block_number

            AND {{ incremental_days_forward_predicate('ct.block_time', 'cd.base_time', days_forward ) }}
            AND {{ incremental_days_forward_predicate('t.block_time', 'cd.base_time', days_forward ) }}

          where 
            1=1

            AND {{ incremental_days_forward_predicate('ct.block_time', 'cd.base_time', days_forward ) }}
            AND {{ incremental_days_forward_predicate('t.block_time', 'cd.base_time', days_forward ) }}
        {% if chain == 'zksync' %}
          UNION ALL
          select 
            '{{chain}}' AS blockchain
            ,ct.deployerAddress as trace_creator_address
            ,ct.contractAddress as contract_address
            ,ct.evt_block_time as created_time
            ,ct.evt_block_number as created_block_number
            ,ct.evt_tx_hash as created_tx_hash
            ,t."from" AS created_tx_from
            ,t.to AS created_tx_to
            ,bytearray_substring(t.data,1,4) AS created_tx_method_id
            ,t.index as created_tx_index
            ,ct.bytecodeHash as code
            ,bytearray_length(ct.bytecodeHash) AS code_bytelength
            
          from {{ source( chain , 'transactions') }} as t 
          cross join check_date cd
          inner join  {{ source( 'zksync_era_zksync' , 'ContractDeployer_evt_ContractDeployed') }} as ct 
            ON t.hash = ct.evt_tx_hash
            AND t.block_time = ct.evt_block_time
            AND t.block_number = ct.evt_block_number

            AND {{ incremental_days_forward_predicate('ct.evt_block_time', 'cd.base_time', days_forward ) }}
            AND {{ incremental_days_forward_predicate('t.block_time', 'cd.base_time', days_forward ) }}

          where 
            1=1

            AND {{ incremental_days_forward_predicate('ct.evt_block_time', 'cd.base_time', days_forward ) }}
            AND {{ incremental_days_forward_predicate('t.block_time', 'cd.base_time', days_forward ) }}
        {% endif %}
        ) x
) y
  WHERE reinitialize_rank = 1 --ensures one row per contract

{% endmacro %}