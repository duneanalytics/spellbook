 {{
  config(
        tags = ['dunesql'],
        schema = 'contracts',
        alias = alias('daily_aggregated_usage'),
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key=['blockchain', 'contract_address', 'block_date'],
        partition_by=['blockchain', 'block_month'],
        pre_hook='{{ enforce_join_distribution("PARTITIONED") }}',
        post_hook='{{ expose_spells(\'["ethereum", "optimism", "arbitrum", "avalanche_c", "polygon", "bnb", "gnosis", "fantom", "base", "goerli"]\',
                                    "sector",
                                    "contracts",
                                    \'["msilb7"]\') }}'
  )
}}

SELECT
blockchain, block_month, block_date, contract_address
    , COUNT(*) AS num_txs_called_trace
    , COUNT(DISTINCT tx_from) AS num_tx_sender_addresses_called_trace
    , SUM(native_fee) AS native_fees_txs_called_trace
    , SUM(num_calls) AS num_trace_calls
    , SUM(tx_gas_used) AS total_tx_gas_used_called_trace
    
FROM (
    SELECT 
        r.blockchain,
        date_trunc('month', r.block_time) as block_month,
        date_trunc('day', r.block_time) as block_date,
        ct.address as contract_address,
        t.hash, t."from" AS tx_from
        , AVG( COALESCE(t.l1_fee,0)/1e18
                + (
                        t.gas_used/1e18
                        *COALESCE(t.effective_gas_price,t.gas_price)/1e18
                 ) ) AS native_fee
        , COUNT(*) AS num_calls
        , SUM(r.gas_used) AS trace_gas_used
        , AVG(t.gas_used
                - CASE WHEN t.blockchain = 'arbitrum' THEN t.l1_gas_used ELSE 0 END 
                ) AS tx_gas_used

    FROM {{ ref('evms_traces') }} r
        INNER JOIN {{ ref('evms_creation_traces') }} ct --ensure it's a contract
                ON ct.blockchain = r.blockchain
                AND ct.address = r.to
                AND r.block_time >= ct.block_time
        INNER JOIN {{ ref('evms_transactions') }} t
                ON 1=1
                AND t.blockchain = r.blockchain AND t.blockchain = ct.blockchain
                AND t.block_number = r.block_number
                AND t.block_time = r.block_time
                AND t.hash = r.tx_hash
                AND t.block_time >= ct.block_time
                {% if is_incremental() %}
                AND t.block_time >= NOW() - interval '7' day
                {% endif %}
        WHERE 1=1 
                {% if is_incremental() %}
                AND t.block_time >= NOW() - interval '7' day
                AND r.block_time >= NOW() - interval '7' day
                {% endif %}
                AND t.gas_price > 0
                AND r.type ='call'
                AND r.to IS NOT NULL
                AND t.to IS NOT NULL
        GROUP BY 1,2,3,4,5,6

        ) a
    GROUP BY 1,2,3,4