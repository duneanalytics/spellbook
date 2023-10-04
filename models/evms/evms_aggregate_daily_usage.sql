   {{
  config(
        tags = ['dunesql'],
        schema = 'evms',
        alias = alias('aggregate_daily_usage'),
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key=['blockchain', 'block_date'],
        partition_by=['blockchain', 'block_date'],
        pre_hook='{{ enforce_join_distribution("PARTITIONED") }}',
        post_hook='{{ expose_spells(\'["ethereum", "optimism", "arbitrum", "avalanche_c", "polygon", "bnb", "gnosis", "fantom", "base", "goerli"]\',
                                    "sector",
                                    "contracts",
                                    \'["msilb7"]\') }}'
  )
}}

  SELECT
    i.blockchain, i.name,
    DATE_TRUNC('day',tx.block_time) AS block_date,
    ,DATE_DIFF('second',MIN(tx.block_time), MAX(tx.block_time)) AS active_secs_per_day_excl_first_block
    ,COUNT(DISTINCT block_number) AS num_blocks
    ,COUNT(*) AS num_txs_per_day
    ,COUNT(CASE WHEN success = true then 1 else 0 end) AS num_success_txs_per_day
    ,SUM(CASE WHEN gas_price > 0 THEN 1 ELSE 0 END) AS num_user_txs_per_day
    ,SUM(CASE WHEN gas_price > 0 AND success = true THEN 1 ELSE 0 END) AS num_success_user_txs_per_day
    ,SUM(case when gas_price = 0 AND to = 0x4200000000000000000000000000000000000015 then 1 else 0 end) as l2_num_attr_deposit_txs_per_day --op configured
    ,SUM(case when gas_price = 0 AND to = 0x4200000000000000000000000000000000000007 then 1 else 0 end) as l2_num_user_deposit_txs_per_day --op configured
    ,COUNT(DISTINCT CASE WHEN gas_price > 0 THEN  tx."from" ELSE NULL END) AS num_user_addresses_per_day
    
    
    ,SUM(CASE WHEN gas_price = 0 THEN 0 ELSE cast(tx.gas_used as double)*cast(gas_price as double)/1e18 END) AS native_fees_per_day
    ,SUM(CASE WHEN gas_price = 0 THEN 0 ELSE cast(tx.gas_used as double)*cast(coalesce(base_fee_per_gas, gas_price) as double)/1e18 END) AS native_fees_per_day_base_fee_contribution
    ,SUM(CASE WHEN gas_price = 0 THEN 0 ELSE cast(tx.gas_used as double)*cast(gas_price-coalesce(base_fee_per_gas,gas_price) as double)/1e18 END) AS native_fees_per_day_priority_fee_contribution
    
    ,SUM(CASE WHEN gas_price = 0 THEN 0 ELSE cast(l1_fee as double) /1e18 END) --if l2 gas price = 0, then all 0
        AS native_fees_per_day_l1_fee_contribution
    
    ,SUM(CASE WHEN gas_price = 0 THEN 0 ELSE price*cast(tx.gas_used as double)*cast(gas_price as double)/1e18 END) AS usd_fees_per_day
    ,SUM(CASE WHEN gas_price = 0 THEN 0 ELSE price*cast(tx.gas_used as double)*cast(coalesce(base_fee_per_gas, gas_price) as double)/1e18 END) AS usd_fees_per_day_base_fee_contribution
    ,SUM(CASE WHEN gas_price = 0 THEN 0 ELSE price*cast(tx.gas_used as double)*cast(gas_price-coalesce(base_fee_per_gas,gas_price) as double)/1e18 END) AS usd_fees_per_day_priority_fee_contribution
    ,SUM(CASE WHEN gas_price = 0 THEN 0 ELSE price*cast(l1_fee as double)/1e18 END) --if l2 gas price = cast(0 as uint256, then all 0
        AS usd_fees_per_day_l1_fee_contribution_usd
        
        ,SUM(tc.l1_gas_used) AS l1_gas_used
    
        ,SUM(tx.gas_used) AS gas_used

        ,SUM(CASE WHEN tx.gas_price >0 THEN cast(tx.gas_used as double)*cast(tx.gas_price as double)/1e9 ELSE NULL END)
                / SUM(CASE WHEN tx.gas_price > 0 THEN tx.gas_used ELSE NULL END) AS avg_gas_price_gwei --if not free
        ,SUM(CASE WHEN tx.gas_price >0 THEN cast(tx.gas_used as double)*cast(coalesce(base_fee_per_gas,gas_price)/1e9 as double) ELSE NULL END)
                / SUM(CASE WHEN tx.gas_price > 0 THEN tx.gas_used ELSE NULL END) AS avg_base_fee_gwei --if not free
        ,SUM(CASE WHEN tx.l1_gas_price >0 THEN cast(tx.l1_gas_used as double)*cast(l1_gas_price/1e9 as double) ELSE NULL END)
                / SUM(CASE WHEN tx.l1_gas_price > 0 THEN tx.l1_gas_used ELSE NULL END) AS avg_l1_gas_price_on_l2_gwei --if not free
    
    , SUM(bytearray_length(data)) AS calldata_bytes_per_day
    , SUM(
        16 * ( bytearray_length(data) - (length(from_utf8(data)) - length(replace(from_utf8(data), chr(0), ''))) ) --nonzero bytes
                + 4 * ( (length(from_utf8(data)) - length(replace(from_utf8(data), chr(0), ''))) )
        ) AS calldata_gas_per_day
    
    , SUM(case when gas_price > 0 then l1_gas_used else 0 end) AS l1_gas_used_user_txs_per_day
    , SUM(case when gas_price > 0 then bytearray_length(data) else 0 end) AS calldata_bytes_user_txs_per_day
    , SUM(case when gas_price > 0 then 
            16 * ( bytearray_length(data) - (length(from_utf8(data)) - length(replace(from_utf8(data), chr(0), ''))) ) --nonzero bytes
                + 4 * ( (length(from_utf8(data)) - length(replace(from_utf8(data), chr(0), ''))) )
            else 0 end) AS calldata_gas_user_txs_per_day
    ,SUM(case when tx.gas_price > 0 then tx.gas_used else 0 end) AS gas_used_user_txs_per_day

    FROM {{ref('evms_transactions')}} tx
        INNER JOIN {{ref('evms_info')}} i 
                ON i.blockchain = tx.blockchain
        INNER JOIN {{ref('evms_blocks')}} b
                ON b.blockchain = tx.blockchain
                AND b.number = tx.block_number
                AND b.time = tx.block_time
                {% if is_incremental() %}
                AND b.time >= NOW() - intveral '7' day
                {% endif %}
        LEFT JOIN {{ref('prices_usd')}} p
                ON p.minute = DATE_TRUNC('minute',tx.block_time)
                AND p.blockchain IS NULL
                AND p.symbol = i.native_token_symbol
                {% if is_incremental() %}
                AND p.minute >= NOW() - intveral '7' day
                {% endif %}
        

    WHERE 1=1
    {% if is_incremental() %}
    AND tx.block_time >= NOW() - intveral '7' day
        AND b.time >= NOW() - intveral '7' day
    {% endif %}
    GROUP BY 1,2,3