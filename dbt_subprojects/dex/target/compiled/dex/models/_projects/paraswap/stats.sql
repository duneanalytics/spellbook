


  
    
        select 'delta-v1-single' as entity, 'ethereum' as blockchain, contract_address, call_block_time, call_tx_hash from paraswapdelta_ethereum.ParaswapDeltav1_call_settleSwap
        where 
            (call_block_time BETWEEN DATE_TRUNC('day', CURRENT_TIMESTAMP) - INTERVAL '1' day AND DATE_TRUNC('day', CURRENT_TIMESTAMP))
            
         union all 
    
        select 'delta-v1-batch' as entity, 'ethereum' as blockchain, contract_address, call_block_time, call_tx_hash from paraswapdelta_ethereum.ParaswapDeltav1_call_safeSettleBatchSwap
        where 
            (call_block_time BETWEEN DATE_TRUNC('day', CURRENT_TIMESTAMP) - INTERVAL '1' day AND DATE_TRUNC('day', CURRENT_TIMESTAMP))
            
         union all 
    
        select 'delta-v2' as entity, 'ethereum' as blockchain, contract_address, evt_block_time, evt_tx_hash from paraswapdelta_ethereum.ParaswapDeltav2_evt_OrderSettled
        where 
            (evt_block_time BETWEEN DATE_TRUNC('day', CURRENT_TIMESTAMP) - INTERVAL '1' day AND DATE_TRUNC('day', CURRENT_TIMESTAMP))
            
         union all 
    
        select 'delta-v2' as entity, 'base' as blockchain, contract_address, evt_block_time, evt_tx_hash from paraswapdelta_base.ParaswapDeltav2_evt_OrderSettled
        where 
            (evt_block_time BETWEEN DATE_TRUNC('day', CURRENT_TIMESTAMP) - INTERVAL '1' day AND DATE_TRUNC('day', CURRENT_TIMESTAMP))
            
         union all 
    
        select 'augustus' as entity, 'ethereum' as blockchain, project_contract_address, block_time, tx_hash from dex_aggregator.trades
        where 
            (block_time BETWEEN DATE_TRUNC('day', CURRENT_TIMESTAMP) - INTERVAL '1' day AND DATE_TRUNC('day', CURRENT_TIMESTAMP))
            
            AND project='paraswap'
            
        
    