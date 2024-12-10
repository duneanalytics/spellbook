


  
with entities as (
    
        select 'delta-v1-single' as entity, 'ethereum' as blockchain, contract_address as contract_address, call_block_time, call_tx_hash from paraswapdelta_ethereum.ParaswapDeltav1_call_settleSwap
        where 
            (call_block_time BETWEEN DATE_TRUNC('day', CURRENT_TIMESTAMP) - INTERVAL '1' day AND DATE_TRUNC('day', CURRENT_TIMESTAMP))
            
         union all 
    
        select 'delta-v1-batch' as entity, 'ethereum' as blockchain, contract_address as contract_address, call_block_time, call_tx_hash from paraswapdelta_ethereum.ParaswapDeltav1_call_safeSettleBatchSwap
        where 
            (call_block_time BETWEEN DATE_TRUNC('day', CURRENT_TIMESTAMP) - INTERVAL '1' day AND DATE_TRUNC('day', CURRENT_TIMESTAMP))
            
         union all 
    
        select 'delta-v2' as entity, 'ethereum' as blockchain, contract_address as contract_address, evt_block_time, evt_tx_hash from paraswapdelta_ethereum.ParaswapDeltav2_evt_OrderSettled
        where 
            (evt_block_time BETWEEN DATE_TRUNC('day', CURRENT_TIMESTAMP) - INTERVAL '1' day AND DATE_TRUNC('day', CURRENT_TIMESTAMP))
            
         union all 
    
        select 'delta-v2' as entity, 'base' as blockchain, contract_address as contract_address, evt_block_time, evt_tx_hash from paraswapdelta_base.ParaswapDeltav2_evt_OrderSettled
        where 
            (evt_block_time BETWEEN DATE_TRUNC('day', CURRENT_TIMESTAMP) - INTERVAL '1' day AND DATE_TRUNC('day', CURRENT_TIMESTAMP))
            
         union all 
    
        select 'augustus' as entity, 'ethereum' as blockchain, project_contract_address as contract_address, block_time, tx_hash from dex_aggregator.trades
        where 
            (block_time BETWEEN DATE_TRUNC('day', CURRENT_TIMESTAMP) - INTERVAL '1' day AND DATE_TRUNC('day', CURRENT_TIMESTAMP))
            
            AND project='paraswap' and blockchain='ethereum'
            
         union all 
    
        select 'augustus' as entity, 'polygon' as blockchain, project_contract_address as contract_address, block_time, tx_hash from dex_aggregator.trades
        where 
            (block_time BETWEEN DATE_TRUNC('day', CURRENT_TIMESTAMP) - INTERVAL '1' day AND DATE_TRUNC('day', CURRENT_TIMESTAMP))
            
            AND project='paraswap' and blockchain='polygon'
            
         union all 
    
        select 'augustus' as entity, 'bnb' as blockchain, project_contract_address as contract_address, block_time, tx_hash from dex_aggregator.trades
        where 
            (block_time BETWEEN DATE_TRUNC('day', CURRENT_TIMESTAMP) - INTERVAL '1' day AND DATE_TRUNC('day', CURRENT_TIMESTAMP))
            
            AND project='paraswap' and blockchain='bnb'
            
         union all 
    
        select 'augustus' as entity, 'arbitrum' as blockchain, project_contract_address as contract_address, block_time, tx_hash from dex_aggregator.trades
        where 
            (block_time BETWEEN DATE_TRUNC('day', CURRENT_TIMESTAMP) - INTERVAL '1' day AND DATE_TRUNC('day', CURRENT_TIMESTAMP))
            
            AND project='paraswap' and blockchain='arbitrum'
            
         union all 
    
        select 'augustus' as entity, 'avalanche_c' as blockchain, project_contract_address as contract_address, block_time, tx_hash from dex_aggregator.trades
        where 
            (block_time BETWEEN DATE_TRUNC('day', CURRENT_TIMESTAMP) - INTERVAL '1' day AND DATE_TRUNC('day', CURRENT_TIMESTAMP))
            
            AND project='paraswap' and blockchain='avalanche_c'
            
         union all 
    
        select 'augustus' as entity, 'fantom' as blockchain, project_contract_address as contract_address, block_time, tx_hash from dex_aggregator.trades
        where 
            (block_time BETWEEN DATE_TRUNC('day', CURRENT_TIMESTAMP) - INTERVAL '1' day AND DATE_TRUNC('day', CURRENT_TIMESTAMP))
            
            AND project='paraswap' and blockchain='fantom'
            
         union all 
    
        select 'augustus' as entity, 'optimism' as blockchain, project_contract_address as contract_address, block_time, tx_hash from dex_aggregator.trades
        where 
            (block_time BETWEEN DATE_TRUNC('day', CURRENT_TIMESTAMP) - INTERVAL '1' day AND DATE_TRUNC('day', CURRENT_TIMESTAMP))
            
            AND project='paraswap' and blockchain='optimism'
            
         union all 
    
        select 'augustus' as entity, 'base' as blockchain, project_contract_address as contract_address, block_time, tx_hash from dex_aggregator.trades
        where 
            (block_time BETWEEN DATE_TRUNC('day', CURRENT_TIMESTAMP) - INTERVAL '1' day AND DATE_TRUNC('day', CURRENT_TIMESTAMP))
            
            AND project='paraswap' and blockchain='base'
            
        
    
)
select blockchain, entity, contract_address, count(*) as qty
from entities
group by 1,2,3
order by qty desc