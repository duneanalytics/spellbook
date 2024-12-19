-- NB: this is a generated query, do not edit it directly, instead edit the template and re-generate the query
-- hydrate the generated ouput here: https://dune.com/queries/4407620






  
with entities as (
    
        select 'delta-v1-single' as entity, 'ethereum' as blockchain, contract_address as contract_address, call_block_time as block_time, call_tx_hash as tx_hash from paraswapdelta_ethereum.ParaswapDeltav1_call_settleSwap
        where 
            (call_block_time BETWEEN timestamp '{{date_from}}' AND timestamp '{{date_to}}')
            
         union all 
    
        select 'delta-v1-batch' as entity, 'ethereum' as blockchain, contract_address as contract_address, call_block_time as block_time, call_tx_hash as tx_hash from paraswapdelta_ethereum.ParaswapDeltav1_call_safeSettleBatchSwap
        where 
            (call_block_time BETWEEN timestamp '{{date_from}}' AND timestamp '{{date_to}}')
            
         union all 
    
        select 'delta-v2' as entity, 'ethereum' as blockchain, contract_address as contract_address, evt_block_time as block_time, evt_tx_hash as tx_hash from paraswapdelta_ethereum.ParaswapDeltav2_evt_OrderSettled
        where 
            (evt_block_time BETWEEN timestamp '{{date_from}}' AND timestamp '{{date_to}}')
            
         union all 
    
        select 'delta-v2' as entity, 'base' as blockchain, contract_address as contract_address, evt_block_time as block_time, evt_tx_hash as tx_hash from paraswapdelta_base.ParaswapDeltav2_evt_OrderSettled
        where 
            (evt_block_time BETWEEN timestamp '{{date_from}}' AND timestamp '{{date_to}}')
            
            AND evt_tx_from <> 0xace5ae3de4baffc4a45028659c5ee330764e4f53
            
         union all 
    
        select 'augustus' as entity, 'ethereum' as blockchain, project_contract_address as contract_address, block_time as block_time, tx_hash as tx_hash from dex_aggregator.trades
        where 
            (block_time BETWEEN timestamp '{{date_from}}' AND timestamp '{{date_to}}')
            
            AND project='paraswap' and blockchain='ethereum'
            
         union all 
    
        select 'augustus' as entity, 'polygon' as blockchain, project_contract_address as contract_address, block_time as block_time, tx_hash as tx_hash from dex_aggregator.trades
        where 
            (block_time BETWEEN timestamp '{{date_from}}' AND timestamp '{{date_to}}')
            
            AND project='paraswap' and blockchain='polygon'
            
         union all 
    
        select 'augustus' as entity, 'bnb' as blockchain, project_contract_address as contract_address, block_time as block_time, tx_hash as tx_hash from dex_aggregator.trades
        where 
            (block_time BETWEEN timestamp '{{date_from}}' AND timestamp '{{date_to}}')
            
            AND project='paraswap' and blockchain='bnb'
            
         union all 
    
        select 'augustus' as entity, 'arbitrum' as blockchain, project_contract_address as contract_address, block_time as block_time, tx_hash as tx_hash from dex_aggregator.trades
        where 
            (block_time BETWEEN timestamp '{{date_from}}' AND timestamp '{{date_to}}')
            
            AND project='paraswap' and blockchain='arbitrum'
            
         union all 
    
        select 'augustus' as entity, 'avalanche_c' as blockchain, project_contract_address as contract_address, block_time as block_time, tx_hash as tx_hash from dex_aggregator.trades
        where 
            (block_time BETWEEN timestamp '{{date_from}}' AND timestamp '{{date_to}}')
            
            AND project='paraswap' and blockchain='avalanche_c'
            
         union all 
    
        select 'augustus' as entity, 'fantom' as blockchain, project_contract_address as contract_address, block_time as block_time, tx_hash as tx_hash from dex_aggregator.trades
        where 
            (block_time BETWEEN timestamp '{{date_from}}' AND timestamp '{{date_to}}')
            
            AND project='paraswap' and blockchain='fantom'
            
         union all 
    
        select 'augustus' as entity, 'optimism' as blockchain, project_contract_address as contract_address, block_time as block_time, tx_hash as tx_hash from dex_aggregator.trades
        where 
            (block_time BETWEEN timestamp '{{date_from}}' AND timestamp '{{date_to}}')
            
            AND project='paraswap' and blockchain='optimism'
            
         union all 
    
        select 'augustus' as entity, 'base' as blockchain, project_contract_address as contract_address, block_time as block_time, tx_hash as tx_hash from dex_aggregator.trades
        where 
            (block_time BETWEEN timestamp '{{date_from}}' AND timestamp '{{date_to}}')
            
            AND project='paraswap' and blockchain='base'
            
        
    
)

select 
    entity, blockchain, contract_address, block_time, tx_hash    
from entities
where blockchain = '{{blockchain}}' and contract_address = {{contract_address}}
order by block_time desc, tx_hash