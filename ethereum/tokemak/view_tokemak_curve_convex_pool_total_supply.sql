DROP MATERIALIZED VIEW IF EXISTS tokemak.view_tokemak_curve_convex_pool_total_supply
;

CREATE MATERIALIZED VIEW tokemak.view_tokemak_curve_convex_pool_total_supply (
	"date"
    ,address
    ,symbol
    ,total_supply
) AS (
WITH calendar AS  
        (SELECT i::date as "date"
            ,tl.address
            ,tl.symbol
            ,tl.decimals
        FROM tokemak."view_tokemak_lookup_tokens" tl
        CROSS JOIN generate_series('2021-08-01'::date, current_date, '1 day') t(i)
        WHERE tl.is_pool = true order by "date" desc
 ) 

    , result AS(
        SELECT symbol, contract_address as address, date_trunc('day', "date")::date as "date", total_supply[1] as evt_block_number, total_supply[3] as total_supply FROM (
        --3Crv
            (SELECT symbol, pool_token_address as contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , mp.pool_token_address                    
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."threepool_swap_evt_AddLiquidity"
                    INNER JOIN tokemak."view_tokemak_lookup_metapools" mp ON mp.base_pool_address = contract_address
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = mp.pool_token_address
                    GROUP BY symbol, mp.pool_token_address,  "date"
                UNION 
                SELECT symbol
                    , mp.pool_token_address                   
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."threepool_swap_evt_RemoveLiquidity"
                    INNER JOIN tokemak."view_tokemak_lookup_metapools" mp ON mp.base_pool_address = contract_address
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = mp.pool_token_address
                    GROUP BY symbol, mp.pool_token_address,  "date"
                UNION
                SELECT symbol
                    , mp.pool_token_address                  
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."threepool_swap_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_metapools" mp ON mp.base_pool_address = contract_address
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = mp.pool_token_address
                    GROUP BY symbol, mp.pool_token_address,  "date"
            ) as t GROUP BY  symbol, pool_token_address,  "date")
            UNION
        --eth/stETH
            (SELECT symbol, pool_token_address as contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , mp.pool_token_address                    
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."steth_swap_evt_AddLiquidity"
                    INNER JOIN tokemak."view_tokemak_lookup_metapools" mp ON mp.base_pool_address = contract_address
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = mp.pool_token_address
                    GROUP BY symbol, mp.pool_token_address,  "date"
                UNION 
                SELECT symbol
                    , mp.pool_token_address                   
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."steth_swap_evt_RemoveLiquidity"
                    INNER JOIN tokemak."view_tokemak_lookup_metapools" mp ON mp.base_pool_address = contract_address
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = mp.pool_token_address
                    GROUP BY symbol, mp.pool_token_address,  "date"
                UNION
                SELECT symbol
                    , mp.pool_token_address                  
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."steth_swap_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_metapools" mp ON mp.base_pool_address = contract_address
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = mp.pool_token_address
                    GROUP BY symbol, mp.pool_token_address,  "date"
            ) as t GROUP BY  symbol, pool_token_address,  "date")
            UNION
            --frax/USDC
            (SELECT symbol, pool_token_address as contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , mp.pool_token_address                    
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."frax_base_pool_fraxbp_evt_AddLiquidity"
                    INNER JOIN tokemak."view_tokemak_lookup_metapools" mp ON mp.base_pool_address = contract_address
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = mp.pool_token_address
                    GROUP BY symbol, mp.pool_token_address,  "date"
                UNION 
                SELECT symbol
                    , mp.pool_token_address                   
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."frax_base_pool_fraxbp_evt_RemoveLiquidity"
                    INNER JOIN tokemak."view_tokemak_lookup_metapools" mp ON mp.base_pool_address = contract_address
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = mp.pool_token_address
                    GROUP BY symbol, mp.pool_token_address,  "date"
                UNION
                SELECT symbol
                    , mp.pool_token_address                  
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."frax_base_pool_fraxbp_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_metapools" mp ON mp.base_pool_address = contract_address
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = mp.pool_token_address
                    GROUP BY symbol, mp.pool_token_address,  "date"
            ) as t GROUP BY  symbol, pool_token_address,  "date")
            UNION
            --alUSD3CRV
            (SELECT symbol, contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."alusd_evt_AddLiquidity"
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION 
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."alusd_evt_RemoveLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                  
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."alusd_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                  
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."alusd_evt_RemoveLiquidityOne" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
            ) as t GROUP BY  symbol, contract_address,  "date")
            UNION
            --LUSD
            (SELECT symbol, contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."lusd_swap_evt_AddLiquidity"
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION 
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."lusd_swap_evt_RemoveLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                  
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."lusd_swap_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                  
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."lusd_swap_evt_RemoveLiquidityOne"
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
            ) as t GROUP BY  symbol, contract_address,  "date")
            UNION
        --wormhole3CRV
            (SELECT symbol, contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."wormhole_v2_evt_AddLiquidity"
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION 
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."wormhole_v2_evt_RemoveLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                  
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."wormhole_v2_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                  
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."wormhole_v2_evt_RemoveLiquidityOne" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
            ) as t GROUP BY  symbol, contract_address,  "date")
            UNION
            --FRAX3crv
            (SELECT symbol, contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."frax_evt_AddLiquidity"
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION 
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."frax_evt_RemoveLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                  
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."frax_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                  
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."frax_evt_RemoveLiquidityOne" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
            ) as t GROUP BY  symbol, contract_address,  "date")
            UNION
            --WETH
            (SELECT symbol, contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tWETH_WETH_evt_AddLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION 
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tWETH_WETH_evt_RemoveLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                  
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tWETH_WETH_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                  
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tWETH_WETH_evt_RemoveLiquidityOne" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
            ) as t GROUP BY  symbol, contract_address,  "date")
            --ALCX
            UNION
            (SELECT symbol, contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tALCX_evt_AddLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION 
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tALCX_evt_RemoveLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time)::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tALCX_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tALCX_evt_RemoveLiquidityOne" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
            ) as t GROUP BY symbol, contract_address,  "date")
            --TCR
            UNION
            (SELECT symbol, contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tTCR_evt_AddLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION 
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tTCR_evt_RemoveLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                  
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tTCR_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tTCR_evt_RemoveLiquidityOne" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
            ) as t GROUP BY  symbol, contract_address,  "date")
            --sushi
            UNION
            (SELECT symbol, contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , contract_address                  
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tSUSHI_evt_AddLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION 
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tSUSHI_evt_RemoveLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tSUSHI_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tSUSHI_evt_RemoveLiquidityOne" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
            ) as t GROUP BY  symbol, contract_address,  "date")
            --fxs
            UNION
            (SELECT symbol, contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tFXS_evt_AddLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION 
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tFXS_evt_RemoveLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tFXS_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tFXS_evt_RemoveLiquidityOne" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  evt_block_time
            ) as t GROUP BY  symbol, contract_address,  "date")
            --fox
            UNION
            (SELECT symbol, contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tFOX_evt_AddLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION 
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tFOX_evt_RemoveLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tFOX_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tFOX_evt_RemoveLiquidityOne" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
            ) as t GROUP BY  symbol, contract_address,  "date")
            --apw
            UNION
            (SELECT symbol, contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tAPW_evt_AddLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION 
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tAPW_evt_RemoveLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tAPW_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tAPW_evt_RemoveLiquidityOne" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
            ) as t GROUP BY  symbol, contract_address,  "date")
            --snx
            UNION
            (SELECT symbol, contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tSNX_evt_AddLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION 
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tSNX_evt_RemoveLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tSNX_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tSNX_evt_RemoveLiquidityOne" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  evt_block_time
            ) as t GROUP BY  symbol, contract_address,  "date")
            --gamma
            UNION
            (SELECT symbol, contract_address,  "date",MAX(total_supply) as total_supply FROM (
                SELECT symbol
                    , contract_address                    
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tGAMMA_evt_AddLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION 
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tGAMMA_evt_RemoveLiquidity" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tGAMMA_evt_RemoveLiquidityImbalance" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
                UNION
                SELECT symbol
                    , contract_address                   
                    , date_trunc('day', evt_block_time) ::date as "date"
                    , MAX(ARRAY[evt_block_number, evt_index, token_supply/10^tl.decimals]) AS total_supply 
                    FROM curvefi."tGAMMA_evt_RemoveLiquidityOne" 
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = contract_address
                    GROUP BY symbol, contract_address,  "date"
            ) as t GROUP BY  symbol, contract_address,  "date")
        ) as t order by "date" desc )

    , temp_table AS ( 
        SELECT 
            c."date"
            , c.address
            , c.symbol
            , total_supply
            , count(total_supply) OVER (PARTITION BY c.address ORDER BY c."date") AS grpSupply
        FROM calendar c 
        LEFT OUTER JOIN result r on r."date" = c."date" and r.address = c.address)
    
    , res_temp AS(    
    SELECT 
        "date"::date
        ,address
        ,symbol
        ,first_value(total_supply) OVER (PARTITION BY symbol, address, grpSupply ORDER BY "date") AS total_supply
    FROM  temp_table 
    order by "date" desc, symbol)

    SELECT "date"
        , address
        , symbol
        , total_supply
    FROM res_temp
    WHERE total_supply>0
    
);

CREATE UNIQUE INDEX ON tokemak.view_tokemak_curve_convex_pool_total_supply (
   "date", address
);

-- INSERT INTO cron.job(schedule, command)
-- VALUES ('1 * * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY tokemak.view_tokemak_curve_convex_pool_total_supply$$)
-- ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;