{{ config(
    schema = 'sushiswap_optimism'
    , alias = 'pool_incentives_mappings'
    )
}}

-- Map the pool id (pid) to the underlying pool address
-- We can only get this from contract reads (for now)

SELECT
    chain as blockchain, contract_address, pid, lpToken AS lp_address
FROM {{ source('sushi_multichain','minichefv2_evt_logpooladdition') }}
WHERE chain = 'optimism'
GROUP BY 1,2,3,4