{{ config(
    alias = alias('pool_incentives_mappings')
    )
}}

-- Map the pool id (pid) to the underlying pool address
-- We can only get this from contract reads (for now)

SELECT 'optimism' as blockchain, contract_address, pid, lp_address

FROM (
    SELECT
        contract_address, pid, lpToken AS lp_address
        FROM {{ source('sushi_optimism','MiniChefV2_evt_LogPoolAddition') }}
        GROUP BY 1,2,3


) a (contract_address, pid, lp_address)