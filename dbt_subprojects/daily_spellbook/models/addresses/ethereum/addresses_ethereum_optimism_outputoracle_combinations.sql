{{ config(
        schema = 'addresses_ethereum',
        alias = 'optimism_outputoracle_combinations',
        tags=['static'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "addresses",
                                    \'["msilb7"]\') }}') }}
 
with from_addresses_output AS (
    SELECT 
        protocol_name,
        address AS "from_address"
    FROM 
        {{ ref('addresses_ethereum_l2_batch_submitters') }}
    WHERE 
        submitter_type IN ('L2OutputOracle', 'State Commitment Chain')
        AND role_type = 'from_address'
        AND codebase = 'Optimism'
), to_addresses_output AS (
    SELECT 
        protocol_name,
        address AS "to_address"
    FROM 
        {{ ref('addresses_ethereum_l2_batch_submitters') }}
    WHERE 
        submitter_type IN ('L2OutputOracleProxy', 'State Commitment Chain')
        AND role_type = 'to_address'
        AND codebase = 'Optimism'
)
SELECT 
    f.protocol_name,
    f.from_address AS l2_output_oracle_from_address,
    t.to_address AS l2_output_oracle_to_address
FROM 
    from_addresses_output f
JOIN 
    to_addresses_output t ON f.protocol_name = t.protocol_name
GROUP BY 1,2,3
