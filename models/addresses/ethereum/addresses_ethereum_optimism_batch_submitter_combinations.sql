{{ config(alias = 'optimism_batch_submitter_combinations',
        tags=['static'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "addresses",
                                    \'["msilb7"]\') }}') }}

WITH inbox AS (   
    , from_addresses_inbox AS (
        SELECT 
            protocol_name,
            address AS "from_address"
        FROM 
            {{ ref('addresses_ethereum_l2_batch_submitters') }}
        WHERE 
            submitter_type IN ('L1BatchInbox', 'Canonical Transaction Chain')
            AND role_type = 'from_address'
            AND codebase = 'Optimism'
    ), to_addresses_inbox AS (
        SELECT 
            protocol_name,
            address AS "to_address"
        FROM 
            {{ ref('addresses_ethereum_l2_batch_submitters') }}
        WHERE 
            submitter_type IN ('L1BatchInbox', 'Canonical Transaction Chain')
            AND role_type = 'to_address'
            AND codebase = 'Optimism'
    )
    SELECT 
        f.protocol_name,
        f.from_address,
        t.to_address
    FROM 
        from_addresses_inbox f
    JOIN 
        to_addresses_inbox t ON f.protocol_name = t.protocol_name
    GROUP BY 1,2,3
    )

, output AS (   
    , from_addresses_output AS (
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
        f.from_address,
        t.to_address
    FROM 
        from_addresses_output f
    JOIN 
        to_addresses_output t ON f.protocol_name = t.protocol_name
    GROUP BY 1,2,3
    )

SELECT distinct
i.protocol_name
    , i.from_address AS l1_batch_inbox_from_address, i.to_address AS l1_batch_inbox_to_address
    , o.from_address AS l2_output_oracle_from_address, o.to_address AS l2_output_oracle_to_address

FROM inbox i
left join output o
    ON i.protocol_name = o.protocol_name

