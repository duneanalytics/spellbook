{{ config(
        schema = 'addresses_ethereum',
        alias = 'optimism_batchinbox_combinations',
        tags=['static'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "addresses",
                                    \'["msilb7"]\') }}') }}

with from_addresses_inbox AS (
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
    f.from_address AS l1_batch_inbox_from_address,
    t.to_address AS l1_batch_inbox_to_address
FROM 
    from_addresses_inbox f
JOIN 
    to_addresses_inbox t ON f.protocol_name = t.protocol_name
GROUP BY 1,2,3