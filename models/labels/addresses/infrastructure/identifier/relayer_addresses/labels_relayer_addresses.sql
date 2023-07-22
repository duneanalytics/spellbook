{{config(
    alias = alias('relayer_addresses'),
    tags=['dunesql'],
    post_hook='{{ expose_spells(\'["ethereum", "bnb", "polygon", "arbitrum", "optimism", "fantom", "avalanche_c", "gnosis"]\',
                                "sector",
                                "labels",
                                \'["msilb7"]\') }}'
)}}

SELECT blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type
FROM
(   
    SELECT *, ROW_NUMBER() OVER (PARTITION BY address) AS rn 
    FROM (
        SELECT
        'optimism' as blockchain, wallet_address AS address, project_name AS name, 'infrastructure' as category
            , 'msilb7' as contributor, 'static' as source
            , TIMESTAMP '2023-07-22' AS created_at, NOW() AS updated_at
            , 'relayers' as model_name, 'identifier' as model_type
        FROM {{ ref('addresses_optimism_relayers_curated')}}

        UNION ALL

        SELECT
        dest_chain as blockchain, bonder_address AS address, 'Hop Protocol Bonders' AS name, 'infrastructure' as category
            , 'msilb7' as contributor, 'query' as source
            , TIMESTAMP '2023-07-22' AS created_at, NOW() AS updated_at
            , 'hop_protocol_bonders' as model_name, 'identifier' as model_type

        FROM ref('hop_protocol_bonders')
        GROUP BY dest_chain, bonder_address

    ) st
) a
WHERE rn = 1 --ensure no dupes
