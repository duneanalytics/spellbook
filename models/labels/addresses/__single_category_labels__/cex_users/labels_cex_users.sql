{{config(tags=['dunesql'],alias = alias('cex_users'),
        post_hook='{{ expose_spells(\'["optimism","ethereum"]\',
                                    "sector",
                                    "labels",
                                    \'["msilb7"]\') }}'
)}}

{% set chains = [
    'optimism',
    'ethereum'
] %}

SELECT
blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type
FROM (

    SELECT
    'optimism' as blockchain,
    address,
    cex_name || ' User' AS name,
    'cex users' AS category,
    'msilb7' AS contributor,
    'query' AS source,
    DATE '2023-03-11'  as created_at,
    now() as updated_at,
    'cex_users_withdrawals' model_name,
    'persona' as label_type

    FROM { ref('erc20_optimism_evt_transfer_legacy') } t
        INNER JOIN { ref('addresses_optimism_cex_legacy') } c
        ON t."from" = c.address

    UNION ALL

    SELECT
    'optimism' as blockchain,
    address,
    cex_name || ' User' AS name,
    'cex users' AS category,
    'msilb7' AS contributor,
    'query' AS source,
    DATE '2023-03-11'  as created_at,
    now() as updated_at,
    'cex_users_withdrawals' model_name,
    'persona' as label_type


    FROM { ref('erc20_optimism_evt_transfer_legacy') } t
        INNER JOIN { ref('addresses_optimism_cex_legacy') } c
        ON t."from" = c.address


    UNION ALL


    SELECT
    'ethereum' as blockchain,
    address,
    cex_name || ' User' AS name,
    'cex users' AS category,
    'msilb7' AS contributor,
    'query' AS source,
    DATE '2023-03-11'  as created_at,
    now() as updated_at,
    'cex_users_withdrawals' model_name,
    'persona' as label_type

    FROM { ref('erc20_ethereum_evt_transfer_legacy') } t
        INNER JOIN { ref('addresses_ethereum_cex_legacy') } c
        ON t."from" = c.address

    UNION ALL

    SELECT
    'ethereum' as blockchain,
    address,
    cex_name || ' User' AS name,
    'cex users' AS category,
    'msilb7' AS contributor,
    'query' AS source,
    DATE '2023-03-11'  as created_at,
    now() as updated_at,
    'cex_users_withdrawals' model_name,
    'persona' as label_type


    FROM { ref('erc20_ethereum_evt_transfer_legacy') } t
        INNER JOIN { ref('addresses_ethereum_cex_legacy') } c
        ON t."from" = c.address



) a
GROUP BY 1,2,3,4,5,6,7,8,9,10 --distinct if erc20 and eth