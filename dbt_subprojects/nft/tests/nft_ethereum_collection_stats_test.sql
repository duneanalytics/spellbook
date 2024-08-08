WITH test_data AS
(
    SELECT
        CAST(block_date as TIMESTAMP) as block_date
        ,nft_contract_address
        ,volume_eth
        ,price_p5_eth
        ,price_max_eth
        ,price_min_eth
    FROM
    (VALUES
        ('2023-01-31',0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d,732.3399999999999,66.211,310,66.16),
        ('2023-01-19',0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d,1321.1373999999998,65.45,136.9,65.25),
        ('2023-01-22',0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d,1730.6302,67.85109,194.2,65.5)
    ) AS temp_table (block_date, nft_contract_address, volume_eth, price_p5_eth, price_max_eth, price_min_eth)
)
, target_data AS
(
    SELECT
        block_date
        ,nft_contract_address
        ,volume_eth
        ,price_p5_eth
        ,price_max_eth
        ,price_min_eth
    FROM
        {{ ref('nft_ethereum_collection_stats') }}
    WHERE
        (block_date = TIMESTAMP '2023-01-31' OR block_date = TIMESTAMP '2023-01-19' OR block_date = TIMESTAMP '2023-01-22')
        AND nft_contract_address = 0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d
)
, test AS
(
    SELECT
        round(target_data.volume_eth, 0) as target_volume
        , round(test_data.volume_eth, 0) as test_volume
        , round(target_data.price_p5_eth, 0) as target_p5
        , round(test_data.price_p5_eth, 0) as test_p5
        , round(target_data.price_max_eth, 0) as target_max
        , round(test_data.price_max_eth, 0) as test_max
        , round(target_data.price_min_eth, 0) as target_min
        , round(test_data.price_min_eth, 0) as test_min
        , CASE
            WHEN
                (
                    round(target_data.volume_eth, 0) = round(test_data.volume_eth, 0)
                    AND round(target_data.price_p5_eth, 0) = round(test_data.price_p5_eth, 0)
                    AND round(target_data.price_max_eth, 0) = round(test_data.price_max_eth, 0)
                    AND round(target_data.price_min_eth, 0) = round(test_data.price_min_eth, 0)
                )
            THEN true
            ELSE false
        END as value_test
    FROM
        test_data
    LEFT JOIN
        target_data
        ON target_data.block_date = test_data.block_date
        and target_data.nft_contract_address = test_data.nft_contract_address
)
SELECT
    *
FROM
    test
WHERE
    value_test = false
