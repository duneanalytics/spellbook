WITH unit_test1
    AS (SELECT CASE
                 WHEN variable_borrow_apy == 0.024106036652769853 THEN TRUE
                 ELSE FALSE
               END AS test
        FROM   {{ ref('aave_v2_ethereum_interest_rates' )}}
        WHERE  reserve = '0xdac17f958d2ee523a2206206994597c13d831ec7'
               AND evt_block_time = '2022-08-22 12:00'),
    unit_test2
    AS (SELECT CASE WHEN deposit_apy == 0.004223674732695224 THEN TRUE
                 ELSE FALSE
               END AS test
        FROM   {{ ref('aave_v2_ethereum_interest_rates' )}}
        WHERE  symbol = 'USDC'
               AND evt_block_time = '2022-08-25 09:00')
SELECT *
FROM   (SELECT *
       FROM   unit_test1
       UNION
       SELECT *
       FROM   unit_test2)
WHERE  test = FALSE
