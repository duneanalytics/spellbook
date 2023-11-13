WITH unit_test AS (
    SELECT
        test.reward_amount_raw = actual.reward_amount_raw AS reward_amount_test
    FROM {{ ref('slugs_optimism_rewards') }} AS actual
    INNER JOIN {{ ref('slugs_optimism_rewards_seed') }} AS test
    ON actual.tx_hash = test.tx_hash
)

SELECT
    *
FROM
    unit_test
WHERE
    reward_amount_test = FALSE
