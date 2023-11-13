WITH unit_test AS (
    SELECT
        test.reward_amount_raw = actual.reward_amount_raw AS reward_amount_test,
        test.is_referral = actual.is_referral AS is_referral_test
    FROM {{ ref('slugs_optimism_rewards') }} AS actual
    INNER JOIN {{ ref('slugs_optimism_rewards_seed') }} AS test
    ON LOWER(
        actual.tx_hash
    ) = LOWER(
        test.tx_hash
    )
)

SELECT
    *
FROM
    unit_test
WHERE
    reward_amount_test = FALSE
    OR is_referral_test = FALSE
