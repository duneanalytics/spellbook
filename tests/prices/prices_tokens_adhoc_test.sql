--note: intended to be a one-time test during development to ensure prices.tokens end result is as expected after breaking out by blockchain

WITH src as
(
    SELECT
        COUNT(1) as src_count
    FROM
        {{ ref('prices_tokens') }}
), tgt as
(
    SELECT
        COUNT(1) as tgt_count
    FROM
        prices.tokens
), test as
(
    SELECT
        CASE WHEN src_count != tgt_count
            THEN false
            ELSE true
        END as test_case
    FROM
        src
    FULL OUTER JOIN
        tgt
    ON
        1=1
)
SELECT
    *
FROM
    test
WHERE
    test_case = false