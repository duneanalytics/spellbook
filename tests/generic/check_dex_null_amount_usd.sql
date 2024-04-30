{% test check_dex_null_amount_usd(model) %}

    {%- set threshold = 0.05 -%}

    WITH base AS (
        SELECT
            COUNT(*) AS total_rows,
            COUNT(CASE WHEN amount_usd IS NULL THEN 1 END) AS null_rows
        FROM {{ model }}
    )

    result AS (
        SELECT
            CASE
                WHEN null_rows * 1.0 / total_rows > {{ threshold }} THEN
                    'Fail: The percentage of rows with null amount_usd is above the threshold'
                ELSE
                    NULL
            END AS result
        FROM base
    )

    SELECT result
    FROM result
    WHERE result IS NOT NULL

{% endtest %}