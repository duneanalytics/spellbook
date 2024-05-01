{% test check_dex_null_amount_usd(model, group_by='blockchain') %}

    {%- set threshold = 0.05 -%}

    WITH base AS (
        SELECT
            {{ group_by }},
            COUNT(*) AS total_rows,
            COUNT(CASE WHEN amount_usd IS NULL THEN 1 END) AS null_rows
        FROM {{ model }}
        GROUP BY {{ group_by }}
    ),

    result AS (
        SELECT
            {{ group_by }},
            null_rows * 1.0 / total_rows AS null_amount_usd_percentage,
            CASE
                WHEN null_rows * 1.0 / total_rows > {{ threshold }} THEN
                    'Fail: The percentage of rows with null amount_usd is above the threshold'
                ELSE
                    NULL
            END AS result
        FROM base
    )

    SELECT *
    FROM result
    WHERE result IS NOT NULL

{% endtest %}