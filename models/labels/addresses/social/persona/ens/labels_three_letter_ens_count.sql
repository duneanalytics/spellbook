{{config(
    
    alias = 'three_letter_ens_count'
)}}

WITH three_letter_ens_count AS (
    SELECT 
        owner,
        count(owner) as ens_count
    FROM
        {{ ref('ens_view_registrations') }}
    WHERE
        length(name) = 3

    GROUP BY owner
    ORDER BY ens_count DESC
)

SELECT
    'ethereum' as blockchain,
    owner as address,
    'most_three_letter_ens_owner' as model_name,
    'spanish-or-vanish' as contributor,
    'query' as source,
    'social' as category,
    TIMESTAMP '2022-03-03' as created_at,
    now() as updated_at,
    'personas' as label_type,
    concat('Number of three letter ENS Domains owned: ', CAST(ens_count AS VARCHAR)) as name
FROM three_letter_ens_count
WHERE owner is not null