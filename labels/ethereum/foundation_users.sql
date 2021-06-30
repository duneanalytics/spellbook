SELECT
    address,
    name AS label,
    'foundation user' as type,
    'foundation' as author
FROM
    foundation.user_names
WHERE
    updated_at >= '{{timestamp}}'
