WITH aux AS (
    SELECT
        owner AS address,
        lower(name) AS label,
        'ens name' AS type,
        'hagaetc' AS author
    FROM
        ethereumnameservice."ETHRegistrarController_1_evt_NameRegistered"
    WHERE
        evt_block_time >= '{{timestamp}}'
    UNION
    SELECT
        owner AS address,
        lower(name) AS label,
        'ens name' AS type,
        'hagaetc' AS author
    FROM
        ethereumnameservice."ETHRegistrarController_2_evt_NameRegistered"
    WHERE
        evt_block_time >= '{{timestamp}}'
    UNION
    SELECT
        owner AS address,
        lower(name) AS label,
        'ens name' AS type,
        'hagaetc' AS author
    FROM
        ethereumnameservice."ETHRegistrarController_3_evt_NameRegistered"
    WHERE evt_block_time >= '{{timestamp}}'
)
SELECT
    address,
    label,
    type,
    author
FROM aux
WHERE LENGTH(label) < 100
    AND regexp_replace(btrim(aux.label, ' '::text), '(\s+)'::text, ' '::text, 'g'::text) = aux.label IS TRUE;
