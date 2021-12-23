WITH nft_users_collections_traded_clean AS (
    SELECT
        buyer AS address,
        LOWER(nft_project_name) AS label,
        regexp_replace(btrim(LOWER(nft_project_name), ' '::text), '(\s+)'::text, ' '::text, 'g'::text) as label_trim,
        'nft collection buyer' AS type,
        'masquot' AS author
    FROM nft.trades
    WHERE
        block_time >= '{{timestamp}}' AND nft_project_name IS NOT NULL
    UNION
    SELECT
        seller AS address,
        LOWER(nft_project_name) AS label,
        regexp_replace(btrim(LOWER(nft_project_name), ' '::text), '(\s+)'::text, ' '::text, 'g'::text) as label_trim,
        'nft collection seller' AS type,
        'masquot' AS author
    FROM nft.trades
    WHERE
        block_time >= '{{timestamp}}' AND nft_project_name IS NOT NULL
) 
SELECT 
    address,
    label,
    type,
    author
FROM nft_users_collections_traded_clean 
WHERE label = label_trim
    AND LENGTH(label) < 45;;
