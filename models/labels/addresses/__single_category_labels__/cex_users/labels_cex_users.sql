{{config(
        tags = ['dunesql'],
        alias = alias('cex_users'),
        post_hook='{{ expose_spells(\'["optimism","ethereum"]\',
                                    "sector",
                                    "labels",
                                    \'["msilb7"]\') }}'
)}}

{% set chains = [
    'optimism',
    'ethereum'
] %}

SELECT blockchain,
       address,
       name,
       category,
       contributor,
       source,
       created_at,
       updated_at,
       model_name,
       label_type
FROM (
{% for chain in chains %}
    SELECT
        '{{chain}}' as blockchain,
        address,
        cex_name || ' User' AS name,
        'cex users' AS category,
        'msilb7' AS contributor,
        'query' AS source,
        TIMESTAMP '2023-03-11' as created_at,
        now() as updated_at,
        'cex_users_withdrawals' model_name,
        'persona' as label_type
    FROM {{source('erc20_' + chain, 'evt_transfer')}} t
    INNER JOIN {{ref('cex_addresses')}} c ON '{{chain}}' = c.blockchain
        AND t."from" = c.address

    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
) a
GROUP BY 1,2,3,4,5,6,7,8,9,10 --distinct if erc20 and eth