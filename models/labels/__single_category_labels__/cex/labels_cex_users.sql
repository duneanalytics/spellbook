{{config(alias='cex_users',
        post_hook='{{ expose_spells(\'["optimism","ethereum"]\',
                                    "sector",
                                    "labels",
                                    \'["msilb7"]\') }}'
)}}

{% set chains = [
    'ethereum',
    'optimism'
    ] %}

SELECT *
FROM (
{% for chain in chains %}
    SELECT
    '{{chain}}' as blockchain,
    address,
    cex_name || ' User' name,
    'cex users' AS category,
    'msilb7' AS contributor,
    'query' AS source,
    timestamp('2023-03-11') as created_at,
    now() as updated_at,
    model_name,
    'persona' as label_type


    FROM {{source('erc20_' + chain, 'evt_transfer')}} t
        INNER JOIN {{ref('addresses_{{chain}}_cex')}} c
        ON t.`from` = c.address

    UNION ALL

    SELECT
    '{{chain}}' as blockchain,
    address,
    cex_name || ' User' name,
    'cex users' AS category,
    'msilb7' AS contributor,
    'query' AS source,
    timestamp('2023-03-11') as created_at,
    now() as updated_at,
    model_name,
    'persona' as label_type


    FROM {{source('erc20_' + chain, 'evt_transfer')}} t
        INNER JOIN {{ref('addresses_{{chain}}_cex')}} c
        ON t.`from` = c.address

    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
) a