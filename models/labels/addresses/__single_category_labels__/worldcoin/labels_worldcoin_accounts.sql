{{config(alias = alias('worldcoin_accounts'),
        tags = ['dunesql'],
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "labels",
                                    \'["msilb7"]\') }}')
}}

{% set models = [
    ('optimism', ref('worldcoin_optimism_accounts'))
] %}
{% for model in models %}

SELECT
    '{{ model[0] }}' as blockchain,
    account_address as address,
    'Worldcoin Account' AS name,
    'worldcoin' AS category,
    'msilb7' AS contributor,
    'query' AS source,
    TIMESTAMP '2023-07-15' as created_at,
    now() as updated_at,
    'worldcoin_accounts' AS model_name,
    'persona' AS label_type
FROM {{ model[1] }}

{% if not loop.last %}
{% if is_incremental() %}
{% endif %}
UNION ALL
{% endif %}
{% endfor %}