{{
    config(
        alias = 'likely_bot_labels',
        post_hook='{{ expose_spells(\'["optimism","base","zora","bob","ink","worldchain","shape","mode","unichain"]\',
        "sector",
        "labels",
        \'["msilb7"]\') }}'
    )
}}

{% set chains = [
    'optimism',
    'base',
    'zora',
    'bob',
    'ink',
    'worldchain',
    'shape',
    'mode',
    'unichain'
] %}

{% for chain in chains %}
    {% if not loop.first %}UNION ALL{% endif %}
    SELECT * FROM {{ ref('labels_' ~ chain ~ '_likely_bot_addresses') }}
    UNION ALL
    SELECT * FROM {{ ref('labels_' ~ chain ~ '_likely_bot_contracts') }}
{% endfor %}