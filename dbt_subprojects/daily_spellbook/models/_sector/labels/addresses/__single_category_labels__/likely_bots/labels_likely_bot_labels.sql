{{
    config(
        alias = 'likely_bot_labels'
        , post_hook='{{ hide_spells() }}'
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