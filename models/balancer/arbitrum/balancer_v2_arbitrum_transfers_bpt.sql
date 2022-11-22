{{
    config(
        alias='transfers_bpt',
        post_hook='{{ expose_spells(\'["arbitrum"]\',
                                    "project",
                                    "balancer_v2",
                                    \'["stefenon"]\') }}'
    ) 
}}

{% set query %}
    SELECT DISTINCT CONCAT(namespace, '_arbitrum', '.', name, '_evt_Transfer') AS event
    FROM {{ source('arbitrum', 'contracts') }} c
    JOIN {{ source ('balancer_v2_arbitrum', 'Vault_evt_PoolRegistered') }} p
    ON p.poolAddress = c.address
{% endset %}

{% set results = run_query(query) %}
{% if execute %}
{# Return the first column #}
{% set transfer_tables = results.columns[0].values() %}
{% else %}
{% set transfer_tables = [] %}
{% endif %}

SELECT DISTINCT * FROM (
    {% for transfer_table in transfer_tables %}
    SELECT * FROM {{transfer_table}}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}) transfers
