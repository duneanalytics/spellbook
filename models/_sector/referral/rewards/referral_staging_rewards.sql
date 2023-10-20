{{ config(
    schema = 'referral',
    alias = alias('staging_rewards'),
    tags = ['dunesql'],
    materialized = 'view'
    )
}}


{% set models = [
 ref('zora_ethereum_rewards'),
 ref('zora_optimism_rewards'),
 ref('zora_base_rewards'),
 ref('rabbithole_arbitrum_rewards'),
 ref('rabbithole_base_rewards'),
 ref('rabbithole_optimism_rewards'),
 ref('rabbithole_polygon_rewards'),
 ref('soundxyz_ethereum_rewards'),
 ref('soundxyz_optimism_rewards')
] %}


SELECT *
FROM (
    {% for model in models %}
    SELECT
        blockchain,
        project,
        version,
        block_number,
        block_time,
        block_date,
        block_month,
        tx_hash,
        tx_from,
        tx_to,
        category,
        referrer_address,
        referee_address,
        currency_contract,
        reward_amount_raw,
        project_contract_address,
        sub_tx_id
    FROM {{ model }}
    {% if is_incremental() %}
    where {{incremental_predicate('block_time')}}
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
)


