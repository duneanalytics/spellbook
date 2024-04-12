{{ config(
    schema = 'referral',
    alias = 'staging_rewards',
    materialized = 'view'
    )
}}


{% set models = [
 ref('zora_ethereum_rewards'),
 ref('zora_optimism_rewards'),
 ref('zora_base_rewards'),
 ref('zora_zora_rewards'),
 ref('rabbithole_arbitrum_rewards'),
 ref('rabbithole_base_rewards'),
 ref('rabbithole_optimism_rewards'),
 ref('rabbithole_polygon_rewards'),
 ref('soundxyz_v1_ethereum_rewards'),
 ref('soundxyz_v1_optimism_rewards'),
 ref('soundxyz_v2_ethereum_rewards'),
 ref('soundxyz_v2_optimism_rewards'),
 ref('soundxyz_v2_base_rewards'),
 ref('slugs_optimism_rewards'),
 ref('mintfun_ethereum_rewards'),
 ref('mintfun_optimism_rewards'),
 ref('mintfun_base_rewards'),
 ref('mintfun_zora_rewards'),
 ref('mirror_optimism_rewards'),
 ref('mirror_base_rewards'),
 ref('paragraph_optimism_rewards'),
 ref('paragraph_base_rewards'),
 ref('paragraph_zora_rewards'),
 ref('paragraph_polygon_rewards'),
 ref('fabric_ethereum_rewards'),
 ref('fabric_optimism_rewards'),
 ref('fabric_base_rewards'),
 ref('fabric_zora_rewards'),
 ref('basepaint_base_rewards')
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


