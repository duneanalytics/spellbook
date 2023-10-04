{{ config(
    schema = 'referral',
    alias = alias('rewards'),
    tags = ['dunesql'],
    partition_by = ['blockchain','project','block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','tx_hash','sub_tx_id'],
    incremental_predicates = ['DBT_INTERNAL_DEST.block_time >= date_trunc(\'day\', now() - interval \'7\' day)'],
    post_hook='{{ expose_spells(\'["ethereum","optimism","arbitrum"]\',
                    "sector",
                    "referral",
                    \'["0xRob"]\') }}')
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
        reward_amount,
        reward_amount_usd,
        project_contract_address,
        sub_tx_id
    FROM {{ model }}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
)


