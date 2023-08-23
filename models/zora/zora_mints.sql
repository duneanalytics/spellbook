{{ config(
    tags = ['dunesql'],
    schema = 'zora',
    partition_by=['block_date'],
    alias = alias('mints'),
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["ethereum","optimism","base","goerli"]\',
                    "project",
                    "zora",
                    \'["hildobby"]\') }}')
}}

{% set zora_mints_models = [
 ref('zora_ethereum_mints')
,ref('zora_optimism_mints')
,ref('zora_base_mints')
,ref('zora_goerli_mints')
] %}

SELECT *
FROM (
    {% for zora_mints_model in zora_mints_models %}
    SELECT
          blockchain
        , block_time
        , block_number
        , minter
        , nft_recipient
        , nft_type
        , nft_contract_address
        , nft_token_id
        , amount
        , price
        , tx_hash
        , marketplace_fee
        , marketplace_fee_recipient
        , creator_fee
        , creator_fee_recipient
        , create_referral_reward
        , create_referral_reward_recipient
        , first_minter_reward
        , first_minter_reward_recipient
        , mint_referral_reward
        , mint_referral_reward_recipient
        , evt_index
        , contract_address
        , rewards_version
    FROM {{ zora_mints_model }}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)