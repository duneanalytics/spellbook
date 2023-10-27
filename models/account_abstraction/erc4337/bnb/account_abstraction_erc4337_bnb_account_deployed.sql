{{ config(
    alias = 'account_deployed',
    
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash'],
    post_hook='{{ expose_spells(\'["bnb"]\',
                                    "project",
                                    "erc4337",
                                    \'["0xbitfly"]\') }}'
)}}


{% set erc4337_bnb_models = [
    ref('account_abstraction_erc4337_bnb_v0_6_account_deployed')
] %}

SELECT  
          blockchain
        , version
        , block_time
        , block_month
        , userop_hash
        , entrypoint_contract
        , tx_hash
        , sender
        , paymaster
        , factory
FROM (
    {% for erc4337_model in erc4337_bnb_models %}
    SELECT 
          blockchain
        , version
        , block_time
        , block_month
        , userop_hash
        , entrypoint_contract
        , tx_hash
        , sender
        , paymaster
        , factory
    FROM {{ erc4337_model }}
    
    {% if is_incremental() %}
    WHERE block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)