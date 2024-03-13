{{ config(
    alias = 'account_deployed',
    
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash'],
    post_hook='{{ expose_spells(\'["base"]\',
                                    "project",
                                    "erc4337",
                                    \'["0xbitfly", "peterrliem"]\') }}'
)}}


{% set erc4337_base_models = [
    ref('account_abstraction_erc4337_base_v0_6_account_deployed')
    , ref('account_abstraction_erc4337_base_v0_7_account_deployed')
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
    {% for erc4337_model in erc4337_base_models %}
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