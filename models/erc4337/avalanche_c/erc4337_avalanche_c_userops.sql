{{ config
(
    schema ='erc4337_avalanche_c',
    alias = 'userops',
    partition_by = ['block_time'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash'],
    post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                    "project",
                                    "erc4337",
                                    \'["0xbitfly"]\') }}'
)
}}


{% set erc4337_models = [
ref('erc4337_v0_5_avalanche_c_userops')
, ref('erc4337_v0_6_avalanche_c_userops')
] %}

SELECT *
FROM (
    {% for erc4337_model in erc4337_models %}
      SELECT blockchain
        , version
        , block_time
        , entrypoint_contract
        , tx_hash
        , sender
        , userop_hash
        , success
        , paymaster
        , op_fee
        , op_fee_usd
        , bundler
        , tx_to
        , gas_symbol
        , tx_fee
        , tx_fee_usd
        , beneficiary
    FROM {{ erc4337_model }}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %} 
)