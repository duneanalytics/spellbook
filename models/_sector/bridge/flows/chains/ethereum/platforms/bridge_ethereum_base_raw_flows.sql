{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridge_' + blockchain,
    alias = 'base_raw_flows',
    partition_by = ['blockchain','project','block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain','tx_hash','evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set bridge_models = [
 (source('base_ethereum', 'L1StandardBridge_evt_ERC20BridgeInitiated'), 'erc20', 'initialised')
 , (source('base_ethereum', 'L1StandardBridge_evt_ETHBridgeInitiated'), 'native', 'initialised')
 , (source('base_ethereum', 'L1StandardBridge_evt_ERC20BridgeFinalized'), 'erc20', 'finalised')
 , (source('base_ethereum', 'L1StandardBridge_evt_ETHBridgeFinalized'), 'native', 'finalised')
] %}

WITH base_union AS (
    SELECT * FROM  (
    {% for bridge_model in bridge_models %}

        {{bridge_opstack_flows(
            blockchain = blockchain
            , project = blockchain
            , project_version = '1'
            , events = bridge_model[0]
            , token_standard = bridge_model[1]
            , flows_type = bridge_model[2]
            )}}

        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )
    )

SELECT * FROM base_union