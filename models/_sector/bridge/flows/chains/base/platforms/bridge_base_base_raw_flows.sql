{% set blockchain = 'base' %}

{{ config(
    schema = 'bridge_' + blockchain,
    alias = 'base_raw_flows',
    materialized = 'view',
    )
}}

{% set bridge_models = [
 (source('base_base', 'L2StandardBridge_evt_ERC20BridgeInitiated'), 'erc20', 'sent')
 , (source('base_base', 'L2StandardBridge_evt_ETHBridgeInitiated'), 'native', 'sent')
 , (source('base_base', 'L2StandardBridge_evt_ERC20BridgeFinalized'), 'erc20', 'received')
 , (source('base_base', 'L2StandardBridge_evt_ETHBridgeFinalized'), 'native', 'received')
] %}

WITH base_union AS (
    SELECT * FROM  (
    {% for bridge_model in bridge_models %}

        {{bridge_opstack_flows(
            blockchain = blockchain
            , project = 'base'
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